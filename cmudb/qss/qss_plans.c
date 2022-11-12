#include <assert.h>

#include "qss.h"
#include "qss_features.h"
#include "qss_writer.h"

#include "access/nbtree.h"
#include "access/heapam.h"
#include "access/relation.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/index.h"
#include "cmudb/qss/qss.h"
#include "cmudb/tscout/marker.h"
#include "commands/explain.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/ruleutils.h"
#include "pgstat.h"

/** Whether we registered on_shmem_exit(). */
static bool registered_shmem_exit = false;

/**
 * All memory for ExecutorInstrument is charged to the query context that is executing
 * the query that we are attempting to instrument. We do not use qss_MemoryContext
 * for allocating any of this memory.
 */
struct ExecutorInstrument {
	int64 queryId; /** Query identifier */
	int generation; /** Generation */
	char* params; /** Parameters */
	int64_t params_len; /* Length of the params string. */
	TimestampTz statement_ts; /** Statement Timestamp. */

	bool capture; /** Whether capture or not. */
	EState* estate;
	List* statement_instrs; /** List of additional instrumentations. */
	struct ExecutorInstrument* prev; /** Previous query context. */
};

/** Current query nesting level. */
int nesting_level = 0;
/** Top of the ExecutorStart stack. */
struct ExecutorInstrument* top = NULL;

void qss_Abort()
{
	ActiveQSSInstrumentation = NULL;
	top = NULL;
	nesting_level = 0;
}

/**
 * Alloc a new instrumentation context.
 */
Instrumentation* qss_AllocInstrumentation(const char *ou, bool need_timer) {
	MemoryContext oldcontext = NULL;
	Instrumentation* instr = NULL;
	if (top == NULL) {
		// No ExecutorStart yet.
		return NULL;
	}

	if (!qss_capture_enabled ||
		!qss_capture_exec_stats ||
		(qss_output_format != QSS_OUTPUT_FORMAT_NOISEPAGE) ||
		!top->capture) {
		// Not enabled.
		return NULL;
	}

	oldcontext = MemoryContextSwitchTo(top->estate->es_query_cxt);

	/** Allocate a new instrumentation unit. */
	instr = palloc0(sizeof(Instrumentation));
	InstrInit(instr, (need_timer ? INSTRUMENT_TIMER : 0) | INSTRUMENT_BUFFERS);
	instr->plan_node_id = PLAN_INDEPENDENT_ID;
	instr->ou = ou;

	/** Add to the statement instrumentations list. */
	if (top->statement_instrs == NULL) {
		top->statement_instrs = list_make1(instr);
	} else {
		top->statement_instrs = lappend(top->statement_instrs, instr);
	}

	MemoryContextSwitchTo(oldcontext);
	return instr;
}

/**
 * Hook override for ExecutorStart
 */
void qss_ExecutorStart(QueryDesc *query_desc, int eflags) {
	MemoryContext oldcontext = NULL;
	struct ExecutorInstrument* exec = NULL;
	bool need_instrument;
	bool need_total;
	bool capture;
	nesting_level++;
	query_desc->nesting_level = nesting_level;

	/** Whether we should capture this query at all. */
	need_total = qss_capture_enabled && (qss_capture_nested || nesting_level == 1);
	/** Whether we should be capturing at all or not. */
	capture = qss_capture_exec_stats;
	/** Whether we need instrumentation. */
	need_instrument = need_total &&
					  capture &&
					  query_desc->generation >= 0 &&
					  (!query_desc->dest || query_desc->dest->mydest != DestSQLFunction);

	/** Attach INSTRUMENT_TIMER if we want those statistics. And get the buffer counts while we are at it... */
	if (need_instrument) {
		query_desc->instrument_options |= (INSTRUMENT_TIMER | INSTRUMENT_BUFFERS);
	}

	/** Initialize the Executor. */
	if (qss_prev_ExecutorStart != NULL) {
		qss_prev_ExecutorStart(query_desc, eflags);
	} else {
		standard_ExecutorStart(query_desc, eflags);
	}

	/** Switch to the query memory context to allocate the context. */
	oldcontext = MemoryContextSwitchTo(query_desc->estate->es_query_cxt);
	exec = palloc0(sizeof(struct ExecutorInstrument));
	// FIXME: This is probably not going to capture re-runs, but we're on REPEATABLE_READ isolation level.
	exec->statement_ts = GetCurrentStatementStartTimestamp();
	exec->queryId = query_desc->plannedstmt->queryId;
	exec->generation = query_desc->generation;
	exec->capture = need_instrument;
	exec->estate = query_desc->estate;
	if (query_desc->params != NULL) {
		/** Get the parameters string. */
		exec->params = BuildParamLogString(query_desc->params, NULL, -1);
		exec->params_len = strlen(exec->params);
	}

	if (need_total && query_desc->totaltime == NULL) {
		/** Capture the total time. */
		query_desc->totaltime = InstrAlloc(1, INSTRUMENT_TIMER | INSTRUMENT_BUFFERS, false, 0);
	}
	exec->prev = top;
	top = exec;

	/** Switch the MemoryContext back. */
	MemoryContextSwitchTo(oldcontext);

	if (!registered_shmem_exit)
	{
		on_shmem_exit(qss_OutputData, (Datum) 0);
		registered_shmem_exit = true;
	}
}

/**
 * Process an EXPLAIN for TEXT/JSON formats.
 */
StringInfo ProcessQueryExplain(QueryDesc *query_desc, bool instrument, bool verbose)
{
	/** Case where we haven't found the query plan yet. */
	ExplainState *es = NULL;
	Bitmapset *rels_used = NULL;
	StringInfo ret;

	/** Prepare the ExplainState in the query context. */
	MemoryContext oldcontext = MemoryContextSwitchTo(query_desc->estate->es_query_cxt);

	es = NewExplainState();
	es->analyze = instrument;
	es->format = EXPLAIN_FORMAT_NOISEPAGE;
	// FIXME: VERBOSE output tuple outputs are "fixed". No-one should be using those anyways.
	es->verbose = verbose;
	es->pstmt = query_desc->plannedstmt;
	es->rtable = query_desc->plannedstmt->rtable;
	ExplainPreScanNode(query_desc->planstate, &rels_used);
	es->rtable_names = select_rtable_names_for_explain(es->rtable, rels_used);
	es->deparse_cxt = deparse_context_for_plan_tree(query_desc->plannedstmt, es->rtable_names);

	ExplainBeginOutput(es);
	// FIXME: This does not output trigger information correctly.
	// But no other modeling pipeline can really use trigger information correctly for now.
	OutputPlanToExplain(query_desc, es);
	ExplainEndOutput(es);

	ret = es->str;
	es->str = NULL;

	MemoryContextSwitchTo(oldcontext);
	return ret;
}

/**
 * Process an internal in-memory write.
 */
static void ProcessQueryInternalTable(QueryDesc *query_desc, bool instrument) {
	uint64_t queryId = top->queryId;
	int64_t generation = top->generation;
	int64_t timestamp = top->statement_ts;
	if (qss_PlansHTAB != NULL)
	{
		/** Check if we've already inserted the query plan. */
		bool found = true;
		struct QSSPlan* entry = NULL;
		struct QSSPlanKey key;
		key.queryId = top->queryId;
		key.generation = generation;

		entry = hash_search(qss_PlansHTAB, &key, HASH_ENTER, &found);
		if (entry != NULL && !found)
		{
			MemoryContext oldcontext;
			StringInfo plan;
			entry->key = key;
			entry->statement_ts = timestamp;
			plan = ProcessQueryExplain(query_desc, instrument, true);
			if (plan != NULL)
			{
				/** Switch to qss_MemoryContext so this data survives the query context exploding. */
				oldcontext = MemoryContextSwitchTo(qss_MemoryContext);

				/** Copy the pointer to the plan over. */
				entry->query_plan = malloc(plan->len + 1);
				memcpy(entry->query_plan, plan->data, plan->len);
				entry->query_plan[plan->len] = '\0';

				/** Revert the MemoryContext. */
				MemoryContextSwitchTo(oldcontext);
			}
		}
	}

	if (query_desc->totaltime)
	{
		// Propagate the blocking information outwards.
		struct QSSStats* stats = GetStatsEntry();
		if (stats != NULL)
		{
			/** Insert the QSSStats entry corresponding to the query. */
			stats->queryId = queryId;
			stats->generation = generation;
			stats->timestamp = timestamp;
			stats->plan_node_id = -1;
			stats->elapsed_us = query_desc->totaltime->total * 1000000.0;
			stats->startup_time = query_desc->totaltime->startup * 1000000.0;
			stats->nloops = query_desc->totaltime->nloops;
			stats->counter0 = 1;
			stats->counter1 = query_desc->totaltime->ntuples;
			stats->txn = GetCurrentTransactionId();

			stats->blk_hit = query_desc->totaltime->bufusage.shared_blks_hit;
			stats->blk_miss = query_desc->totaltime->bufusage.shared_blks_read;
			stats->blk_dirty = query_desc->totaltime->bufusage.shared_blks_dirtied;
			stats->blk_write = query_desc->totaltime->bufusage.shared_blks_written;

			if (top->params != NULL)
			{
				stats->comment = MemoryContextAlloc(qss_MemoryContext, top->params_len + 1);
				memcpy(stats->comment, top->params, top->params_len);
				stats->comment[top->params_len] = '\0';
			}
		}
	}

	if (qss_capture_exec_stats && instrument)
	{
		ListCell *lc;
		foreach (lc, top->statement_instrs)
		{
			/** Write instrumentation allocated during query execution. */
			Instrumentation *instr = (Instrumentation*)lfirst(lc);
			if (instr != NULL) {
				WriteInstrumentation(NULL, instr, queryId, generation, timestamp);
			}
		}

		/** Write out the instrumentation from the plan nodes. */
		WritePlanInstrumentation(query_desc->planstate->plan, query_desc->planstate, queryId, generation, timestamp);
	}
}

void qss_ExecutorEnd(QueryDesc *query_desc) {
	MemoryContext oldcontext;
	EState *estate = query_desc->estate;

	/* Switch into per-query memory context */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	Assert(query_desc->nesting_level == nesting_level);

	if (qss_capture_enabled && query_desc->totaltime != NULL && top != NULL && !qss_in_explain) {
		/** End the loop on the main counter. */
		InstrEndLoop(query_desc->totaltime);
		ProcessQueryInternalTable(query_desc, top->capture);
	}

	if (top != NULL) {
		// Just pop the context. The memory should get freed by the MemoryContext.
		top = top->prev;
	}

	/** Reset the memory context. */
	MemoryContextSwitchTo(oldcontext);

	/** Call the regular ExecutorEnd. */
	if (qss_prev_ExecutorEnd != NULL) {
		qss_prev_ExecutorEnd(query_desc);
	} else {
		standard_ExecutorEnd(query_desc);
	}

	/** Unnest. */
	nesting_level--;
}
