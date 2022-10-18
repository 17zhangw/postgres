#include "qss.h"
#include "qss_features.h"

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
#include "utils/builtins.h"

/**
 CREATE UNLOGGED TABLE pg_catalog.pg_qss_ddl(
   db_id integer,
   statement_timestamp bigint,
   query text,
   command text
 )
 */
#define DDL_TABLE_NAME "pg_qss_ddl"
#define DDL_TABLE_COLUMNS 4

/**
 CREATE UNLOGGED TABLE pg_catalog.pg_qss_plans(
	query_id BIGINT,
	generation INTEGER,
	db_id INTEGER,
	pid INTEGER,
	timestamp BIGINT,
	features TEXT,
	primary key(query_id, generation, db_id, pid)
 )
 */
#define QUERY_TABLE_NAME "pg_qss_plans"
#define QUERY_INDEX_NAME "pg_qss_plans_pkey"
#define QUERY_TABLE_COLUMNS 6

/**
 CREATE UNLOGGED TABLE pg_catalog.pg_qss_stats(
	query_id bigint,
	generation bigint,
	db_id integer,
	pid integer,
	timestamp bigint,
	plan_node_id int,
	elapsed_us float8,
	counter0 float8,
	counter1 float8,
	counter2 float8,
	counter3 float8,
	counter4 float8,
	counter5 float8,
	counter6 float8,
	counter7 float8,
	counter8 float8,
	counter9 float8,
	blk_hit integer,
	blk_miss integer,
	blk_dirty integer,
	blk_write integer,
	payload bigint,
	txn bigint,
	comment text
 )

 In the logged data, if the plan_node_id == -1, then we have a "query invocation message".
 counter0 in this case is repurposed to be an indicator if it executed or hit an ABORT.
 QueryId is guaranteed to be nonzero in this case.

 If queryId == 0, then it is guaranteed that comment is "TxnAbort".  txn indicates the
 transaction ID that aborted.
 */
#define STATS_TABLE_NAME "pg_qss_stats"
#define STATS_TABLE_COLUMNS 24

#define STATS_PLAN_NODE_ID_IDX (5)
#define STATS_ELAPSED_US_IDX (6)
#define STATS_COUNTER_IDX (7)
#define STATS_TXN_IDX (22)
#define STATS_TABLE_COMMENT_IDX 23

#define STATS_COUNTER0 (STATS_COUNTER_IDX + 0)
#define STATS_COUNTER1 (STATS_COUNTER_IDX + 1)
#define STATS_COUNTER2 (STATS_COUNTER_IDX + 2)
#define STATS_COUNTER3 (STATS_COUNTER_IDX + 3)
#define STATS_COUNTER4 (STATS_COUNTER_IDX + 4)
#define STATS_COUNTER5 (STATS_COUNTER_IDX + 5)
#define STATS_COUNTER6 (STATS_COUNTER_IDX + 6)
#define STATS_COUNTER7 (STATS_COUNTER_IDX + 7)
#define STATS_COUNTER8 (STATS_COUNTER_IDX + 8)
#define STATS_COUNTER9 (STATS_COUNTER_IDX + 9)
#define STATS_BLK_HITS (STATS_COUNTER_IDX + 10)
#define STATS_BLK_READ (STATS_COUNTER_IDX + 11)
#define STATS_BLK_DIRTY (STATS_COUNTER_IDX + 12)
#define STATS_BLK_WRITE (STATS_COUNTER_IDX + 13)
#define STATS_PAYLOAD (STATS_COUNTER_IDX + 14)

void qss_ProcessUtility(PlannedStmt *pstmt,
						const char *queryString,
						bool readOnlyTree,
						ProcessUtilityContext context,
						ParamListInfo params,
						QueryEnvironment *queryEnv,
						DestReceiver *dest,
						QueryCompletion *qc) {
	Node *parsetree = pstmt->utilityStmt;
	if (nodeTag(parsetree) == T_AlterTableStmt && queryString != NULL) {
		// Try to find out whether this is an ALTER TABLE [table] SET options.
		bool set = false;
		ListCell *cell = NULL;
		AlterTableStmt *astmt = (AlterTableStmt *)parsetree;
		foreach (cell, astmt->cmds) {
			AlterTableCmd *cmd = (AlterTableCmd *)lfirst(cell);
			if (cmd->subtype == AT_SetRelOptions) {
				set = true;
				break;
			}
		}

		if (set) {
			// Create the memory context that we need to use.
			MemoryContext tmpCtx = AllocSetContextCreate(qss_MemoryContext,
														 "qss_UtilityContext",
														 ALLOCSET_DEFAULT_SIZES);
			MemoryContext old = MemoryContextSwitchTo(tmpCtx);

			// Initialize all the heap tuple values.
			Datum values[DDL_TABLE_COLUMNS];
			bool is_nulls[DDL_TABLE_COLUMNS];
			Oid ddl_table_oid = RelnameGetRelid(DDL_TABLE_NAME);
			Relation ddl_table_relation = table_open(ddl_table_oid, RowExclusiveLock);
			HeapTuple heap_tup = NULL;
			memset(values, 0, sizeof(values));
			memset(is_nulls, 0, sizeof(is_nulls));

			values[0] = ObjectIdGetDatum(MyDatabaseId);
			values[1] = Int64GetDatumFast(GetCurrentStatementStartTimestamp());

			is_nulls[2] = false;
			values[2] = CStringGetTextDatum(queryString);

			is_nulls[3] = false;
			values[3] = CStringGetTextDatum("AlterTableOptions");

			heap_tup = heap_form_tuple(ddl_table_relation->rd_att, values, is_nulls);
			do_heap_insert(ddl_table_relation, heap_tup,
						   GetCurrentTransactionId(),
						   GetCurrentCommandId(true),
						   HEAP_INSERT_FROZEN,
						   NULL);
			pfree(heap_tup);

			// Purge the memory contexts.
			table_close(ddl_table_relation, RowExclusiveLock);
			MemoryContextSwitchTo(old);
			MemoryContextDelete(tmpCtx);
		}
	}

	if (qss_prev_ProcessUtility) {
		(*qss_prev_ProcessUtility)(pstmt,
								   queryString,
								   readOnlyTree,
								   context,
								   params,
								   queryEnv,
								   dest,
								   qc);
	} else {
		standard_ProcessUtility(pstmt,
								queryString,
								readOnlyTree,
								context,
								params,
								queryEnv,
								dest,
								qc);
	}
}

static bool IndexLookup(Snapshot snapshot, Relation heap_relation, Relation index_relation, IndexTuple itup) {
	bool unique = false;
	uint32 specToken = 0;
	BTInsertStateData insertstate;
	BTScanInsert itup_key;
	BTStack stack;

	itup_key = _bt_mkscankey(index_relation, itup);
	itup_key->scantid = NULL;

	insertstate.itup = itup;
	insertstate.itemsz = MAXALIGN(IndexTupleSize(itup));
	insertstate.itup_key = itup_key;
	insertstate.bounds_valid = false;
	insertstate.buf = InvalidBuffer;
	insertstate.postingoff = 0;

	stack = _bt_search_insert(index_relation, &insertstate);
	_bt_check_unique(index_relation, &insertstate, heap_relation, UNIQUE_CHECK_YES, &unique, &specToken, false /*raiseError*/);

	if (BufferIsValid(insertstate.buf)) {
		_bt_relbuf(index_relation, insertstate.buf);
	}

	if (stack) {
		_bt_freestack(stack);
	}
	pfree(itup_key);
	return !unique;
}

static void WriteInstrumentation(Plan *plan, Instrumentation *instr, Relation stats_table_relation, Datum *values, bool *nulls) {
	HeapTuple heap_tup = NULL;
	const char* nodeName = NULL;
	InstrEndLoop(instr);
	values[(STATS_PLAN_NODE_ID_IDX)] = Int32GetDatum(plan ? plan->plan_node_id : instr->plan_node_id);
	values[(STATS_ELAPSED_US_IDX)] = Float8GetDatum(instr->total * 1000000.0);
	values[STATS_COUNTER0] = Float8GetDatum(instr->counter0);
	values[STATS_COUNTER1] = Float8GetDatum(instr->counter1);
	values[STATS_COUNTER2] = Float8GetDatum(instr->counter2);
	values[STATS_COUNTER3] = Float8GetDatum(instr->counter3);
	values[STATS_COUNTER4] = Float8GetDatum(instr->counter4);
	values[STATS_COUNTER5] = Float8GetDatum(instr->counter5);
	values[STATS_COUNTER6] = Float8GetDatum(instr->counter6);
	values[STATS_COUNTER7] = Float8GetDatum(instr->counter7);
	values[STATS_COUNTER8] = Float8GetDatum(instr->counter8);
	values[STATS_COUNTER9] = Float8GetDatum(instr->counter9);
	values[STATS_BLK_HITS] = Int32GetDatum(instr->bufusage.shared_blks_hit);
	values[STATS_BLK_READ] = Int32GetDatum(instr->bufusage.shared_blks_read);
	values[STATS_BLK_DIRTY] = Int32GetDatum(instr->bufusage.shared_blks_dirtied);
	values[STATS_BLK_WRITE] = Int32GetDatum(instr->bufusage.shared_blks_written);
	values[STATS_PAYLOAD] = Int64GetDatum(instr->payload);
	values[(STATS_TXN_IDX)] = TransactionIdGetDatum(GetCurrentTransactionId());

	if (plan) {
		if (plan->type == T_ModifyTable) {
			ModifyTable* mt = (ModifyTable*)plan;
			if (mt->operation == CMD_INSERT) {
				nodeName = "ModifyTableInsert";
			} else if (mt->operation == CMD_UPDATE) {
				nodeName = "ModifyTableUpdate";
			} else {
				Assert(mt->operation == CMD_DELETE);
				nodeName = "ModifyTableDelete";
			}
		} else {
			nodeName = NodeToName((Node*)plan);
		}
	} else {
		nodeName = instr->ou ? instr->ou : "";
	}

	values[STATS_TABLE_COMMENT_IDX] = PointerGetDatum(cstring_to_text(nodeName));
	nulls[STATS_TABLE_COMMENT_IDX] = false;

	heap_tup = heap_form_tuple(stats_table_relation->rd_att, values, nulls);
	do_heap_insert(stats_table_relation, heap_tup,
				   GetCurrentTransactionId(),
				   GetCurrentCommandId(true),
				   HEAP_INSERT_FROZEN,
				   NULL);
	pfree(heap_tup);
}

static void WritePlanInstrumentation(Plan *plan, PlanState *ps, Relation stats_table_relation, Datum *values, bool *nulls) {
	Instrumentation *instr = ps->instrument;
	if (instr != NULL) {
		WriteInstrumentation(plan, instr, stats_table_relation, values, nulls);
	}

	if (outerPlanState(ps) != NULL) {
		WritePlanInstrumentation(outerPlan(plan), outerPlanState(ps), stats_table_relation, values, nulls);
	}

	if (innerPlanState(ps) != NULL) {
		WritePlanInstrumentation(innerPlan(plan), innerPlanState(ps), stats_table_relation, values, nulls);
	}
}

// All memory for ExecutorInstrument is charged to the query context that is executing
// the query that we are attempting to instrument. We do not use qss_MemoryContext
// for allocating any of this memory.
struct ExecutorInstrument {
	int64 queryId;
	int generation;
	char* params;
	TimestampTz statement_ts;

	bool capture;
	EState* estate;
	List* statement_instrs;
	struct ExecutorInstrument* prev;
};

TransactionId last_commit_xact = InvalidTransactionId;
int nesting_level = 0;
struct ExecutorInstrument* top = NULL;

void qss_Abort() {
	if (qss_capture_abort && last_commit_xact != InvalidTransactionId) {
		MemoryContext tmpCtx = AllocSetContextCreate(qss_MemoryContext,
													 "qss_AbortContext",
													 ALLOCSET_DEFAULT_SIZES);
		MemoryContext old = MemoryContextSwitchTo(tmpCtx);

		struct ExecutorInstrument *head = top;
		Datum values[STATS_TABLE_COLUMNS];
		bool is_nulls[STATS_TABLE_COLUMNS];
		Oid stats_table_oid = RelnameGetRelid(STATS_TABLE_NAME);
		Relation stats_table_relation = table_open(stats_table_oid, RowExclusiveLock);
		while (head != NULL) {
			HeapTuple heap_tup = NULL;
			memset(values, 0, sizeof(values));
			memset(is_nulls, 0, sizeof(is_nulls));
			values[0] = Int64GetDatumFast(top->queryId);
			values[1] = Int32GetDatum(top->generation);
			values[2] = ObjectIdGetDatum(MyDatabaseId);
			values[3] = Int32GetDatum(MyProcPid);
			values[4] = Int64GetDatumFast(top->statement_ts);

			values[(STATS_PLAN_NODE_ID_IDX + 0)] = Int32GetDatum(-1);
			values[(STATS_ELAPSED_US_IDX)] = Float8GetDatum(0.0);
			values[(STATS_COUNTER_IDX)] = Float8GetDatum(0.0);
			values[STATS_TXN_IDX] = TransactionIdGetDatum(GetCurrentTransactionId());

			is_nulls[STATS_TABLE_COMMENT_IDX] = (top->params == NULL);
			if (top->params != NULL) {
				values[STATS_TABLE_COMMENT_IDX] = CStringGetTextDatum(top->params);
			}

			heap_tup = heap_form_tuple(stats_table_relation->rd_att, values, is_nulls);
			do_heap_insert(stats_table_relation, heap_tup,
						   GetCurrentTransactionId(),
						   GetCurrentCommandId(true),
						   HEAP_INSERT_FROZEN,
						   NULL);
			pfree(heap_tup);
			head = head->prev;
		}

		// Need to generically log the fact that the particular transaction has aborted.
		{
			HeapTuple heap_tup = NULL;
			memset(values, 0, sizeof(values));
			memset(is_nulls, 0, sizeof(is_nulls));
			values[0] = Int64GetDatumFast(0);
			values[1] = Int32GetDatum(0);
			values[2] = ObjectIdGetDatum(MyDatabaseId);
			values[3] = Int32GetDatum(MyProcPid);
			values[4] = Int64GetDatumFast(GetCurrentStatementStartTimestamp());

			values[(STATS_PLAN_NODE_ID_IDX)] = Int32GetDatum(-1);
			values[(STATS_ELAPSED_US_IDX)] = Float8GetDatum(0.0);
			values[STATS_TXN_IDX] = TransactionIdGetDatum(GetCurrentTransactionId());

			is_nulls[STATS_TABLE_COMMENT_IDX] = false;
			values[STATS_TABLE_COMMENT_IDX] = CStringGetTextDatum("TxnAbort");

			heap_tup = heap_form_tuple(stats_table_relation->rd_att, values, is_nulls);
			do_heap_insert(stats_table_relation, heap_tup,
						   GetCurrentTransactionId(),
						   GetCurrentCommandId(true),
						   HEAP_INSERT_FROZEN,
						   NULL);

		}

		table_close(stats_table_relation, RowExclusiveLock);

		MemoryContextSwitchTo(old);
		MemoryContextDelete(tmpCtx);
	}

	// These should get freed by the query MemoryContext.
	ActiveQSSInstrumentation = NULL;
	top = NULL;
	nesting_level = 0;
}

void qss_xact_callback(XactEvent event, void* arg) {
	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_PARALLEL_COMMIT) {
		last_commit_xact = GetCurrentTransactionIdIfAny();
	} else if (event == XACT_EVENT_PRE_ABORT) {
		qss_Abort();
	}
}

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

	instr = palloc0(sizeof(Instrumentation));
	InstrInit(instr, (need_timer ? INSTRUMENT_TIMER : 0) | INSTRUMENT_BUFFERS);
	instr->plan_node_id = PLAN_INDEPENDENT_ID;
	instr->ou = ou;

	if (top->statement_instrs == NULL) {
		top->statement_instrs = list_make1(instr);
	} else {
		top->statement_instrs = lappend(top->statement_instrs, instr);
	}

	MemoryContextSwitchTo(oldcontext);
	return instr;
}

void qss_ExecutorStart(QueryDesc *query_desc, int eflags) {
	MemoryContext oldcontext = NULL;
	struct ExecutorInstrument* exec = NULL;
	bool need_instrument;
	bool need_total;
	bool capture;
	nesting_level++;

	query_desc->nesting_level = nesting_level;

	need_total = qss_capture_enabled && (qss_capture_nested || nesting_level == 1);
	capture = (qss_capture_exec_stats && qss_output_format == QSS_OUTPUT_FORMAT_NOISEPAGE) || (!qss_capture_exec_stats && qss_output_format != QSS_OUTPUT_FORMAT_NOISEPAGE);
	need_instrument = qss_capture_enabled &&
					  capture &&
					  (qss_capture_nested || nesting_level == 1) &&
					  query_desc->generation >= 0 &&
					  (!query_desc->dest || query_desc->dest->mydest != DestSQLFunction);

	// Attach INSTRUMENT_TIMER if we want those statistics.
	// And get the buffer counts while we are at it...
	if (need_instrument) {
		query_desc->instrument_options |= (INSTRUMENT_TIMER | INSTRUMENT_BUFFERS);
	}

	// Initialize the plan.
	if (qss_prev_ExecutorStart != NULL) {
		qss_prev_ExecutorStart(query_desc, eflags);
	} else {
		standard_ExecutorStart(query_desc, eflags);
	}

	Assert(query_desc->estate != NULL);
	oldcontext = MemoryContextSwitchTo(query_desc->estate->es_query_cxt);
	exec = palloc0(sizeof(struct ExecutorInstrument));
	// TODO(wz2): This is probably not going to capture re-runs but we're on REPEATABLE_READ.
	exec->statement_ts = GetCurrentStatementStartTimestamp();
	exec->queryId = query_desc->plannedstmt->queryId;
	exec->generation = query_desc->generation;
	exec->capture = need_instrument;
	exec->estate = query_desc->estate;
	if (query_desc->params != NULL) {
		exec->params = BuildParamLogString(query_desc->params, NULL, -1);
	}
	exec->prev = top;
	top = exec;

	if (need_total && query_desc->totaltime == NULL) {
		// Attach an instrument so we capture totaltime.
		query_desc->totaltime = InstrAlloc(1, INSTRUMENT_TIMER | INSTRUMENT_BUFFERS, false, 0);
	}

	MemoryContextSwitchTo(oldcontext);
}

static void ProcessQueryExplain(QueryDesc *query_desc, bool instrument, bool verbose) {
	// Setup ExplainState
	ExplainState *es = NULL;
	es = NewExplainState();
	es->analyze = instrument;
	es->verbose = verbose;
	es->timing = true;
	if (qss_output_format == QSS_OUTPUT_FORMAT_JSON) {
		es->format = EXPLAIN_FORMAT_JSON;
	} else if (qss_output_format == QSS_OUTPUT_FORMAT_TEXT) {
		es->format = EXPLAIN_FORMAT_TEXT;
	}

	// OUtput all relevant information.
	ExplainBeginOutput(es);
	ExplainQueryText(es, query_desc);
	ExplainPropertyInteger("start_time", NULL, top->statement_ts, es);
	ExplainPropertyFloat("elapsed_us", NULL, query_desc->totaltime->total * 1000000.0, 9, es);
	ExplainPropertyInteger("query_id", NULL, top->queryId, es);
	ExplainPropertyInteger("txn", NULL, GetCurrentTransactionId(), es);
	ExplainPrintPlan(es, query_desc);
	if (es->analyze)
		ExplainPrintTriggers(es, query_desc);
	ExplainEndOutput(es);

	if (qss_output_format == QSS_OUTPUT_FORMAT_JSON) {
		if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
			es->str->data[--es->str->len] = '\0';

		/* Fix JSON to output an object */
		es->str->data[0] = '{';
		es->str->data[es->str->len - 1] = '}';
	}

	ereport(LOG, (errmsg("%s", es->str->data), errhidestmt(true)));
}

static void ProcessQueryInternalTable(QueryDesc *query_desc, bool instrument) {
	Datum values[STATS_TABLE_COLUMNS];
	bool is_nulls[STATS_TABLE_COLUMNS];
	Oid plans_index_oid = RelnameGetRelid(QUERY_INDEX_NAME);
	Oid plans_table_oid = RelnameGetRelid(QUERY_TABLE_NAME);
	Oid stats_table_oid = RelnameGetRelid(STATS_TABLE_NAME);
	if (plans_index_oid > 0 && plans_table_oid > 0) {
		IndexTuple ind_tup = NULL;
		Relation table_relation = table_open(plans_table_oid, RowExclusiveLock);
		Relation index_relation = index_open(plans_index_oid, RowExclusiveLock);
		Assert(table_relation != NULL && index_relation != NULL);

		memset(is_nulls, 0, sizeof(is_nulls));
		values[0] = Int64GetDatumFast(top->queryId);
		values[1] = Int32GetDatum(query_desc->generation);
		values[2] = ObjectIdGetDatum(MyDatabaseId);
		values[3] = Int32GetDatum(MyProcPid);
		ind_tup = index_form_tuple(index_relation->rd_att, values, is_nulls);
		if (!IndexLookup(query_desc->estate->es_snapshot, table_relation, index_relation, ind_tup)) {
			HeapTuple heap_tup = NULL;
			ItemPointer tid = NULL;
			ExplainState *es = NULL;
			IndexInfo *index_info = BuildIndexInfo(index_relation);

			// Store the statement timestamp.
			values[4] = Int64GetDatumFast(top->statement_ts);

			// Make the query plan.
			es = NewExplainState();
			es->analyze = true;
			es->format = EXPLAIN_FORMAT_NOISEPAGE;
			ExplainBeginOutput(es);
			OutputPlanToExplain(query_desc, es);
			ExplainEndOutput(es);
			values[5] = PointerGetDatum(cstring_to_text_with_len(es->str->data, es->str->len));

			heap_tup = heap_form_tuple(table_relation->rd_att, values, is_nulls);
			do_heap_insert(table_relation, heap_tup,
						   GetCurrentTransactionId(),
						   GetCurrentCommandId(true),
						   HEAP_INSERT_FROZEN,
						   NULL);

			/* Get new tid and add one entry to index. */
			tid = &(heap_tup->t_self);
			btinsert(index_relation, values, is_nulls, tid, table_relation, UNIQUE_CHECK_YES, false, index_info);
			pfree(heap_tup);
		}

		pfree(ind_tup);
		table_close(table_relation, RowExclusiveLock);
		index_close(index_relation, RowExclusiveLock);
	}

	if (stats_table_oid > 0) {
		ListCell* lc;
		Relation stats_table_relation = table_open(stats_table_oid, RowExclusiveLock);

		memset(values, 0, sizeof(values));
		memset(is_nulls, 0, sizeof(is_nulls));
		values[0] = Int64GetDatumFast(top->queryId);
		values[1] = Int32GetDatum(top->generation);
		values[2] = ObjectIdGetDatum(MyDatabaseId);
		values[3] = Int32GetDatum(MyProcPid);
		values[4] = Int64GetDatumFast(top->statement_ts);
		if (query_desc->totaltime) {
			HeapTuple heap_tup = NULL;
			values[STATS_PLAN_NODE_ID_IDX] = Int32GetDatum(-1);
			values[STATS_ELAPSED_US_IDX] = Float8GetDatum(query_desc->totaltime->total * 1000000.0);
			values[STATS_COUNTER_IDX] = Float8GetDatum(1.0);
			values[STATS_COUNTER1] = Float8GetDatum(query_desc->totaltime->ntuples);
			values[STATS_TXN_IDX] = TransactionIdGetDatum(GetCurrentTransactionId());

			values[STATS_BLK_HITS] = Int32GetDatum(query_desc->totaltime->bufusage.shared_blks_hit);
			values[STATS_BLK_READ] = Int32GetDatum(query_desc->totaltime->bufusage.shared_blks_read);
			values[STATS_BLK_DIRTY] = Int32GetDatum(query_desc->totaltime->bufusage.shared_blks_dirtied);
			values[STATS_BLK_WRITE] = Int32GetDatum(query_desc->totaltime->bufusage.shared_blks_written);

			is_nulls[STATS_TABLE_COMMENT_IDX] = (top->params == NULL);
			if (top->params != NULL) {
				values[STATS_TABLE_COMMENT_IDX] = CStringGetTextDatum(top->params);
			}

			heap_tup = heap_form_tuple(stats_table_relation->rd_att, values, is_nulls);
			do_heap_insert(stats_table_relation, heap_tup,
						   GetCurrentTransactionId(),
						   GetCurrentCommandId(true),
						   HEAP_INSERT_FROZEN,
						   NULL);
			pfree(heap_tup);
		}

		if (qss_capture_exec_stats && instrument) {
			foreach(lc, top->statement_instrs) {
				Instrumentation *instr = (Instrumentation*)lfirst(lc);
				if (instr != NULL) {
					WriteInstrumentation(NULL, instr, stats_table_relation, values, is_nulls);
				}
			}

			WritePlanInstrumentation(query_desc->planstate->plan, query_desc->planstate, stats_table_relation, values, is_nulls);
		}

		table_close(stats_table_relation, RowExclusiveLock);
	}
}

void qss_ExecutorEnd(QueryDesc *query_desc) {
	MemoryContext oldcontext;
	EState *estate = query_desc->estate;

	/* Switch into per-query memory context */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	Assert(query_desc->nesting_level == nesting_level);

	if (qss_capture_enabled && query_desc->totaltime != NULL && top != NULL) {
		// End the loop on this counter.
		InstrEndLoop(query_desc->totaltime);

		if (qss_output_format == QSS_OUTPUT_FORMAT_NOISEPAGE) {
			ProcessQueryInternalTable(query_desc, top->capture);
		} else if (qss_output_format == QSS_OUTPUT_FORMAT_JSON || qss_output_format == QSS_OUTPUT_FORMAT_TEXT) {
			ProcessQueryExplain(query_desc, top->capture, true);
		}
	}

	if (top != NULL) {
		// Just pop the context. The memory should get freed by the MemoryContext.
		top = top->prev;
	}

	MemoryContextSwitchTo(oldcontext);

	if (qss_prev_ExecutorEnd != NULL) {
		qss_prev_ExecutorEnd(query_desc);
	} else {
		standard_ExecutorEnd(query_desc);
	}

	nesting_level--;
}
