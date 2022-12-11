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
#include "pgstat.h"

/**
 * Header for the pg_qss_plans_{pid}.csv file.
 */
char QSS_PLAN_CSV_HEADER[] = "query_id,generation,db_id,pid,statement_timestamp,features\n";

/**
 * Header for the pg_qss_stats_{pid}.csv file.
 */
char QSS_STATS_CSV_HEADER[] = "query_id,generation,db_id,pid,statement_timestamp,plan_node_id,"
"elapsed_us,startup_time,nloops,counter0,counter1,counter2,counter3,counter4,counter5,counter6,counter7,counter8,"
"counter9,blk_hit,blk_miss,blk_dirty,blk_write,payload,txn,comment\n";

/** Start of buffering execution stats. */
static struct QSSStatsChunk* qss_front_stats = NULL;

/** Current head of the buffering execution stats. */
static struct QSSStatsChunk* qss_head_stats = NULL;

static inline void appendCSVLiteral(StringInfo buf, const char *data)
{
	const char *p = data;
	char		c;

	/* avoid confusing an empty string with NULL */
	if (p == NULL)
		return;

	appendStringInfoCharMacro(buf, '"');
	while ((c = *p++) != '\0')
	{
		if (c == '"')
			appendStringInfoCharMacro(buf, '"');
		appendStringInfoCharMacro(buf, c);
	}
	appendStringInfoCharMacro(buf, '"');
}

struct QSSStats* GetStatsEntry(void)
{
	struct QSSStats* stats = NULL;
	if (qss_capture_plan_only)
	{
		return NULL;
	}

	if (qss_head_stats != NULL)
	{
		if (qss_head_stats->num_stats < QSSSTATS_PER_CHUNK)
		{
			stats = &qss_head_stats->stats[qss_head_stats->num_stats];
			qss_head_stats->num_stats++;
		}
		else
		{
			struct QSSStatsChunk* new_head = MemoryContextAllocExtended(qss_MemoryContext, 4096, MCXT_ALLOC_NO_OOM);
			if (new_head != NULL)
			{
				memset(new_head, 0x0, 4096);
				stats = &new_head->stats[0];
				new_head->next = NULL;
				new_head->num_stats = 1;

				qss_head_stats->next = new_head;
				qss_head_stats = new_head;
			}
		}
	}

	return stats;
}

/**
 * Write an instrumentation record into QSSStats in-memory.
 */
void WriteInstrumentation(Plan *plan, Instrumentation *instr, uint64_t queryId, int64_t generation, int64_t timestamp) {
	const char* nodeName = NULL;
	struct QSSStats* stats = GetStatsEntry();

	InstrEndLoop(instr);
	if (stats != NULL)
	{
		stats->queryId = queryId;
		stats->generation = generation;
		stats->timestamp = timestamp;
		stats->plan_node_id = plan ? plan->plan_node_id : instr->plan_node_id;
		stats->elapsed_us = instr->total * 1000000.0;
		stats->startup_time = instr->startup * 1000000.0;
		stats->nloops = instr->nloops;
		stats->counter0 = instr->counter0;
		stats->counter1 = instr->counter1;
		stats->counter2 = instr->counter2;
		stats->counter3 = instr->counter3;
		stats->counter4 = instr->counter4;
		stats->counter5 = instr->counter5;
		stats->counter6 = instr->counter6;
		stats->counter7 = instr->counter7;
		stats->counter8 = instr->counter8;
		stats->counter9 = instr->counter9;
		stats->blk_hit = instr->bufusage.shared_blks_hit;
		stats->blk_miss = instr->bufusage.shared_blks_read;
		stats->blk_dirty = instr->bufusage.shared_blks_dirtied;
		stats->blk_write = instr->bufusage.shared_blks_written;
		stats->payload = instr->payload;
		stats->txn = GetCurrentTransactionId();

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
		stats->comment = (char*)nodeName;
	}
}

void WritePlanInstrumentation(Plan *plan, PlanState *ps, uint64_t queryId, int64_t generation, int64_t timestamp) {
	Instrumentation *instr = ps->instrument;
	if (instr != NULL) {
		WriteInstrumentation(plan, instr, queryId, generation, timestamp);
	}

	if (outerPlanState(ps) != NULL) {
		WritePlanInstrumentation(outerPlan(plan), outerPlanState(ps), queryId, generation, timestamp);
	}

	if (innerPlanState(ps) != NULL) {
		WritePlanInstrumentation(innerPlan(plan), innerPlanState(ps), queryId, generation, timestamp);
	}
}

void qss_InitPlansHashTable(void)
{
	HASHCTL hash_ctl;
	hash_ctl.keysize = sizeof(struct QSSPlanKey);
	hash_ctl.entrysize = sizeof(struct QSSPlan);
	hash_ctl.hcxt = qss_MemoryContext;
	qss_PlansHTAB = hash_create("QSSPlans", 32, &hash_ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	qss_front_stats = qss_head_stats = MemoryContextAllocExtended(qss_MemoryContext, 4096, MCXT_ALLOC_NO_OOM);
	if (qss_head_stats != NULL)
	{
		memset(qss_head_stats, 0x00, 4096);
		qss_head_stats->next = NULL;
		qss_head_stats->num_stats = 0;
	}
}

void qss_OutputData(int code, Datum arg)
{
	bool created = false;
	FILE* qfile = NULL;
	HASH_SEQ_STATUS hash_seq;
	struct QSSPlan* entry;
	struct QSSStatsChunk* statschunk = NULL;
	StringInfoData buf;
	char query_path[256];
	char stats_path[256];
	size_t wrote;

	/* Don't try to dump during a crash. */
	if (code)
		return;

	/* Safety check ... shouldn't get here unless memory is set up. */
	if (!qss_PlansHTAB || !qss_front_stats)
		return;

    /** Initialize the buffer for CSV files. */
	initStringInfo(&buf);
	snprintf(query_path, 256, "%s/pg_qss_plans_%d.csv", PGSTAT_STAT_PERMANENT_DIRECTORY, MyProcPid);
	snprintf(stats_path, 256, "%s/pg_qss_stats_%d.csv", PGSTAT_STAT_PERMANENT_DIRECTORY, MyProcPid);

	hash_seq_init(&hash_seq, qss_PlansHTAB);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (!created)
		{
			/** Create the pg_qss_plans file. */
			qfile = AllocateFile(query_path, PG_BINARY_W);
			if (qfile == NULL)
				goto error;

			/** Write the header for pg_qss_plans. */
			if ((wrote = fwrite(QSS_PLAN_CSV_HEADER, 1, sizeof(QSS_PLAN_CSV_HEADER) - 1, qfile)) != sizeof(QSS_PLAN_CSV_HEADER) - 1)
				goto error;

			created = true;
		}

        /** Serialize each plan entry as a CSV row. */
		appendStringInfo(&buf, "%ld,", entry->key.queryId);
		appendStringInfo(&buf, "%ld,", entry->key.generation);
		appendStringInfo(&buf, "%d,", MyDatabaseId);
		appendStringInfo(&buf, "%d,", MyProcPid);
		appendStringInfo(&buf, "%ld,", entry->statement_ts);
		appendCSVLiteral(&buf, entry->query_plan);
		appendStringInfoChar(&buf, '\n');
		if (fwrite(buf.data, 1, buf.len, qfile) != buf.len)
			goto error;
		resetStringInfo(&buf);
	}

	if (qfile != NULL)
	{
		FreeFile(qfile);
		qfile = NULL;
	}

	created = false;
	statschunk = qss_front_stats;
	while (statschunk != NULL)
	{
		for (int i = 0; i < statschunk->num_stats; i++)
		{
			struct QSSStats* stat = &statschunk->stats[i];
			if (!created)
			{
				/** Create the pg_qss_stats file. */
				qfile = AllocateFile(stats_path, PG_BINARY_W);
				if (qfile == NULL)
					goto error;

				/** Write the header for pg_qss_stats. */
				if (fwrite(QSS_STATS_CSV_HEADER, 1, sizeof(QSS_STATS_CSV_HEADER) - 1, qfile) != sizeof(QSS_STATS_CSV_HEADER) - 1)
					goto error;

				created = true;
			}

            /** Write each stats entry out. */
			appendStringInfo(&buf, "%ld,", stat->queryId);
			appendStringInfo(&buf, "%ld,", stat->generation);
			appendStringInfo(&buf, "%d,", MyDatabaseId);
			appendStringInfo(&buf, "%d,", MyProcPid);
			appendStringInfo(&buf, "%ld,", stat->timestamp);
			appendStringInfo(&buf, "%d,", stat->plan_node_id);
			appendStringInfo(&buf, "%g,", stat->elapsed_us);
			appendStringInfo(&buf, "%g,", stat->startup_time);
			appendStringInfo(&buf, "%g,", stat->nloops);
			appendStringInfo(&buf, "%g,", stat->counter0);
			appendStringInfo(&buf, "%g,", stat->counter1);
			appendStringInfo(&buf, "%g,", stat->counter2);
			appendStringInfo(&buf, "%g,", stat->counter3);
			appendStringInfo(&buf, "%g,", stat->counter4);
			appendStringInfo(&buf, "%g,", stat->counter5);
			appendStringInfo(&buf, "%g,", stat->counter6);
			appendStringInfo(&buf, "%g,", stat->counter7);
			appendStringInfo(&buf, "%g,", stat->counter8);
			appendStringInfo(&buf, "%g,", stat->counter9);
			appendStringInfo(&buf, "%d,", stat->blk_hit);
			appendStringInfo(&buf, "%d,", stat->blk_miss);
			appendStringInfo(&buf, "%d,", stat->blk_dirty);
			appendStringInfo(&buf, "%d,", stat->blk_write);
			appendStringInfo(&buf, "%ld,", stat->payload);
			appendStringInfo(&buf, "%ld,", stat->txn);
			if (stat->comment != NULL)
			{
				appendCSVLiteral(&buf, stat->comment);
			}
			appendStringInfoChar(&buf, '\n');
			if (fwrite(buf.data, 1, buf.len, qfile) != buf.len)
				goto error;
			resetStringInfo(&buf);
		}
		statschunk = statschunk->next;
	}

	if (qfile != NULL)
	{
		FreeFile(qfile);
		qfile = NULL;
	}
	return;

error:
	if (qfile)
		FreeFile(qfile);
}
