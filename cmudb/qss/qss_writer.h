
/**
 * Struct that defines a query plan key (query identifier and plan generation).
 */
struct QSSPlanKey {
	uint64_t queryId;
	int64_t generation;
};

/**
 * Struct that defines a query plan.
 */
struct QSSPlan {
	struct QSSPlanKey key;
	int64_t statement_ts;
	char* query_plan;
};

/**
 * Struct that holds an Instrumentation result.
 *
 * In the logged data, if the plan_node_id == -1, then we have a "query invocation message".
 * counter0 in this case is repurposed to be an indicator if it executed or hit an ABORT.
 * QueryId is guaranteed to be nonzero in this case.
 *
 * If queryId == 0, then it is guaranteed that comment is "TxnAbort".  txn indicates the
 * transaction ID that aborted.
 */
struct QSSStats {
	uint64_t queryId;
	int64_t generation;
	int64_t timestamp;
	int32_t plan_node_id;
	float elapsed_us;
	float startup_time;
	float nloops;
	float counter0;
	float counter1;
	float counter2;
	float counter3;
	float counter4;
	float counter5;
	float counter6;
	float counter7;
	float counter8;
	float counter9;
	int32_t blk_hit;
	int32_t blk_miss;
	int32_t blk_dirty;
	int32_t blk_write;
	int64_t payload;
	int64_t txn;
	char* comment;
};



/**
 * Struct that defines a chunk of stats.
 */
#define QSSSTATS_PER_CHUNK (32)
struct QSSStatsChunk {
	struct QSSStats stats[QSSSTATS_PER_CHUNK];
	struct QSSStatsChunk* next;
	int32_t num_stats;
};

/** Assert that this fits in a page size allocation. */
static_assert(sizeof(struct QSSStats) == 120, "QSSStats is too large");
/** Assert that this fits in a page size allocation. */
static_assert(sizeof(struct QSSStatsChunk) < 4096, "QSSStatsChunk is too large");

/** Gets the next QSSStats entry that we can populate. */
struct QSSStats* GetStatsEntry(void);
void WriteInstrumentation(Plan *plan, Instrumentation *instr, uint64_t queryId, int64_t generation, int64_t timestamp);
/** Writes a plan's instrumentation to be buffered. */
void WritePlanInstrumentation(Plan *plan, PlanState *ps, uint64_t queryId, int64_t generation, int64_t timestamp);
/** Output the accumulated plan data. */
void qss_OutputData(int code, Datum arg);
