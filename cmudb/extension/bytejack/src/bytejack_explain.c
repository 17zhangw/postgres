#include "bytejack_explain.h"

#include "../../../../src/include/lib/stringinfo.h"
#include "../../../../src/include/nodes/parsenodes.h"
#include "../../../../src/include/nodes/pg_list.h"
#include "../../../../src/include/parser/parse_node.h"
#include "../../bytejack_rs/target/bytejack_rs.h"
#include "bytejack.h"
#include "bytejack_util.h"

//ExplainOneQuery_hook_type bytejack_prev_ExplainOneQuery_hook = NULL;
ExplainIntercept_hook_type bytejack_prev_ExplainIntercept_hook = NULL;

static void bytejack_ExplainOneQuery(
	JumbleState *jstate,
        Query *query, int cursorOptions,
        IntoClause *into, ExplainState *es,
        const char *queryString, ParamListInfo params,
        QueryEnvironment *queryEnv)
{
    //if (bytejack_prev_ExplainOneQuery_hook)
    //{
    //    (*bytejack_prev_ExplainOneQuery_hook)(
    //        query, cursorOptions,
    //        into, es,
    //        queryString, params,
    //        queryEnv);
    //}

    const char *queryStringNormalized;
    int queryLen;

    PlannedStmt *plan;
    instr_time	planstart, planduration;
    BufferUsage bufusage_start, bufusage;

	MemoryContext ccxt = CurrentMemoryContext;
    // Get the current length of data in ExplainState.
    int current_len = es->str->len;

    queryLen = strlen(queryString);
    queryStringNormalized = generate_normalized_query(jstate, queryString, 0, &queryLen);

    if (es->buffers)
        bufusage_start = pgBufferUsage;
    INSTR_TIME_SET_CURRENT(planstart);

    /* plan the query */
    plan = pg_plan_query(query, queryString, cursorOptions, params);

    if (bytejack_enable)
    {
        CacheResult *cacheResult = NULL;
        if (bytejack_intercept_explain_analyze && es->analyze) {
            cacheResult = cache_get_explain(redis_con, plan->rtable, plan->planTree, queryStringNormalized);
        }

        if (cacheResult)
        {
            appendStringInfoString(es->str, (const char *)cache_result_bytes(cacheResult));
			cache_result_free(cacheResult);
            return;
        }
    }

    INSTR_TIME_SET_CURRENT(planduration);
    INSTR_TIME_SUBTRACT(planduration, planstart);

    /* calc differences of buffer counters. */
    if (es->buffers)
    {
        memset(&bufusage, 0, sizeof(BufferUsage));
        BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
    }

    PG_TRY();
    {
        /* run it (if needed) and produce output */
        ExplainOnePlan(plan, into, es, queryString, params, queryEnv, &planduration, (es->buffers ? &bufusage : NULL));
    }
    PG_CATCH();
    {
		MemoryContext ecxt = MemoryContextSwitchTo(ccxt);
		ErrorData  *errdata = CopyErrorData();

        if (errdata->sqlerrcode == ERRCODE_QUERY_CANCELED) {
            // Query got canceled.
            if (bytejack_intercept_explain_analyze && es->analyze) {
		// Zero out the data.
		memset(es->str->data + current_len, 0x00, es->str->maxlen - current_len);
		es->str->len = current_len;
                // Try to get the raw query plan.
                es->analyze = false;
		es->indent = 1;
		es->grouping_stack = lcons_int(0, es->grouping_stack);

                ExplainOnePlan(plan, into, es, queryString, params, queryEnv, &planduration, (es->buffers ? &bufusage : NULL));
                cache_append_explain(redis_con, plan->rtable, plan->planTree, queryStringNormalized, &es->str->data[current_len], false);
            }
        }

        MemoryContextSwitchTo(ecxt);
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (bytejack_intercept_explain_analyze && es->analyze) {
        cache_append_explain(redis_con, plan->rtable, plan->planTree, queryStringNormalized, &es->str->data[current_len], true);
    }
}

void bytejack_ExplainIntercept(JumbleState *jstate, const char *queryString, ParamListInfo params, ParseState *pstate,
                               ExplainState *es, List *rewritten) {
  if (bytejack_prev_ExplainIntercept_hook) {
    (*bytejack_prev_ExplainIntercept_hook)(jstate, queryString, params, pstate, es, rewritten);
  }

  ExplainBeginOutput(es);
  if (rewritten == NIL) {
	  if (es->format == EXPLAIN_FORMAT_TEXT) {
		  appendStringInfoString(es->str, "Query rewrites to nothing\n");
	  }
  } else {
	  ListCell *l;
	  foreach (l, rewritten) {
		  bytejack_ExplainOneQuery(jstate, lfirst_node(Query, l), CURSOR_OPT_PARALLEL_OK, NULL, es, pstate->p_sourcetext, params,
				  pstate->p_queryEnv);
		  if (lnext(rewritten, l) != NULL) {
			  ExplainSeparatePlans(es);
		  }
	  }
  }
  ExplainEndOutput(es);
  Assert(es->indent == 0);
}
