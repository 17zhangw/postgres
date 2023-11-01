#pragma once

// clang-format off
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
// clang-format on

#include "../../../../src/include/commands/explain.h"
#include "../../../../src/include/tcop/tcopprot.h"
#include "../../../../src/include/nodes/plannodes.h"

extern ExplainIntercept_hook_type bytejack_prev_ExplainIntercept_hook;
//extern ExplainOneQuery_hook_type bytejack_prev_ExplainOneQuery_hook;

void bytejack_ExplainIntercept(JumbleState *jstate, const char *queryString, ParamListInfo params, ParseState *pstate,
                               ExplainState *es, List *rewritten);


//void bytejack_ExplainOneQuery(Query *query, int cursorOptions,
//				IntoClause *into, ExplainState *es,
//				const char *queryString, ParamListInfo params,
//				QueryEnvironment *queryEnv);
