#pragma once

// clang-format off
#include "postgres.h"
#include "fmgr.h"
// clang-format on

#include "commands/explain.h"

extern ExplainIntercept_hook_type boot_prev_ExplainIntercept_hook;

void boot_ExplainIntercept(JumbleState *jstate, const char *queryString, ParamListInfo params, ParseState *pstate,
                           ExplainState *es, List *rewritten);
