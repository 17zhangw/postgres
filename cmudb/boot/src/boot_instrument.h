#pragma once

// clang-format off
#include "postgres.h"
#include "fmgr.h"
// clang-format on

#include "executor/instrument.h"
#include "nodes/execnodes.h"

extern InstrAddTupleBatchTimes_hook_type boot_prev_InstrAddTupleBatchTimes_hook;

bool boot_InstrAddTupleBatchTimes(struct PlanState *node, double n_tuples, double accumulated_us);
