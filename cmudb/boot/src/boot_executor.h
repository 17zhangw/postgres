#pragma once

// clang-format off
#include "postgres.h"
#include "fmgr.h"
// clang-format on

#include "access/sdir.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "../../boot_rs/target/boot_rs.h"

extern RuntimeState *runtime_state;
extern ExecutorStart_hook_type boot_prev_ExecutorStart_hook;
extern ExecutorRun_hook_type boot_prev_ExecutorRun_hook;
extern ExecutorFinish_hook_type boot_prev_ExecutorFinish_hook;
extern ExecutorEnd_hook_type boot_prev_ExecutorEnd_hook;

void boot_ExecutorStart(QueryDesc *queryDesc, int eflags);
void boot_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once);
void boot_ExecutorFinish(QueryDesc *queryDesc);
void boot_ExecutorEnd(QueryDesc *queryDesc);
