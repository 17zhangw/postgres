#pragma once

#include "c.h"  // bool
#include "postgres.h"
#include "executor/instrument.h"
#include "nodes/execnodes.h"

extern bool qss_capture_enabled;
extern bool qss_capture_nested;
extern bool qss_capture_exec_stats;

enum qss_output_format_type
{
	QSS_OUTPUT_FORMAT_NOISEPAGE = 0,
	QSS_OUTPUT_FORMAT_JSON,
	QSS_OUTPUT_FORMAT_TEXT,
};
extern int qss_output_format;

/** Invalid plan node identifier. */
#define PLAN_INVALID_ID (-1)

/**
 * This is the Plan ID that should be used to capture any actions that are separate
 * from a plan invocation (i.e., triggers). Caller is responsible for ensuring that the
 * action's counters are separate from any other action when using this ID.
 */
#define PLAN_INDEPENDENT_ID (-2)

/** Current instrumentation target. */
extern PGDLLIMPORT Instrumentation* ActiveQSSInstrumentation;

/** Allocate an instrumentation target. */
typedef Instrumentation* (*qss_AllocInstrumentation_type) (const char *ou, bool need_timer);
typedef void (*qss_QSSAbort_type)(void);
extern PGDLLIMPORT qss_QSSAbort_type qss_QSSAbort_hook;
extern PGDLLIMPORT qss_AllocInstrumentation_type qss_AllocInstrumentation_hook;
Instrumentation* AllocQSSInstrumentation(const char *ou, bool need_timer);
void QSSAbort(void);

#define QSSInstrumentAddCounter(node, i, val)                                                       \
	do {                                                                                            \
		PlanState* ps = (PlanState *)node;                                                          \
		Instrumentation* inst = ps ? ps->instrument : NULL;                                         \
		if (inst && qss_capture_exec_stats) {                                                       \
			inst->counter##i += val;                                                                \
		}                                                                                           \
	} while(0)

#define QSSInstrumentAddCounterDirect(inst, i, val)                                                 \
	do {                                                                                            \
		if (inst && qss_capture_exec_stats) {                                                       \
			inst->counter##i += val;                                                                \
		}                                                                                           \
	} while(0)

#define ActiveQSSInstrumentAddCounter(i, val)                                                       \
	do {                                                                                            \
		Instrumentation* inst = (Instrumentation*)ActiveQSSInstrumentation;                         \
		if (inst && qss_capture_exec_stats) {                                                       \
			inst->counter##i += val;                                                                \
		}                                                                                           \
	} while(0)
