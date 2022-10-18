#include "cmudb/qss/qss.h"

bool qss_capture_enabled = false;
bool qss_capture_exec_stats = false;
bool qss_capture_nested = false;
int qss_output_format = QSS_OUTPUT_FORMAT_NOISEPAGE;
bool qss_capture_abort = false;

qss_AllocInstrumentation_type qss_AllocInstrumentation_hook = NULL;
qss_QSSAbort_type qss_QSSAbort_hook = NULL;
Instrumentation* ActiveQSSInstrumentation = NULL;

Instrumentation* AllocQSSInstrumentation(const char *ou, bool need_timer) {
	if (qss_capture_enabled && qss_capture_exec_stats && (qss_output_format == QSS_OUTPUT_FORMAT_NOISEPAGE) && qss_AllocInstrumentation_hook) {
		return qss_AllocInstrumentation_hook(ou, need_timer);
	}

	return NULL;
}

void QSSAbort() {
	if (qss_QSSAbort_hook) {
		qss_QSSAbort_hook();
	}

	ActiveQSSInstrumentation = NULL;
}
