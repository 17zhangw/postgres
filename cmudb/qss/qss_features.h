#ifndef __QSS_FEATURES_H__
#define __QSS_FEATURES_H__

#include "postgres.h"
#include "fmgr.h"
#include <inttypes.h>

#include "nodes/execnodes.h"

void ExplainEntry(Node *obj, ExplainState *es, EState *estate);

char* NodeToName(Node* obj);


void OutputPlanToExplain(QueryDesc* queryDesc, ExplainState* state);

#endif
