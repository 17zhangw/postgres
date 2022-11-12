#include <assert.h>

#include "qss.h"
#include "qss_features.h"

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
  CREATE UNLOGGED TABLE pg_catalog.pg_qss_ddl(
  db_id integer,
  statement_timestamp bigint,
  query text,
  command text
  )
  */
#define DDL_TABLE_NAME "pg_qss_ddl"
#define DDL_TABLE_COLUMNS 4

/**
 * Inserts a record into pg_qss_ddl.
 */
static void qss_InsertDDLRecord(const char* queryString, const char* command) {
	// Create the memory context that we need to use.
	Oid ddl_table_oid = RelnameGetRelid(DDL_TABLE_NAME);
	if (ddl_table_oid > 0) {
		MemoryContext tmpCtx = AllocSetContextCreate(qss_MemoryContext,
				"qss_UtilityContext",
				ALLOCSET_DEFAULT_SIZES);
		MemoryContext old = MemoryContextSwitchTo(tmpCtx);

		// Initialize all the heap tuple values.
		Datum values[DDL_TABLE_COLUMNS];
		bool is_nulls[DDL_TABLE_COLUMNS];
		Relation ddl_table_relation = table_open(ddl_table_oid, RowExclusiveLock);
		HeapTuple heap_tup = NULL;
		memset(values, 0, sizeof(values));
		memset(is_nulls, 0, sizeof(is_nulls));

		values[0] = ObjectIdGetDatum(MyDatabaseId);
		values[1] = Int64GetDatumFast(GetCurrentStatementStartTimestamp());

		is_nulls[2] = false;
		values[2] = CStringGetTextDatum(queryString);

		is_nulls[3] = false;
		values[3] = CStringGetTextDatum(command);

		heap_tup = heap_form_tuple(ddl_table_relation->rd_att, values, is_nulls);
		do_heap_insert(ddl_table_relation, heap_tup,
				GetCurrentTransactionId(),
				GetCurrentCommandId(true),
				HEAP_INSERT_FROZEN,
				NULL);
		pfree(heap_tup);

		// Purge the memory contexts.
		table_close(ddl_table_relation, RowExclusiveLock);
		MemoryContextSwitchTo(old);
		MemoryContextDelete(tmpCtx);
	}
}

/**
 * Hook override for process utility for logging.
 */
void qss_ProcessUtility(PlannedStmt *pstmt,
		const char *queryString,
		bool readOnlyTree,
		ProcessUtilityContext context,
		ParamListInfo params,
		QueryEnvironment *queryEnv,
		DestReceiver *dest,
		QueryCompletion *qc) {
	Node *parsetree = pstmt->utilityStmt;
	if (qss_capture_enabled)
	{
		if (nodeTag(parsetree) == T_AlterTableStmt && queryString != NULL) {
			// Try to find out whether this is an ALTER TABLE [table] SET options.
			bool set = false;
			ListCell *cell = NULL;
			AlterTableStmt *astmt = (AlterTableStmt *)parsetree;
			foreach (cell, astmt->cmds) {
				AlterTableCmd *cmd = (AlterTableCmd *)lfirst(cell);
				if (cmd->subtype == AT_SetRelOptions) {
					set = true;
					break;
				}
			}

			if (set) {
				qss_InsertDDLRecord(queryString, "AlterTableOptions");
			}
		}
		else if (nodeTag(parsetree) == T_IndexStmt && queryString != NULL) {
			qss_InsertDDLRecord(queryString, "CreateIndex");
		}
		else if (nodeTag(parsetree) == T_DropStmt && queryString != NULL) {
			DropStmt *dstmt = (DropStmt*)parsetree;
			if (dstmt->removeType == OBJECT_INDEX) {
				qss_InsertDDLRecord(queryString, "DropIndex");
			}
		}
	}

	if (qss_prev_ProcessUtility) {
		(*qss_prev_ProcessUtility)(pstmt,
				queryString,
				readOnlyTree,
				context,
				params,
				queryEnv,
				dest,
				qc);
	} else {
		standard_ProcessUtility(pstmt,
				queryString,
				readOnlyTree,
				context,
				params,
				queryEnv,
				dest,
				qc);
	}
}
