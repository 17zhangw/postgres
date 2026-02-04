#include "postgres.h"
#include "fmgr.h"
#include <inttypes.h>

#include "access/nbtree.h"
#include "access/heapam.h"
#include "access/relation.h"
#include "access/skey.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "commands/createas.h"
#include "commands/explain.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "cmudb/qss/qss.h"
#include "qss.h"
#include "qss_features.h"

#define PLAN_NODE_ID(p) (p ? p->plan_node_id : PLAN_INVALID_ID)

static Oid __attribute__((unused)) GetScanTableOid(Index rti, EState *estate) {
	Relation rel;
	Assert(rti > 0 && rti <= estate->es_range_table_size);
	rel = estate->es_relations[rti - 1];
	if (rel == NULL) {
		RangeTblEntry *rte = exec_rt_fetch(rti, estate);
		return rte->relid;
	}

	return rel->rd_id;
}

static void ExplainInsertUpdateDeleteTable(Index rti, ExplainState *es, EState *estate, CmdType operation) {
    Relation rel;
    List *oids;
    Assert(rti > 0 && rti <= estate->es_range_table_size);

    rel = ExecGetRangeTableRelation(estate, rti);
    if (operation != CMD_DELETE) {
        Assert(rel != NULL);
        oids = RelationGetIndexList(rel);

        ExplainPropertyInteger("ModifyTable_indexupdates_num", NULL, oids ? oids->length : 0, es);
        ExplainPropertyOidList("ModifyTable_indexupdates_oids", oids, es);
    }

    ExplainPropertyInteger("ModifyTable_target_oid", NULL, rel->rd_id, es);

    {
        List* trigger_oids = NIL;
        TriggerDesc *trig = rel->trigdesc;
        int type = (operation == CMD_INSERT) ? TRIGGER_TYPE_INSERT : ((operation == CMD_UPDATE) ? TRIGGER_TYPE_UPDATE : TRIGGER_TYPE_DELETE);
        if (trig && trig->trig_insert_after_row) {
            int i;
            for (i = 0; i < trig->numtriggers; i++) {
                Trigger *trigger = &trig->triggers[i];
                if (TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, type)) {
                    trigger_oids = lappend_oid(trigger_oids, trigger->tgoid);
                }
            }
        }
        ExplainPropertyOidList("ModifyTable_ar_triggers", trigger_oids, es);
    }
}

static void AugmentPlan(struct Plan *plan, PlanState *ps, ExplainState *es, EState *estate) {
    if (plan->type == T_ModifyTable) {
        ModifyTable* mt = (ModifyTable*)plan;
        if (mt->operation == CMD_INSERT) {
            ExplainPropertyText("node_type", "ModifyTableInsert", es);
        } else if (mt->operation == CMD_UPDATE) {
            ExplainPropertyText("node_type", "ModifyTableUpdate", es);
        } else {
            Assert(mt->operation == CMD_DELETE);
            ExplainPropertyText("node_type", "ModifyTableDelete", es);
        }
    } else {
        ExplainPropertyText("node_type", NodeToName((struct Node*)plan), es);
    }
    ExplainPropertyInteger("plan_node_id", NULL, plan->plan_node_id, es);
    ExplainPropertyInteger("left_child_node_id", NULL, PLAN_NODE_ID(plan->lefttree), es);
    ExplainPropertyInteger("right_child_node_id", NULL, PLAN_NODE_ID(plan->righttree), es);

    if (plan->type == T_ModifyTable) {
        Index rti;
        ModifyTable *modifyTable = (ModifyTable*)plan;
        // If the current plan is a ModifyTable, then also note the number of input rows.
        Assert(outerPlan(plan) != NULL);
        Assert(modifyTable->resultRelations != NULL);
        ExplainPropertyFloat("ModifyTable_input_plan_rows", NULL, outerPlan(plan)->plan_rows, 9, es);
        ExplainPropertyInteger("ModifyTable_input_plan_width", NULL, outerPlan(plan)->plan_width, es);

        // For INSERT/UPDATE, this captures the number of indexes that need to be
        // inserted into in the worst case. For HOT Update, we might not update any
        // indexes at all.
        rti = linitial_int(modifyTable->resultRelations);
        ExplainInsertUpdateDeleteTable(rti, es, estate, modifyTable->operation);

        if (modifyTable->operation == CMD_UPDATE || modifyTable->operation == CMD_DELETE) {
            // For UPDATE/DELETE, this captures the number of repeated scans that we
            // might have had to perform. This is also an upper bound. In reality, if
            // there's no concurrent transaction, this can be much lower.
            double rows = IsolationUsesXactSnapshot() ? 0 : outerPlan(plan)->plan_rows;
            ExplainPropertyFloat("ModifyTable_recheck_rows", NULL, rows, 9, es);
        }
    }

    if (plan->type == T_Agg) {
        ExplainPropertyFloat("Agg_input_plan_rows", NULL, outerPlan(plan)->plan_rows, 9, es);
        ExplainPropertyInteger("Agg_input_plan_width", NULL, outerPlan(plan)->plan_width, es);
    }

    if (plan->type == T_LockRows) {
        // In this case, we'll actually have to execute the entire subplan
        // multiple times. As such, we note that repeated times == # output rows.
        LockRows *lockRows = (LockRows*)plan;
        double rows = IsolationUsesXactSnapshot() ? 0 : outerPlan(lockRows)->plan_rows;
        ExplainPropertyFloat("LockRows_recheck_rows", NULL, rows, 9, es);
    }
}

static void WritePlannedStmtExplain(Node *obj, ExplainState *es, EState *estate){
	PlannedStmt* node = NULL;
	Assert(obj->type == T_PlannedStmt);
	node = (PlannedStmt*)obj;
	(void)node;
	ExplainPropertyInteger("PlannedStmt_type", NULL, (*node).type, es);
	ExplainPropertyInteger("PlannedStmt_commandType", NULL, (*node).commandType, es);
	ExplainPropertyUInteger("PlannedStmt_queryId", NULL, (*node).queryId, es);
	ExplainPropertyBool("PlannedStmt_hasReturning", (*node).hasReturning, es);
	ExplainPropertyBool("PlannedStmt_hasModifyingCTE", (*node).hasModifyingCTE, es);
	ExplainPropertyBool("PlannedStmt_canSetTag", (*node).canSetTag, es);
	ExplainPropertyBool("PlannedStmt_transientPlan", (*node).transientPlan, es);
	ExplainPropertyBool("PlannedStmt_dependsOnRole", (*node).dependsOnRole, es);
	ExplainPropertyBool("PlannedStmt_parallelModeNeeded", (*node).parallelModeNeeded, es);
	ExplainPropertyInteger("PlannedStmt_jitFlags", NULL, (*node).jitFlags, es);
	ExplainPropertyInteger("PlannedStmt_rtable_length", NULL, (*node).rtable ? (*node).rtable->length : 0, es);
	ExplainPropertyInteger("PlannedStmt_permInfos_length", NULL, (*node).permInfos ? (*node).permInfos->length : 0, es);
	ExplainPropertyInteger("PlannedStmt_resultRelations_length", NULL, (*node).resultRelations ? (*node).resultRelations->length : 0, es);
	ExplainPropertyInteger("PlannedStmt_appendRelations_length", NULL, (*node).appendRelations ? (*node).appendRelations->length : 0, es);
	ExplainPropertyInteger("PlannedStmt_subplans_length", NULL, (*node).subplans ? (*node).subplans->length : 0, es);
	ExplainPropertyInteger("PlannedStmt_rowMarks_length", NULL, (*node).rowMarks ? (*node).rowMarks->length : 0, es);
	ExplainPropertyInteger("PlannedStmt_relationOids_length", NULL, (*node).relationOids ? (*node).relationOids->length : 0, es);
	ExplainPropertyInteger("PlannedStmt_invalItems_length", NULL, (*node).invalItems ? (*node).invalItems->length : 0, es);
	ExplainPropertyInteger("PlannedStmt_paramExecTypes_length", NULL, (*node).paramExecTypes ? (*node).paramExecTypes->length : 0, es);
	ExplainPropertyInteger("PlannedStmt_stmt_location", NULL, (*node).stmt_location, es);
	ExplainPropertyInteger("PlannedStmt_stmt_len", NULL, (*node).stmt_len, es);
}


static void WriteResultExplain(Node *obj, ExplainState *es, EState *estate){
	Result* node = NULL;
	Assert(obj->type == T_Result);
	node = (Result*)obj;
	(void)node;
}


static void WriteProjectSetExplain(Node *obj, ExplainState *es, EState *estate){
	ProjectSet* node = NULL;
	Assert(obj->type == T_ProjectSet);
	node = (ProjectSet*)obj;
	(void)node;
}


static void WriteModifyTableExplain(Node *obj, ExplainState *es, EState *estate){
	ModifyTable* node = NULL;
	Assert(obj->type == T_ModifyTable);
	node = (ModifyTable*)obj;
	(void)node;
	ExplainPropertyInteger("ModifyTable_operation", NULL, (*node).operation, es);
	ExplainPropertyBool("ModifyTable_canSetTag", (*node).canSetTag, es);
	ExplainPropertyUInteger("ModifyTable_nominalRelation", NULL, (*node).nominalRelation, es);
	ExplainPropertyUInteger("ModifyTable_rootRelation", NULL, (*node).rootRelation, es);
	ExplainPropertyBool("ModifyTable_partColsUpdated", (*node).partColsUpdated, es);
	ExplainPropertyInteger("ModifyTable_resultRelations_length", NULL, (*node).resultRelations ? (*node).resultRelations->length : 0, es);
	ExplainPropertyInteger("ModifyTable_updateColnosLists_length", NULL, (*node).updateColnosLists ? (*node).updateColnosLists->length : 0, es);
	ExplainPropertyInteger("ModifyTable_withCheckOptionLists_length", NULL, (*node).withCheckOptionLists ? (*node).withCheckOptionLists->length : 0, es);
	ExplainPropertyInteger("ModifyTable_returningLists_length", NULL, (*node).returningLists ? (*node).returningLists->length : 0, es);
	ExplainPropertyInteger("ModifyTable_fdwPrivLists_length", NULL, (*node).fdwPrivLists ? (*node).fdwPrivLists->length : 0, es);
	ExplainPropertyInteger("ModifyTable_rowMarks_length", NULL, (*node).rowMarks ? (*node).rowMarks->length : 0, es);
	ExplainPropertyInteger("ModifyTable_epqParam", NULL, (*node).epqParam, es);
	ExplainPropertyInteger("ModifyTable_onConflictAction", NULL, (*node).onConflictAction, es);
	ExplainPropertyInteger("ModifyTable_arbiterIndexes_length", NULL, (*node).arbiterIndexes ? (*node).arbiterIndexes->length : 0, es);
	ExplainPropertyInteger("ModifyTable_onConflictSet_length", NULL, (*node).onConflictSet ? (*node).onConflictSet->length : 0, es);
	ExplainPropertyInteger("ModifyTable_onConflictCols_length", NULL, (*node).onConflictCols ? (*node).onConflictCols->length : 0, es);
	ExplainPropertyUInteger("ModifyTable_exclRelRTI", NULL, (*node).exclRelRTI, es);
	ExplainPropertyInteger("ModifyTable_exclRelTlist_length", NULL, (*node).exclRelTlist ? (*node).exclRelTlist->length : 0, es);
	ExplainPropertyInteger("ModifyTable_mergeActionLists_length", NULL, (*node).mergeActionLists ? (*node).mergeActionLists->length : 0, es);
	ExplainPropertyInteger("ModifyTable_mergeJoinConditions_length", NULL, (*node).mergeJoinConditions ? (*node).mergeJoinConditions->length : 0, es);
}


static void WriteAppendExplain(Node *obj, ExplainState *es, EState *estate){
	Append* node = NULL;
	Assert(obj->type == T_Append);
	node = (Append*)obj;
	(void)node;
	ExplainPropertyInteger("Append_appendplans_length", NULL, (*node).appendplans ? (*node).appendplans->length : 0, es);
	ExplainPropertyInteger("Append_nasyncplans", NULL, (*node).nasyncplans, es);
	ExplainPropertyInteger("Append_first_partial_plan", NULL, (*node).first_partial_plan, es);
}


static void WriteMergeAppendExplain(Node *obj, ExplainState *es, EState *estate){
	MergeAppend* node = NULL;
	Assert(obj->type == T_MergeAppend);
	node = (MergeAppend*)obj;
	(void)node;
	ExplainPropertyInteger("MergeAppend_mergeplans_length", NULL, (*node).mergeplans ? (*node).mergeplans->length : 0, es);
	ExplainPropertyInteger("MergeAppend_numCols", NULL, (*node).numCols, es);
}


static void WriteRecursiveUnionExplain(Node *obj, ExplainState *es, EState *estate){
	RecursiveUnion* node = NULL;
	Assert(obj->type == T_RecursiveUnion);
	node = (RecursiveUnion*)obj;
	(void)node;
	ExplainPropertyInteger("RecursiveUnion_wtParam", NULL, (*node).wtParam, es);
	ExplainPropertyInteger("RecursiveUnion_numCols", NULL, (*node).numCols, es);
	ExplainPropertyInteger("RecursiveUnion_numGroups", NULL, (*node).numGroups, es);
}


static void WriteBitmapAndExplain(Node *obj, ExplainState *es, EState *estate){
	BitmapAnd* node = NULL;
	Assert(obj->type == T_BitmapAnd);
	node = (BitmapAnd*)obj;
	(void)node;
	ExplainPropertyInteger("BitmapAnd_bitmapplans_length", NULL, (*node).bitmapplans ? (*node).bitmapplans->length : 0, es);
}


static void WriteBitmapOrExplain(Node *obj, ExplainState *es, EState *estate){
	BitmapOr* node = NULL;
	Assert(obj->type == T_BitmapOr);
	node = (BitmapOr*)obj;
	(void)node;
	ExplainPropertyBool("BitmapOr_isshared", (*node).isshared, es);
	ExplainPropertyInteger("BitmapOr_bitmapplans_length", NULL, (*node).bitmapplans ? (*node).bitmapplans->length : 0, es);
}


static void WriteSeqScanExplain(Node *obj, ExplainState *es, EState *estate){
	SeqScan* node = NULL;
	Assert(obj->type == T_SeqScan);
	node = (SeqScan*)obj;
	(void)node;
}


static void WriteSampleScanExplain(Node *obj, ExplainState *es, EState *estate){
	SampleScan* node = NULL;
	Assert(obj->type == T_SampleScan);
	node = (SampleScan*)obj;
	(void)node;
}


static void WriteIndexScanExplain(Node *obj, ExplainState *es, EState *estate){
	IndexScan* node = NULL;
	Assert(obj->type == T_IndexScan);
	node = (IndexScan*)obj;
	(void)node;
	ExplainPropertyUInteger("IndexScan_indexid", NULL, (*node).indexid, es);
	ExplainPropertyInteger("IndexScan_indexqual_length", NULL, (*node).indexqual ? (*node).indexqual->length : 0, es);
	ExplainPropertyInteger("IndexScan_indexqualorig_length", NULL, (*node).indexqualorig ? (*node).indexqualorig->length : 0, es);
	ExplainPropertyInteger("IndexScan_indexorderby_length", NULL, (*node).indexorderby ? (*node).indexorderby->length : 0, es);
	ExplainPropertyInteger("IndexScan_indexorderbyorig_length", NULL, (*node).indexorderbyorig ? (*node).indexorderbyorig->length : 0, es);
	ExplainPropertyInteger("IndexScan_indexorderbyops_length", NULL, (*node).indexorderbyops ? (*node).indexorderbyops->length : 0, es);
	ExplainPropertyInteger("IndexScan_indexorderdir", NULL, (*node).indexorderdir, es);
}


static void WriteIndexOnlyScanExplain(Node *obj, ExplainState *es, EState *estate){
	IndexOnlyScan* node = NULL;
	Assert(obj->type == T_IndexOnlyScan);
	node = (IndexOnlyScan*)obj;
	(void)node;
	ExplainPropertyUInteger("IndexOnlyScan_indexid", NULL, (*node).indexid, es);
	ExplainPropertyInteger("IndexOnlyScan_indexqual_length", NULL, (*node).indexqual ? (*node).indexqual->length : 0, es);
	ExplainPropertyInteger("IndexOnlyScan_recheckqual_length", NULL, (*node).recheckqual ? (*node).recheckqual->length : 0, es);
	ExplainPropertyInteger("IndexOnlyScan_indexorderby_length", NULL, (*node).indexorderby ? (*node).indexorderby->length : 0, es);
	ExplainPropertyInteger("IndexOnlyScan_indextlist_length", NULL, (*node).indextlist ? (*node).indextlist->length : 0, es);
	ExplainPropertyInteger("IndexOnlyScan_indexorderdir", NULL, (*node).indexorderdir, es);
}


static void WriteBitmapIndexScanExplain(Node *obj, ExplainState *es, EState *estate){
	BitmapIndexScan* node = NULL;
	Assert(obj->type == T_BitmapIndexScan);
	node = (BitmapIndexScan*)obj;
	(void)node;
	ExplainPropertyUInteger("BitmapIndexScan_indexid", NULL, (*node).indexid, es);
	ExplainPropertyBool("BitmapIndexScan_isshared", (*node).isshared, es);
	ExplainPropertyInteger("BitmapIndexScan_indexqual_length", NULL, (*node).indexqual ? (*node).indexqual->length : 0, es);
	ExplainPropertyInteger("BitmapIndexScan_indexqualorig_length", NULL, (*node).indexqualorig ? (*node).indexqualorig->length : 0, es);
}


static void WriteBitmapHeapScanExplain(Node *obj, ExplainState *es, EState *estate){
	BitmapHeapScan* node = NULL;
	Assert(obj->type == T_BitmapHeapScan);
	node = (BitmapHeapScan*)obj;
	(void)node;
	ExplainPropertyInteger("BitmapHeapScan_bitmapqualorig_length", NULL, (*node).bitmapqualorig ? (*node).bitmapqualorig->length : 0, es);
}


static void WriteTidScanExplain(Node *obj, ExplainState *es, EState *estate){
	TidScan* node = NULL;
	Assert(obj->type == T_TidScan);
	node = (TidScan*)obj;
	(void)node;
	ExplainPropertyInteger("TidScan_tidquals_length", NULL, (*node).tidquals ? (*node).tidquals->length : 0, es);
}


static void WriteTidRangeScanExplain(Node *obj, ExplainState *es, EState *estate){
	TidRangeScan* node = NULL;
	Assert(obj->type == T_TidRangeScan);
	node = (TidRangeScan*)obj;
	(void)node;
	ExplainPropertyInteger("TidRangeScan_tidrangequals_length", NULL, (*node).tidrangequals ? (*node).tidrangequals->length : 0, es);
}


static void WriteSubqueryScanExplain(Node *obj, ExplainState *es, EState *estate){
	SubqueryScan* node = NULL;
	Assert(obj->type == T_SubqueryScan);
	node = (SubqueryScan*)obj;
	(void)node;
	ExplainPropertyInteger("SubqueryScan_scanstatus", NULL, (*node).scanstatus, es);
}


static void WriteFunctionScanExplain(Node *obj, ExplainState *es, EState *estate){
	FunctionScan* node = NULL;
	Assert(obj->type == T_FunctionScan);
	node = (FunctionScan*)obj;
	(void)node;
	ExplainPropertyInteger("FunctionScan_functions_length", NULL, (*node).functions ? (*node).functions->length : 0, es);
	ExplainPropertyBool("FunctionScan_funcordinality", (*node).funcordinality, es);
}


static void WriteValuesScanExplain(Node *obj, ExplainState *es, EState *estate){
	ValuesScan* node = NULL;
	Assert(obj->type == T_ValuesScan);
	node = (ValuesScan*)obj;
	(void)node;
	ExplainPropertyInteger("ValuesScan_values_lists_length", NULL, (*node).values_lists ? (*node).values_lists->length : 0, es);
}


static void WriteTableFuncScanExplain(Node *obj, ExplainState *es, EState *estate){
	TableFuncScan* node = NULL;
	Assert(obj->type == T_TableFuncScan);
	node = (TableFuncScan*)obj;
	(void)node;
}


static void WriteCteScanExplain(Node *obj, ExplainState *es, EState *estate){
	CteScan* node = NULL;
	Assert(obj->type == T_CteScan);
	node = (CteScan*)obj;
	(void)node;
	ExplainPropertyInteger("CteScan_ctePlanId", NULL, (*node).ctePlanId, es);
	ExplainPropertyInteger("CteScan_cteParam", NULL, (*node).cteParam, es);
}


static void WriteNamedTuplestoreScanExplain(Node *obj, ExplainState *es, EState *estate){
	NamedTuplestoreScan* node = NULL;
	Assert(obj->type == T_NamedTuplestoreScan);
	node = (NamedTuplestoreScan*)obj;
	(void)node;
}


static void WriteWorkTableScanExplain(Node *obj, ExplainState *es, EState *estate){
	WorkTableScan* node = NULL;
	Assert(obj->type == T_WorkTableScan);
	node = (WorkTableScan*)obj;
	(void)node;
	ExplainPropertyInteger("WorkTableScan_wtParam", NULL, (*node).wtParam, es);
}


static void WriteForeignScanExplain(Node *obj, ExplainState *es, EState *estate){
	ForeignScan* node = NULL;
	Assert(obj->type == T_ForeignScan);
	node = (ForeignScan*)obj;
	(void)node;
	ExplainPropertyInteger("ForeignScan_operation", NULL, (*node).operation, es);
	ExplainPropertyUInteger("ForeignScan_resultRelation", NULL, (*node).resultRelation, es);
	ExplainPropertyUInteger("ForeignScan_checkAsUser", NULL, (*node).checkAsUser, es);
	ExplainPropertyUInteger("ForeignScan_fs_server", NULL, (*node).fs_server, es);
	ExplainPropertyInteger("ForeignScan_fdw_exprs_length", NULL, (*node).fdw_exprs ? (*node).fdw_exprs->length : 0, es);
	ExplainPropertyInteger("ForeignScan_fdw_private_length", NULL, (*node).fdw_private ? (*node).fdw_private->length : 0, es);
	ExplainPropertyInteger("ForeignScan_fdw_scan_tlist_length", NULL, (*node).fdw_scan_tlist ? (*node).fdw_scan_tlist->length : 0, es);
	ExplainPropertyInteger("ForeignScan_fdw_recheck_quals_length", NULL, (*node).fdw_recheck_quals ? (*node).fdw_recheck_quals->length : 0, es);
	ExplainPropertyBool("ForeignScan_fsSystemCol", (*node).fsSystemCol, es);
}


static void WriteCustomScanExplain(Node *obj, ExplainState *es, EState *estate){
	CustomScan* node = NULL;
	Assert(obj->type == T_CustomScan);
	node = (CustomScan*)obj;
	(void)node;
	ExplainPropertyUInteger("CustomScan_flags", NULL, (*node).flags, es);
	ExplainPropertyInteger("CustomScan_custom_plans_length", NULL, (*node).custom_plans ? (*node).custom_plans->length : 0, es);
	ExplainPropertyInteger("CustomScan_custom_exprs_length", NULL, (*node).custom_exprs ? (*node).custom_exprs->length : 0, es);
	ExplainPropertyInteger("CustomScan_custom_private_length", NULL, (*node).custom_private ? (*node).custom_private->length : 0, es);
	ExplainPropertyInteger("CustomScan_custom_scan_tlist_length", NULL, (*node).custom_scan_tlist ? (*node).custom_scan_tlist->length : 0, es);
}


static void WriteNestLoopExplain(Node *obj, ExplainState *es, EState *estate){
	NestLoop* node = NULL;
	Assert(obj->type == T_NestLoop);
	node = (NestLoop*)obj;
	(void)node;
	ExplainPropertyInteger("NestLoop_nestParams_length", NULL, (*node).nestParams ? (*node).nestParams->length : 0, es);
}


static void WriteNestLoopParamExplain(Node *obj, ExplainState *es, EState *estate){
	NestLoopParam* node = NULL;
	Assert(obj->type == T_NestLoopParam);
	node = (NestLoopParam*)obj;
	(void)node;
	ExplainPropertyInteger("NestLoopParam_type", NULL, (*node).type, es);
	ExplainPropertyInteger("NestLoopParam_paramno", NULL, (*node).paramno, es);
}


static void WriteMergeJoinExplain(Node *obj, ExplainState *es, EState *estate){
	MergeJoin* node = NULL;
	Assert(obj->type == T_MergeJoin);
	node = (MergeJoin*)obj;
	(void)node;
	ExplainPropertyBool("MergeJoin_skip_mark_restore", (*node).skip_mark_restore, es);
	ExplainPropertyInteger("MergeJoin_mergeclauses_length", NULL, (*node).mergeclauses ? (*node).mergeclauses->length : 0, es);
}


static void WriteHashJoinExplain(Node *obj, ExplainState *es, EState *estate){
	HashJoin* node = NULL;
	Assert(obj->type == T_HashJoin);
	node = (HashJoin*)obj;
	(void)node;
	ExplainPropertyInteger("HashJoin_hashclauses_length", NULL, (*node).hashclauses ? (*node).hashclauses->length : 0, es);
	ExplainPropertyInteger("HashJoin_hashoperators_length", NULL, (*node).hashoperators ? (*node).hashoperators->length : 0, es);
	ExplainPropertyInteger("HashJoin_hashcollations_length", NULL, (*node).hashcollations ? (*node).hashcollations->length : 0, es);
	ExplainPropertyInteger("HashJoin_hashkeys_length", NULL, (*node).hashkeys ? (*node).hashkeys->length : 0, es);
}


static void WriteMaterialExplain(Node *obj, ExplainState *es, EState *estate){
	Material* node = NULL;
	Assert(obj->type == T_Material);
	node = (Material*)obj;
	(void)node;
}


static void WriteMemoizeExplain(Node *obj, ExplainState *es, EState *estate){
	Memoize* node = NULL;
	Assert(obj->type == T_Memoize);
	node = (Memoize*)obj;
	(void)node;
	ExplainPropertyInteger("Memoize_numKeys", NULL, (*node).numKeys, es);
	ExplainPropertyInteger("Memoize_param_exprs_length", NULL, (*node).param_exprs ? (*node).param_exprs->length : 0, es);
	ExplainPropertyBool("Memoize_singlerow", (*node).singlerow, es);
	ExplainPropertyBool("Memoize_binary_mode", (*node).binary_mode, es);
	ExplainPropertyUInteger("Memoize_est_entries", NULL, (*node).est_entries, es);
}


static void WriteSortExplain(Node *obj, ExplainState *es, EState *estate){
	Sort* node = NULL;
	Assert(obj->type == T_Sort);
	node = (Sort*)obj;
	(void)node;
	ExplainPropertyInteger("Sort_numCols", NULL, (*node).numCols, es);
}


static void WriteIncrementalSortExplain(Node *obj, ExplainState *es, EState *estate){
	IncrementalSort* node = NULL;
	Assert(obj->type == T_IncrementalSort);
	node = (IncrementalSort*)obj;
	(void)node;
	ExplainPropertyInteger("IncrementalSort_sort_numCols", NULL, (*node).sort.numCols, es);
	ExplainPropertyInteger("IncrementalSort_nPresortedCols", NULL, (*node).nPresortedCols, es);
}


static void WriteGroupExplain(Node *obj, ExplainState *es, EState *estate){
	Group* node = NULL;
	Assert(obj->type == T_Group);
	node = (Group*)obj;
	(void)node;
	ExplainPropertyInteger("Group_numCols", NULL, (*node).numCols, es);
}


static void WriteAggExplain(Node *obj, ExplainState *es, EState *estate){
	Agg* node = NULL;
	Assert(obj->type == T_Agg);
	node = (Agg*)obj;
	(void)node;
	ExplainPropertyInteger("Agg_aggstrategy", NULL, (*node).aggstrategy, es);
	ExplainPropertyInteger("Agg_aggsplit", NULL, (*node).aggsplit, es);
	ExplainPropertyInteger("Agg_numCols", NULL, (*node).numCols, es);
	ExplainPropertyInteger("Agg_numGroups", NULL, (*node).numGroups, es);
	ExplainPropertyUInteger("Agg_transitionSpace", NULL, (*node).transitionSpace, es);
	ExplainPropertyInteger("Agg_groupingSets_length", NULL, (*node).groupingSets ? (*node).groupingSets->length : 0, es);
	ExplainPropertyInteger("Agg_chain_length", NULL, (*node).chain ? (*node).chain->length : 0, es);
}


static void WriteWindowAggExplain(Node *obj, ExplainState *es, EState *estate){
	WindowAgg* node = NULL;
	Assert(obj->type == T_WindowAgg);
	node = (WindowAgg*)obj;
	(void)node;
	ExplainPropertyUInteger("WindowAgg_winref", NULL, (*node).winref, es);
	ExplainPropertyInteger("WindowAgg_partNumCols", NULL, (*node).partNumCols, es);
	ExplainPropertyInteger("WindowAgg_ordNumCols", NULL, (*node).ordNumCols, es);
	ExplainPropertyInteger("WindowAgg_frameOptions", NULL, (*node).frameOptions, es);
	ExplainPropertyInteger("WindowAgg_runCondition_length", NULL, (*node).runCondition ? (*node).runCondition->length : 0, es);
	ExplainPropertyInteger("WindowAgg_runConditionOrig_length", NULL, (*node).runConditionOrig ? (*node).runConditionOrig->length : 0, es);
	ExplainPropertyUInteger("WindowAgg_startInRangeFunc", NULL, (*node).startInRangeFunc, es);
	ExplainPropertyUInteger("WindowAgg_endInRangeFunc", NULL, (*node).endInRangeFunc, es);
	ExplainPropertyUInteger("WindowAgg_inRangeColl", NULL, (*node).inRangeColl, es);
	ExplainPropertyBool("WindowAgg_inRangeAsc", (*node).inRangeAsc, es);
	ExplainPropertyBool("WindowAgg_inRangeNullsFirst", (*node).inRangeNullsFirst, es);
	ExplainPropertyBool("WindowAgg_topWindow", (*node).topWindow, es);
}


static void WriteUniqueExplain(Node *obj, ExplainState *es, EState *estate){
	Unique* node = NULL;
	Assert(obj->type == T_Unique);
	node = (Unique*)obj;
	(void)node;
	ExplainPropertyInteger("Unique_numCols", NULL, (*node).numCols, es);
}


static void WriteGatherExplain(Node *obj, ExplainState *es, EState *estate){
	Gather* node = NULL;
	Assert(obj->type == T_Gather);
	node = (Gather*)obj;
	(void)node;
	ExplainPropertyInteger("Gather_num_workers", NULL, (*node).num_workers, es);
	ExplainPropertyInteger("Gather_rescan_param", NULL, (*node).rescan_param, es);
	ExplainPropertyBool("Gather_single_copy", (*node).single_copy, es);
	ExplainPropertyBool("Gather_invisible", (*node).invisible, es);
}


static void WriteGatherMergeExplain(Node *obj, ExplainState *es, EState *estate){
	GatherMerge* node = NULL;
	Assert(obj->type == T_GatherMerge);
	node = (GatherMerge*)obj;
	(void)node;
	ExplainPropertyInteger("GatherMerge_num_workers", NULL, (*node).num_workers, es);
	ExplainPropertyInteger("GatherMerge_rescan_param", NULL, (*node).rescan_param, es);
	ExplainPropertyInteger("GatherMerge_numCols", NULL, (*node).numCols, es);
}


static void WriteHashExplain(Node *obj, ExplainState *es, EState *estate){
	Hash* node = NULL;
	Assert(obj->type == T_Hash);
	node = (Hash*)obj;
	(void)node;
	ExplainPropertyInteger("Hash_hashkeys_length", NULL, (*node).hashkeys ? (*node).hashkeys->length : 0, es);
	ExplainPropertyUInteger("Hash_skewTable", NULL, (*node).skewTable, es);
	ExplainPropertyInteger("Hash_skewColumn", NULL, (*node).skewColumn, es);
	ExplainPropertyBool("Hash_skewInherit", (*node).skewInherit, es);
	ExplainPropertyFloat("Hash_rows_total", NULL, (*node).rows_total, 9, es);
}


static void WriteSetOpExplain(Node *obj, ExplainState *es, EState *estate){
	SetOp* node = NULL;
	Assert(obj->type == T_SetOp);
	node = (SetOp*)obj;
	(void)node;
	ExplainPropertyInteger("SetOp_cmd", NULL, (*node).cmd, es);
	ExplainPropertyInteger("SetOp_strategy", NULL, (*node).strategy, es);
	ExplainPropertyInteger("SetOp_numCols", NULL, (*node).numCols, es);
	ExplainPropertyInteger("SetOp_flagColIdx", NULL, (*node).flagColIdx, es);
	ExplainPropertyInteger("SetOp_firstFlag", NULL, (*node).firstFlag, es);
	ExplainPropertyInteger("SetOp_numGroups", NULL, (*node).numGroups, es);
}


static void WriteLockRowsExplain(Node *obj, ExplainState *es, EState *estate){
	LockRows* node = NULL;
	Assert(obj->type == T_LockRows);
	node = (LockRows*)obj;
	(void)node;
	ExplainPropertyInteger("LockRows_rowMarks_length", NULL, (*node).rowMarks ? (*node).rowMarks->length : 0, es);
	ExplainPropertyInteger("LockRows_epqParam", NULL, (*node).epqParam, es);
}


static void WriteLimitExplain(Node *obj, ExplainState *es, EState *estate){
	Limit* node = NULL;
	Assert(obj->type == T_Limit);
	node = (Limit*)obj;
	(void)node;
	ExplainPropertyInteger("Limit_limitOption", NULL, (*node).limitOption, es);
	ExplainPropertyInteger("Limit_uniqNumCols", NULL, (*node).uniqNumCols, es);
}


static void WritePlanRowMarkExplain(Node *obj, ExplainState *es, EState *estate){
	PlanRowMark* node = NULL;
	Assert(obj->type == T_PlanRowMark);
	node = (PlanRowMark*)obj;
	(void)node;
	ExplainPropertyInteger("PlanRowMark_type", NULL, (*node).type, es);
	ExplainPropertyUInteger("PlanRowMark_rti", NULL, (*node).rti, es);
	ExplainPropertyUInteger("PlanRowMark_prti", NULL, (*node).prti, es);
	ExplainPropertyUInteger("PlanRowMark_rowmarkId", NULL, (*node).rowmarkId, es);
	ExplainPropertyInteger("PlanRowMark_markType", NULL, (*node).markType, es);
	ExplainPropertyInteger("PlanRowMark_allMarkTypes", NULL, (*node).allMarkTypes, es);
	ExplainPropertyInteger("PlanRowMark_strength", NULL, (*node).strength, es);
	ExplainPropertyInteger("PlanRowMark_waitPolicy", NULL, (*node).waitPolicy, es);
	ExplainPropertyBool("PlanRowMark_isParent", (*node).isParent, es);
}


static void WritePartitionPruneInfoExplain(Node *obj, ExplainState *es, EState *estate){
	PartitionPruneInfo* node = NULL;
	Assert(obj->type == T_PartitionPruneInfo);
	node = (PartitionPruneInfo*)obj;
	(void)node;
	ExplainPropertyInteger("PartitionPruneInfo_type", NULL, (*node).type, es);
	ExplainPropertyInteger("PartitionPruneInfo_prune_infos_length", NULL, (*node).prune_infos ? (*node).prune_infos->length : 0, es);
}


static void WritePartitionedRelPruneInfoExplain(Node *obj, ExplainState *es, EState *estate){
	PartitionedRelPruneInfo* node = NULL;
	Assert(obj->type == T_PartitionedRelPruneInfo);
	node = (PartitionedRelPruneInfo*)obj;
	(void)node;
	ExplainPropertyInteger("PartitionedRelPruneInfo_type", NULL, (*node).type, es);
	ExplainPropertyUInteger("PartitionedRelPruneInfo_rtindex", NULL, (*node).rtindex, es);
	ExplainPropertyInteger("PartitionedRelPruneInfo_nparts", NULL, (*node).nparts, es);
	ExplainPropertyInteger("PartitionedRelPruneInfo_initial_pruning_steps_length", NULL, (*node).initial_pruning_steps ? (*node).initial_pruning_steps->length : 0, es);
	ExplainPropertyInteger("PartitionedRelPruneInfo_exec_pruning_steps_length", NULL, (*node).exec_pruning_steps ? (*node).exec_pruning_steps->length : 0, es);
}


static void WritePartitionPruneStepOpExplain(Node *obj, ExplainState *es, EState *estate){
	PartitionPruneStepOp* node = NULL;
	Assert(obj->type == T_PartitionPruneStepOp);
	node = (PartitionPruneStepOp*)obj;
	(void)node;
	ExplainPropertyUInteger("PartitionPruneStepOp_opstrategy", NULL, (*node).opstrategy, es);
	ExplainPropertyInteger("PartitionPruneStepOp_exprs_length", NULL, (*node).exprs ? (*node).exprs->length : 0, es);
	ExplainPropertyInteger("PartitionPruneStepOp_cmpfns_length", NULL, (*node).cmpfns ? (*node).cmpfns->length : 0, es);
}


static void WritePartitionPruneStepCombineExplain(Node *obj, ExplainState *es, EState *estate){
	PartitionPruneStepCombine* node = NULL;
	Assert(obj->type == T_PartitionPruneStepCombine);
	node = (PartitionPruneStepCombine*)obj;
	(void)node;
	ExplainPropertyInteger("PartitionPruneStepCombine_combineOp", NULL, (*node).combineOp, es);
	ExplainPropertyInteger("PartitionPruneStepCombine_source_stepids_length", NULL, (*node).source_stepids ? (*node).source_stepids->length : 0, es);
}


static void WritePlanInvalItemExplain(Node *obj, ExplainState *es, EState *estate){
	PlanInvalItem* node = NULL;
	Assert(obj->type == T_PlanInvalItem);
	node = (PlanInvalItem*)obj;
	(void)node;
	ExplainPropertyInteger("PlanInvalItem_type", NULL, (*node).type, es);
	ExplainPropertyInteger("PlanInvalItem_cacheId", NULL, (*node).cacheId, es);
	ExplainPropertyUInteger("PlanInvalItem_hashValue", NULL, (*node).hashValue, es);
}


static void WriteExprStateExplain(Node *obj, ExplainState *es, EState *estate){
	ExprState* node = NULL;
	Assert(obj->type == T_ExprState);
	node = (ExprState*)obj;
	(void)node;
	ExplainPropertyInteger("ExprState_type", NULL, (*node).type, es);
	ExplainPropertyUInteger("ExprState_flags", NULL, (*node).flags, es);
	ExplainPropertyBool("ExprState_resnull", (*node).resnull, es);
	ExplainPropertyUInteger("ExprState_resvalue", NULL, (*node).resvalue, es);
	ExplainPropertyInteger("ExprState_steps_len", NULL, (*node).steps_len, es);
	ExplainPropertyInteger("ExprState_steps_alloc", NULL, (*node).steps_alloc, es);
}


static void WriteIndexInfoExplain(Node *obj, ExplainState *es, EState *estate){
	IndexInfo* node = NULL;
	Assert(obj->type == T_IndexInfo);
	node = (IndexInfo*)obj;
	(void)node;
	ExplainPropertyInteger("IndexInfo_type", NULL, (*node).type, es);
	ExplainPropertyInteger("IndexInfo_ii_NumIndexAttrs", NULL, (*node).ii_NumIndexAttrs, es);
	ExplainPropertyInteger("IndexInfo_ii_NumIndexKeyAttrs", NULL, (*node).ii_NumIndexKeyAttrs, es);
	ExplainPropertyInteger("IndexInfo_ii_Expressions_length", NULL, (*node).ii_Expressions ? (*node).ii_Expressions->length : 0, es);
	ExplainPropertyInteger("IndexInfo_ii_ExpressionsState_length", NULL, (*node).ii_ExpressionsState ? (*node).ii_ExpressionsState->length : 0, es);
	ExplainPropertyInteger("IndexInfo_ii_Predicate_length", NULL, (*node).ii_Predicate ? (*node).ii_Predicate->length : 0, es);
	ExplainPropertyBool("IndexInfo_ii_Unique", (*node).ii_Unique, es);
	ExplainPropertyBool("IndexInfo_ii_NullsNotDistinct", (*node).ii_NullsNotDistinct, es);
	ExplainPropertyBool("IndexInfo_ii_ReadyForInserts", (*node).ii_ReadyForInserts, es);
	ExplainPropertyBool("IndexInfo_ii_CheckedUnchanged", (*node).ii_CheckedUnchanged, es);
	ExplainPropertyBool("IndexInfo_ii_IndexUnchanged", (*node).ii_IndexUnchanged, es);
	ExplainPropertyBool("IndexInfo_ii_Concurrent", (*node).ii_Concurrent, es);
	ExplainPropertyBool("IndexInfo_ii_BrokenHotChain", (*node).ii_BrokenHotChain, es);
	ExplainPropertyBool("IndexInfo_ii_Summarizing", (*node).ii_Summarizing, es);
	ExplainPropertyInteger("IndexInfo_ii_ParallelWorkers", NULL, (*node).ii_ParallelWorkers, es);
	ExplainPropertyUInteger("IndexInfo_ii_Am", NULL, (*node).ii_Am, es);
}


static void WriteExprContextExplain(Node *obj, ExplainState *es, EState *estate){
	ExprContext* node = NULL;
	Assert(obj->type == T_ExprContext);
	node = (ExprContext*)obj;
	(void)node;
	ExplainPropertyInteger("ExprContext_type", NULL, (*node).type, es);
	ExplainPropertyUInteger("ExprContext_caseValue_datum", NULL, (*node).caseValue_datum, es);
	ExplainPropertyBool("ExprContext_caseValue_isNull", (*node).caseValue_isNull, es);
	ExplainPropertyUInteger("ExprContext_domainValue_datum", NULL, (*node).domainValue_datum, es);
	ExplainPropertyBool("ExprContext_domainValue_isNull", (*node).domainValue_isNull, es);
}


static void WriteReturnSetInfoExplain(Node *obj, ExplainState *es, EState *estate){
	ReturnSetInfo* node = NULL;
	Assert(obj->type == T_ReturnSetInfo);
	node = (ReturnSetInfo*)obj;
	(void)node;
	ExplainPropertyInteger("ReturnSetInfo_type", NULL, (*node).type, es);
	ExplainPropertyInteger("ReturnSetInfo_allowedModes", NULL, (*node).allowedModes, es);
	ExplainPropertyInteger("ReturnSetInfo_returnMode", NULL, (*node).returnMode, es);
	ExplainPropertyInteger("ReturnSetInfo_isDone", NULL, (*node).isDone, es);
}


static void WriteProjectionInfoExplain(Node *obj, ExplainState *es, EState *estate){
	ProjectionInfo* node = NULL;
	Assert(obj->type == T_ProjectionInfo);
	node = (ProjectionInfo*)obj;
	(void)node;
	ExplainPropertyInteger("ProjectionInfo_type", NULL, (*node).type, es);
	ExplainPropertyInteger("ProjectionInfo_pi_state_type", NULL, (*node).pi_state.type, es);
	ExplainPropertyUInteger("ProjectionInfo_pi_state_flags", NULL, (*node).pi_state.flags, es);
	ExplainPropertyBool("ProjectionInfo_pi_state_resnull", (*node).pi_state.resnull, es);
	ExplainPropertyUInteger("ProjectionInfo_pi_state_resvalue", NULL, (*node).pi_state.resvalue, es);
	ExplainPropertyInteger("ProjectionInfo_pi_state_steps_len", NULL, (*node).pi_state.steps_len, es);
	ExplainPropertyInteger("ProjectionInfo_pi_state_steps_alloc", NULL, (*node).pi_state.steps_alloc, es);
}


static void WriteJunkFilterExplain(Node *obj, ExplainState *es, EState *estate){
	JunkFilter* node = NULL;
	Assert(obj->type == T_JunkFilter);
	node = (JunkFilter*)obj;
	(void)node;
	ExplainPropertyInteger("JunkFilter_type", NULL, (*node).type, es);
	ExplainPropertyInteger("JunkFilter_jf_targetList_length", NULL, (*node).jf_targetList ? (*node).jf_targetList->length : 0, es);
}


static void WriteOnConflictSetStateExplain(Node *obj, ExplainState *es, EState *estate){
	OnConflictSetState* node = NULL;
	Assert(obj->type == T_OnConflictSetState);
	node = (OnConflictSetState*)obj;
	(void)node;
	ExplainPropertyInteger("OnConflictSetState_type", NULL, (*node).type, es);
}


static void WriteMergeActionStateExplain(Node *obj, ExplainState *es, EState *estate){
	MergeActionState* node = NULL;
	Assert(obj->type == T_MergeActionState);
	node = (MergeActionState*)obj;
	(void)node;
	ExplainPropertyInteger("MergeActionState_type", NULL, (*node).type, es);
}


static void WriteResultRelInfoExplain(Node *obj, ExplainState *es, EState *estate){
	ResultRelInfo* node = NULL;
	Assert(obj->type == T_ResultRelInfo);
	node = (ResultRelInfo*)obj;
	(void)node;
	ExplainPropertyInteger("ResultRelInfo_type", NULL, (*node).type, es);
	ExplainPropertyUInteger("ResultRelInfo_ri_RangeTableIndex", NULL, (*node).ri_RangeTableIndex, es);
	ExplainPropertyInteger("ResultRelInfo_ri_NumIndices", NULL, (*node).ri_NumIndices, es);
	ExplainPropertyInteger("ResultRelInfo_ri_RowIdAttNo", NULL, (*node).ri_RowIdAttNo, es);
	ExplainPropertyBool("ResultRelInfo_ri_projectNewInfoValid", (*node).ri_projectNewInfoValid, es);
	ExplainPropertyBool("ResultRelInfo_ri_needLockTagTuple", (*node).ri_needLockTagTuple, es);
	ExplainPropertyBool("ResultRelInfo_ri_usesFdwDirectModify", (*node).ri_usesFdwDirectModify, es);
	ExplainPropertyInteger("ResultRelInfo_ri_NumSlots", NULL, (*node).ri_NumSlots, es);
	ExplainPropertyInteger("ResultRelInfo_ri_NumSlotsInitialized", NULL, (*node).ri_NumSlotsInitialized, es);
	ExplainPropertyInteger("ResultRelInfo_ri_BatchSize", NULL, (*node).ri_BatchSize, es);
	ExplainPropertyInteger("ResultRelInfo_ri_WithCheckOptions_length", NULL, (*node).ri_WithCheckOptions ? (*node).ri_WithCheckOptions->length : 0, es);
	ExplainPropertyInteger("ResultRelInfo_ri_WithCheckOptionExprs_length", NULL, (*node).ri_WithCheckOptionExprs ? (*node).ri_WithCheckOptionExprs->length : 0, es);
	ExplainPropertyInteger("ResultRelInfo_ri_NumGeneratedNeededI", NULL, (*node).ri_NumGeneratedNeededI, es);
	ExplainPropertyInteger("ResultRelInfo_ri_NumGeneratedNeededU", NULL, (*node).ri_NumGeneratedNeededU, es);
	ExplainPropertyInteger("ResultRelInfo_ri_returningList_length", NULL, (*node).ri_returningList ? (*node).ri_returningList->length : 0, es);
	ExplainPropertyInteger("ResultRelInfo_ri_onConflictArbiterIndexes_length", NULL, (*node).ri_onConflictArbiterIndexes ? (*node).ri_onConflictArbiterIndexes->length : 0, es);
	ExplainPropertyBool("ResultRelInfo_ri_ChildToRootMapValid", (*node).ri_ChildToRootMapValid, es);
	ExplainPropertyBool("ResultRelInfo_ri_RootToChildMapValid", (*node).ri_RootToChildMapValid, es);
	ExplainPropertyInteger("ResultRelInfo_ri_ancestorResultRels_length", NULL, (*node).ri_ancestorResultRels ? (*node).ri_ancestorResultRels->length : 0, es);
}


static void WriteEStateExplain(Node *obj, ExplainState *es, EState *estate){
	EState* node = NULL;
	Assert(obj->type == T_EState);
	node = (EState*)obj;
	(void)node;
	ExplainPropertyInteger("EState_type", NULL, (*node).type, es);
	ExplainPropertyInteger("EState_es_direction", NULL, (*node).es_direction, es);
	ExplainPropertyInteger("EState_es_range_table_length", NULL, (*node).es_range_table ? (*node).es_range_table->length : 0, es);
	ExplainPropertyUInteger("EState_es_range_table_size", NULL, (*node).es_range_table_size, es);
	ExplainPropertyInteger("EState_es_rteperminfos_length", NULL, (*node).es_rteperminfos ? (*node).es_rteperminfos->length : 0, es);
	ExplainPropertyUInteger("EState_es_output_cid", NULL, (*node).es_output_cid, es);
	ExplainPropertyInteger("EState_es_opened_result_relations_length", NULL, (*node).es_opened_result_relations ? (*node).es_opened_result_relations->length : 0, es);
	ExplainPropertyInteger("EState_es_tuple_routing_result_relations_length", NULL, (*node).es_tuple_routing_result_relations ? (*node).es_tuple_routing_result_relations->length : 0, es);
	ExplainPropertyInteger("EState_es_trig_target_relations_length", NULL, (*node).es_trig_target_relations ? (*node).es_trig_target_relations->length : 0, es);
	ExplainPropertyInteger("EState_es_tupleTable_length", NULL, (*node).es_tupleTable ? (*node).es_tupleTable->length : 0, es);
	ExplainPropertyUInteger("EState_es_processed", NULL, (*node).es_processed, es);
	ExplainPropertyUInteger("EState_es_total_processed", NULL, (*node).es_total_processed, es);
	ExplainPropertyInteger("EState_es_top_eflags", NULL, (*node).es_top_eflags, es);
	ExplainPropertyInteger("EState_es_instrument", NULL, (*node).es_instrument, es);
	ExplainPropertyBool("EState_es_finished", (*node).es_finished, es);
	ExplainPropertyInteger("EState_es_exprcontexts_length", NULL, (*node).es_exprcontexts ? (*node).es_exprcontexts->length : 0, es);
	ExplainPropertyInteger("EState_es_subplanstates_length", NULL, (*node).es_subplanstates ? (*node).es_subplanstates->length : 0, es);
	ExplainPropertyInteger("EState_es_auxmodifytables_length", NULL, (*node).es_auxmodifytables ? (*node).es_auxmodifytables->length : 0, es);
	ExplainPropertyBool("EState_es_use_parallel_mode", (*node).es_use_parallel_mode, es);
	ExplainPropertyInteger("EState_es_jit_flags", NULL, (*node).es_jit_flags, es);
	ExplainPropertyInteger("EState_es_insert_pending_result_relations_length", NULL, (*node).es_insert_pending_result_relations ? (*node).es_insert_pending_result_relations->length : 0, es);
	ExplainPropertyInteger("EState_es_insert_pending_modifytables_length", NULL, (*node).es_insert_pending_modifytables ? (*node).es_insert_pending_modifytables->length : 0, es);
}


static void WriteWindowFuncExprStateExplain(Node *obj, ExplainState *es, EState *estate){
	WindowFuncExprState* node = NULL;
	Assert(obj->type == T_WindowFuncExprState);
	node = (WindowFuncExprState*)obj;
	(void)node;
	ExplainPropertyInteger("WindowFuncExprState_type", NULL, (*node).type, es);
	ExplainPropertyInteger("WindowFuncExprState_args_length", NULL, (*node).args ? (*node).args->length : 0, es);
	ExplainPropertyInteger("WindowFuncExprState_wfuncno", NULL, (*node).wfuncno, es);
}


static void WriteSetExprStateExplain(Node *obj, ExplainState *es, EState *estate){
	SetExprState* node = NULL;
	Assert(obj->type == T_SetExprState);
	node = (SetExprState*)obj;
	(void)node;
	ExplainPropertyInteger("SetExprState_type", NULL, (*node).type, es);
	ExplainPropertyInteger("SetExprState_args_length", NULL, (*node).args ? (*node).args->length : 0, es);
	ExplainPropertyBool("SetExprState_funcReturnsTuple", (*node).funcReturnsTuple, es);
	ExplainPropertyBool("SetExprState_funcReturnsSet", (*node).funcReturnsSet, es);
	ExplainPropertyBool("SetExprState_setArgsValid", (*node).setArgsValid, es);
	ExplainPropertyBool("SetExprState_shutdown_reg", (*node).shutdown_reg, es);
}


static void WriteSubPlanStateExplain(Node *obj, ExplainState *es, EState *estate){
	SubPlanState* node = NULL;
	Assert(obj->type == T_SubPlanState);
	node = (SubPlanState*)obj;
	(void)node;
	ExplainPropertyInteger("SubPlanState_type", NULL, (*node).type, es);
	ExplainPropertyInteger("SubPlanState_args_length", NULL, (*node).args ? (*node).args->length : 0, es);
	ExplainPropertyUInteger("SubPlanState_curArray", NULL, (*node).curArray, es);
	ExplainPropertyBool("SubPlanState_havehashrows", (*node).havehashrows, es);
	ExplainPropertyBool("SubPlanState_havenullrows", (*node).havenullrows, es);
	ExplainPropertyInteger("SubPlanState_numCols", NULL, (*node).numCols, es);
}


static void WriteDomainConstraintStateExplain(Node *obj, ExplainState *es, EState *estate){
	DomainConstraintState* node = NULL;
	Assert(obj->type == T_DomainConstraintState);
	node = (DomainConstraintState*)obj;
	(void)node;
	ExplainPropertyInteger("DomainConstraintState_type", NULL, (*node).type, es);
	ExplainPropertyInteger("DomainConstraintState_constrainttype", NULL, (*node).constrainttype, es);
}


static void WriteResultStateExplain(Node *obj, ExplainState *es, EState *estate){
	ResultState* node = NULL;
	Assert(obj->type == T_ResultState);
	node = (ResultState*)obj;
	(void)node;
	ExplainPropertyBool("ResultState_rs_done", (*node).rs_done, es);
	ExplainPropertyBool("ResultState_rs_checkqual", (*node).rs_checkqual, es);
}


static void WriteProjectSetStateExplain(Node *obj, ExplainState *es, EState *estate){
	ProjectSetState* node = NULL;
	Assert(obj->type == T_ProjectSetState);
	node = (ProjectSetState*)obj;
	(void)node;
	ExplainPropertyInteger("ProjectSetState_nelems", NULL, (*node).nelems, es);
	ExplainPropertyBool("ProjectSetState_pending_srf_tuples", (*node).pending_srf_tuples, es);
}


static void WriteModifyTableStateExplain(Node *obj, ExplainState *es, EState *estate){
	ModifyTableState* node = NULL;
	Assert(obj->type == T_ModifyTableState);
	node = (ModifyTableState*)obj;
	(void)node;
	ExplainPropertyInteger("ModifyTableState_operation", NULL, (*node).operation, es);
	ExplainPropertyBool("ModifyTableState_canSetTag", (*node).canSetTag, es);
	ExplainPropertyBool("ModifyTableState_mt_done", (*node).mt_done, es);
	ExplainPropertyInteger("ModifyTableState_mt_nrels", NULL, (*node).mt_nrels, es);
	ExplainPropertyBool("ModifyTableState_fireBSTriggers", (*node).fireBSTriggers, es);
	ExplainPropertyInteger("ModifyTableState_mt_resultOidAttno", NULL, (*node).mt_resultOidAttno, es);
	ExplainPropertyUInteger("ModifyTableState_mt_lastResultOid", NULL, (*node).mt_lastResultOid, es);
	ExplainPropertyInteger("ModifyTableState_mt_lastResultIndex", NULL, (*node).mt_lastResultIndex, es);
	ExplainPropertyInteger("ModifyTableState_mt_merge_subcommands", NULL, (*node).mt_merge_subcommands, es);
	ExplainPropertyFloat("ModifyTableState_mt_merge_inserted", NULL, (*node).mt_merge_inserted, 9, es);
	ExplainPropertyFloat("ModifyTableState_mt_merge_updated", NULL, (*node).mt_merge_updated, 9, es);
	ExplainPropertyFloat("ModifyTableState_mt_merge_deleted", NULL, (*node).mt_merge_deleted, 9, es);
}


static void WriteAppendStateExplain(Node *obj, ExplainState *es, EState *estate){
	AppendState* node = NULL;
	Assert(obj->type == T_AppendState);
	node = (AppendState*)obj;
	(void)node;
	ExplainPropertyInteger("AppendState_as_nplans", NULL, (*node).as_nplans, es);
	ExplainPropertyInteger("AppendState_as_whichplan", NULL, (*node).as_whichplan, es);
	ExplainPropertyBool("AppendState_as_begun", (*node).as_begun, es);
	ExplainPropertyInteger("AppendState_as_nasyncplans", NULL, (*node).as_nasyncplans, es);
	ExplainPropertyInteger("AppendState_as_nasyncresults", NULL, (*node).as_nasyncresults, es);
	ExplainPropertyBool("AppendState_as_syncdone", (*node).as_syncdone, es);
	ExplainPropertyInteger("AppendState_as_nasyncremain", NULL, (*node).as_nasyncremain, es);
	ExplainPropertyInteger("AppendState_as_first_partial_plan", NULL, (*node).as_first_partial_plan, es);
	ExplainPropertyInteger("AppendState_pstate_len", NULL, (*node).pstate_len, es);
	ExplainPropertyBool("AppendState_as_valid_subplans_identified", (*node).as_valid_subplans_identified, es);
}


static void WriteMergeAppendStateExplain(Node *obj, ExplainState *es, EState *estate){
	MergeAppendState* node = NULL;
	Assert(obj->type == T_MergeAppendState);
	node = (MergeAppendState*)obj;
	(void)node;
	ExplainPropertyInteger("MergeAppendState_ms_nplans", NULL, (*node).ms_nplans, es);
	ExplainPropertyInteger("MergeAppendState_ms_nkeys", NULL, (*node).ms_nkeys, es);
	ExplainPropertyBool("MergeAppendState_ms_initialized", (*node).ms_initialized, es);
}


static void WriteRecursiveUnionStateExplain(Node *obj, ExplainState *es, EState *estate){
	RecursiveUnionState* node = NULL;
	Assert(obj->type == T_RecursiveUnionState);
	node = (RecursiveUnionState*)obj;
	(void)node;
	ExplainPropertyBool("RecursiveUnionState_recursing", (*node).recursing, es);
	ExplainPropertyBool("RecursiveUnionState_intermediate_empty", (*node).intermediate_empty, es);
}


static void WriteBitmapAndStateExplain(Node *obj, ExplainState *es, EState *estate){
	BitmapAndState* node = NULL;
	Assert(obj->type == T_BitmapAndState);
	node = (BitmapAndState*)obj;
	(void)node;
	ExplainPropertyInteger("BitmapAndState_nplans", NULL, (*node).nplans, es);
}


static void WriteBitmapOrStateExplain(Node *obj, ExplainState *es, EState *estate){
	BitmapOrState* node = NULL;
	Assert(obj->type == T_BitmapOrState);
	node = (BitmapOrState*)obj;
	(void)node;
	ExplainPropertyInteger("BitmapOrState_nplans", NULL, (*node).nplans, es);
}


static void WriteScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	ScanState* node = NULL;
	Assert(obj->type == T_ScanState);
	node = (ScanState*)obj;
	(void)node;
}


static void WriteSeqScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	SeqScanState* node = NULL;
	Assert(obj->type == T_SeqScanState);
	node = (SeqScanState*)obj;
	(void)node;
	ExplainPropertyInteger("SeqScanState_pscan_len", NULL, (*node).pscan_len, es);
}


static void WriteSampleScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	SampleScanState* node = NULL;
	Assert(obj->type == T_SampleScanState);
	node = (SampleScanState*)obj;
	(void)node;
	ExplainPropertyInteger("SampleScanState_args_length", NULL, (*node).args ? (*node).args->length : 0, es);
	ExplainPropertyBool("SampleScanState_use_bulkread", (*node).use_bulkread, es);
	ExplainPropertyBool("SampleScanState_use_pagemode", (*node).use_pagemode, es);
	ExplainPropertyBool("SampleScanState_begun", (*node).begun, es);
	ExplainPropertyUInteger("SampleScanState_seed", NULL, (*node).seed, es);
	ExplainPropertyInteger("SampleScanState_donetuples", NULL, (*node).donetuples, es);
	ExplainPropertyBool("SampleScanState_haveblock", (*node).haveblock, es);
	ExplainPropertyBool("SampleScanState_done", (*node).done, es);
}


static void WriteIndexScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	IndexScanState* node = NULL;
	Assert(obj->type == T_IndexScanState);
	node = (IndexScanState*)obj;
	(void)node;
	ExplainPropertyInteger("IndexScanState_indexorderbyorig_length", NULL, (*node).indexorderbyorig ? (*node).indexorderbyorig->length : 0, es);

	ExplainOpenGroup("IndexScanState_iss_ScanKeys", "IndexScanState_iss_ScanKeys", false, es);
	if ((*node).iss_ScanKeys) {
		struct ScanKeyData * value = (*node).iss_ScanKeys;
		for (size_t i = 0; i < (*node).iss_NumScanKeys; i++) {
			ExplainOpenGroup("IndexScanState_iss_ScanKeysScanKeyData", NULL, true, es);
			ExplainPropertyInteger("IndexScanState_iss_ScanKeys_sk_flags", NULL, (value[i]).sk_flags, es);
			ExplainPropertyInteger("IndexScanState_iss_ScanKeys_sk_attno", NULL, (value[i]).sk_attno, es);
			ExplainPropertyUInteger("IndexScanState_iss_ScanKeys_sk_strategy", NULL, (value[i]).sk_strategy, es);
			ExplainPropertyUInteger("IndexScanState_iss_ScanKeys_sk_subtype", NULL, (value[i]).sk_subtype, es);
			ExplainPropertyUInteger("IndexScanState_iss_ScanKeys_sk_collation", NULL, (value[i]).sk_collation, es);
			ExplainPropertyUInteger("IndexScanState_iss_ScanKeys_sk_argument", NULL, (value[i]).sk_argument, es);
			ExplainCloseGroup("IndexScanState_iss_ScanKeysScanKeyData", NULL, true, es);
		}
	}
	ExplainCloseGroup("IndexScanState_iss_ScanKeys", "IndexScanState_iss_ScanKeys", false, es);

	ExplainPropertyInteger("IndexScanState_iss_NumScanKeys", NULL, (*node).iss_NumScanKeys, es);

	ExplainOpenGroup("IndexScanState_iss_OrderByKeys", "IndexScanState_iss_OrderByKeys", false, es);
	if ((*node).iss_OrderByKeys) {
		struct ScanKeyData * value = (*node).iss_OrderByKeys;
		for (size_t i = 0; i < (*node).iss_NumOrderByKeys; i++) {
			ExplainOpenGroup("IndexScanState_iss_OrderByKeysScanKeyData", NULL, true, es);
			ExplainPropertyInteger("IndexScanState_iss_OrderByKeys_sk_flags", NULL, (value[i]).sk_flags, es);
			ExplainPropertyInteger("IndexScanState_iss_OrderByKeys_sk_attno", NULL, (value[i]).sk_attno, es);
			ExplainPropertyUInteger("IndexScanState_iss_OrderByKeys_sk_strategy", NULL, (value[i]).sk_strategy, es);
			ExplainPropertyUInteger("IndexScanState_iss_OrderByKeys_sk_subtype", NULL, (value[i]).sk_subtype, es);
			ExplainPropertyUInteger("IndexScanState_iss_OrderByKeys_sk_collation", NULL, (value[i]).sk_collation, es);
			ExplainPropertyUInteger("IndexScanState_iss_OrderByKeys_sk_argument", NULL, (value[i]).sk_argument, es);
			ExplainCloseGroup("IndexScanState_iss_OrderByKeysScanKeyData", NULL, true, es);
		}
	}
	ExplainCloseGroup("IndexScanState_iss_OrderByKeys", "IndexScanState_iss_OrderByKeys", false, es);

	ExplainPropertyInteger("IndexScanState_iss_NumOrderByKeys", NULL, (*node).iss_NumOrderByKeys, es);

	ExplainOpenGroup("IndexScanState_iss_RuntimeKeys", "IndexScanState_iss_RuntimeKeys", false, es);
	if ((*node).iss_RuntimeKeys) {
		IndexRuntimeKeyInfo * value = (*node).iss_RuntimeKeys;
		for (size_t i = 0; i < (*node).iss_NumRuntimeKeys; i++) {
			ExplainOpenGroup("IndexScanState_iss_RuntimeKeysIndexRuntimeKeyInfo", NULL, true, es);
			Assert((value[i]).scan_key != NULL);
			ExplainPropertyInteger("IndexScanState_iss_RuntimeKeys_scan_key_sk_flags", NULL, (*((value[i]).scan_key)).sk_flags, es);
			ExplainPropertyInteger("IndexScanState_iss_RuntimeKeys_scan_key_sk_attno", NULL, (*((value[i]).scan_key)).sk_attno, es);
			ExplainPropertyUInteger("IndexScanState_iss_RuntimeKeys_scan_key_sk_strategy", NULL, (*((value[i]).scan_key)).sk_strategy, es);
			ExplainPropertyUInteger("IndexScanState_iss_RuntimeKeys_scan_key_sk_subtype", NULL, (*((value[i]).scan_key)).sk_subtype, es);
			ExplainPropertyUInteger("IndexScanState_iss_RuntimeKeys_scan_key_sk_collation", NULL, (*((value[i]).scan_key)).sk_collation, es);
			ExplainPropertyUInteger("IndexScanState_iss_RuntimeKeys_scan_key_sk_argument", NULL, (*((value[i]).scan_key)).sk_argument, es);
			ExplainPropertyBool("IndexScanState_iss_RuntimeKeys_key_toastable", (value[i]).key_toastable, es);
			ExplainCloseGroup("IndexScanState_iss_RuntimeKeysIndexRuntimeKeyInfo", NULL, true, es);
		}
	}
	ExplainCloseGroup("IndexScanState_iss_RuntimeKeys", "IndexScanState_iss_RuntimeKeys", false, es);

	ExplainPropertyInteger("IndexScanState_iss_NumRuntimeKeys", NULL, (*node).iss_NumRuntimeKeys, es);
	ExplainPropertyBool("IndexScanState_iss_RuntimeKeysReady", (*node).iss_RuntimeKeysReady, es);
	ExplainPropertyBool("IndexScanState_iss_ReachedEnd", (*node).iss_ReachedEnd, es);
	ExplainPropertyInteger("IndexScanState_iss_PscanLen", NULL, (*node).iss_PscanLen, es);
}


static void WriteIndexOnlyScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	IndexOnlyScanState* node = NULL;
	Assert(obj->type == T_IndexOnlyScanState);
	node = (IndexOnlyScanState*)obj;
	(void)node;

	ExplainOpenGroup("IndexOnlyScanState_ioss_ScanKeys", "IndexOnlyScanState_ioss_ScanKeys", false, es);
	if ((*node).ioss_ScanKeys) {
		struct ScanKeyData * value = (*node).ioss_ScanKeys;
		for (size_t i = 0; i < (*node).ioss_NumScanKeys; i++) {
			ExplainOpenGroup("IndexOnlyScanState_ioss_ScanKeysScanKeyData", NULL, true, es);
			ExplainPropertyInteger("IndexOnlyScanState_ioss_ScanKeys_sk_flags", NULL, (value[i]).sk_flags, es);
			ExplainPropertyInteger("IndexOnlyScanState_ioss_ScanKeys_sk_attno", NULL, (value[i]).sk_attno, es);
			ExplainPropertyUInteger("IndexOnlyScanState_ioss_ScanKeys_sk_strategy", NULL, (value[i]).sk_strategy, es);
			ExplainPropertyUInteger("IndexOnlyScanState_ioss_ScanKeys_sk_subtype", NULL, (value[i]).sk_subtype, es);
			ExplainPropertyUInteger("IndexOnlyScanState_ioss_ScanKeys_sk_collation", NULL, (value[i]).sk_collation, es);
			ExplainPropertyUInteger("IndexOnlyScanState_ioss_ScanKeys_sk_argument", NULL, (value[i]).sk_argument, es);
			ExplainCloseGroup("IndexOnlyScanState_ioss_ScanKeysScanKeyData", NULL, true, es);
		}
	}
	ExplainCloseGroup("IndexOnlyScanState_ioss_ScanKeys", "IndexOnlyScanState_ioss_ScanKeys", false, es);

	ExplainPropertyInteger("IndexOnlyScanState_ioss_NumScanKeys", NULL, (*node).ioss_NumScanKeys, es);

	ExplainOpenGroup("IndexOnlyScanState_ioss_OrderByKeys", "IndexOnlyScanState_ioss_OrderByKeys", false, es);
	if ((*node).ioss_OrderByKeys) {
		struct ScanKeyData * value = (*node).ioss_OrderByKeys;
		for (size_t i = 0; i < (*node).ioss_NumOrderByKeys; i++) {
			ExplainOpenGroup("IndexOnlyScanState_ioss_OrderByKeysScanKeyData", NULL, true, es);
			ExplainPropertyInteger("IndexOnlyScanState_ioss_OrderByKeys_sk_flags", NULL, (value[i]).sk_flags, es);
			ExplainPropertyInteger("IndexOnlyScanState_ioss_OrderByKeys_sk_attno", NULL, (value[i]).sk_attno, es);
			ExplainPropertyUInteger("IndexOnlyScanState_ioss_OrderByKeys_sk_strategy", NULL, (value[i]).sk_strategy, es);
			ExplainPropertyUInteger("IndexOnlyScanState_ioss_OrderByKeys_sk_subtype", NULL, (value[i]).sk_subtype, es);
			ExplainPropertyUInteger("IndexOnlyScanState_ioss_OrderByKeys_sk_collation", NULL, (value[i]).sk_collation, es);
			ExplainPropertyUInteger("IndexOnlyScanState_ioss_OrderByKeys_sk_argument", NULL, (value[i]).sk_argument, es);
			ExplainCloseGroup("IndexOnlyScanState_ioss_OrderByKeysScanKeyData", NULL, true, es);
		}
	}
	ExplainCloseGroup("IndexOnlyScanState_ioss_OrderByKeys", "IndexOnlyScanState_ioss_OrderByKeys", false, es);

	ExplainPropertyInteger("IndexOnlyScanState_ioss_NumOrderByKeys", NULL, (*node).ioss_NumOrderByKeys, es);

	ExplainOpenGroup("IndexOnlyScanState_ioss_RuntimeKeys", "IndexOnlyScanState_ioss_RuntimeKeys", false, es);
	if ((*node).ioss_RuntimeKeys) {
		IndexRuntimeKeyInfo * value = (*node).ioss_RuntimeKeys;
		for (size_t i = 0; i < (*node).ioss_NumRuntimeKeys; i++) {
			ExplainOpenGroup("IndexOnlyScanState_ioss_RuntimeKeysIndexRuntimeKeyInfo", NULL, true, es);
			Assert((value[i]).scan_key != NULL);
			ExplainPropertyInteger("IndexOnlyScanState_ioss_RuntimeKeys_scan_key_sk_flags", NULL, (*((value[i]).scan_key)).sk_flags, es);
			ExplainPropertyInteger("IndexOnlyScanState_ioss_RuntimeKeys_scan_key_sk_attno", NULL, (*((value[i]).scan_key)).sk_attno, es);
			ExplainPropertyUInteger("IndexOnlyScanState_ioss_RuntimeKeys_scan_key_sk_strategy", NULL, (*((value[i]).scan_key)).sk_strategy, es);
			ExplainPropertyUInteger("IndexOnlyScanState_ioss_RuntimeKeys_scan_key_sk_subtype", NULL, (*((value[i]).scan_key)).sk_subtype, es);
			ExplainPropertyUInteger("IndexOnlyScanState_ioss_RuntimeKeys_scan_key_sk_collation", NULL, (*((value[i]).scan_key)).sk_collation, es);
			ExplainPropertyUInteger("IndexOnlyScanState_ioss_RuntimeKeys_scan_key_sk_argument", NULL, (*((value[i]).scan_key)).sk_argument, es);
			ExplainPropertyBool("IndexOnlyScanState_ioss_RuntimeKeys_key_toastable", (value[i]).key_toastable, es);
			ExplainCloseGroup("IndexOnlyScanState_ioss_RuntimeKeysIndexRuntimeKeyInfo", NULL, true, es);
		}
	}
	ExplainCloseGroup("IndexOnlyScanState_ioss_RuntimeKeys", "IndexOnlyScanState_ioss_RuntimeKeys", false, es);

	ExplainPropertyInteger("IndexOnlyScanState_ioss_NumRuntimeKeys", NULL, (*node).ioss_NumRuntimeKeys, es);
	ExplainPropertyBool("IndexOnlyScanState_ioss_RuntimeKeysReady", (*node).ioss_RuntimeKeysReady, es);
	ExplainPropertyInteger("IndexOnlyScanState_ioss_VMBuffer", NULL, (*node).ioss_VMBuffer, es);
	ExplainPropertyInteger("IndexOnlyScanState_ioss_PscanLen", NULL, (*node).ioss_PscanLen, es);
	ExplainPropertyInteger("IndexOnlyScanState_ioss_NameCStringCount", NULL, (*node).ioss_NameCStringCount, es);
}


static void WriteBitmapIndexScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	BitmapIndexScanState* node = NULL;
	Assert(obj->type == T_BitmapIndexScanState);
	node = (BitmapIndexScanState*)obj;
	(void)node;

	ExplainOpenGroup("BitmapIndexScanState_biss_ScanKeys", "BitmapIndexScanState_biss_ScanKeys", false, es);
	if ((*node).biss_ScanKeys) {
		struct ScanKeyData * value = (*node).biss_ScanKeys;
		for (size_t i = 0; i < (*node).biss_NumScanKeys; i++) {
			ExplainOpenGroup("BitmapIndexScanState_biss_ScanKeysScanKeyData", NULL, true, es);
			ExplainPropertyInteger("BitmapIndexScanState_biss_ScanKeys_sk_flags", NULL, (value[i]).sk_flags, es);
			ExplainPropertyInteger("BitmapIndexScanState_biss_ScanKeys_sk_attno", NULL, (value[i]).sk_attno, es);
			ExplainPropertyUInteger("BitmapIndexScanState_biss_ScanKeys_sk_strategy", NULL, (value[i]).sk_strategy, es);
			ExplainPropertyUInteger("BitmapIndexScanState_biss_ScanKeys_sk_subtype", NULL, (value[i]).sk_subtype, es);
			ExplainPropertyUInteger("BitmapIndexScanState_biss_ScanKeys_sk_collation", NULL, (value[i]).sk_collation, es);
			ExplainPropertyUInteger("BitmapIndexScanState_biss_ScanKeys_sk_argument", NULL, (value[i]).sk_argument, es);
			ExplainCloseGroup("BitmapIndexScanState_biss_ScanKeysScanKeyData", NULL, true, es);
		}
	}
	ExplainCloseGroup("BitmapIndexScanState_biss_ScanKeys", "BitmapIndexScanState_biss_ScanKeys", false, es);

	ExplainPropertyInteger("BitmapIndexScanState_biss_NumScanKeys", NULL, (*node).biss_NumScanKeys, es);

	ExplainOpenGroup("BitmapIndexScanState_biss_RuntimeKeys", "BitmapIndexScanState_biss_RuntimeKeys", false, es);
	if ((*node).biss_RuntimeKeys) {
		IndexRuntimeKeyInfo * value = (*node).biss_RuntimeKeys;
		for (size_t i = 0; i < (*node).biss_NumRuntimeKeys; i++) {
			ExplainOpenGroup("BitmapIndexScanState_biss_RuntimeKeysIndexRuntimeKeyInfo", NULL, true, es);
			Assert((value[i]).scan_key != NULL);
			ExplainPropertyInteger("BitmapIndexScanState_biss_RuntimeKeys_scan_key_sk_flags", NULL, (*((value[i]).scan_key)).sk_flags, es);
			ExplainPropertyInteger("BitmapIndexScanState_biss_RuntimeKeys_scan_key_sk_attno", NULL, (*((value[i]).scan_key)).sk_attno, es);
			ExplainPropertyUInteger("BitmapIndexScanState_biss_RuntimeKeys_scan_key_sk_strategy", NULL, (*((value[i]).scan_key)).sk_strategy, es);
			ExplainPropertyUInteger("BitmapIndexScanState_biss_RuntimeKeys_scan_key_sk_subtype", NULL, (*((value[i]).scan_key)).sk_subtype, es);
			ExplainPropertyUInteger("BitmapIndexScanState_biss_RuntimeKeys_scan_key_sk_collation", NULL, (*((value[i]).scan_key)).sk_collation, es);
			ExplainPropertyUInteger("BitmapIndexScanState_biss_RuntimeKeys_scan_key_sk_argument", NULL, (*((value[i]).scan_key)).sk_argument, es);
			ExplainPropertyBool("BitmapIndexScanState_biss_RuntimeKeys_key_toastable", (value[i]).key_toastable, es);
			ExplainCloseGroup("BitmapIndexScanState_biss_RuntimeKeysIndexRuntimeKeyInfo", NULL, true, es);
		}
	}
	ExplainCloseGroup("BitmapIndexScanState_biss_RuntimeKeys", "BitmapIndexScanState_biss_RuntimeKeys", false, es);

	ExplainPropertyInteger("BitmapIndexScanState_biss_NumRuntimeKeys", NULL, (*node).biss_NumRuntimeKeys, es);
	ExplainPropertyInteger("BitmapIndexScanState_biss_NumArrayKeys", NULL, (*node).biss_NumArrayKeys, es);
	ExplainPropertyBool("BitmapIndexScanState_biss_RuntimeKeysReady", (*node).biss_RuntimeKeysReady, es);
}


static void WriteBitmapHeapScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	BitmapHeapScanState* node = NULL;
	Assert(obj->type == T_BitmapHeapScanState);
	node = (BitmapHeapScanState*)obj;
	(void)node;
	ExplainPropertyInteger("BitmapHeapScanState_pvmbuffer", NULL, (*node).pvmbuffer, es);
	ExplainPropertyInteger("BitmapHeapScanState_exact_pages", NULL, (*node).exact_pages, es);
	ExplainPropertyInteger("BitmapHeapScanState_lossy_pages", NULL, (*node).lossy_pages, es);
	ExplainPropertyInteger("BitmapHeapScanState_prefetch_pages", NULL, (*node).prefetch_pages, es);
	ExplainPropertyInteger("BitmapHeapScanState_prefetch_target", NULL, (*node).prefetch_target, es);
	ExplainPropertyInteger("BitmapHeapScanState_prefetch_maximum", NULL, (*node).prefetch_maximum, es);
	ExplainPropertyBool("BitmapHeapScanState_initialized", (*node).initialized, es);
}


static void WriteTidScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	TidScanState* node = NULL;
	Assert(obj->type == T_TidScanState);
	node = (TidScanState*)obj;
	(void)node;
	ExplainPropertyInteger("TidScanState_tss_tidexprs_length", NULL, (*node).tss_tidexprs ? (*node).tss_tidexprs->length : 0, es);
	ExplainPropertyBool("TidScanState_tss_isCurrentOf", (*node).tss_isCurrentOf, es);
	ExplainPropertyInteger("TidScanState_tss_NumTids", NULL, (*node).tss_NumTids, es);
	ExplainPropertyInteger("TidScanState_tss_TidPtr", NULL, (*node).tss_TidPtr, es);
}


static void WriteTidRangeScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	TidRangeScanState* node = NULL;
	Assert(obj->type == T_TidRangeScanState);
	node = (TidRangeScanState*)obj;
	(void)node;
	ExplainPropertyInteger("TidRangeScanState_trss_tidexprs_length", NULL, (*node).trss_tidexprs ? (*node).trss_tidexprs->length : 0, es);
	ExplainPropertyBool("TidRangeScanState_trss_inScan", (*node).trss_inScan, es);
}


static void WriteSubqueryScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	SubqueryScanState* node = NULL;
	Assert(obj->type == T_SubqueryScanState);
	node = (SubqueryScanState*)obj;
	(void)node;
}


static void WriteFunctionScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	FunctionScanState* node = NULL;
	Assert(obj->type == T_FunctionScanState);
	node = (FunctionScanState*)obj;
	(void)node;
	ExplainPropertyInteger("FunctionScanState_eflags", NULL, (*node).eflags, es);
	ExplainPropertyBool("FunctionScanState_ordinality", (*node).ordinality, es);
	ExplainPropertyBool("FunctionScanState_simple", (*node).simple, es);
	ExplainPropertyInteger("FunctionScanState_ordinal", NULL, (*node).ordinal, es);
	ExplainPropertyInteger("FunctionScanState_nfuncs", NULL, (*node).nfuncs, es);
}


static void WriteValuesScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	ValuesScanState* node = NULL;
	Assert(obj->type == T_ValuesScanState);
	node = (ValuesScanState*)obj;
	(void)node;
	ExplainPropertyInteger("ValuesScanState_array_len", NULL, (*node).array_len, es);
	ExplainPropertyInteger("ValuesScanState_curr_idx", NULL, (*node).curr_idx, es);
}


static void WriteTableFuncScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	TableFuncScanState* node = NULL;
	Assert(obj->type == T_TableFuncScanState);
	node = (TableFuncScanState*)obj;
	(void)node;
	ExplainPropertyInteger("TableFuncScanState_colexprs_length", NULL, (*node).colexprs ? (*node).colexprs->length : 0, es);
	ExplainPropertyInteger("TableFuncScanState_coldefexprs_length", NULL, (*node).coldefexprs ? (*node).coldefexprs->length : 0, es);
	ExplainPropertyInteger("TableFuncScanState_colvalexprs_length", NULL, (*node).colvalexprs ? (*node).colvalexprs->length : 0, es);
	ExplainPropertyInteger("TableFuncScanState_passingvalexprs_length", NULL, (*node).passingvalexprs ? (*node).passingvalexprs->length : 0, es);
	ExplainPropertyInteger("TableFuncScanState_ns_names_length", NULL, (*node).ns_names ? (*node).ns_names->length : 0, es);
	ExplainPropertyInteger("TableFuncScanState_ns_uris_length", NULL, (*node).ns_uris ? (*node).ns_uris->length : 0, es);
	ExplainPropertyInteger("TableFuncScanState_ordinal", NULL, (*node).ordinal, es);
}


static void WriteCteScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	CteScanState* node = NULL;
	Assert(obj->type == T_CteScanState);
	node = (CteScanState*)obj;
	(void)node;
	ExplainPropertyInteger("CteScanState_eflags", NULL, (*node).eflags, es);
	ExplainPropertyInteger("CteScanState_readptr", NULL, (*node).readptr, es);
	ExplainPropertyBool("CteScanState_eof_cte", (*node).eof_cte, es);
}


static void WriteNamedTuplestoreScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	NamedTuplestoreScanState* node = NULL;
	Assert(obj->type == T_NamedTuplestoreScanState);
	node = (NamedTuplestoreScanState*)obj;
	(void)node;
	ExplainPropertyInteger("NamedTuplestoreScanState_readptr", NULL, (*node).readptr, es);
}


static void WriteWorkTableScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	WorkTableScanState* node = NULL;
	Assert(obj->type == T_WorkTableScanState);
	node = (WorkTableScanState*)obj;
	(void)node;
}


static void WriteForeignScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	ForeignScanState* node = NULL;
	Assert(obj->type == T_ForeignScanState);
	node = (ForeignScanState*)obj;
	(void)node;
	ExplainPropertyInteger("ForeignScanState_pscan_len", NULL, (*node).pscan_len, es);
}


static void WriteCustomScanStateExplain(Node *obj, ExplainState *es, EState *estate){
	CustomScanState* node = NULL;
	Assert(obj->type == T_CustomScanState);
	node = (CustomScanState*)obj;
	(void)node;
	ExplainPropertyUInteger("CustomScanState_flags", NULL, (*node).flags, es);
	ExplainPropertyInteger("CustomScanState_custom_ps_length", NULL, (*node).custom_ps ? (*node).custom_ps->length : 0, es);
	ExplainPropertyInteger("CustomScanState_pscan_len", NULL, (*node).pscan_len, es);
}


static void WriteJoinStateExplain(Node *obj, ExplainState *es, EState *estate){
	JoinState* node = NULL;
	Assert(obj->type == T_JoinState);
	node = (JoinState*)obj;
	(void)node;
	ExplainPropertyInteger("JoinState_jointype", NULL, (*node).jointype, es);
	ExplainPropertyBool("JoinState_single_match", (*node).single_match, es);
}


static void WriteNestLoopStateExplain(Node *obj, ExplainState *es, EState *estate){
	NestLoopState* node = NULL;
	Assert(obj->type == T_NestLoopState);
	node = (NestLoopState*)obj;
	(void)node;
	ExplainPropertyInteger("NestLoopState_js_jointype", NULL, (*node).js.jointype, es);
	ExplainPropertyBool("NestLoopState_js_single_match", (*node).js.single_match, es);
	ExplainPropertyBool("NestLoopState_nl_NeedNewOuter", (*node).nl_NeedNewOuter, es);
	ExplainPropertyBool("NestLoopState_nl_MatchedOuter", (*node).nl_MatchedOuter, es);
}


static void WriteMergeJoinStateExplain(Node *obj, ExplainState *es, EState *estate){
	MergeJoinState* node = NULL;
	Assert(obj->type == T_MergeJoinState);
	node = (MergeJoinState*)obj;
	(void)node;
	ExplainPropertyInteger("MergeJoinState_js_jointype", NULL, (*node).js.jointype, es);
	ExplainPropertyBool("MergeJoinState_js_single_match", (*node).js.single_match, es);
	ExplainPropertyInteger("MergeJoinState_mj_NumClauses", NULL, (*node).mj_NumClauses, es);
	ExplainPropertyInteger("MergeJoinState_mj_JoinState", NULL, (*node).mj_JoinState, es);
	ExplainPropertyBool("MergeJoinState_mj_SkipMarkRestore", (*node).mj_SkipMarkRestore, es);
	ExplainPropertyBool("MergeJoinState_mj_ExtraMarks", (*node).mj_ExtraMarks, es);
	ExplainPropertyBool("MergeJoinState_mj_ConstFalseJoin", (*node).mj_ConstFalseJoin, es);
	ExplainPropertyBool("MergeJoinState_mj_FillOuter", (*node).mj_FillOuter, es);
	ExplainPropertyBool("MergeJoinState_mj_FillInner", (*node).mj_FillInner, es);
	ExplainPropertyBool("MergeJoinState_mj_MatchedOuter", (*node).mj_MatchedOuter, es);
	ExplainPropertyBool("MergeJoinState_mj_MatchedInner", (*node).mj_MatchedInner, es);
}


static void WriteHashJoinStateExplain(Node *obj, ExplainState *es, EState *estate){
	HashJoinState* node = NULL;
	Assert(obj->type == T_HashJoinState);
	node = (HashJoinState*)obj;
	(void)node;
	ExplainPropertyInteger("HashJoinState_js_jointype", NULL, (*node).js.jointype, es);
	ExplainPropertyBool("HashJoinState_js_single_match", (*node).js.single_match, es);
	ExplainPropertyInteger("HashJoinState_hj_OuterHashKeys_length", NULL, (*node).hj_OuterHashKeys ? (*node).hj_OuterHashKeys->length : 0, es);
	ExplainPropertyInteger("HashJoinState_hj_HashOperators_length", NULL, (*node).hj_HashOperators ? (*node).hj_HashOperators->length : 0, es);
	ExplainPropertyInteger("HashJoinState_hj_Collations_length", NULL, (*node).hj_Collations ? (*node).hj_Collations->length : 0, es);
	ExplainPropertyUInteger("HashJoinState_hj_CurHashValue", NULL, (*node).hj_CurHashValue, es);
	ExplainPropertyInteger("HashJoinState_hj_CurBucketNo", NULL, (*node).hj_CurBucketNo, es);
	ExplainPropertyInteger("HashJoinState_hj_CurSkewBucketNo", NULL, (*node).hj_CurSkewBucketNo, es);
	ExplainPropertyInteger("HashJoinState_hj_JoinState", NULL, (*node).hj_JoinState, es);
	ExplainPropertyBool("HashJoinState_hj_MatchedOuter", (*node).hj_MatchedOuter, es);
	ExplainPropertyBool("HashJoinState_hj_OuterNotEmpty", (*node).hj_OuterNotEmpty, es);
}


static void WriteMaterialStateExplain(Node *obj, ExplainState *es, EState *estate){
	MaterialState* node = NULL;
	Assert(obj->type == T_MaterialState);
	node = (MaterialState*)obj;
	(void)node;
	ExplainPropertyInteger("MaterialState_eflags", NULL, (*node).eflags, es);
	ExplainPropertyBool("MaterialState_eof_underlying", (*node).eof_underlying, es);
}


static void WriteMemoizeStateExplain(Node *obj, ExplainState *es, EState *estate){
	MemoizeState* node = NULL;
	Assert(obj->type == T_MemoizeState);
	node = (MemoizeState*)obj;
	(void)node;
	ExplainPropertyInteger("MemoizeState_mstatus", NULL, (*node).mstatus, es);
	ExplainPropertyInteger("MemoizeState_nkeys", NULL, (*node).nkeys, es);
	ExplainPropertyUInteger("MemoizeState_mem_used", NULL, (*node).mem_used, es);
	ExplainPropertyUInteger("MemoizeState_mem_limit", NULL, (*node).mem_limit, es);
	ExplainPropertyBool("MemoizeState_singlerow", (*node).singlerow, es);
	ExplainPropertyBool("MemoizeState_binary_mode", (*node).binary_mode, es);
}


static void WriteSortStateExplain(Node *obj, ExplainState *es, EState *estate){
	SortState* node = NULL;
	Assert(obj->type == T_SortState);
	node = (SortState*)obj;
	(void)node;
	ExplainPropertyBool("SortState_randomAccess", (*node).randomAccess, es);
	ExplainPropertyBool("SortState_bounded", (*node).bounded, es);
	ExplainPropertyInteger("SortState_bound", NULL, (*node).bound, es);
	ExplainPropertyBool("SortState_sort_Done", (*node).sort_Done, es);
	ExplainPropertyBool("SortState_bounded_Done", (*node).bounded_Done, es);
	ExplainPropertyInteger("SortState_bound_Done", NULL, (*node).bound_Done, es);
	ExplainPropertyBool("SortState_am_worker", (*node).am_worker, es);
	ExplainPropertyBool("SortState_datumSort", (*node).datumSort, es);
}


static void WriteIncrementalSortStateExplain(Node *obj, ExplainState *es, EState *estate){
	IncrementalSortState* node = NULL;
	Assert(obj->type == T_IncrementalSortState);
	node = (IncrementalSortState*)obj;
	(void)node;
	ExplainPropertyBool("IncrementalSortState_bounded", (*node).bounded, es);
	ExplainPropertyInteger("IncrementalSortState_bound", NULL, (*node).bound, es);
	ExplainPropertyBool("IncrementalSortState_outerNodeDone", (*node).outerNodeDone, es);
	ExplainPropertyInteger("IncrementalSortState_bound_Done", NULL, (*node).bound_Done, es);
	ExplainPropertyInteger("IncrementalSortState_execution_status", NULL, (*node).execution_status, es);
	ExplainPropertyInteger("IncrementalSortState_n_fullsort_remaining", NULL, (*node).n_fullsort_remaining, es);
	ExplainPropertyBool("IncrementalSortState_am_worker", (*node).am_worker, es);
}


static void WriteGroupStateExplain(Node *obj, ExplainState *es, EState *estate){
	GroupState* node = NULL;
	Assert(obj->type == T_GroupState);
	node = (GroupState*)obj;
	(void)node;
	ExplainPropertyBool("GroupState_grp_done", (*node).grp_done, es);
}


static void WriteAggStateExplain(Node *obj, ExplainState *es, EState *estate){
	AggState* node = NULL;
	Assert(obj->type == T_AggState);
	node = (AggState*)obj;
	(void)node;
	ExplainPropertyInteger("AggState_aggs_length", NULL, (*node).aggs ? (*node).aggs->length : 0, es);
	ExplainPropertyInteger("AggState_numaggs", NULL, (*node).numaggs, es);
	ExplainPropertyInteger("AggState_numtrans", NULL, (*node).numtrans, es);
	ExplainPropertyInteger("AggState_aggstrategy", NULL, (*node).aggstrategy, es);
	ExplainPropertyInteger("AggState_aggsplit", NULL, (*node).aggsplit, es);
	ExplainPropertyInteger("AggState_numphases", NULL, (*node).numphases, es);
	ExplainPropertyInteger("AggState_current_phase", NULL, (*node).current_phase, es);
	ExplainPropertyBool("AggState_input_done", (*node).input_done, es);
	ExplainPropertyBool("AggState_agg_done", (*node).agg_done, es);
	ExplainPropertyInteger("AggState_projected_set", NULL, (*node).projected_set, es);
	ExplainPropertyInteger("AggState_current_set", NULL, (*node).current_set, es);
	ExplainPropertyInteger("AggState_all_grouped_cols_length", NULL, (*node).all_grouped_cols ? (*node).all_grouped_cols->length : 0, es);
	ExplainPropertyInteger("AggState_max_colno_needed", NULL, (*node).max_colno_needed, es);
	ExplainPropertyBool("AggState_all_cols_needed", (*node).all_cols_needed, es);
	ExplainPropertyInteger("AggState_maxsets", NULL, (*node).maxsets, es);
	ExplainPropertyBool("AggState_table_filled", (*node).table_filled, es);
	ExplainPropertyInteger("AggState_num_hashes", NULL, (*node).num_hashes, es);
	ExplainPropertyInteger("AggState_hash_batches_length", NULL, (*node).hash_batches ? (*node).hash_batches->length : 0, es);
	ExplainPropertyBool("AggState_hash_ever_spilled", (*node).hash_ever_spilled, es);
	ExplainPropertyBool("AggState_hash_spill_mode", (*node).hash_spill_mode, es);
	ExplainPropertyInteger("AggState_hash_mem_limit", NULL, (*node).hash_mem_limit, es);
	ExplainPropertyUInteger("AggState_hash_ngroups_limit", NULL, (*node).hash_ngroups_limit, es);
	ExplainPropertyInteger("AggState_hash_planned_partitions", NULL, (*node).hash_planned_partitions, es);
	ExplainPropertyFloat("AggState_hashentrysize", NULL, (*node).hashentrysize, 9, es);
	ExplainPropertyInteger("AggState_hash_mem_peak", NULL, (*node).hash_mem_peak, es);
	ExplainPropertyUInteger("AggState_hash_ngroups_current", NULL, (*node).hash_ngroups_current, es);
	ExplainPropertyUInteger("AggState_hash_disk_used", NULL, (*node).hash_disk_used, es);
	ExplainPropertyInteger("AggState_hash_batches_used", NULL, (*node).hash_batches_used, es);
}


static void WriteWindowAggStateExplain(Node *obj, ExplainState *es, EState *estate){
	WindowAggState* node = NULL;
	Assert(obj->type == T_WindowAggState);
	node = (WindowAggState*)obj;
	(void)node;
	ExplainPropertyInteger("WindowAggState_funcs_length", NULL, (*node).funcs ? (*node).funcs->length : 0, es);
	ExplainPropertyInteger("WindowAggState_numfuncs", NULL, (*node).numfuncs, es);
	ExplainPropertyInteger("WindowAggState_numaggs", NULL, (*node).numaggs, es);
	ExplainPropertyInteger("WindowAggState_current_ptr", NULL, (*node).current_ptr, es);
	ExplainPropertyInteger("WindowAggState_framehead_ptr", NULL, (*node).framehead_ptr, es);
	ExplainPropertyInteger("WindowAggState_frametail_ptr", NULL, (*node).frametail_ptr, es);
	ExplainPropertyInteger("WindowAggState_grouptail_ptr", NULL, (*node).grouptail_ptr, es);
	ExplainPropertyInteger("WindowAggState_spooled_rows", NULL, (*node).spooled_rows, es);
	ExplainPropertyInteger("WindowAggState_currentpos", NULL, (*node).currentpos, es);
	ExplainPropertyInteger("WindowAggState_frameheadpos", NULL, (*node).frameheadpos, es);
	ExplainPropertyInteger("WindowAggState_frametailpos", NULL, (*node).frametailpos, es);
	ExplainPropertyInteger("WindowAggState_aggregatedbase", NULL, (*node).aggregatedbase, es);
	ExplainPropertyInteger("WindowAggState_aggregatedupto", NULL, (*node).aggregatedupto, es);
	ExplainPropertyInteger("WindowAggState_status", NULL, (*node).status, es);
	ExplainPropertyInteger("WindowAggState_frameOptions", NULL, (*node).frameOptions, es);
	ExplainPropertyUInteger("WindowAggState_startOffsetValue", NULL, (*node).startOffsetValue, es);
	ExplainPropertyUInteger("WindowAggState_endOffsetValue", NULL, (*node).endOffsetValue, es);
	ExplainPropertyUInteger("WindowAggState_inRangeColl", NULL, (*node).inRangeColl, es);
	ExplainPropertyBool("WindowAggState_inRangeAsc", (*node).inRangeAsc, es);
	ExplainPropertyBool("WindowAggState_inRangeNullsFirst", (*node).inRangeNullsFirst, es);
	ExplainPropertyInteger("WindowAggState_currentgroup", NULL, (*node).currentgroup, es);
	ExplainPropertyInteger("WindowAggState_frameheadgroup", NULL, (*node).frameheadgroup, es);
	ExplainPropertyInteger("WindowAggState_frametailgroup", NULL, (*node).frametailgroup, es);
	ExplainPropertyInteger("WindowAggState_groupheadpos", NULL, (*node).groupheadpos, es);
	ExplainPropertyInteger("WindowAggState_grouptailpos", NULL, (*node).grouptailpos, es);
	ExplainPropertyBool("WindowAggState_use_pass_through", (*node).use_pass_through, es);
	ExplainPropertyBool("WindowAggState_top_window", (*node).top_window, es);
	ExplainPropertyBool("WindowAggState_all_first", (*node).all_first, es);
	ExplainPropertyBool("WindowAggState_partition_spooled", (*node).partition_spooled, es);
	ExplainPropertyBool("WindowAggState_more_partitions", (*node).more_partitions, es);
	ExplainPropertyBool("WindowAggState_framehead_valid", (*node).framehead_valid, es);
	ExplainPropertyBool("WindowAggState_frametail_valid", (*node).frametail_valid, es);
	ExplainPropertyBool("WindowAggState_grouptail_valid", (*node).grouptail_valid, es);
}


static void WriteUniqueStateExplain(Node *obj, ExplainState *es, EState *estate){
	UniqueState* node = NULL;
	Assert(obj->type == T_UniqueState);
	node = (UniqueState*)obj;
	(void)node;
}


static void WriteGatherStateExplain(Node *obj, ExplainState *es, EState *estate){
	GatherState* node = NULL;
	Assert(obj->type == T_GatherState);
	node = (GatherState*)obj;
	(void)node;
	ExplainPropertyBool("GatherState_initialized", (*node).initialized, es);
	ExplainPropertyBool("GatherState_need_to_scan_locally", (*node).need_to_scan_locally, es);
	ExplainPropertyInteger("GatherState_tuples_needed", NULL, (*node).tuples_needed, es);
	ExplainPropertyInteger("GatherState_nworkers_launched", NULL, (*node).nworkers_launched, es);
	ExplainPropertyInteger("GatherState_nreaders", NULL, (*node).nreaders, es);
	ExplainPropertyInteger("GatherState_nextreader", NULL, (*node).nextreader, es);
}


static void WriteGatherMergeStateExplain(Node *obj, ExplainState *es, EState *estate){
	GatherMergeState* node = NULL;
	Assert(obj->type == T_GatherMergeState);
	node = (GatherMergeState*)obj;
	(void)node;
	ExplainPropertyBool("GatherMergeState_initialized", (*node).initialized, es);
	ExplainPropertyBool("GatherMergeState_gm_initialized", (*node).gm_initialized, es);
	ExplainPropertyBool("GatherMergeState_need_to_scan_locally", (*node).need_to_scan_locally, es);
	ExplainPropertyInteger("GatherMergeState_tuples_needed", NULL, (*node).tuples_needed, es);
	ExplainPropertyInteger("GatherMergeState_gm_nkeys", NULL, (*node).gm_nkeys, es);
	ExplainPropertyInteger("GatherMergeState_nworkers_launched", NULL, (*node).nworkers_launched, es);
	ExplainPropertyInteger("GatherMergeState_nreaders", NULL, (*node).nreaders, es);
}


static void WriteHashStateExplain(Node *obj, ExplainState *es, EState *estate){
	HashState* node = NULL;
	Assert(obj->type == T_HashState);
	node = (HashState*)obj;
	(void)node;
	ExplainPropertyInteger("HashState_hashkeys_length", NULL, (*node).hashkeys ? (*node).hashkeys->length : 0, es);
}


static void WriteSetOpStateExplain(Node *obj, ExplainState *es, EState *estate){
	SetOpState* node = NULL;
	Assert(obj->type == T_SetOpState);
	node = (SetOpState*)obj;
	(void)node;
	ExplainPropertyBool("SetOpState_setop_done", (*node).setop_done, es);
	ExplainPropertyInteger("SetOpState_numOutput", NULL, (*node).numOutput, es);
	ExplainPropertyBool("SetOpState_table_filled", (*node).table_filled, es);
}


static void WriteLockRowsStateExplain(Node *obj, ExplainState *es, EState *estate){
	LockRowsState* node = NULL;
	Assert(obj->type == T_LockRowsState);
	node = (LockRowsState*)obj;
	(void)node;
	ExplainPropertyInteger("LockRowsState_lr_arowMarks_length", NULL, (*node).lr_arowMarks ? (*node).lr_arowMarks->length : 0, es);
}


static void WriteLimitStateExplain(Node *obj, ExplainState *es, EState *estate){
	LimitState* node = NULL;
	Assert(obj->type == T_LimitState);
	node = (LimitState*)obj;
	(void)node;
	ExplainPropertyInteger("LimitState_limitOption", NULL, (*node).limitOption, es);
	ExplainPropertyInteger("LimitState_offset", NULL, (*node).offset, es);
	ExplainPropertyInteger("LimitState_count", NULL, (*node).count, es);
	ExplainPropertyBool("LimitState_noCount", (*node).noCount, es);
	ExplainPropertyInteger("LimitState_lstate", NULL, (*node).lstate, es);
	ExplainPropertyInteger("LimitState_position", NULL, (*node).position, es);
}



void ExplainEntry(Node *obj, ExplainState *es, EState *estate) {
	switch (obj->type) {
		case T_PlannedStmt: WritePlannedStmtExplain(obj, es, estate); return;
		case T_Result: WriteResultExplain(obj, es, estate); return;
		case T_ProjectSet: WriteProjectSetExplain(obj, es, estate); return;
		case T_ModifyTable: WriteModifyTableExplain(obj, es, estate); return;
		case T_Append: WriteAppendExplain(obj, es, estate); return;
		case T_MergeAppend: WriteMergeAppendExplain(obj, es, estate); return;
		case T_RecursiveUnion: WriteRecursiveUnionExplain(obj, es, estate); return;
		case T_BitmapAnd: WriteBitmapAndExplain(obj, es, estate); return;
		case T_BitmapOr: WriteBitmapOrExplain(obj, es, estate); return;
		case T_SeqScan: WriteSeqScanExplain(obj, es, estate); return;
		case T_SampleScan: WriteSampleScanExplain(obj, es, estate); return;
		case T_IndexScan: WriteIndexScanExplain(obj, es, estate); return;
		case T_IndexOnlyScan: WriteIndexOnlyScanExplain(obj, es, estate); return;
		case T_BitmapIndexScan: WriteBitmapIndexScanExplain(obj, es, estate); return;
		case T_BitmapHeapScan: WriteBitmapHeapScanExplain(obj, es, estate); return;
		case T_TidScan: WriteTidScanExplain(obj, es, estate); return;
		case T_TidRangeScan: WriteTidRangeScanExplain(obj, es, estate); return;
		case T_SubqueryScan: WriteSubqueryScanExplain(obj, es, estate); return;
		case T_FunctionScan: WriteFunctionScanExplain(obj, es, estate); return;
		case T_ValuesScan: WriteValuesScanExplain(obj, es, estate); return;
		case T_TableFuncScan: WriteTableFuncScanExplain(obj, es, estate); return;
		case T_CteScan: WriteCteScanExplain(obj, es, estate); return;
		case T_NamedTuplestoreScan: WriteNamedTuplestoreScanExplain(obj, es, estate); return;
		case T_WorkTableScan: WriteWorkTableScanExplain(obj, es, estate); return;
		case T_ForeignScan: WriteForeignScanExplain(obj, es, estate); return;
		case T_CustomScan: WriteCustomScanExplain(obj, es, estate); return;
		case T_NestLoop: WriteNestLoopExplain(obj, es, estate); return;
		case T_NestLoopParam: WriteNestLoopParamExplain(obj, es, estate); return;
		case T_MergeJoin: WriteMergeJoinExplain(obj, es, estate); return;
		case T_HashJoin: WriteHashJoinExplain(obj, es, estate); return;
		case T_Material: WriteMaterialExplain(obj, es, estate); return;
		case T_Memoize: WriteMemoizeExplain(obj, es, estate); return;
		case T_Sort: WriteSortExplain(obj, es, estate); return;
		case T_IncrementalSort: WriteIncrementalSortExplain(obj, es, estate); return;
		case T_Group: WriteGroupExplain(obj, es, estate); return;
		case T_Agg: WriteAggExplain(obj, es, estate); return;
		case T_WindowAgg: WriteWindowAggExplain(obj, es, estate); return;
		case T_Unique: WriteUniqueExplain(obj, es, estate); return;
		case T_Gather: WriteGatherExplain(obj, es, estate); return;
		case T_GatherMerge: WriteGatherMergeExplain(obj, es, estate); return;
		case T_Hash: WriteHashExplain(obj, es, estate); return;
		case T_SetOp: WriteSetOpExplain(obj, es, estate); return;
		case T_LockRows: WriteLockRowsExplain(obj, es, estate); return;
		case T_Limit: WriteLimitExplain(obj, es, estate); return;
		case T_PlanRowMark: WritePlanRowMarkExplain(obj, es, estate); return;
		case T_PartitionPruneInfo: WritePartitionPruneInfoExplain(obj, es, estate); return;
		case T_PartitionedRelPruneInfo: WritePartitionedRelPruneInfoExplain(obj, es, estate); return;
		case T_PartitionPruneStepOp: WritePartitionPruneStepOpExplain(obj, es, estate); return;
		case T_PartitionPruneStepCombine: WritePartitionPruneStepCombineExplain(obj, es, estate); return;
		case T_PlanInvalItem: WritePlanInvalItemExplain(obj, es, estate); return;
		case T_ExprState: WriteExprStateExplain(obj, es, estate); return;
		case T_IndexInfo: WriteIndexInfoExplain(obj, es, estate); return;
		case T_ExprContext: WriteExprContextExplain(obj, es, estate); return;
		case T_ReturnSetInfo: WriteReturnSetInfoExplain(obj, es, estate); return;
		case T_ProjectionInfo: WriteProjectionInfoExplain(obj, es, estate); return;
		case T_JunkFilter: WriteJunkFilterExplain(obj, es, estate); return;
		case T_OnConflictSetState: WriteOnConflictSetStateExplain(obj, es, estate); return;
		case T_MergeActionState: WriteMergeActionStateExplain(obj, es, estate); return;
		case T_ResultRelInfo: WriteResultRelInfoExplain(obj, es, estate); return;
		case T_EState: WriteEStateExplain(obj, es, estate); return;
		case T_WindowFuncExprState: WriteWindowFuncExprStateExplain(obj, es, estate); return;
		case T_SetExprState: WriteSetExprStateExplain(obj, es, estate); return;
		case T_SubPlanState: WriteSubPlanStateExplain(obj, es, estate); return;
		case T_DomainConstraintState: WriteDomainConstraintStateExplain(obj, es, estate); return;
		case T_ResultState: WriteResultStateExplain(obj, es, estate); return;
		case T_ProjectSetState: WriteProjectSetStateExplain(obj, es, estate); return;
		case T_ModifyTableState: WriteModifyTableStateExplain(obj, es, estate); return;
		case T_AppendState: WriteAppendStateExplain(obj, es, estate); return;
		case T_MergeAppendState: WriteMergeAppendStateExplain(obj, es, estate); return;
		case T_RecursiveUnionState: WriteRecursiveUnionStateExplain(obj, es, estate); return;
		case T_BitmapAndState: WriteBitmapAndStateExplain(obj, es, estate); return;
		case T_BitmapOrState: WriteBitmapOrStateExplain(obj, es, estate); return;
		case T_ScanState: WriteScanStateExplain(obj, es, estate); return;
		case T_SeqScanState: WriteSeqScanStateExplain(obj, es, estate); return;
		case T_SampleScanState: WriteSampleScanStateExplain(obj, es, estate); return;
		case T_IndexScanState: WriteIndexScanStateExplain(obj, es, estate); return;
		case T_IndexOnlyScanState: WriteIndexOnlyScanStateExplain(obj, es, estate); return;
		case T_BitmapIndexScanState: WriteBitmapIndexScanStateExplain(obj, es, estate); return;
		case T_BitmapHeapScanState: WriteBitmapHeapScanStateExplain(obj, es, estate); return;
		case T_TidScanState: WriteTidScanStateExplain(obj, es, estate); return;
		case T_TidRangeScanState: WriteTidRangeScanStateExplain(obj, es, estate); return;
		case T_SubqueryScanState: WriteSubqueryScanStateExplain(obj, es, estate); return;
		case T_FunctionScanState: WriteFunctionScanStateExplain(obj, es, estate); return;
		case T_ValuesScanState: WriteValuesScanStateExplain(obj, es, estate); return;
		case T_TableFuncScanState: WriteTableFuncScanStateExplain(obj, es, estate); return;
		case T_CteScanState: WriteCteScanStateExplain(obj, es, estate); return;
		case T_NamedTuplestoreScanState: WriteNamedTuplestoreScanStateExplain(obj, es, estate); return;
		case T_WorkTableScanState: WriteWorkTableScanStateExplain(obj, es, estate); return;
		case T_ForeignScanState: WriteForeignScanStateExplain(obj, es, estate); return;
		case T_CustomScanState: WriteCustomScanStateExplain(obj, es, estate); return;
		case T_JoinState: WriteJoinStateExplain(obj, es, estate); return;
		case T_NestLoopState: WriteNestLoopStateExplain(obj, es, estate); return;
		case T_MergeJoinState: WriteMergeJoinStateExplain(obj, es, estate); return;
		case T_HashJoinState: WriteHashJoinStateExplain(obj, es, estate); return;
		case T_MaterialState: WriteMaterialStateExplain(obj, es, estate); return;
		case T_MemoizeState: WriteMemoizeStateExplain(obj, es, estate); return;
		case T_SortState: WriteSortStateExplain(obj, es, estate); return;
		case T_IncrementalSortState: WriteIncrementalSortStateExplain(obj, es, estate); return;
		case T_GroupState: WriteGroupStateExplain(obj, es, estate); return;
		case T_AggState: WriteAggStateExplain(obj, es, estate); return;
		case T_WindowAggState: WriteWindowAggStateExplain(obj, es, estate); return;
		case T_UniqueState: WriteUniqueStateExplain(obj, es, estate); return;
		case T_GatherState: WriteGatherStateExplain(obj, es, estate); return;
		case T_GatherMergeState: WriteGatherMergeStateExplain(obj, es, estate); return;
		case T_HashState: WriteHashStateExplain(obj, es, estate); return;
		case T_SetOpState: WriteSetOpStateExplain(obj, es, estate); return;
		case T_LockRowsState: WriteLockRowsStateExplain(obj, es, estate); return;
		case T_LimitState: WriteLimitStateExplain(obj, es, estate); return;
		default: abort();
	}
}


char* NodeToName(Node* obj) {
	switch (obj->type) {
		case T_PlannedStmt: return "PlannedStmt";
		case T_Result: return "Result";
		case T_ProjectSet: return "ProjectSet";
		case T_ModifyTable: return "ModifyTable";
		case T_Append: return "Append";
		case T_MergeAppend: return "MergeAppend";
		case T_RecursiveUnion: return "RecursiveUnion";
		case T_BitmapAnd: return "BitmapAnd";
		case T_BitmapOr: return "BitmapOr";
		case T_SeqScan: return "SeqScan";
		case T_SampleScan: return "SampleScan";
		case T_IndexScan: return "IndexScan";
		case T_IndexOnlyScan: return "IndexOnlyScan";
		case T_BitmapIndexScan: return "BitmapIndexScan";
		case T_BitmapHeapScan: return "BitmapHeapScan";
		case T_TidScan: return "TidScan";
		case T_TidRangeScan: return "TidRangeScan";
		case T_SubqueryScan: return "SubqueryScan";
		case T_FunctionScan: return "FunctionScan";
		case T_ValuesScan: return "ValuesScan";
		case T_TableFuncScan: return "TableFuncScan";
		case T_CteScan: return "CteScan";
		case T_NamedTuplestoreScan: return "NamedTuplestoreScan";
		case T_WorkTableScan: return "WorkTableScan";
		case T_ForeignScan: return "ForeignScan";
		case T_CustomScan: return "CustomScan";
		case T_NestLoop: return "NestLoop";
		case T_NestLoopParam: return "NestLoopParam";
		case T_MergeJoin: return "MergeJoin";
		case T_HashJoin: return "HashJoin";
		case T_Material: return "Material";
		case T_Memoize: return "Memoize";
		case T_Sort: return "Sort";
		case T_IncrementalSort: return "IncrementalSort";
		case T_Group: return "Group";
		case T_Agg: return "Agg";
		case T_WindowAgg: return "WindowAgg";
		case T_Unique: return "Unique";
		case T_Gather: return "Gather";
		case T_GatherMerge: return "GatherMerge";
		case T_Hash: return "Hash";
		case T_SetOp: return "SetOp";
		case T_LockRows: return "LockRows";
		case T_Limit: return "Limit";
		case T_PlanRowMark: return "PlanRowMark";
		case T_PartitionPruneInfo: return "PartitionPruneInfo";
		case T_PartitionedRelPruneInfo: return "PartitionedRelPruneInfo";
		case T_PartitionPruneStepOp: return "PartitionPruneStepOp";
		case T_PartitionPruneStepCombine: return "PartitionPruneStepCombine";
		case T_PlanInvalItem: return "PlanInvalItem";
		case T_ExprState: return "ExprState";
		case T_IndexInfo: return "IndexInfo";
		case T_ExprContext: return "ExprContext";
		case T_ReturnSetInfo: return "ReturnSetInfo";
		case T_ProjectionInfo: return "ProjectionInfo";
		case T_JunkFilter: return "JunkFilter";
		case T_OnConflictSetState: return "OnConflictSetState";
		case T_MergeActionState: return "MergeActionState";
		case T_ResultRelInfo: return "ResultRelInfo";
		case T_EState: return "EState";
		case T_WindowFuncExprState: return "WindowFuncExprState";
		case T_SetExprState: return "SetExprState";
		case T_SubPlanState: return "SubPlanState";
		case T_DomainConstraintState: return "DomainConstraintState";
		case T_ResultState: return "ResultState";
		case T_ProjectSetState: return "ProjectSetState";
		case T_ModifyTableState: return "ModifyTableState";
		case T_AppendState: return "AppendState";
		case T_MergeAppendState: return "MergeAppendState";
		case T_RecursiveUnionState: return "RecursiveUnionState";
		case T_BitmapAndState: return "BitmapAndState";
		case T_BitmapOrState: return "BitmapOrState";
		case T_ScanState: return "ScanState";
		case T_SeqScanState: return "SeqScanState";
		case T_SampleScanState: return "SampleScanState";
		case T_IndexScanState: return "IndexScanState";
		case T_IndexOnlyScanState: return "IndexOnlyScanState";
		case T_BitmapIndexScanState: return "BitmapIndexScanState";
		case T_BitmapHeapScanState: return "BitmapHeapScanState";
		case T_TidScanState: return "TidScanState";
		case T_TidRangeScanState: return "TidRangeScanState";
		case T_SubqueryScanState: return "SubqueryScanState";
		case T_FunctionScanState: return "FunctionScanState";
		case T_ValuesScanState: return "ValuesScanState";
		case T_TableFuncScanState: return "TableFuncScanState";
		case T_CteScanState: return "CteScanState";
		case T_NamedTuplestoreScanState: return "NamedTuplestoreScanState";
		case T_WorkTableScanState: return "WorkTableScanState";
		case T_ForeignScanState: return "ForeignScanState";
		case T_CustomScanState: return "CustomScanState";
		case T_JoinState: return "JoinState";
		case T_NestLoopState: return "NestLoopState";
		case T_MergeJoinState: return "MergeJoinState";
		case T_HashJoinState: return "HashJoinState";
		case T_MaterialState: return "MaterialState";
		case T_MemoizeState: return "MemoizeState";
		case T_SortState: return "SortState";
		case T_IncrementalSortState: return "IncrementalSortState";
		case T_GroupState: return "GroupState";
		case T_AggState: return "AggState";
		case T_WindowAggState: return "WindowAggState";
		case T_UniqueState: return "UniqueState";
		case T_GatherState: return "GatherState";
		case T_GatherMergeState: return "GatherMergeState";
		case T_HashState: return "HashState";
		case T_SetOpState: return "SetOpState";
		case T_LockRowsState: return "LockRowsState";
		case T_LimitState: return "LimitState";
		default: abort();
	}
}


static void WalkPlan(struct Plan *plan, PlanState *ps,
                     ExplainState *es, EState *estate,
                     const char* relationship,
                     List *ancestors) {
    Assert(plan != NULL);
    ExplainEntry((struct Node*)plan, es, estate);
    if (nodeTag(es) == T_IndexScanState || nodeTag(es) == T_IndexOnlyScanState) {
        ExplainEntry((struct Node*)ps, es, estate);
    }
    AugmentPlan(plan, ps, es, estate);
    ExplainNodeMetadata(ps, ancestors, relationship, NULL, es, !qss_in_explain);

    if (ps->instrument && qss_in_explain)
    {
        InstrEndLoop(ps->instrument);
        ExplainPropertyFloat("counter0", NULL, ps->instrument->counter0, 9, es);
        ExplainPropertyFloat("counter1", NULL, ps->instrument->counter1, 9, es);
        ExplainPropertyFloat("counter2", NULL, ps->instrument->counter2, 9, es);
        ExplainPropertyFloat("counter3", NULL, ps->instrument->counter3, 9, es);
        ExplainPropertyFloat("counter4", NULL, ps->instrument->counter4, 9, es);
        ExplainPropertyFloat("counter5", NULL, ps->instrument->counter5, 9, es);
        ExplainPropertyFloat("counter6", NULL, ps->instrument->counter6, 9, es);
        ExplainPropertyFloat("counter7", NULL, ps->instrument->counter7, 9, es);
        ExplainPropertyFloat("counter8", NULL, ps->instrument->counter8, 9, es);
        ExplainPropertyFloat("counter9", NULL, ps->instrument->counter9, 9, es);
        ExplainPropertyFloat("blk_hit", NULL, ps->instrument->bufusage.shared_blks_hit, 9, es);
        ExplainPropertyFloat("blk_miss", NULL, ps->instrument->bufusage.shared_blks_read, 9, es);
        ExplainPropertyFloat("blk_dirty", NULL, ps->instrument->bufusage.shared_blks_dirtied, 9, es);
        ExplainPropertyFloat("blk_write", NULL, ps->instrument->bufusage.shared_blks_written, 9, es);
    }

    if (outerPlan(plan) != NULL || innerPlan(plan) != NULL) {
        ExplainOpenGroup("Plans", "Plans", false, es);
        ancestors = lcons(plan, ancestors);
    }

    if (outerPlan(plan) != NULL) {
        Assert(outerPlanState(ps) != NULL);
        ExplainOpenGroup("left-child", NULL, true, es);
        WalkPlan(outerPlan(plan), outerPlanState(ps), es, estate, "Outer", ancestors);
        ExplainCloseGroup("left-child", NULL, true, es);
    }

    if (innerPlan(plan) != NULL) {
        Assert(innerPlanState(ps) != NULL);
        ExplainOpenGroup("right-child", NULL, true, es);
        WalkPlan(innerPlan(plan), innerPlanState(ps), es, estate, "Inner", ancestors);
        ExplainCloseGroup("right-child", NULL, true, es);
	}

    if (outerPlan(plan) != NULL || innerPlan(plan) != NULL) {
        ExplainCloseGroup("Plans", "Plans", false, es);
    }

    if (ps->initPlan ||
		IsA(plan, Append) ||
		IsA(plan, MergeAppend) ||
		IsA(plan, BitmapAnd) ||
		IsA(plan, BitmapOr) ||
		IsA(plan, SubqueryScan) ||
		(IsA(ps, CustomScanState) && ((CustomScanState *) ps)->custom_ps != NIL) ||
		ps->subPlan) {
        ExplainPropertyBool("incomplete", true, es);
    } else {
        ExplainPropertyBool("incomplete", false, es);
    }
}

void OutputPlanToExplain(QueryDesc* queryDesc, ExplainState* state) {
    ExplainState* es = state;
    ExplainOpenGroup("NoisePage", NULL, true, state);
    if (queryDesc->sourceText != NULL) {
        ExplainPropertyText("query_text", queryDesc->sourceText, state);
    } else {
        ExplainPropertyText("query_text", "", state);
    }

    if (queryDesc->plannedstmt->commandType == CMD_SELECT && queryDesc->tupDesc) {
        int i = 0;
        int varlen = 0;
        struct Plan* plan = queryDesc->planstate->plan;
        Assert(plan != NULL);

        ExplainPropertyFloat("startup_cost", NULL, 0.0, 9, es);
        ExplainPropertyFloat("total_cost", NULL, 0.0, 9, es);
        ExplainPropertyFloat("DestReceiverRemote_output_plan_rows", NULL, plan->plan_rows, 9, es);
        ExplainPropertyInteger("DestReceiverRemote_output_plan_width", NULL, plan->plan_width, es);
        ExplainPropertyInteger("DestReceiverRemote_output_columns", NULL, queryDesc->tupDesc->natts, es);
        for (i = 0; i < queryDesc->tupDesc->natts; ++i) {
            Form_pg_attribute att = TupleDescAttr(queryDesc->tupDesc, i);
            varlen += (att->attlen >= 0 ? 0 : 1);
        }
        ExplainPropertyInteger("DestReceiverRemote_output_varlen", NULL, varlen, es);
        ExplainPropertyInteger("DestReceiverRemote_output_fixedlen", NULL, queryDesc->tupDesc->natts - varlen, es);
        ExplainPropertyText("node_type", "DestReceiverRemote", es);
        ExplainPropertyInteger("plan_node_id", NULL, PLAN_INDEPENDENT_ID, es);
        ExplainOpenGroup("Plans", "Plans", false, es);
        ExplainOpenGroup("left-child", NULL, true, es);
    }

    WalkPlan(queryDesc->planstate->plan, queryDesc->planstate, state, queryDesc->estate, NULL, NIL);

    if (queryDesc->plannedstmt->commandType == CMD_SELECT) {
        ExplainCloseGroup("left-child", NULL, true, es);
        ExplainCloseGroup("Plans", "Plans", false, es);
    }

    if (queryDesc->totaltime && qss_in_explain)
    {
        InstrEndLoop(queryDesc->totaltime);
        ExplainPropertyFloat("elapsed_us", NULL, queryDesc->totaltime->total * 1000000.0, 9, es);
        ExplainPropertyFloat("startup_time", NULL, queryDesc->totaltime->startup * 1000000.0, 9, es);
        ExplainPropertyFloat("blk_hit", NULL, queryDesc->totaltime->bufusage.shared_blks_hit, 9, es);
        ExplainPropertyFloat("blk_miss", NULL, queryDesc->totaltime->bufusage.shared_blks_read, 9, es);
        ExplainPropertyFloat("blk_dirty", NULL, queryDesc->totaltime->bufusage.shared_blks_dirtied, 9, es);
        ExplainPropertyFloat("blk_write", NULL, queryDesc->totaltime->bufusage.shared_blks_written, 9, es);
    }

    ExplainCloseGroup("NoisePage", NULL, true, state);
}
