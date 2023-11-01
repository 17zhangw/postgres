use pgrx_pg_sys::NodeTag;
use pgrx_pg_sys::Node;
use pgrx_pg_sys::Plan;
use pgrx_pg_sys::Scan;
use pgrx_pg_sys::SeqScan;
use pgrx_pg_sys::IndexScan;
use pgrx_pg_sys::IndexOnlyScan;
use pgrx_pg_sys::BitmapIndexScan;
use pgrx_pg_sys::BitmapHeapScan;
use pgrx_pg_sys::NestLoop;
use pgrx_pg_sys::MergeJoin;
use pgrx_pg_sys::HashJoin;
use pgrx_pg_sys::Hash;
use pgrx_pg_sys::Memoize;
use pgrx_pg_sys::Sort;
use pgrx_pg_sys::IncrementalSort;
use pgrx_pg_sys::Group;
use pgrx_pg_sys::Agg;
use pgrx_pg_sys::Limit;
use pgrx_pg_sys::Gather;
use pgrx_pg_sys::GatherMerge;
use pgrx_pg_sys::BitmapAnd;
use pgrx_pg_sys::BitmapOr;
use pgrx_pg_sys::Append;
use pgrx_pg_sys::SubqueryScan;
use pgrx_pg_sys::Integer;
use pgrx_pg_sys::error;
use pgrx_pg_sys::explain_get_index_name_hook;
use pgrx_pg_sys::get_rel_name;
use pgrx_pg_sys::List;
use pgrx::pg_sys::rt_fetch;

use std::ffi::CStr;

pub unsafe fn get_alias(scan: &Scan, pi: *mut List) -> String {
    let rte = rt_fetch(scan.scanrelid, pi);
    if rte.is_null() {
        return String::from("UNKNOWN");
    }

    String::from_utf8_unchecked(CStr::from_ptr((*(*rte).eref).aliasname).to_bytes().to_vec())
}

pub unsafe fn get_index(indexid: pgrx_pg_sys::Oid) -> String {
    let mut indexname: Option<*const std::os::raw::c_char> = None;

    if explain_get_index_name_hook.is_some() {
        indexname = Some(explain_get_index_name_hook.unwrap()(indexid));
    }

    if indexname.is_none() {
        indexname = Some(get_rel_name(indexid));
    }

    String::from_utf8_unchecked(CStr::from_ptr(indexname.unwrap()).to_bytes().to_vec())
}

pub unsafe fn compute_settings_signature() -> String {
    format!("{} {} {} {} {}",
            pgrx_pg_sys::NBuffers,
            pgrx_pg_sys::num_temp_buffers,
            pgrx_pg_sys::work_mem,
            pgrx_pg_sys::hash_mem_multiplier,
            pgrx_pg_sys::effective_io_concurrency)
}

pub unsafe fn compute_node_signature(node: *mut Node, output: &mut String) {
    match (*node).type_ {
        NodeTag::T_Integer => {
            output.push_str(format!(" {} ", (*(node as *const Integer)).ival).as_str());
        }
        _ => error!("Unhandled node type: {:?}", (*node).type_)
    }
}

pub unsafe fn compute_plan_signature(pi: *mut List, node: *mut Plan, output: &mut String) {
    if node.is_null() {
        return
    }

    match (*node).type_ {
        NodeTag::T_SeqScan => {
            let rscan = &*(node as *const SeqScan);
            let tbl = get_alias(&rscan.scan, pi);
            output.push_str(format!("SeqScan {}", tbl).as_str());
        }
        NodeTag::T_IndexScan => {
            let rscan = &*(node as *const IndexScan);
            let tbl = get_alias(&rscan.scan, pi);
            let idxname = get_index(rscan.indexid);
            output.push_str(format!("IndexScan {} {}", tbl, idxname).as_str());
        }
        NodeTag::T_IndexOnlyScan => {
            let rscan = &*(node as *const IndexOnlyScan);
            let tbl = get_alias(&rscan.scan, pi);
            let idxname = get_index(rscan.indexid);
            output.push_str(format!("IndexOnlyScan {} {}", tbl, idxname).as_str());
        }
        NodeTag::T_BitmapIndexScan => {
            let rscan = &*(node as *const BitmapIndexScan);
            let tbl = get_alias(&rscan.scan, pi);
            let idxname = get_index(rscan.indexid);
            output.push_str(format!("BitmapIndexScan {} {}", tbl, idxname).as_str());
        }
        NodeTag::T_BitmapHeapScan => {
            let rscan = &*(node as *const BitmapHeapScan);
            let tbl = get_alias(&rscan.scan, pi);
            output.push_str(format!("BitmapHeapScan {}", tbl).as_str());
        }
        NodeTag::T_NestLoop => {
            let rnestloop = &*(node as *const NestLoop);
            output.push_str(format!("NestLoop {}", rnestloop.join.jointype).as_str());
        }
        NodeTag::T_MergeJoin => {
            let rmergejoin = &*(node as *const MergeJoin);
            let clauses = pgrx::list::List::<*mut core::ffi::c_void>::downcast_ptr(rmergejoin.mergeclauses).unwrap();
            output.push_str(format!("MergeJoin {} {}", rmergejoin.join.jointype, clauses.len()).as_str());
        }
        NodeTag::T_HashJoin => {
            let rhashjoin = &*(node as *const HashJoin);
            let keys = pgrx::list::List::<*mut core::ffi::c_void>::downcast_ptr(rhashjoin.hashkeys).unwrap();
            output.push_str(format!("HashJoin {} {}", rhashjoin.join.jointype, keys.len()).as_str());
        }
        NodeTag::T_Hash => {
            let rhash = &*(node as *const Hash);
            let keys = pgrx::list::List::<*mut core::ffi::c_void>::downcast_ptr(rhash.hashkeys).unwrap();
            output.push_str(format!("Hash {}", keys.len()).as_str());
        }
        NodeTag::T_Material => {
            output.push_str("Material");
        }
        NodeTag::T_Memoize => {
            let rmemo = &*(node as *const Memoize);
            output.push_str(format!("Memoize {}", rmemo.numKeys).as_str());
        }
        NodeTag::T_Sort => {
            let rsort = &*(node as *const Sort);
            output.push_str(format!("Sort {}", rsort.numCols).as_str());
        }
        NodeTag::T_IncrementalSort => {
            let rincsort = &*(node as *const IncrementalSort);
            output.push_str(format!("IncrementalSort {} {}",
                                    rincsort.sort.numCols,
                                    rincsort.nPresortedCols).as_str());
        }
        NodeTag::T_Group => {
            let rgroup = &*(node as *const Group);
            output.push_str(format!("Group {}", rgroup.numCols).as_str());
        }
        NodeTag::T_Agg => {
            let ragg = &*(node as *const Agg);
            output.push_str(format!("Agg {} {}", ragg.aggstrategy, ragg.aggsplit).as_str());
        }
        NodeTag::T_Limit => {
            let rlim = &*(node as *const Limit);
            output.push_str("Limit");
            compute_node_signature(rlim.limitOffset, output);
            compute_node_signature(rlim.limitCount, output);
            output.push_str(format!("{}", rlim.limitOption).as_str());
        }
        NodeTag::T_Gather => {
            let rgather = &*(node as *const Gather);
            output.push_str(format!("Gather {}", rgather.num_workers).as_str());
        }
        NodeTag::T_GatherMerge => {
            let rgathermerge = &*(node as *const GatherMerge);
            output.push_str(format!("GatherMerge {} {}", rgathermerge.num_workers, rgathermerge.numCols).as_str());
        }
        NodeTag::T_BitmapAnd => {
            let rbitmapand = &*(node as *const BitmapAnd);
            let plans = pgrx::list::List::<*mut core::ffi::c_void>::downcast_ptr(rbitmapand.bitmapplans).unwrap();

            output.push_str("BitmapAnd [");
            for (i, _plan) in plans.iter().enumerate() {
                compute_plan_signature(pi, _plan.cast(), output);
                if i != plans.len() - 1 {
                    output.push(',');
                }
            }
            output.push(']');
        }
        NodeTag::T_BitmapOr => {
            let rbitmapor = &*(node as *const BitmapOr);
            let plans = pgrx::list::List::<*mut core::ffi::c_void>::downcast_ptr(rbitmapor.bitmapplans).unwrap();

            output.push_str("BitmapOr [");
            for (i, _plan) in plans.iter().enumerate() {
                compute_plan_signature(pi, _plan.cast(), output);
                if i != plans.len() - 1 {
                    output.push(',');
                }
            }
            output.push(']');
        }
        NodeTag::T_Result => {
            output.push_str("Result");
        }
        NodeTag::T_Append => {
            let rappend = &*(node as *const Append);
            let plans = pgrx::list::List::<*mut core::ffi::c_void>::downcast_ptr(rappend.appendplans).unwrap();

            output.push_str("Append [");
            for (i, _plan) in plans.iter().enumerate() {
                compute_plan_signature(pi, _plan.cast(), output);
                if i != plans.len() - 1 {
                    output.push(',');
                }
            }
            output.push(']');
        }
        NodeTag::T_SubqueryScan => {
            let sscan = &*(node as *const SubqueryScan);
            output.push_str("SubqueryScan [");
            compute_plan_signature(pi, sscan.subplan, output);
            output.push(']');
        }
        _ => error!("Unhandled plan type: {:?}", (*node).type_)
    }

    if !(*node).lefttree.is_null() || !(*node).righttree.is_null() {
        output.push(' ');
        output.push('(');

        if !(*node).lefttree.is_null() {
            compute_plan_signature(pi, (*node).lefttree, output);
            if !(*node).righttree.is_null() {
                output.push(',');
            }
        }

        if !(*node).righttree.is_null() {
            compute_plan_signature(pi, (*node).righttree, output);
        }

        output.push(')');
    }
}
