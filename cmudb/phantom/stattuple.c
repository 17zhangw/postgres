/*
 * contrib/pgstattuple/pgstattuple.c
 *
 * Copyright (c) 2001,2002	Tatsuo Ishii
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose, without fee, and without a
 * written agreement is hereby granted, provided that the above
 * copyright notice and this paragraph and the following two
 * paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT,
 * INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
 * IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

#include "postgres.h"

#include "access/multixact.h"
#include "access/gist_private.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "catalog/pg_am_d.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/varlena.h"
#include "utils/fmgroids.h"
#include "pgstat.h"

#include "stattuple.h"

static struct PhantomStatsChunk* phantom_front_stats = NULL;
static struct PhantomStatsChunk* phantom_head_stats = NULL;

struct PhantomStatTuple* GetPhantomStatsEntry(void)
{
	struct PhantomStatTuple* stats = NULL;
	if (phantom_head_stats != NULL)
	{
		if (phantom_head_stats->num_stats < PHANTOM_STATS_PER_CHUNK)
		{
			stats = &phantom_head_stats->stats[phantom_head_stats->num_stats];
			phantom_head_stats->num_stats++;
		}
		else
		{
			struct PhantomStatsChunk* new_head = MemoryContextAllocZero(TopMemoryContext, 4096);
			if (new_head != NULL)
			{
				stats = &new_head->stats[0];
				new_head->next = NULL;
				new_head->num_stats = 1;

				phantom_head_stats->next = new_head;
				phantom_head_stats = new_head;
			}
		}
	}

	return stats;
}

static HeapTuple
StatScanPgRelation(Oid targetRelId, bool indexOK)
{
	HeapTuple	pg_class_tuple;
	Relation	pg_class_desc;
	SysScanDesc pg_class_scan;
	ScanKeyData key[1];
	Snapshot	snapshot = NULL;

	/*
	 * If something goes wrong during backend startup, we might find ourselves
	 * trying to read pg_class before we've selected a database.  That ain't
	 * gonna work, so bail out with a useful error message.  If this happens,
	 * it probably means a relcache entry that needs to be nailed isn't.
	 */
	if (!OidIsValid(MyDatabaseId))
		return NULL;

	/*
	 * form a scan key
	 */
	ScanKeyInit(&key[0],
			Anum_pg_class_oid,
			BTEqualStrategyNumber, F_OIDEQ,
			ObjectIdGetDatum(targetRelId));

	/*
	 * Open pg_class and fetch a tuple.  Force heap scan if we haven't yet
	 * built the critical relcache entries (this includes initdb and startup
	 * without a pg_internal.init file).  The caller can also force a heap
	 * scan by setting indexOK == false.
	 */
	pg_class_desc = table_open(RelationRelationId, AccessShareLock);

	pg_class_scan = systable_beginscan(pg_class_desc, ClassOidIndexId,
			indexOK && criticalRelcachesBuilt,
			snapshot,
			1, key);

	pg_class_tuple = systable_getnext(pg_class_scan);

	/*
	 * Must copy tuple before releasing buffer.
	 */
	if (HeapTupleIsValid(pg_class_tuple))
		pg_class_tuple = heap_copytuple(pg_class_tuple);

	/* all done */
	systable_endscan(pg_class_scan);
	table_close(pg_class_desc, AccessShareLock);

	return pg_class_tuple;
}

HTSV_Result
static StatHeapTupleSatisfiesVacuumHorizon(HeapTuple htup, TransactionId *dead_after)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);
	Assert(dead_after != NULL);

	*dead_after = InvalidTransactionId;

	/*
	 * Has inserting transaction committed?
	 *
	 * If the inserting transaction aborted, then the tuple was never visible
	 * to any other transaction, so we can delete it immediately.
	 */
	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return HEAPTUPLE_DEAD;
		/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			if (TransactionIdIsInProgress(xvac))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			if (TransactionIdDidCommit(xvac))
			{
				return HEAPTUPLE_DEAD;
			}
		}
		/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			if (TransactionIdIsInProgress(xvac))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			if (!TransactionIdDidCommit(xvac))
			{
				return HEAPTUPLE_DEAD;
			}
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			/* only locked? run infomask-only check first, for performance */
			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) ||
					HeapTupleHeaderIsOnlyLocked(tuple))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			/* inserted and then deleted by same xact */
			if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(tuple)))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			/* deleting subtransaction must have aborted */
			return HEAPTUPLE_INSERT_IN_PROGRESS;
		}
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
		{
			/*
			 * It'd be possible to discern between INSERT/DELETE in progress
			 * here by looking at xmax - but that doesn't seem beneficial for
			 * the majority of callers and even detrimental for some. We'd
			 * rather have callers look at/wait for xmin than xmax. It's
			 * always correct to return INSERT_IN_PROGRESS because that's
			 * what's happening from the view of other backends.
			 */
			return HEAPTUPLE_INSERT_IN_PROGRESS;
		}
		else if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			return HEAPTUPLE_DEAD;
		}

		/*
		 * At this point the xmin is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Okay, the inserter committed, so it was good at some point.  Now what
	 * about the deleting transaction?
	 */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return HEAPTUPLE_LIVE;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		/*
		 * "Deleting" xact really only locked it, so the tuple is live in any
		 * case.  However, we should make sure that either XMAX_COMMITTED or
		 * XMAX_INVALID gets set once the xact is gone, to reduce the costs of
		 * examining the tuple for future xacts.
		 */
		if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
		{
			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				/*
				 * If it's a pre-pg_upgrade tuple, the multixact cannot
				 * possibly be running; otherwise have to check.
				 */
				if (!HEAP_LOCKED_UPGRADED(tuple->t_infomask) &&
						MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true))
				{
					return HEAPTUPLE_LIVE;
				}
			}
			else
			{
				if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
					return HEAPTUPLE_LIVE;
			}
		}

		/*
		 * We don't really care whether xmax did commit, abort or crash. We
		 * know that xmax did lock the tuple, but it did not and will never
		 * actually update it.
		 */

		return HEAPTUPLE_LIVE;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax = HeapTupleGetUpdateXid(tuple);

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsInProgress(xmax))
		{
			return HEAPTUPLE_DELETE_IN_PROGRESS;
		}
		else if (TransactionIdDidCommit(xmax))
		{
			/*
			 * The multixact might still be running due to lockers.  Need to
			 * allow for pruning if below the xid horizon regardless --
			 * otherwise we could end up with a tuple where the updater has to
			 * be removed due to the horizon, but is not pruned away.  It's
			 * not a problem to prune that tuple, because any remaining
			 * lockers will also be present in newer tuple versions.
			 */
			*dead_after = xmax;
			return HEAPTUPLE_RECENTLY_DEAD;
		}

		return HEAPTUPLE_LIVE;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
		{
			return HEAPTUPLE_DELETE_IN_PROGRESS;
		}
		else if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			return HEAPTUPLE_LIVE;
		}

		/*
		 * At this point the xmax is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Deleter committed, allow caller to check if it was recent enough that
	 * some open transactions could still see the tuple.
	 */
	*dead_after = HeapTupleHeaderGetRawXmax(tuple);
	return HEAPTUPLE_RECENTLY_DEAD;
}

	HTSV_Result
static StatHeapTupleSatisfiesVacuum(HeapTuple htup)
{
	TransactionId dead_after = InvalidTransactionId;
	HTSV_Result res;

	res = StatHeapTupleSatisfiesVacuumHorizon(htup, &dead_after);

	if (res == HEAPTUPLE_RECENTLY_DEAD)
	{
		Assert(TransactionIdIsValid(dead_after));
	}
	else
	{
		Assert(!TransactionIdIsValid(dead_after));
	}

	return res;
}

extern void phantom_init(void)
{
	phantom_front_stats = phantom_head_stats = palloc0(4096);
	phantom_head_stats->next = NULL;
	phantom_head_stats->num_stats = 0;
}

extern void phantom_stat_relation(Oid relid, char* page)
{
	HeapTuple pg_class_tuple;
	Form_pg_class relp;
	RelFileNode rd_node;
	SMgrRelation smgr;
	BlockNumber nblocks;
	BlockNumber block;
	PhantomStatTuple* stat = NULL;

	/** This is really sad because we need the xact still. */
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();

	/** Get the tuple from pg_class */
	pg_class_tuple = StatScanPgRelation(relid, true);
	if (!HeapTupleIsValid(pg_class_tuple))
	{
		/** Commit. */
		CommitTransactionCommand();
		return;
	}
	/** Get the relation */
	relp = (Form_pg_class) GETSTRUCT(pg_class_tuple);
	/** Get the relation file node */
	rd_node.spcNode = MyDatabaseTableSpace;
	rd_node.dbNode = MyDatabaseId;
	rd_node.relNode = relp->relfilenode;
	/** Free tuple */
	heap_freetuple(pg_class_tuple);

	/** Get the stats entry */
	stat = GetPhantomStatsEntry();
	stat->relid = relid;
	stat->ts = GetCurrentTimestamp();

	/** Open the actual relation on disk. Bypass the buffer pool. */
	smgr = smgropen(rd_node, InvalidBackendId);
	nblocks = smgrnblocks(smgr, MAIN_FORKNUM);

	/** Set the table len at this moment. */
	stat->table_len = (uint64) nblocks * BLCKSZ;

	for (block = 0; block < nblocks; block++)
	{
		OffsetNumber lineoff;
		ItemId lpp;

		smgrread(smgr, MAIN_FORKNUM, block, page);
		if (!PageIsNew(page))
			stat->free_space += PageGetHeapFreeSpace(page);
		else
			stat->free_space += BLCKSZ - SizeOfPageHeaderData;

		/** Skip checking if empty */
		if (PageIsNew(page) || PageIsEmpty(page))
		{
			continue;
		}

		if (!PageIsVerifiedExtended((Page)page, block, 0))
		{
			/** Ignore pages that seem corrupt. */
			continue;
		}

		for (lineoff = FirstOffsetNumber, lpp = PageGetItemId(page, lineoff);
				lineoff <= PageGetMaxOffsetNumber(page);
				lineoff++, lpp++)
		{
			HeapTupleData loctup;
			if (!ItemIdIsUsed(lpp) || ItemIdIsRedirected(lpp) || ItemIdIsDead(lpp))
			{
				continue;
			}

			ItemPointerSet(&(loctup.t_self), block, lineoff);
			loctup.t_data = (HeapTupleHeader) PageGetItem(page, lpp);
			loctup.t_len = ItemIdGetLength(lpp);
			loctup.t_tableOid = relid;
			switch (StatHeapTupleSatisfiesVacuum(&loctup))
			{
				case HEAPTUPLE_LIVE:
				case HEAPTUPLE_DELETE_IN_PROGRESS:
					stat->tuple_len += loctup.t_len;
					stat->tuple_count++;
					break;
				case HEAPTUPLE_DEAD:
				case HEAPTUPLE_RECENTLY_DEAD:
				case HEAPTUPLE_INSERT_IN_PROGRESS:
					stat->dead_tuple_len += loctup.t_len;
					stat->dead_tuple_count++;
					break;
				default:
					elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
					break;
			}
		}
	}

	/** Commit. */
	CommitTransactionCommand();

	smgrclose(smgr);
}

char PHANTOM_STATS_CSV_HEADER[] = "oid,time,table_len,tuple_count,tuple_len,dead_tuple_count,dead_tuple_len,free_space\n";

extern void phantom_dump(void)
{
	FILE* qfile;
	char path[256];
	struct PhantomStatsChunk* statschunk = NULL;
	StringInfoData buf;

	/** Get the path to write data to. */
	initStringInfo(&buf);
	snprintf(path, 256, "%s/phantom_stats_%d_%d.csv", PGSTAT_STAT_PERMANENT_DIRECTORY, MyDatabaseId, MyProcPid);

	qfile = AllocateFile(path, PG_BINARY_W);
	if (qfile == NULL)
	{
		goto error;
	}

	/** Write the header */
	if (fwrite(PHANTOM_STATS_CSV_HEADER, 1, sizeof(PHANTOM_STATS_CSV_HEADER) - 1, qfile) != sizeof(PHANTOM_STATS_CSV_HEADER) - 1)
	{
		goto error;
	}

	statschunk = phantom_front_stats;
	while (statschunk != NULL)
	{
		for (int i = 0; i < statschunk->num_stats; i++)
		{
			/** Write each stats entry out. */
			struct PhantomStatTuple* stat = &statschunk->stats[i];
			appendStringInfo(&buf, "%d,", stat->relid);
			appendStringInfo(&buf, "%ld,", stat->ts);
			appendStringInfo(&buf, "%lu,", stat->table_len);
			appendStringInfo(&buf, "%lu,", stat->tuple_count);
			appendStringInfo(&buf, "%lu,", stat->tuple_len);
			appendStringInfo(&buf, "%lu,", stat->dead_tuple_count);
			appendStringInfo(&buf, "%lu,", stat->dead_tuple_len);
			appendStringInfo(&buf, "%lu", stat->free_space);
			appendStringInfoChar(&buf, '\n');
			if (fwrite(buf.data, 1, buf.len, qfile) != buf.len)
				goto error;
			resetStringInfo(&buf);
		}
		statschunk = statschunk->next;
	}
	FreeFile(qfile);
	return;

error:
	if (qfile)
		FreeFile(qfile);
}
