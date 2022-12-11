#include "postgres.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"
#include "catalog/namespace.h"

#include "stattuple.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

PG_FUNCTION_INFO_V1(phantom_start_worker);
PG_FUNCTION_INFO_V1(phantom_stop_worker);
PG_FUNCTION_INFO_V1(phantom_worker_exists);

#define NUM_WORKITEMS	16
#define NUM_WORKERS 4

/** Metadata for a single worker. */
typedef struct PhantomWorker
{
	BackgroundWorkerHandle handle;
	Oid dbid;
	pid_t pid;
	int32_t naptime;
	int32_t num_tbls;
	Oid tbls[NUM_WORKITEMS];
	bool sent_term;
} PhantomWorker;

/** Metadata for shared state. */
typedef struct PhantomSharedState
{
	LWLock *lock;
	struct PhantomWorker workers[NUM_WORKERS];
} PhantomSharedState;

static PhantomSharedState *phantom = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
void phantom_worker_main(Datum) pg_attribute_noreturn();

static void
phantom_shmem_startup(void)
{
	bool		found;

	/** Invoke previous startup hook. */
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/** Attach shared memory state. */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	phantom = ShmemInitStruct("phantom", sizeof(PhantomSharedState), &found);
	if (!found)
	{
		memset(phantom, 0x00, sizeof(PhantomSharedState));
		phantom->lock = &(GetNamedLWLockTranche("phantom"))->lock;
	}
	LWLockRelease(AddinShmemInitLock);
}

/**
 * Entrypoint of this module.
 */
void
_PG_init(void)
{
	/** Initialize shared resources. */
	RequestAddinShmemSpace(sizeof(PhantomSharedState));
	RequestNamedLWLockTranche("phantom", 1);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = phantom_shmem_startup;
}

void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
}

void phantom_worker_main(Datum main_arg)
{
	long nap_millis = 0;
	TimestampTz current_time = 0;
	struct PhantomWorker *worker = (struct PhantomWorker*)DatumGetPointer(main_arg);
	char* page = palloc(BLCKSZ);

	/** Mark default txns as read-only. */
	DefaultXactReadOnly = true;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnectionByOid(worker->dbid, InvalidOid, 0);

	/** Perform auxiliary initialization. */
	phantom_init();

	/*
	 * Main loop: do this until SIGTERM is received and processed by ProcessInterrupts.
	 */
	HOLD_INTERRUPTS();
	for (;;)
	{
		int i = 0;
		long secs = 0;
		int microsecs = 0;
		if (nap_millis > 0)
		{
			/** Wait till we next need to execute. */
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 nap_millis,
							 PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
		}

		/** Block signals so we CHECK.. and ProcDiePending is consistent. */
		BackgroundWorkerBlockSignals();

		if (ProcDiePending)
		{
			/** Dump data out before die-ing. */
			phantom_dump();
			worker->dbid = InvalidOid;
		}

		RESUME_INTERRUPTS();

		/** Check whether interrupts need to be assessed. */
		CHECK_FOR_INTERRUPTS();

		/** Now unblock the signals again. */
		HOLD_INTERRUPTS();
		BackgroundWorkerUnblockSignals();

		/**
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/** Stat all the relations. */
		current_time = GetCurrentTimestamp();
		for (i = 0; i < worker->num_tbls; i++)
		{
			phantom_stat_relation(worker->tbls[i], page);
		}

		TimestampDifference(current_time, GetCurrentTimestamp(), &secs, &microsecs);
		if (secs >= worker->naptime)
		{
			/** Don't wait. */
			nap_millis = 0;
		}
		else
		{
			/** Compute adjusted sleep time between sample. */
			long wait_millis = secs * 1000L + microsecs / 1000L;
			nap_millis = (worker->naptime * 1000L) - wait_millis;
		}
	}
}

Datum
phantom_start_worker(PG_FUNCTION_ARGS)
{
	char relname[NAMEDATALEN];
	struct PhantomWorker *shworker;
	int i;

	/** Tables. */
	Datum *tbl_datums;
	bool *tbl_nulls;
	int tbl_num;

	/** Background worker. */
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	int32 nap = PG_GETARG_INT32(1);
	ArrayType *tbl_array = PG_GETARG_ARRAYTYPE_P(0);

	/** Extract all the tables. */
	deconstruct_array(tbl_array,
					  TEXTOID, -1, false, TYPALIGN_INT,
					  &tbl_datums, &tbl_nulls, &tbl_num);

	if (tbl_num == 0)
	{
		elog(ERROR, "Unable to deconstruct list of tables.");
	}

	for (i = 0; i < tbl_num; i++)
	{
		if (tbl_nulls[i])
		{
			elog(ERROR, "NULL table specified.");
		}
	}

	/** Find a free worker slot to occupy. */
	LWLockAcquire(phantom->lock, LW_EXCLUSIVE);
	for (i = 0; i < NUM_WORKERS; i++)
	{
		if (phantom->workers[i].dbid == MyDatabaseId)
		{
			LWLockRelease(phantom->lock);
			elog(ERROR, "Worker already exists for database (%d)", MyDatabaseId);
		}
		else if (phantom->workers[i].dbid == InvalidOid)
		{
			break;
		}
	}

	if (i == NUM_WORKERS)
	{
		LWLockRelease(phantom->lock);
		elog(ERROR, "No more free workers available to process database.");
	}

	/** Mark that we've occupied this slot. */
	shworker = &phantom->workers[i];
	shworker->dbid = MyDatabaseId;
	LWLockRelease(phantom->lock);

	/** Copy relevant metadata over. */
	shworker->naptime = nap;
	shworker->num_tbls = tbl_num;
	for (i = 0; i < tbl_num; i++)
	{
		if (VARSIZE_ANY_EXHDR(tbl_datums[i]) >= NAMEDATALEN)
		{
			elog(ERROR, "Table at index %d is too long.", i);
		}

		/** Extract the relation name. */
		memcpy(relname, VARDATA_ANY(tbl_datums[i]), VARSIZE_ANY_EXHDR(tbl_datums[i]));
		relname[VARSIZE_ANY_EXHDR(tbl_datums[i])] = '\0';

		shworker->tbls[i] = RelnameGetRelid(relname);
		if (shworker->tbls[i] == InvalidOid)
		{
			shworker->dbid = InvalidOid;
			elog(ERROR, "Unknown table encountered (%s)", relname);
		}
	}

	/** Setup the background worker. */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "phantom");
	sprintf(worker.bgw_function_name, "phantom_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "phantom worker %d", MyDatabaseId);
	snprintf(worker.bgw_type, BGW_MAXLEN, "phantom_worker");
	worker.bgw_main_arg = PointerGetDatum(shworker);
	worker.bgw_notify_pid = MyProcPid;

	/** Register the worker. */
	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		shworker->dbid = InvalidOid;
		PG_RETURN_NULL();
	}

	/** Wait for the background worker to startup. */
	status = WaitForBackgroundWorkerStartup(handle, &pid);

	if (status == BGWH_STOPPED)
	{
		shworker->dbid = InvalidOid;
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));
	}

	if (status == BGWH_POSTMASTER_DIED)
	{
		shworker->dbid = InvalidOid;
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	}

	Assert(status == BGWH_STARTED);
	memcpy(&shworker->handle, handle, sizeof(BackgroundWorkerHandle));
	shworker->pid = pid;
	PG_RETURN_INT32(pid);
}

Datum
phantom_stop_worker(PG_FUNCTION_ARGS)
{
	int i = 0;
	struct PhantomWorker* shworker = NULL;
	LWLockAcquire(phantom->lock, LW_EXCLUSIVE);
	for (i = 0; i < NUM_WORKERS; i++)
	{
		if (phantom->workers[i].dbid == MyDatabaseId)
		{
			shworker = &phantom->workers[i];
			break;
		}
	}

	/** Couldn't find the target worker we want to terminate. */
	if (shworker == NULL || shworker->sent_term)
	{
		LWLockRelease(phantom->lock);
		PG_RETURN_INT32(0);
	}

	shworker->sent_term = true;
	LWLockRelease(phantom->lock);

	/** Kill the background worker. */
	TerminateBackgroundWorker(&shworker->handle);
	PG_RETURN_INT32(1);
}

Datum
phantom_worker_exists(PG_FUNCTION_ARGS)
{
	int i = 0;
	bool ret = true;
	struct PhantomWorker* shworker = NULL;
	LWLockAcquire(phantom->lock, LW_EXCLUSIVE);
	for (i = 0; i < NUM_WORKERS; i++)
	{
		if (phantom->workers[i].dbid == MyDatabaseId)
		{
			shworker = &phantom->workers[i];
			break;
		}
	}

	/** Couldn't find the target worker we want to terminate. */
	if (shworker == NULL)
	{
		ret = false;
	}

	LWLockRelease(phantom->lock);
	PG_RETURN_INT32(ret);
}
