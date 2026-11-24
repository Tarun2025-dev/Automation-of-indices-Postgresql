
#include "postgres.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;

static int monitor_interval = 30;
static char *monitor_database = "test1";
static int inactivity_threshold = 1;

static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

PGDLLEXPORT void index_auto_drop_main(Datum main_arg);
static void monitor_index_usage(void);
static void drop_inactive_indexes(void);

static void
index_auto_drop_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void
index_auto_drop_sighup(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

void
_PG_init(void)
{
    BackgroundWorker worker;

    DefineCustomIntVariable(
        "index_auto_drop.interval",
        "Seconds between index usage checks.",
        NULL, &monitor_interval,
        30, 1, 86400,
        PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomStringVariable(
        "index_auto_drop.database",
        "Database monitored by the index usage worker.",
        NULL, &monitor_database,
        "test1",
        PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable(
        "index_auto_drop.inactivity_threshold",
        "Inactive checks before an index becomes a candidate.",
        NULL, &inactivity_threshold,
        1, 1, 1000000,
        PGC_SIGHUP, 0, NULL, NULL, NULL);

    MarkGUCPrefixReserved("index_auto_drop");

    if (!process_shared_preload_libraries_in_progress)
        return;

    memset(&worker, 0, sizeof(worker));

    worker.bgw_flags =
        BGWORKER_SHMEM_ACCESS |
        BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;

    snprintf(worker.bgw_name, BGW_MAXLEN,
             "index auto drop monitor");
    snprintf(worker.bgw_type, BGW_MAXLEN,
             "index auto drop monitor");
    snprintf(worker.bgw_library_name, BGW_MAXLEN,
             "index_auto_drop");
    snprintf(worker.bgw_function_name, BGW_MAXLEN,
             "index_auto_drop_main");

    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}

/*
 * Update scan-count snapshots for eligible indexes.
 * Increased OR decreased scan counts reset inactivity.
 * Unchanged scan counts increment inactivity.
 */
static void
monitor_index_usage(void)
{
    int rc;
    uint64 row;

    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "index_auto_drop: SPI_connect failed");

    rc = SPI_execute(
        "INSERT INTO public.index_usage_monitor "
        " (index_oid, index_name, table_name, previous_scans, "
        "  inactive_checks, last_checked) "
        "SELECT i.indexrelid, i.indexrelname, i.relname, i.idx_scan, 0, now() "
        "FROM pg_stat_user_indexes i "
        "JOIN pg_index pi ON pi.indexrelid = i.indexrelid "
        "JOIN pg_class tbl ON tbl.oid = i.relid "
        "JOIN pg_namespace ns ON ns.oid = tbl.relnamespace "
        "WHERE ns.nspname NOT IN ('pg_catalog', 'information_schema') "
        "  AND tbl.relname <> 'index_usage_monitor' "
        "  AND pi.indisvalid AND pi.indisready AND pi.indislive "
        "  AND NOT pi.indisprimary "
        "  AND NOT pi.indisunique "
        "  AND NOT pi.indisexclusion "
        "  AND NOT EXISTS ("
        "      SELECT 1 FROM pg_constraint c "
        "      WHERE c.conindid = i.indexrelid) "
        "ON CONFLICT (index_oid) DO UPDATE SET "
        " inactive_checks = CASE "
        "   WHEN EXCLUDED.previous_scans <> "
        "        index_usage_monitor.previous_scans THEN 0 "
        "   ELSE index_usage_monitor.inactive_checks + 1 "
        " END, "
        " previous_scans = EXCLUDED.previous_scans, "
        " index_name = EXCLUDED.index_name, "
        " table_name = EXCLUDED.table_name, "
        " last_checked = now()",
        false, 0);

    if (rc < 0)
        elog(ERROR, "index_auto_drop: snapshot update failed: %d", rc);

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

    /* Log current candidates. */
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "index_auto_drop: SPI_connect failed");

    rc = SPI_execute(
        "SELECT index_name, table_name, inactive_checks "
        "FROM public.index_usage_monitor "
        "WHERE inactive_checks >= "
        "current_setting("
        " 'index_auto_drop.inactivity_threshold')::integer",
        true, 0);

    if (rc < 0)
        elog(ERROR, "index_auto_drop: candidate query failed: %d", rc);

    for (row = 0; row < SPI_processed; row++)
    {
        bool isnull;
        char *index_name;
        char *table_name;
        int checks;

        index_name = SPI_getvalue(
            SPI_tuptable->vals[row], SPI_tuptable->tupdesc, 1);
        table_name = SPI_getvalue(
            SPI_tuptable->vals[row], SPI_tuptable->tupdesc, 2);
        checks = DatumGetInt32(
            SPI_getbinval(SPI_tuptable->vals[row],
                          SPI_tuptable->tupdesc, 3, &isnull));

        elog(LOG,
             "index_auto_drop: candidate index=%s table=%s inactive_checks=%d threshold=%d",
             index_name ? index_name : "(unknown)",
             table_name ? table_name : "(unknown)",
             checks, inactivity_threshold);
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
}

/*
 * Drop every eligible index.
 *
 * First collect candidate OIDs. Then process each candidate in its own
 * transaction and recheck eligibility immediately before dropping it.
 * OIDs are used to identify indexes safely; schema/index names are quoted
 * by PostgreSQL's format('%I.%I', ...) before being used in DROP INDEX.
 */
static void
drop_inactive_indexes(void)
{
    Oid *candidate_oids = NULL;
    uint64 candidate_count = 0;
    uint64 row;
    int rc;

    pgstat_clear_snapshot();

    /* Collect OIDs into TopMemoryContext so they survive SPI/transaction end. */
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "index_auto_drop: SPI_connect failed while collecting candidates");

    rc = SPI_execute(
        "SELECT m.index_oid "
        "FROM public.index_usage_monitor AS m "
        "JOIN pg_stat_user_indexes AS s ON s.indexrelid = m.index_oid "
        "JOIN pg_index AS pi ON pi.indexrelid = m.index_oid "
        "JOIN pg_class AS idx ON idx.oid = m.index_oid "
        "JOIN pg_namespace AS ns ON ns.oid = idx.relnamespace "
        "WHERE m.inactive_checks >= current_setting(" 
        " 'index_auto_drop.inactivity_threshold')::integer "
        "  AND s.idx_scan = m.previous_scans "
        "  AND ns.nspname NOT IN ('pg_catalog', 'information_schema') "
        "  AND ns.nspname NOT LIKE 'pg_toast%' "
        "  AND m.table_name <> 'index_usage_monitor' "
        "  AND pi.indisvalid AND pi.indisready AND pi.indislive "
        "  AND NOT pi.indisprimary "
        "  AND NOT pi.indisunique "
        "  AND NOT pi.indisexclusion "
        "  AND NOT EXISTS (SELECT 1 FROM pg_constraint AS c "
        "                  WHERE c.conindid = m.index_oid) "
        "ORDER BY m.index_oid",
        true, 0);

    if (rc != SPI_OK_SELECT)
        elog(ERROR, "index_auto_drop: candidate query failed: %d", rc);

    candidate_count = SPI_processed;
    if (candidate_count > 0)
    {
        MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
        candidate_oids = palloc(sizeof(Oid) * candidate_count);
        MemoryContextSwitchTo(oldcontext);

        for (row = 0; row < candidate_count; row++)
        {
            bool isnull;
            Datum d = SPI_getbinval(SPI_tuptable->vals[row],
                                    SPI_tuptable->tupdesc, 1, &isnull);
            if (isnull)
                elog(ERROR, "index_auto_drop: candidate index OID is NULL");
            candidate_oids[row] = DatumGetObjectId(d);
        }
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

    elog(LOG, "index_auto_drop: found %llu inactive index candidate(s)",
         (unsigned long long) candidate_count);

    /* Process each candidate independently, rechecking before the DROP. */
    for (row = 0; row < candidate_count; row++)
    {
        Oid target_oid = candidate_oids[row];
        char *qualified_name = NULL;
        char *drop_command = NULL;
        MemoryContext oldcontext;

        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());

        if (SPI_connect() != SPI_OK_CONNECT)
            elog(ERROR, "index_auto_drop: SPI_connect failed for candidate OID %u", target_oid);

        rc = SPI_execute_with_args(
            "SELECT format('%I.%I', ns.nspname, idx.relname) "
            "FROM public.index_usage_monitor AS m "
            "JOIN pg_stat_user_indexes AS s ON s.indexrelid = m.index_oid "
            "JOIN pg_index AS pi ON pi.indexrelid = m.index_oid "
            "JOIN pg_class AS idx ON idx.oid = m.index_oid "
            "JOIN pg_namespace AS ns ON ns.oid = idx.relnamespace "
            "WHERE m.index_oid = $1 "
            "  AND m.inactive_checks >= current_setting(" 
            "      'index_auto_drop.inactivity_threshold')::integer "
            "  AND s.idx_scan = m.previous_scans "
            "  AND ns.nspname NOT IN ('pg_catalog', 'information_schema') "
            "  AND ns.nspname NOT LIKE 'pg_toast%' "
            "  AND m.table_name <> 'index_usage_monitor' "
            "  AND pi.indisvalid AND pi.indisready AND pi.indislive "
            "  AND NOT pi.indisprimary "
            "  AND NOT pi.indisunique "
            "  AND NOT pi.indisexclusion "
            "  AND NOT EXISTS (SELECT 1 FROM pg_constraint AS c "
            "                  WHERE c.conindid = m.index_oid)",
            1, (Oid[]) {OIDOID}, (Datum[]) {ObjectIdGetDatum(target_oid)},
            (char[]) {' '}, true, 1);

        if (rc != SPI_OK_SELECT)
            elog(ERROR, "index_auto_drop: eligibility recheck failed for OID %u", target_oid);

        if (SPI_processed == 0)
        {
            elog(LOG,
                 "index_auto_drop: keeping candidate OID %u; it is no longer eligible",
                 target_oid);
            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
            continue;
        }

        {
            char *result = SPI_getvalue(SPI_tuptable->vals[0],
                                        SPI_tuptable->tupdesc, 1);
            if (result == NULL)
                elog(ERROR, "index_auto_drop: qualified name is NULL for OID %u", target_oid);

            /* SPI-owned strings do not survive SPI_finish/another SPI call. */
            oldcontext = MemoryContextSwitchTo(TopMemoryContext);
            qualified_name = pstrdup(result);
            drop_command = psprintf("DROP INDEX %s", qualified_name);
            MemoryContextSwitchTo(oldcontext);
        }

        elog(LOG, "index_auto_drop: attempting to drop index %s (OID %u)",
             qualified_name, target_oid);

        rc = SPI_execute(drop_command, false, 0);
        if (rc != SPI_OK_UTILITY)
            elog(ERROR, "index_auto_drop: DROP INDEX failed for %s (rc=%d)",
                 qualified_name, rc);

        {
            Oid argtypes[1] = {OIDOID};
            Datum values[1] = {ObjectIdGetDatum(target_oid)};
            char nulls[1] = {' '};

            rc = SPI_execute_with_args(
                "DELETE FROM public.index_usage_monitor WHERE index_oid = $1",
                1, argtypes, values, nulls, false, 0);
            if (rc < 0)
                elog(ERROR, "index_auto_drop: monitor cleanup failed for OID %u", target_oid);
        }

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();

        elog(LOG, "index_auto_drop: successfully dropped index %s (OID %u)",
             qualified_name, target_oid);

        pfree(drop_command);
        pfree(qualified_name);
    }

    if (candidate_oids != NULL)
        pfree(candidate_oids);
}

void
index_auto_drop_main(Datum main_arg)
{
    pqsignal(SIGTERM, index_auto_drop_sigterm);
    pqsignal(SIGHUP, index_auto_drop_sighup);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection(monitor_database, NULL, 0);

    elog(LOG,
         "index_auto_drop: worker started database=%s interval=%d threshold=%d",
         monitor_database, monitor_interval, inactivity_threshold);

    for (;;)
    {
        int rc;

        rc = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            (long) monitor_interval * 1000L,
            PG_WAIT_EXTENSION);

        ResetLatch(MyLatch);

        if (got_sigterm)
            proc_exit(0);

        if (got_sighup)
        {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);

            elog(LOG,
                 "index_auto_drop: configuration reloaded interval=%d threshold=%d database=%s",
                 monitor_interval, inactivity_threshold, monitor_database);
        }

        CHECK_FOR_INTERRUPTS();

        if (rc & WL_TIMEOUT)
        {
            elog(LOG,
                 "index_auto_drop: running usage check database=%s",
                 monitor_database);

            monitor_index_usage();
            drop_inactive_indexes();
        }
    }
}

