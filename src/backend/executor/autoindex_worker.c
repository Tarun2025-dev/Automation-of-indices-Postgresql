#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/autoindex.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"

typedef enum AutoIndexWorkerResult
{
	AUTO_INDEX_WORKER_CREATED,
	AUTO_INDEX_WORKER_ALREADY_EXISTS,
	AUTO_INDEX_WORKER_INVALID
} AutoIndexWorkerResult;

bool AutoIndexLaunchBuilder(uint32 job_id)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle = NULL;

	MemSet(&worker, 0, sizeof(worker));

	snprintf(worker.bgw_name,
			 BGW_MAXLEN,
			 "auto-index builder %u",
			 job_id);

	snprintf(worker.bgw_type,
			 BGW_MAXLEN,
			 "auto-index builder");

	worker.bgw_flags =
		BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;

	worker.bgw_start_time =
		BgWorkerStart_RecoveryFinished;

	worker.bgw_restart_time =
		BGW_NEVER_RESTART;

	snprintf(worker.bgw_library_name,
			 BGW_MAXLEN,
			 "postgres");

	snprintf(worker.bgw_function_name,
			 BGW_MAXLEN,
			 "AutoIndexBuilderMain");

	worker.bgw_main_arg =
		UInt32GetDatum(job_id);

	worker.bgw_notify_pid = 0;

	return RegisterDynamicBackgroundWorker(&worker, &handle);
}

static Oid
autoindex_find_supporting_index(Relation rel,
								const AutoIndexBuildJob *job)
{
	List *index_list;
	ListCell *lc;
	Oid result = InvalidOid;

	index_list = RelationGetIndexList(rel);

	foreach (lc, index_list)
	{
		Oid index_oid;
		Relation index_rel;
		bool matches = false;

		index_oid = lfirst_oid(lc);
		index_rel =
			index_open(index_oid, AccessShareLock);

		if (!index_rel->rd_index->indisvalid ||
			!index_rel->rd_index->indisready ||
			index_rel->rd_index->indnkeyatts < 1 ||
			index_rel->rd_index->indkey.values[0] !=
				job->attnum ||
			RelationGetIndexPredicate(index_rel) != NIL)
		{
			index_close(index_rel, AccessShareLock);
			continue;
		}

		if (OidIsValid(job->collation_oid) &&
			index_rel->rd_indcollation[0] !=
				job->collation_oid)
		{
			index_close(index_rel, AccessShareLock);
			continue;
		}

		switch (job->clause_kind)
		{
		case AUTO_INDEX_CLAUSE_NULL_TEST:
			matches =
				index_rel->rd_indam->amsearchnulls;
			break;

		case AUTO_INDEX_CLAUSE_SCALAR_ARRAY:
			if (index_rel->rd_indam->amsearcharray &&
				OidIsValid(job->operator_oid) &&
				get_op_opfamily_strategy(
					job->operator_oid,
					index_rel->rd_opfamily[0]) != 0)
				matches = true;
			break;

		case AUTO_INDEX_CLAUSE_OPERATOR:
			if (OidIsValid(job->operator_oid) &&
				get_op_opfamily_strategy(
					job->operator_oid,
					index_rel->rd_opfamily[0]) != 0)
				matches = true;
			break;
		}

		index_close(index_rel, AccessShareLock);

		if (matches)
		{
			result = index_oid;
			break;
		}
	}

	list_free(index_list);

	return result;
}

static AutoIndexWorkerResult
autoindex_create_index(const AutoIndexBuildJob *job,
					   Oid *result_index_oid)
{
	Relation rel;
	Form_pg_attribute attribute;
	Oid existing_index_oid;
	Oid namespace_oid;
	char *namespace_name;
	char *access_method_name;
	char *opclass_name;
	char *collation_name = NULL;
	char index_name[NAMEDATALEN];
	const char *qualified_table_name;
	StringInfoData command;
	int spi_result;

	*result_index_oid = InvalidOid;

	rel = try_table_open(job->relation_oid, ShareLock);

	if (rel == NULL)
		return AUTO_INDEX_WORKER_INVALID;

	if (rel->rd_rel->relkind != RELKIND_RELATION ||
		rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
		rel->rd_rel->relispartition ||
		IsCatalogRelation(rel) ||
		IsToastRelation(rel) ||
		job->attnum <= 0 ||
		job->attnum > RelationGetDescr(rel)->natts)
	{
		table_close(rel, ShareLock);
		return AUTO_INDEX_WORKER_INVALID;
	}

	attribute = TupleDescAttr(RelationGetDescr(rel),
							  job->attnum - 1);

	if (attribute->attisdropped)
	{
		table_close(rel, ShareLock);
		return AUTO_INDEX_WORKER_INVALID;
	}

	existing_index_oid =
		autoindex_find_supporting_index(rel, job);

	if (OidIsValid(existing_index_oid))
	{
		*result_index_oid = existing_index_oid;

		table_close(rel, ShareLock);

		return AUTO_INDEX_WORKER_ALREADY_EXISTS;
	}

	namespace_oid = RelationGetNamespace(rel);

	namespace_name =
		get_namespace_name(namespace_oid);

	access_method_name =
		get_am_name(job->access_method_oid);

	opclass_name =
		generate_opclass_name(job->opclass_oid);

	if (OidIsValid(job->collation_oid))
		collation_name =
			generate_collation_name(job->collation_oid);

	if (namespace_name == NULL ||
		access_method_name == NULL ||
		opclass_name == NULL)
	{
		table_close(rel, ShareLock);
		return AUTO_INDEX_WORKER_INVALID;
	}

	snprintf(index_name,
			 NAMEDATALEN,
			 "autoidx_%u_%d_%u_%u",
			 job->relation_oid,
			 job->attnum,
			 job->access_method_oid,
			 job->opclass_oid);

	qualified_table_name =
		quote_qualified_identifier(namespace_name,
								   RelationGetRelationName(rel));

	initStringInfo(&command);

	appendStringInfo(&command,
					 "CREATE INDEX %s ON %s USING %s (%s",
					 quote_identifier(index_name),
					 qualified_table_name,
					 quote_identifier(access_method_name),
					 quote_identifier(NameStr(attribute->attname)));

	if (collation_name != NULL)
		appendStringInfo(&command,
						 " COLLATE %s",
						 collation_name);

	appendStringInfo(&command,
					 " %s)",
					 opclass_name);

	table_close(rel, NoLock);
	rel = NULL;

	elog(LOG,
		 "auto-index phase 3: executing %s",
		 command.data);

	spi_result =
		SPI_execute(command.data, false, 0);

	if (spi_result != SPI_OK_UTILITY)
		ereport(ERROR,
				(errmsg("auto-index worker could not create index"),
				 errdetail("SPI returned %s",
						   SPI_result_code_string(spi_result))));

	CommandCounterIncrement();

	*result_index_oid =
		get_relname_relid(index_name, namespace_oid);

	if (!OidIsValid(*result_index_oid))
		ereport(ERROR,
				(errmsg("auto-index worker created index but could not find its OID")));

	return AUTO_INDEX_WORKER_CREATED;
}

void
AutoIndexBuilderMain(Datum main_arg)
{
	uint32 job_id;
	AutoIndexBuildJob job;

	job_id = DatumGetUInt32(main_arg);

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	if (!AutoIndexClaimBuildJob(job_id, &job))
	{
		elog(LOG,
			 "auto-index phase 3: job_id=%u was no longer queueable",
			 job_id);

		proc_exit(0);
	}

	PG_TRY();
	{
		AutoIndexWorkerResult result;
		Oid index_oid = InvalidOid;
		int spi_result;

		BackgroundWorkerInitializeConnectionByOid(job.database_oid,
												  InvalidOid,
												  0);

		StartTransactionCommand();

		spi_result = SPI_connect();

		if (spi_result != SPI_OK_CONNECT)
			ereport(ERROR,
					(errmsg("auto-index worker could not connect to SPI")));

		PushActiveSnapshot(GetTransactionSnapshot());

		elog(LOG,
			 "auto-index phase 3: candidate BUILDING "
			 "job_id=%u relation_oid=%u attnum=%d",
			 job.job_id,
			 job.relation_oid,
			 job.attnum);

		result = autoindex_create_index(&job, &index_oid);

		spi_result = SPI_finish();

		if (spi_result != SPI_OK_FINISH)
			ereport(ERROR,
					(errmsg("auto-index worker could not finish SPI")));

		PopActiveSnapshot();

		CommitTransactionCommand();

		switch (result)
		{
		case AUTO_INDEX_WORKER_CREATED:
			AutoIndexBuildJobFinished(job_id, index_oid);

			elog(LOG,
				 "auto-index phase 3: candidate CREATED "
				 "job_id=%u relation_oid=%u index_oid=%u",
				 job_id,
				 job.relation_oid,
				 index_oid);
			break;

		case AUTO_INDEX_WORKER_ALREADY_EXISTS:
			AutoIndexBuildJobSuperseded(job_id, index_oid);

			elog(LOG,
				 "auto-index phase 3: candidate SUPERSEDED "
				 "job_id=%u existing_index_oid=%u",
				 job_id,
				 index_oid);
			break;

		case AUTO_INDEX_WORKER_INVALID:
			AutoIndexBuildJobFailed(job_id);

			elog(LOG,
				 "auto-index phase 3: candidate INVALID "
				 "job_id=%u relation_oid=%u",
				 job_id,
				 job.relation_oid);
			break;
		}
	}
	PG_CATCH();
	{
		MemoryContext old_context;
		ErrorData *error_data;

		old_context = MemoryContextSwitchTo(TopMemoryContext);
		error_data = CopyErrorData();
		MemoryContextSwitchTo(old_context);

		FlushErrorState();

		if (IsTransactionState())
			AbortCurrentTransaction();

		AutoIndexBuildJobFailed(job_id);

		elog(LOG,
			 "auto-index phase 3: candidate FAILED "
			 "job_id=%u: %s",
			 job_id,
			 error_data->message);

		FreeErrorData(error_data);
	}
	PG_END_TRY();

	proc_exit(0);
}