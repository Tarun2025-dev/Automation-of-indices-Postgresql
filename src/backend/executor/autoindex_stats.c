#include "postgres.h"

#include "commands/defrem.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/autoindex.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

typedef struct AutoIndexSharedKey
{
	Oid database_oid;
	Oid relation_oid;
	int32 attnum;
	Oid access_method_oid;
	Oid opclass_oid;
	Oid collation_oid;
} AutoIndexSharedKey;

typedef struct AutoIndexSharedEntry
{
	AutoIndexSharedKey key;

	Oid opfamily_oid;
	Oid last_operator_oid;
	StrategyNumber last_strategy;
	AutoIndexClauseKind last_clause_kind;

	uint64 completed_scans;
	double total_seq_cost;
	double total_index_cost;
	double total_benefit;
	double estimated_build_cost;
	double last_selectivity;
	double estimated_index_pages;

	TimestampTz first_seen;
	TimestampTz last_seen;

	bool ready;

	AutoIndexBuildState build_state;
	uint32 job_id;
	int32 builder_pid;
	Oid created_index_oid;
	uint32 build_attempts;
	TimestampTz build_started;
	TimestampTz build_finished;
} AutoIndexSharedEntry;

typedef struct AutoIndexSharedState
{
	uint32 next_job_id;
} AutoIndexSharedState;

static HTAB *AutoIndexCandidateHash = NULL;
static AutoIndexSharedState *AutoIndexShared = NULL;

static bool
autoindex_same_workload(const AutoIndexSharedEntry *entry,
						Oid database_oid,
						Oid relation_oid,
						int32 attnum,
						Oid operator_oid,
						AutoIndexClauseKind clause_kind)
{
	return entry->key.database_oid == database_oid &&
		   entry->key.relation_oid == relation_oid &&
		   entry->key.attnum == attnum &&
		   entry->last_operator_oid == operator_oid &&
		   entry->last_clause_kind == clause_kind;
}

static void
autoindex_entry_to_job(const AutoIndexSharedEntry *entry,
					   AutoIndexBuildJob *job)
{
	MemSet(job, 0, sizeof(*job));

	job->job_id = entry->job_id;
	job->database_oid = entry->key.database_oid;
	job->relation_oid = entry->key.relation_oid;
	job->attnum = entry->key.attnum;
	job->access_method_oid = entry->key.access_method_oid;
	job->opclass_oid = entry->key.opclass_oid;
	job->opfamily_oid = entry->opfamily_oid;
	job->collation_oid = entry->key.collation_oid;
	job->operator_oid = entry->last_operator_oid;
	job->strategy = entry->last_strategy;
	job->clause_kind = entry->last_clause_kind;

	if (entry->completed_scans > 0)
	{
		job->seq_cost =
			entry->total_seq_cost / entry->completed_scans;

		job->index_cost =
			entry->total_index_cost / entry->completed_scans;
	}

	job->build_cost = entry->estimated_build_cost;
	job->selectivity = entry->last_selectivity;
	job->estimated_index_pages = entry->estimated_index_pages;
}

static AutoIndexSharedEntry *
autoindex_find_job_locked(uint32 job_id)
{
	HASH_SEQ_STATUS status;
	AutoIndexSharedEntry *entry;
	AutoIndexSharedEntry *result = NULL;

	hash_seq_init(&status, AutoIndexCandidateHash);

	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->job_id == job_id)
		{
			result = entry;

			hash_seq_term(&status);
			break;
		}
	}

	return result;
}

static bool
autoindex_queue_best_candidate(Oid database_oid,
							   Oid relation_oid,
							   int32 attnum,
							   Oid operator_oid,
							   AutoIndexClauseKind clause_kind,
							   AutoIndexBuildJob *queued_job)
{
	HASH_SEQ_STATUS status;
	AutoIndexSharedEntry *entry;
	AutoIndexSharedEntry *best_entry = NULL;
	double best_score = 0.0;

	if (AutoIndexCandidateHash == NULL || AutoIndexShared == NULL)
		return false;

	LWLockAcquire(AutoIndexStatsLock, LW_EXCLUSIVE);

	hash_seq_init(&status, AutoIndexCandidateHash);

	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (!autoindex_same_workload(entry,
									 database_oid,
									 relation_oid,
									 attnum,
									 operator_oid,
									 clause_kind))
			continue;

		if (entry->build_state == AUTO_INDEX_BUILD_QUEUED ||
			entry->build_state == AUTO_INDEX_BUILD_BUILDING)
		{
			hash_seq_term(&status);
			LWLockRelease(AutoIndexStatsLock);
			return false;
		}
	}

	hash_seq_init(&status, AutoIndexCandidateHash);

	while ((entry = hash_seq_search(&status)) != NULL)
	{
		double score;

		if (!autoindex_same_workload(entry,
									 database_oid,
									 relation_oid,
									 attnum,
									 operator_oid,
									 clause_kind))
			continue;

		if (entry->build_state != AUTO_INDEX_BUILD_READY)
			continue;

		score = entry->total_benefit -
				entry->estimated_build_cost;

		if (best_entry == NULL ||
			score > best_score ||
			(score == best_score &&
			 entry->total_index_cost <
				 best_entry->total_index_cost))
		{
			best_entry = entry;
			best_score = score;
		}
	}

	if (best_entry == NULL)
	{
		LWLockRelease(AutoIndexStatsLock);
		return false;
	}

	best_entry->job_id = AutoIndexShared->next_job_id++;

	if (best_entry->job_id == 0)
		best_entry->job_id = AutoIndexShared->next_job_id++;

	best_entry->build_state = AUTO_INDEX_BUILD_QUEUED;
	best_entry->builder_pid = 0;
	best_entry->build_attempts++;

	autoindex_entry_to_job(best_entry, queued_job);

	LWLockRelease(AutoIndexStatsLock);

	return true;
}

Size AutoIndexShmemSize(void)
{
	Size size;

	size = MAXALIGN(sizeof(AutoIndexSharedState));

	size = add_size(size,
					hash_estimate_size(auto_index_max_candidates,
									   sizeof(AutoIndexSharedEntry)));

	return size;
}

void AutoIndexShmemInit(void)
{
	HASHCTL ctl;
	bool found;

	AutoIndexShared =
		ShmemInitStruct("AutoIndex shared state",
						sizeof(AutoIndexSharedState),
						&found);

	if (!found)
	{
		MemSet(AutoIndexShared, 0, sizeof(AutoIndexSharedState));
		AutoIndexShared->next_job_id = 1;
	}

	MemSet(&ctl, 0, sizeof(ctl));

	ctl.keysize = sizeof(AutoIndexSharedKey);
	ctl.entrysize = sizeof(AutoIndexSharedEntry);

	AutoIndexCandidateHash =
		ShmemInitHash("AutoIndex candidate hash",
					  auto_index_max_candidates,
					  auto_index_max_candidates,
					  &ctl,
					  HASH_ELEM | HASH_BLOBS);
}

void AutoIndexStatsRecord(SeqScanState *node,
						  const AutoIndexPlanInfo *plan_info)
{
	AutoIndexSharedKey key;
	AutoIndexSharedEntry *entry;
	Relation rel;
	Form_pg_attribute attribute;
	bool found;
	bool became_ready = false;
	double benefit;
	uint64 completed_scans;
	double total_benefit;
	double build_cost;
	AutoIndexBuildJob queued_job;
	char *am_name;

	if (AutoIndexCandidateHash == NULL)
		return;

	rel = node->ss.ss_currentRelation;

	benefit = Max(plan_info->seq_cost -
					  plan_info->index_cost,
				  0.0);

	MemSet(&key, 0, sizeof(key));

	key.database_oid = MyDatabaseId;
	key.relation_oid = plan_info->relation_oid;
	key.attnum = plan_info->attnum;
	key.access_method_oid = plan_info->access_method_oid;
	key.opclass_oid = plan_info->opclass_oid;
	key.collation_oid = plan_info->collation_oid;

	LWLockAcquire(AutoIndexStatsLock, LW_EXCLUSIVE);

	entry = hash_search(AutoIndexCandidateHash,
						&key,
						HASH_ENTER_NULL,
						&found);

	if (entry == NULL)
	{
		LWLockRelease(AutoIndexStatsLock);

		ereport(WARNING,
				(errmsg("auto-index candidate table is full"),
				 errhint("Increase auto_index_max_candidates and restart PostgreSQL.")));

		return;
	}

	if (!found)
	{
		entry->opfamily_oid = plan_info->opfamily_oid;
		entry->last_operator_oid = plan_info->operator_oid;
		entry->last_strategy = plan_info->strategy;
		entry->last_clause_kind = plan_info->clause_kind;
		entry->estimated_build_cost = plan_info->build_cost;
		entry->last_selectivity = plan_info->selectivity;
		entry->estimated_index_pages =
			plan_info->estimated_index_pages;
		entry->first_seen = GetCurrentTimestamp();
		entry->build_state = AUTO_INDEX_BUILD_OBSERVING;
	}

	if (entry->build_state == AUTO_INDEX_BUILD_SUPERSEDED &&
		(entry->last_operator_oid != plan_info->operator_oid ||
		 entry->last_clause_kind != plan_info->clause_kind))
	{
		entry->build_state =
			entry->ready ? AUTO_INDEX_BUILD_READY : AUTO_INDEX_BUILD_OBSERVING;
	}

	entry->opfamily_oid = plan_info->opfamily_oid;
	entry->last_operator_oid = plan_info->operator_oid;
	entry->last_strategy = plan_info->strategy;
	entry->last_clause_kind = plan_info->clause_kind;

	entry->completed_scans++;
	entry->total_seq_cost += plan_info->seq_cost;
	entry->total_index_cost += plan_info->index_cost;
	entry->total_benefit += benefit;

	entry->estimated_build_cost = plan_info->build_cost;
	entry->last_selectivity = plan_info->selectivity;
	entry->estimated_index_pages =
		plan_info->estimated_index_pages;
	entry->last_seen = GetCurrentTimestamp();

	if (!entry->ready &&
		entry->build_state == AUTO_INDEX_BUILD_OBSERVING &&
		entry->estimated_build_cost > 0.0 &&
		entry->total_benefit >=
			entry->estimated_build_cost)
	{
		entry->ready = true;
		entry->build_state = AUTO_INDEX_BUILD_READY;
		became_ready = true;
	}

	completed_scans = entry->completed_scans;
	total_benefit = entry->total_benefit;
	build_cost = entry->estimated_build_cost;

	LWLockRelease(AutoIndexStatsLock);

	attribute =
		TupleDescAttr(RelationGetDescr(rel),
					  plan_info->attnum - 1);

	am_name = get_am_name(plan_info->access_method_oid);

	if (auto_index_log_costs)
	{
		ereport(LOG,
				(errmsg("auto-index phase 2: candidate observation relation=\"%s\" column=\"%s\" access_method=\"%s\"",
						RelationGetRelationName(rel),
						NameStr(attribute->attname),
						am_name != NULL ? am_name : "<unknown>"),
				 errdetail("completed_scans=" UINT64_FORMAT
						   " seq_cost=%.6f index_cost=%.6f"
						   " benefit=%.6f accumulated_benefit=%.6f"
						   " build_cost=%.6f selectivity=%.8f",
						   completed_scans,
						   plan_info->seq_cost,
						   plan_info->index_cost,
						   benefit,
						   total_benefit,
						   build_cost,
						   plan_info->selectivity)));
	}

	if (became_ready)
	{
		elog(LOG,
			 "auto-index phase 2: candidate READY "
			 "relation=\"%s\" column=\"%s\" access_method=\"%s\"",
			 RelationGetRelationName(rel),
			 NameStr(attribute->attname),
			 am_name != NULL ? am_name : "<unknown>");
	}

	if (auto_index_create_enabled &&
		autoindex_queue_best_candidate(
			MyDatabaseId,
			plan_info->relation_oid,
			plan_info->attnum,
			plan_info->operator_oid,
			plan_info->clause_kind,
			&queued_job))
	{
		char *selected_am_name;

		selected_am_name =
			get_am_name(queued_job.access_method_oid);

		elog(LOG,
			 "auto-index phase 3: candidate QUEUED "
			 "job_id=%u relation_oid=%u attnum=%d "
			 "access_method=\"%s\"",
			 queued_job.job_id,
			 queued_job.relation_oid,
			 queued_job.attnum,
			 selected_am_name != NULL ? selected_am_name : "<unknown>");

		if (!AutoIndexLaunchBuilder(queued_job.job_id))
		{
			AutoIndexBuildJobLaunchFailed(queued_job.job_id);

			elog(WARNING,
				 "auto-index phase 3: could not start "
				 "background worker for job_id=%u",
				 queued_job.job_id);
		}
	}
}

bool AutoIndexClaimBuildJob(uint32 job_id,
							AutoIndexBuildJob *job)
{
	AutoIndexSharedEntry *entry;

	if (AutoIndexCandidateHash == NULL)
		return false;

	LWLockAcquire(AutoIndexStatsLock, LW_EXCLUSIVE);

	entry = autoindex_find_job_locked(job_id);

	if (entry == NULL ||
		entry->build_state != AUTO_INDEX_BUILD_QUEUED)
	{
		LWLockRelease(AutoIndexStatsLock);
		return false;
	}

	entry->build_state = AUTO_INDEX_BUILD_BUILDING;
	entry->builder_pid = MyProcPid;
	entry->build_started = GetCurrentTimestamp();

	autoindex_entry_to_job(entry, job);

	LWLockRelease(AutoIndexStatsLock);

	return true;
}

static void
autoindex_finish_group_locked(AutoIndexSharedEntry *target,
							  AutoIndexBuildState target_state,
							  Oid index_oid)
{
	HASH_SEQ_STATUS status;
	AutoIndexSharedEntry *entry;
	Oid database_oid;
	Oid relation_oid;
	int32 attnum;
	Oid operator_oid;
	AutoIndexClauseKind clause_kind;
	TimestampTz now;

	database_oid = target->key.database_oid;
	relation_oid = target->key.relation_oid;
	attnum = target->key.attnum;
	operator_oid = target->last_operator_oid;
	clause_kind = target->last_clause_kind;
	now = GetCurrentTimestamp();

	target->build_state = target_state;
	target->created_index_oid = index_oid;
	target->builder_pid = 0;
	target->build_finished = now;

	hash_seq_init(&status, AutoIndexCandidateHash);

	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry == target)
			continue;

		if (!autoindex_same_workload(entry,
									 database_oid,
									 relation_oid,
									 attnum,
									 operator_oid,
									 clause_kind))
			continue;

		if (entry->build_state ==
				AUTO_INDEX_BUILD_OBSERVING ||
			entry->build_state ==
				AUTO_INDEX_BUILD_READY ||
			entry->build_state ==
				AUTO_INDEX_BUILD_FAILED)
		{
			entry->build_state =
				AUTO_INDEX_BUILD_SUPERSEDED;
			entry->created_index_oid = index_oid;
			entry->builder_pid = 0;
			entry->build_finished = now;
		}
	}
}

void AutoIndexBuildJobFinished(uint32 job_id,
							   Oid created_index_oid)
{
	AutoIndexSharedEntry *entry;

	LWLockAcquire(AutoIndexStatsLock, LW_EXCLUSIVE);

	entry = autoindex_find_job_locked(job_id);

	if (entry != NULL)
		autoindex_finish_group_locked(
			entry,
			AUTO_INDEX_BUILD_CREATED,
			created_index_oid);

	LWLockRelease(AutoIndexStatsLock);
}

void AutoIndexBuildJobSuperseded(uint32 job_id,
								 Oid existing_index_oid)
{
	AutoIndexSharedEntry *entry;

	LWLockAcquire(AutoIndexStatsLock, LW_EXCLUSIVE);

	entry = autoindex_find_job_locked(job_id);

	if (entry != NULL)
		autoindex_finish_group_locked(
			entry,
			AUTO_INDEX_BUILD_SUPERSEDED,
			existing_index_oid);

	LWLockRelease(AutoIndexStatsLock);
}

void AutoIndexBuildJobFailed(uint32 job_id)
{
	AutoIndexSharedEntry *entry;

	LWLockAcquire(AutoIndexStatsLock, LW_EXCLUSIVE);

	entry = autoindex_find_job_locked(job_id);

	if (entry != NULL)
	{
		entry->build_state = AUTO_INDEX_BUILD_FAILED;
		entry->builder_pid = 0;
		entry->build_finished = GetCurrentTimestamp();
	}

	LWLockRelease(AutoIndexStatsLock);
}

void AutoIndexBuildJobLaunchFailed(uint32 job_id)
{
	AutoIndexSharedEntry *entry;

	LWLockAcquire(AutoIndexStatsLock, LW_EXCLUSIVE);

	entry = autoindex_find_job_locked(job_id);

	if (entry != NULL &&
		entry->build_state == AUTO_INDEX_BUILD_QUEUED)
	{
		entry->build_state = AUTO_INDEX_BUILD_READY;
		entry->job_id = 0;
		entry->builder_pid = 0;
	}

	LWLockRelease(AutoIndexStatsLock);
}