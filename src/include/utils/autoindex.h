#ifndef AUTO_INDEX_H
#define AUTO_INDEX_H

#include "nodes/execnodes.h"
#include "nodes/pg_list.h"

typedef enum AutoIndexClauseKind
{
	AUTO_INDEX_CLAUSE_OPERATOR,
	AUTO_INDEX_CLAUSE_SCALAR_ARRAY,
	AUTO_INDEX_CLAUSE_NULL_TEST
} AutoIndexClauseKind;

typedef enum AutoIndexBuildState
{
	AUTO_INDEX_BUILD_OBSERVING = 0,
	AUTO_INDEX_BUILD_READY,
	AUTO_INDEX_BUILD_QUEUED,
	AUTO_INDEX_BUILD_BUILDING,
	AUTO_INDEX_BUILD_CREATED,
	AUTO_INDEX_BUILD_SUPERSEDED,
	AUTO_INDEX_BUILD_FAILED
} AutoIndexBuildState;

typedef struct AutoIndexBuildJob
{
	uint32		job_id;
	Oid			database_oid;
	Oid			relation_oid;
	int32		attnum;
	Oid			access_method_oid;
	Oid			opclass_oid;
	Oid			opfamily_oid;
	Oid			collation_oid;
	Oid			operator_oid;
	StrategyNumber strategy;
	AutoIndexClauseKind clause_kind;

	double		seq_cost;
	double		index_cost;
	double		build_cost;
	double		selectivity;
	double		estimated_index_pages;
} AutoIndexBuildJob;

extern bool auto_index_enabled;

extern bool auto_index_log_costs;
extern int	auto_index_max_candidates;

extern bool auto_index_create_enabled;

extern Size AutoIndexShmemSize(void);
extern void AutoIndexShmemInit(void);

extern void AutoIndexStatsRecord(SeqScanState *node, const AutoIndexPlanInfo *plan_info);

extern void AutoIndexInitSeqScan(SeqScanState *node, List *original_quals);
extern void AutoIndexScanExhausted(SeqScanState *node);
extern void AutoIndexResetSeqScan(SeqScanState *node);
extern void AutoIndexEndSeqScan(SeqScanState *node);

extern bool AutoIndexLaunchBuilder(uint32 job_id);

extern bool AutoIndexClaimBuildJob(uint32 job_id, AutoIndexBuildJob *job);

extern void AutoIndexBuildJobFinished(uint32 job_id, Oid created_index_oid);

extern void AutoIndexBuildJobSuperseded(uint32 job_id, Oid existing_index_oid);

extern void AutoIndexBuildJobFailed(uint32 job_id);

extern void AutoIndexBuildJobLaunchFailed(uint32 job_id);

extern PGDLLEXPORT void AutoIndexBuilderMain(Datum main_arg);

#endif							/* AUTO_INDEX_H */
