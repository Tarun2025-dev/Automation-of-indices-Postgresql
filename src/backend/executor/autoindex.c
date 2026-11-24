#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "access/stratnum.h"
#include "catalog/catalog.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "commands/defrem.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "utils/autoindex.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

typedef struct AutoIndexCandidate
{
	AttrNumber attnum;
	Oid operator_oid;
	Oid access_method_oid;
	Oid opclass_oid;
	Oid opfamily_oid;
	Oid collation_oid;
	StrategyNumber strategy;
	AutoIndexClauseKind clause_kind;
} AutoIndexCandidate;

typedef struct AutoIndexScanState
{
	List *candidates;
	bool scan_exhausted;
} AutoIndexScanState;

typedef struct AutoIndexAccessMethod
{
	Oid access_method_oid;
	const char *name;
} AutoIndexAccessMethod;

static const AutoIndexAccessMethod autoindex_access_methods[] = {
	{BTREE_AM_OID, "btree"},
	{HASH_AM_OID, "hash"},
	{GIST_AM_OID, "gist"},
	{SPGIST_AM_OID, "spgist"},
	{GIN_AM_OID, "gin"},
	{BRIN_AM_OID, "brin"}};

bool auto_index_enabled = false;
bool auto_index_log_costs = false;
bool auto_index_create_enabled = false;

int auto_index_max_candidates = 1024;

static const char *autoindex_access_method_name(Oid access_method_oid)
{
	int i;

	for (i = 0; i < lengthof(autoindex_access_methods); i++)
	{
		if (autoindex_access_methods[i].access_method_oid == access_method_oid)
			return autoindex_access_methods[i].name;
	}

	return "unknown";
}

static const char *autoindex_clause_kind_name(AutoIndexClauseKind clause_kind)
{
	switch (clause_kind)
	{
	case AUTO_INDEX_CLAUSE_OPERATOR:
		return "operator";

	case AUTO_INDEX_CLAUSE_SCALAR_ARRAY:
		return "scalar-array";

	case AUTO_INDEX_CLAUSE_NULL_TEST:
		return "null-test";
	}

	return "unknown";
}

static bool autoindex_var_is_scan_column(Var *var, SeqScanState *node)
{
	Relation rel;
	Index scanrelid;
	TupleDesc tuple_desc;

	rel = node->ss.ss_currentRelation;
	scanrelid = ((Scan *)node->ss.ps.plan)->scanrelid;
	tuple_desc = RelationGetDescr(rel);

	if (var->varno != scanrelid)
		return false;

	if (var->varlevelsup != 0)
		return false;

	if (var->varattno <= 0 || var->varattno > tuple_desc->natts)
		return false;

	if (TupleDescAttr(tuple_desc, var->varattno - 1)->attisdropped)
		return false;

	return true;
}

static bool autoindex_collations_match(Oid index_collation, Oid expression_collation)
{
	return !OidIsValid(index_collation) || index_collation == expression_collation;
}

static bool autoindex_candidate_already_recorded(List *candidates, AttrNumber attnum, Oid access_method_oid, Oid opclass_oid, Oid collation_oid)
{
	ListCell *lc;

	foreach (lc, candidates)
	{
		AutoIndexCandidate *candidate;

		candidate = (AutoIndexCandidate *)lfirst(lc);

		if (candidate->attnum == attnum && candidate->access_method_oid == access_method_oid && candidate->opclass_oid == opclass_oid && candidate->collation_oid == collation_oid)
			return true;
	}

	return false;
}

static bool autoindex_has_usable_index(Relation rel, AttrNumber attnum, Oid access_method_oid, Oid opclass_oid, Oid collation_oid)
{
	List *index_oids;
	ListCell *lc;
	bool found;

	index_oids = RelationGetIndexList(rel);
	found = false;

	foreach (lc, index_oids)
	{
		Oid index_oid;
		Relation index_rel;
		bool matches;

		index_oid = lfirst_oid(lc);
		index_rel = index_open(index_oid, AccessShareLock);
		matches = false;

		if (index_rel->rd_index != NULL && index_rel->rd_rel->relam == access_method_oid && index_rel->rd_index->indisvalid && index_rel->rd_index->indisready && index_rel->rd_index->indislive && index_rel->rd_index->indnkeyatts > 0 && index_rel->rd_index->indkey.values[0] == attnum && index_rel->rd_indcollation != NULL && index_rel->rd_indcollation[0] == collation_oid && get_index_column_opclass(index_oid, 1) == opclass_oid && RelationGetIndexPredicate(index_rel) == NIL)
			matches = true;

		index_close(index_rel, AccessShareLock);

		if (matches)
		{
			found = true;
			break;
		}
	}

	list_free(index_oids);

	return found;
}

static void autoindex_add_candidate(SeqScanState *node, Var *var, Oid operator_oid, Oid access_method_oid, Oid opclass_oid, Oid opfamily_oid, StrategyNumber strategy, AutoIndexClauseKind clause_kind, List **candidates)
{
	AutoIndexCandidate *candidate;
	Relation rel;

	rel = node->ss.ss_currentRelation;

	if (autoindex_candidate_already_recorded(*candidates, var->varattno, access_method_oid, opclass_oid, var->varcollid))
		return;

	if (autoindex_has_usable_index(rel, var->varattno, access_method_oid, opclass_oid, var->varcollid))
		return;

	candidate = palloc0(sizeof(AutoIndexCandidate));

	candidate->attnum = var->varattno;
	candidate->operator_oid = operator_oid;
	candidate->access_method_oid = access_method_oid;
	candidate->opclass_oid = opclass_oid;
	candidate->opfamily_oid = opfamily_oid;
	candidate->collation_oid = var->varcollid;
	candidate->strategy = strategy;
	candidate->clause_kind = clause_kind;

	*candidates = lappend(*candidates, candidate);
}

static void autoindex_add_operator_candidates(SeqScanState *node, Var *var, Oid operator_oid, Oid expression_collation, AutoIndexClauseKind clause_kind, bool require_array_search, List **candidates)
{
	int i;

	if (!autoindex_collations_match(var->varcollid, expression_collation))
		return;

	for (i = 0; i < lengthof(autoindex_access_methods); i++)
	{
		Oid access_method_oid;
		Oid opclass_oid;
		Oid opfamily_oid;
		int strategy;
		const IndexAmRoutine *amroutine;

		access_method_oid = autoindex_access_methods[i].access_method_oid;

		if (require_array_search)
		{
			amroutine = GetIndexAmRoutineByAmId(access_method_oid, true);

			if (amroutine == NULL || !amroutine->amsearcharray)
				continue;
		}

		opclass_oid = GetDefaultOpClass(var->vartype, access_method_oid);

		if (!OidIsValid(opclass_oid))
			continue;

		opfamily_oid = get_opclass_family(opclass_oid);

		if (!OidIsValid(opfamily_oid))
			continue;

		strategy = get_op_opfamily_strategy(operator_oid, opfamily_oid);

		if (strategy == 0)
			continue;

		autoindex_add_candidate(node, var, operator_oid, access_method_oid, opclass_oid, opfamily_oid, (StrategyNumber)strategy, clause_kind, candidates);
	}
}

static void autoindex_add_null_candidates(SeqScanState *node, Var *var, List **candidates)
{
	int i;

	for (i = 0; i < lengthof(autoindex_access_methods); i++)
	{
		Oid access_method_oid;
		Oid opclass_oid;
		Oid opfamily_oid;
		const IndexAmRoutine *amroutine;

		access_method_oid = autoindex_access_methods[i].access_method_oid;

		amroutine = GetIndexAmRoutineByAmId(access_method_oid, true);

		if (amroutine == NULL || !amroutine->amsearchnulls)
			continue;

		opclass_oid = GetDefaultOpClass(var->vartype, access_method_oid);

		if (!OidIsValid(opclass_oid))
			continue;

		opfamily_oid = get_opclass_family(opclass_oid);

		if (!OidIsValid(opfamily_oid))
			continue;

		autoindex_add_candidate(node, var, InvalidOid, access_method_oid, opclass_oid, opfamily_oid, (StrategyNumber)0, AUTO_INDEX_CLAUSE_NULL_TEST, candidates);
	}
}

static void autoindex_consider_operator(OpExpr *op, SeqScanState *node, List **candidates)
{
	Node *leftop;
	Node *rightop;
	Node *left_stripped;
	Node *right_stripped;
	Var *var;
	Oid operator_oid;

	if (list_length(op->args) != 2)
		return;

	leftop = (Node *)linitial(op->args);
	rightop = (Node *)lsecond(op->args);

	left_stripped = strip_implicit_coercions(leftop);
	right_stripped = strip_implicit_coercions(rightop);

	operator_oid = op->opno;
	var = NULL;

	if (IsA(left_stripped, Var) && is_pseudo_constant_clause(rightop))
	{
		var = (Var *)left_stripped;
	}
	else if (IsA(right_stripped, Var) && is_pseudo_constant_clause(leftop))
	{
		operator_oid = get_commutator(operator_oid);

		if (!OidIsValid(operator_oid))
			return;

		var = (Var *)right_stripped;
	}
	else
		return;

	if (!autoindex_var_is_scan_column(var, node))
		return;

	autoindex_add_operator_candidates(node, var, operator_oid, op->inputcollid, AUTO_INDEX_CLAUSE_OPERATOR, false, candidates);
}

static void autoindex_consider_scalar_array(ScalarArrayOpExpr *saop, SeqScanState *node, List **candidates)
{
	Node *leftop;
	Node *rightop;
	Node *left_stripped;
	Var *var;

	if (!saop->useOr)
		return;

	if (list_length(saop->args) != 2)
		return;

	leftop = (Node *)linitial(saop->args);
	rightop = (Node *)lsecond(saop->args);

	left_stripped = strip_implicit_coercions(leftop);

	if (!IsA(left_stripped, Var))
		return;

	if (!is_pseudo_constant_clause(rightop))
		return;

	var = (Var *)left_stripped;

	if (!autoindex_var_is_scan_column(var, node))
		return;

	autoindex_add_operator_candidates(node, var, saop->opno, saop->inputcollid, AUTO_INDEX_CLAUSE_SCALAR_ARRAY, true, candidates);
}

static void autoindex_consider_null_test(NullTest *null_test, SeqScanState *node, List **candidates)
{
	Node *argument;
	Var *var;

	if (null_test->argisrow)
		return;

	argument = strip_implicit_coercions((Node *)null_test->arg);

	if (!IsA(argument, Var))
		return;

	var = (Var *)argument;

	if (!autoindex_var_is_scan_column(var, node))
		return;

	autoindex_add_null_candidates(node, var, candidates);
}

static void autoindex_collect_candidates(Node *expr, SeqScanState *node, List **candidates)
{
	if (expr == NULL)
		return;

	if (IsA(expr, BoolExpr))
	{
		BoolExpr *bool_expr;

		bool_expr = (BoolExpr *)expr;

		if (bool_expr->boolop == AND_EXPR)
		{
			ListCell *lc;

			foreach (lc, bool_expr->args)
			{
				autoindex_collect_candidates((Node *)lfirst(lc), node, candidates);
			}
		}

		return;
	}

	if (IsA(expr, OpExpr))
	{
		autoindex_consider_operator((OpExpr *)expr, node, candidates);
		return;
	}

	if (IsA(expr, ScalarArrayOpExpr))
	{
		autoindex_consider_scalar_array((ScalarArrayOpExpr *)expr, node, candidates);
		return;
	}

	if (IsA(expr, NullTest))
	{
		autoindex_consider_null_test((NullTest *)expr, node, candidates);
		return;
	}
}

void AutoIndexInitSeqScan(SeqScanState *node, List *original_quals)
{
	AutoIndexScanState *state;
	Relation rel;
	ListCell *lc;

	node->autoindex_state = NULL;

	if (!auto_index_enabled)
		return;

	rel = node->ss.ss_currentRelation;

	if (rel == NULL)
		return;

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		return;

	if (rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		return;

	if (IsCatalogRelation(rel) || IsToastRelation(rel))
		return;

	if (rel->rd_rel->relispartition)
		return;

	if (original_quals == NIL)
		return;

	state = palloc0(sizeof(AutoIndexScanState));

	foreach (lc, original_quals)
	{
		autoindex_collect_candidates((Node *)lfirst(lc), node, &state->candidates);
	}

	if (state->candidates == NIL)
	{
		pfree(state);
		return;
	}

	node->autoindex_state = state;
}

void AutoIndexScanExhausted(SeqScanState *node)
{
	AutoIndexScanState *state;
	TableScanDesc scan_desc;
	Relation rel;
	ListCell *lc;

	SeqScan *plan;
	ListCell *plan_lc;

	state = node->autoindex_state;

	if (state == NULL || state->scan_exhausted)
		return;

	scan_desc = node->ss.ss_currentScanDesc;

	if (scan_desc != NULL && scan_desc->rs_parallel != NULL)
		return;

	state->scan_exhausted = true;
	rel = node->ss.ss_currentRelation;

	foreach (lc, state->candidates)
	{
		AutoIndexCandidate *candidate;
		Form_pg_attribute attribute;

		candidate = (AutoIndexCandidate *)lfirst(lc);
		attribute = TupleDescAttr(RelationGetDescr(rel), candidate->attnum - 1);

		elog(LOG,
			 "auto-index phase 1: missing-index candidate "
			 "relation=\"%s\" relid=%u "
			 "column=\"%s\" attnum=%d "
			 "access_method=\"%s\" "
			 "opclass=%u opfamily=%u "
			 "operator=%u strategy=%d "
			 "clause=\"%s\"",
			 RelationGetRelationName(rel),
			 RelationGetRelid(rel),
			 NameStr(attribute->attname),
			 candidate->attnum,
			 autoindex_access_method_name(candidate->access_method_oid),
			 candidate->opclass_oid,
			 candidate->opfamily_oid,
			 candidate->operator_oid,
			 candidate->strategy,
			 autoindex_clause_kind_name(candidate->clause_kind));
	}

	plan = castNode(SeqScan, node->ss.ps.plan);

	foreach (plan_lc, plan->autoindex_plans)
	{
		AutoIndexPlanInfo *plan_info;

		plan_info =
			(AutoIndexPlanInfo *)lfirst(plan_lc);

		AutoIndexStatsRecord(node, plan_info);
	}
}

void AutoIndexResetSeqScan(SeqScanState *node)
{
	AutoIndexScanState *state;

	state = node->autoindex_state;

	if (state != NULL)
		state->scan_exhausted = false;
}

void AutoIndexEndSeqScan(SeqScanState *node)
{
	AutoIndexScanState *state;
	ListCell *lc;

	state = node->autoindex_state;

	if (state == NULL)
		return;

	foreach (lc, state->candidates)
		pfree(lfirst(lc));

	list_free(state->candidates);
	pfree(state);

	node->autoindex_state = NULL;
}