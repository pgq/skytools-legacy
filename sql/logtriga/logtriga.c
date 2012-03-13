/* ----------------------------------------------------------------------
 * logtriga.c
 *
 *	  Generic trigger for logging table changes.
 *	  Based on Slony-I log trigger.
 *	  Does not depend on event storage.
 *
 *	Copyright (c) 2003-2006, PostgreSQL Global Development Group
 *	Author: Jan Wieck, Afilias USA INC.
 *
 * Generalized by Marko Kreen.
 * ----------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/spi.h"
#include "commands/trigger.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "utils/typcache.h"
#include "utils/rel.h"

#include "textbuf.h"

PG_FUNCTION_INFO_V1(logtriga);
Datum logtriga(PG_FUNCTION_ARGS);

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
 * There may be several plans to be cached.
 *
 * FIXME: plans are kept in singe-linked list
 * so not very fast access.  Probably they should be
 * handled more intelligently.
 */
typedef struct PlanCache PlanCache;

struct PlanCache {
	PlanCache *next;
	char *query;
	void *plan;
};

/*
 * Cache result allocations.
 */
typedef struct ArgCache
{
	TBuf	   *op_type;
	TBuf       *op_data;
}	ArgCache;


static PlanCache *plan_cache = NULL;
static ArgCache *arg_cache = NULL;

/*
 * Cache helpers
 */

static void *get_plan(const char *query)
{
	PlanCache *c;
	void *plan;
	Oid   plan_types[2];

	for (c = plan_cache; c; c = c->next)
		if (strcmp(query, c->query) == 0)
			return c->plan;

	/*
	 * Plan not cached, prepare new plan then.
	 */
	plan_types[0] = TEXTOID;
	plan_types[1] = TEXTOID;
	plan = SPI_saveplan(SPI_prepare(query, 2, plan_types));
	if (plan == NULL)
		elog(ERROR, "logtriga: SPI_prepare() failed");
	
	/* create cache object */
	c = malloc(sizeof(*c));
	if (!c)
		elog(ERROR, "logtriga: no memory for plan cache");

	c->plan = plan;
	c->query = strdup(query);

	/* insert at start */
	c->next = plan_cache;
	plan_cache = c;

	return plan;
}

static ArgCache *
get_arg_cache(void)
{
	if (arg_cache == NULL) {
		ArgCache *a = malloc(sizeof(*a));
		if (!a)
			elog(ERROR, "logtriga: no memory");
		memset(a, 0, sizeof(*a));
		a->op_type = tbuf_alloc(8);
		a->op_data = tbuf_alloc(8192);
		arg_cache = a;
	}
	return arg_cache;
}

static void
append_key_eq(TBuf *tbuf, const char *col_ident, const char *col_value)
{
	if (col_value == NULL)
		elog(ERROR, "logtriga: Unexpected NULL key value");

	tbuf_encode_cstring(tbuf, col_ident, "quote_ident");
	tbuf_append_char(tbuf, '=');
	tbuf_encode_cstring(tbuf, col_value, "quote_literal");
}

static void
append_normal_eq(TBuf *tbuf, const char *col_ident, const char *col_value)
{
	tbuf_encode_cstring(tbuf, col_ident, "quote_ident");
	tbuf_append_char(tbuf, '=');
	if (col_value != NULL)
		tbuf_encode_cstring(tbuf, col_value, "quote_literal");
	else
		tbuf_append_cstring(tbuf, "NULL");
}

static void process_insert(ArgCache *cs, TriggerData *tg, char *attkind)
{
	HeapTuple	new_row = tg->tg_trigtuple;
	TupleDesc	tupdesc = tg->tg_relation->rd_att;
	int			i;
	int			need_comma = false;
	int			attkind_idx;

	/*
	 * INSERT
	 *
	 * op_type = 'I' op_data = ("non-NULL-col" [, ...]) values ('value' [,
	 * ...])
	 */
	tbuf_append_cstring(cs->op_type, "I");

	/*
	 * Specify all the columns
	 */
	tbuf_append_char(cs->op_data, '(');
	attkind_idx = -1;
	for (i = 0; i < tg->tg_relation->rd_att->natts; i++)
	{
		char *col_ident;

		/* Skip dropped columns */
		if (tupdesc->attrs[i]->attisdropped)
			continue;

		/* Check if allowed by colstring */
		attkind_idx++;
		if (attkind[attkind_idx] == '\0')
			break;
		if (attkind[attkind_idx] == 'i')
			continue;

		if (need_comma)
			tbuf_append_char(cs->op_data, ',');
		else
			need_comma = true;

		/* quote column name */
		col_ident = SPI_fname(tupdesc, i + 1);
		tbuf_encode_cstring(cs->op_data, col_ident, "quote_ident");
	}

	/*
	 * Append the string ") values ("
	 */
	tbuf_append_cstring(cs->op_data, ") values (");

	/*
	 * Append the values
	 */
	need_comma = false;
	attkind_idx = -1;
	for (i = 0; i < tg->tg_relation->rd_att->natts; i++)
	{
		char *col_value;

		/* Skip dropped columns */
		if (tupdesc->attrs[i]->attisdropped)
			continue;

		/* Check if allowed by colstring */
		attkind_idx++;
		if (attkind[attkind_idx] == '\0')
			break;
		if (attkind[attkind_idx] == 'i')
			continue;

		if (need_comma)
			tbuf_append_char(cs->op_data, ',');
		else
			need_comma = true;

		/* quote column value */
		col_value = SPI_getvalue(new_row, tupdesc, i + 1);
		if (col_value == NULL)
			tbuf_append_cstring(cs->op_data, "null");
		else
			tbuf_encode_cstring(cs->op_data, col_value, "quote_literal");
	}

	/*
	 * Terminate and done
	 */
	tbuf_append_char(cs->op_data, ')');
}

static int process_update(ArgCache *cs, TriggerData *tg, char *attkind)
{
	HeapTuple	old_row = tg->tg_trigtuple;
	HeapTuple	new_row = tg->tg_newtuple;
	TupleDesc	tupdesc = tg->tg_relation->rd_att;
	Datum		old_value;
	Datum		new_value;
	bool		old_isnull;
	bool		new_isnull;

	char	   *col_ident;
	char	   *col_value;
	int			i;
	int			need_comma = false;
	int			need_and = false;
	int			attkind_idx;
	int			ignore_count = 0;

	/*
	 * UPDATE
	 *
	 * op_type = 'U' op_data = "col_ident"='value' [, ...] where "pk_ident" =
	 * 'value' [ and ...]
	 */
	tbuf_append_cstring(cs->op_type, "U");

	attkind_idx = -1;
	for (i = 0; i < tg->tg_relation->rd_att->natts; i++)
	{
		/*
		 * Ignore dropped columns
		 */
		if (tupdesc->attrs[i]->attisdropped)
			continue;

		attkind_idx++;
		if (attkind[attkind_idx] == '\0')
			break;

		old_value = SPI_getbinval(old_row, tupdesc, i + 1, &old_isnull);
		new_value = SPI_getbinval(new_row, tupdesc, i + 1, &new_isnull);

		/*
		 * If old and new value are NULL, the column is unchanged
		 */
		if (old_isnull && new_isnull)
			continue;

		/*
		 * If both are NOT NULL, we need to compare the values and skip
		 * setting the column if equal
		 */
		if (!old_isnull && !new_isnull)
		{
			Oid			opr_oid;
			FmgrInfo   *opr_finfo_p;

			/*
			 * Lookup the equal operators function call info using the
			 * typecache if available
			 */
			TypeCacheEntry *type_cache;

			type_cache = lookup_type_cache(SPI_gettypeid(tupdesc, i + 1),
							  TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO);
			opr_oid = type_cache->eq_opr;
			if (opr_oid == ARRAY_EQ_OP)
				opr_oid = InvalidOid;
			else
				opr_finfo_p = &(type_cache->eq_opr_finfo);

			/*
			 * If we have an equal operator, use that to do binary
			 * comparision. Else get the string representation of both
			 * attributes and do string comparision.
			 */
			if (OidIsValid(opr_oid))
			{
				if (DatumGetBool(FunctionCall2(opr_finfo_p,
											   old_value, new_value)))
					continue;
			}
			else
			{
				char	   *old_strval = SPI_getvalue(old_row, tupdesc, i + 1);
				char	   *new_strval = SPI_getvalue(new_row, tupdesc, i + 1);

				if (strcmp(old_strval, new_strval) == 0)
					continue;
			}
		}

		if (attkind[attkind_idx] == 'i')
		{
			/* this change should be ignored */
			ignore_count++;
			continue;
		}

		if (need_comma)
			tbuf_append_char(cs->op_data, ',');
		else
			need_comma = true;

		col_ident = SPI_fname(tupdesc, i + 1);
		col_value = SPI_getvalue(new_row, tupdesc, i + 1);

		append_normal_eq(cs->op_data, col_ident, col_value);
	}

	/*
	 * It can happen that the only UPDATE an application does is to set a
	 * column to the same value again. In that case, we'd end up here with
	 * no columns in the SET clause yet. We add the first key column here
	 * with it's old value to simulate the same for the replication
	 * engine.
	 */
	if (!need_comma)
	{
		/* there was change in ignored columns, skip whole event */
		if (ignore_count > 0)
			return 0;

		for (i = 0, attkind_idx = -1; i < tg->tg_relation->rd_att->natts; i++)
		{
			if (tupdesc->attrs[i]->attisdropped)
				continue;

			attkind_idx++;
			if (attkind[attkind_idx] == 'k')
				break;
		}
		col_ident = SPI_fname(tupdesc, i + 1);
		col_value = SPI_getvalue(old_row, tupdesc, i + 1);

		append_key_eq(cs->op_data, col_ident, col_value);
	}

	tbuf_append_cstring(cs->op_data, " where ");

	for (i = 0, attkind_idx = -1; i < tg->tg_relation->rd_att->natts; i++)
	{
		/*
		 * Ignore dropped columns
		 */
		if (tupdesc->attrs[i]->attisdropped)
			continue;

		attkind_idx++;
		if (attkind[attkind_idx] == '\0')
			break;
		if (attkind[attkind_idx] != 'k')
			continue;
		col_ident = SPI_fname(tupdesc, i + 1);
		col_value = SPI_getvalue(old_row, tupdesc, i + 1);

		if (need_and)
			tbuf_append_cstring(cs->op_data, " and ");
		else
			need_and = true;

		append_key_eq(cs->op_data, col_ident, col_value);
	}
	return 1;
}

static void process_delete(ArgCache *cs, TriggerData *tg, char *attkind)
{
	HeapTuple	old_row = tg->tg_trigtuple;
	TupleDesc	tupdesc = tg->tg_relation->rd_att;
	char	   *col_ident;
	char	   *col_value;
	int			i;
	int			need_and = false;
	int			attkind_idx;

	/*
	 * DELETE
	 *
	 * op_type = 'D' op_data = "pk_ident"='value' [and ...]
	 */
	tbuf_append_cstring(cs->op_type, "D");

	for (i = 0, attkind_idx = -1; i < tg->tg_relation->rd_att->natts; i++)
	{
		if (tupdesc->attrs[i]->attisdropped)
			continue;

		attkind_idx++;
		if (attkind[attkind_idx] == '\0')
			break;
		if (attkind[attkind_idx] != 'k')
			continue;
		col_ident = SPI_fname(tupdesc, i + 1);
		col_value = SPI_getvalue(old_row, tupdesc, i + 1);

		if (need_and)
			tbuf_append_cstring(cs->op_data, " and ");
		else
			need_and = true;

		append_key_eq(cs->op_data, col_ident, col_value);
	}
}

Datum logtriga(PG_FUNCTION_ARGS)
{
	TriggerData *tg;
	Datum		argv[2];
	int			rc;
	ArgCache	*cs;
	TupleDesc	tupdesc;
	int			i;
	int			attcnt;
	char		*attkind;
	char		*kpos;
	char		*query;
	int			need_event = 1;

	/*
	 * Get the trigger call context
	 */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "logtriga not called as trigger");
	tg = (TriggerData *) (fcinfo->context);
	tupdesc = tg->tg_relation->rd_att;

	/*
	 * Check all logTrigger() calling conventions
	 */
	if (!TRIGGER_FIRED_AFTER(tg->tg_event))
		elog(ERROR, "logtriga must be fired AFTER");
	if (!TRIGGER_FIRED_FOR_ROW(tg->tg_event))
		elog(ERROR, "logtriga must be fired FOR EACH ROW");
	if (tg->tg_trigger->tgnargs != 2)
		elog(ERROR, "logtriga must be defined with 2 args");

	/*
	 * Connect to the SPI manager
	 */
	if ((rc = SPI_connect()) < 0)
		elog(ERROR, "logtriga: SPI_connect() failed");

	cs = get_arg_cache();

	tbuf_reset(cs->op_type);
	tbuf_reset(cs->op_data);

	/*
	 * Get all the trigger arguments
	 */
	attkind = tg->tg_trigger->tgargs[0];
	query = tg->tg_trigger->tgargs[1];

	/*
	 * Count number of active columns
	 */
	for (i = 0, attcnt = 0; i < tg->tg_relation->rd_att->natts; i++)
	{
		if (tupdesc->attrs[i]->attisdropped)
			continue;
		attcnt++;
	}

	/*
	 * Make sure all 'k' columns exist and there is at least one of them.
	 */
	kpos = strrchr(attkind, 'k');
	if (kpos == NULL)
		elog(ERROR, "logtriga: need at least one key column");
	if (kpos - attkind >= attcnt)
		elog(ERROR, "logtriga: key column does not exist");

	/*
	 * Determine cmdtype and op_data depending on the command type
	 */
	if (TRIGGER_FIRED_BY_INSERT(tg->tg_event))
		process_insert(cs, tg, attkind);
	else if (TRIGGER_FIRED_BY_UPDATE(tg->tg_event))
		need_event = process_update(cs, tg, attkind);
	else if (TRIGGER_FIRED_BY_DELETE(tg->tg_event))
		process_delete(cs, tg, attkind);
	else
		elog(ERROR, "logtriga fired for unhandled event");

	/*
	 * Construct the parameter array and insert the log row.
	 */
	if (need_event)
	{
		argv[0] = PointerGetDatum(tbuf_look_text(cs->op_type));
		argv[1] = PointerGetDatum(tbuf_look_text(cs->op_data));
		SPI_execp(get_plan(query), argv, NULL, 0);
	}
	SPI_finish();
	return PointerGetDatum(NULL);
}

