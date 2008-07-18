/*
 * common.c - functions used by all trigger variants.
 *
 * Copyright (c) 2007 Marko Kreen, Skype Technologies OÜ
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <postgres.h>

#include <commands/trigger.h>
#include <catalog/pg_type.h>
#include <catalog/pg_namespace.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <utils/memutils.h>
#include <utils/inval.h>
#include <utils/hsearch.h>

#include "common.h"
#include "stringutil.h"

/*
 * Module tag
 */
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
 * primary key info
 */

static MemoryContext tbl_cache_ctx;
static HTAB *tbl_cache_map;

static const char pkey_sql [] =
	"SELECT k.attnum, k.attname FROM pg_index i, pg_attribute k"
	" WHERE i.indrelid = $1 AND k.attrelid = i.indexrelid"
	"   AND i.indisprimary AND k.attnum > 0 AND NOT k.attisdropped"
	" ORDER BY k.attnum";
static void *pkey_plan;

static void relcache_reset_cb(Datum arg, Oid relid);

/*
 * helper for queue insertion.
 *
 * does not support NULL arguments.
 */
void pgq_simple_insert(const char *queue_name, Datum ev_type, Datum ev_data, Datum ev_extra1, Datum ev_extra2)
{
	Datum values[5];
	char nulls[5];
	static void *plan = NULL;
	int res;

	if (!plan) {
		const char *sql;
		Oid   types[5] = { TEXTOID, TEXTOID, TEXTOID, TEXTOID, TEXTOID };

		sql = "select pgq.insert_event($1, $2, $3, $4, $5, null, null)";
		plan = SPI_saveplan(SPI_prepare(sql, 5, types));
		if (plan == NULL)
			elog(ERROR, "logtriga: SPI_prepare() failed");
	}
	values[0] = DirectFunctionCall1(textin, (Datum)queue_name);
	values[1] = ev_type;
	values[2] = ev_data;
	values[3] = ev_extra1;
	values[4] = ev_extra2;
	nulls[0] = ' ';
	nulls[1] = ' ';
	nulls[2] = ' ';
	nulls[3] = ' ';
	nulls[4] = ev_extra2 ? ' ' : 'n';
	res = SPI_execute_plan(plan, values, nulls, false, 0);
	if (res != SPI_OK_SELECT)
		elog(ERROR, "call of pgq.insert_event failed");
}

void pgq_insert_tg_event(PgqTriggerEvent *ev)
{
	pgq_simple_insert(ev->queue_name,
					  pgq_finish_varbuf(ev->ev_type),
					  pgq_finish_varbuf(ev->ev_data),
					  pgq_finish_varbuf(ev->ev_extra1),
					  ev->ev_extra2
					  ? pgq_finish_varbuf(ev->ev_extra2)
					  : (Datum)0);
}

char *pgq_find_table_name(Relation rel)
{
	NameData	tname = rel->rd_rel->relname;
	Oid			nsoid = rel->rd_rel->relnamespace;
	char        namebuf[NAMEDATALEN * 2 + 3];
	HeapTuple   ns_tup;
	Form_pg_namespace ns_struct;
	NameData	nspname;

	/* find namespace info */
	ns_tup = SearchSysCache(NAMESPACEOID,
							ObjectIdGetDatum(nsoid), 0, 0, 0);
	if (!HeapTupleIsValid(ns_tup))
		elog(ERROR, "Cannot find namespace %u", nsoid);
	ns_struct = (Form_pg_namespace) GETSTRUCT(ns_tup);
	nspname = ns_struct->nspname;

	/* fill name */
	sprintf(namebuf, "%s.%s", NameStr(nspname), NameStr(tname));

	ReleaseSysCache(ns_tup);
	return pstrdup(namebuf);
}

static void
init_pkey_plan(void)
{
	Oid types[1] = { OIDOID };
	pkey_plan = SPI_saveplan(SPI_prepare(pkey_sql, 1, types));
	if (pkey_plan == NULL)
		elog(ERROR, "pgq_triggers: SPI_prepare() failed");
}

static void
init_cache(void)
{
	HASHCTL     ctl;
	int         flags;
	int         max_tables = 128;

	/*
	 * create own context
	 */
	tbl_cache_ctx = AllocSetContextCreate(TopMemoryContext,
					      "pgq_triggers table info",
					      ALLOCSET_SMALL_MINSIZE,
					      ALLOCSET_SMALL_INITSIZE,
					      ALLOCSET_SMALL_MAXSIZE);
	/*
	 * init pkey cache.
	 */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(struct PgqTableInfo);
	ctl.hash = oid_hash;
	flags = HASH_ELEM | HASH_FUNCTION;
	tbl_cache_map = hash_create("pgq_triggers pkey cache", max_tables, &ctl, flags);
}

/*
 * Prepare utility plans and plan cache.
 */
static void
init_module(void)
{
	static int callback_init = 0;

	/* htab can be occasinally dropped */
	if (tbl_cache_ctx)
		return;
	init_cache();

	/*
	 * Init plans.
	 */
	if (!pkey_plan)
		init_pkey_plan();

	/* this must be done only once */
	if (!callback_init) {
		CacheRegisterRelcacheCallback(relcache_reset_cb, (Datum)0);
		callback_init = 1;
	}
}

static void
full_reset(void)
{
	if (tbl_cache_ctx) {
		/* needed only if backend has HASH_STATISTICS set */
		/* hash_destroy(tbl_cache_map); */
		MemoryContextDelete(tbl_cache_ctx);
		tbl_cache_map = NULL;
		tbl_cache_ctx = NULL;
	}
}

/*
 * Create new plan for insertion into current queue table.
 */
static void
fill_tbl_info(Relation rel, struct PgqTableInfo *info)
{
	StringInfo pkeys;
	Datum values[1];
	const char *name = pgq_find_table_name(rel);
	TupleDesc desc;
	HeapTuple row;
	bool isnull;
	int res, i, attno;

	values[0] = ObjectIdGetDatum(rel->rd_id);
	res = SPI_execute_plan(pkey_plan, values, NULL, false, 0);
	if (res != SPI_OK_SELECT)
		elog(ERROR, "pkey_plan exec failed");
	desc = SPI_tuptable->tupdesc;

	pkeys = makeStringInfo();
	info->n_pkeys = SPI_processed;
	info->table_name = MemoryContextStrdup(tbl_cache_ctx, name);
	info->pkey_attno = MemoryContextAlloc(tbl_cache_ctx, info->n_pkeys * sizeof(int));

	for (i = 0; i < SPI_processed; i++) {
		row = SPI_tuptable->vals[i];

		attno = DatumGetInt16(SPI_getbinval(row, desc, 1, &isnull));
		name = SPI_getvalue(row, desc, 2);
		info->pkey_attno[i] = attno;
		if (i > 0)
			appendStringInfoChar(pkeys, ',');
		appendStringInfoString(pkeys, name);
	}
	info->pkey_list = MemoryContextStrdup(tbl_cache_ctx, pkeys->data);
}

static void
free_info(struct PgqTableInfo *info)
{
	pfree(info->table_name);
	pfree(info->pkey_attno);
	pfree((void *)info->pkey_list);
}

static void relcache_reset_cb(Datum arg, Oid relid)
{
	if (relid == InvalidOid) {
		full_reset();
	} else if (tbl_cache_map) {
	 	struct PgqTableInfo *entry;
	 	entry = hash_search(tbl_cache_map, &relid, HASH_FIND, NULL);
		if (entry) {
			free_info(entry);
	 		hash_search(tbl_cache_map, &relid, HASH_REMOVE, NULL);
		}
	}
}

/*
 * fetch insert plan from cache.
 */
struct PgqTableInfo *
pgq_find_table_info(Relation rel)
{
	 struct PgqTableInfo *entry;
	 bool did_exist = false;

	 init_module();

	 entry = hash_search(tbl_cache_map, &rel->rd_id, HASH_ENTER, &did_exist);
	 if (!did_exist)
		 fill_tbl_info(rel, entry);
	 return entry;
}

static void
parse_newstyle_args(PgqTriggerEvent *ev, TriggerData *tg)
{
	int i;
	/*
	 * parse args
	 */
	ev->skip = false;
	ev->queue_name = tg->tg_trigger->tgargs[0];
	for (i = 1; i < tg->tg_trigger->tgnargs; i++) {
		const char *arg = tg->tg_trigger->tgargs[i];
		if (strcmp(arg, "SKIP") == 0)
			ev->skip = true;
		else if (strncmp(arg, "ignore=", 7) == 0)
			ev->ignore_list = arg + 7;
		else if (strncmp(arg, "pkey=", 5) == 0)
			ev->pkey_list = arg + 5;
		else if (strcmp(arg, "backup") == 0)
			ev->backup = true;
		else
			elog(ERROR, "bad param to pgq trigger");
	}
}

static void
parse_oldstyle_args(PgqTriggerEvent *ev, TriggerData *tg)
{
	const char *kpos;
	int attcnt, i;
	TupleDesc tupdesc = tg->tg_relation->rd_att;

	ev->skip = false;
	if (tg->tg_trigger->tgnargs < 2 || tg->tg_trigger->tgnargs > 3)
		elog(ERROR, "pgq.logtriga must be used with 2 or 3 args");
	ev->queue_name = tg->tg_trigger->tgargs[0];
	ev->attkind = tg->tg_trigger->tgargs[1];
	ev->attkind_len = strlen(ev->attkind);
	if (tg->tg_trigger->tgnargs > 2)
		ev->table_name =  tg->tg_trigger->tgargs[2];


	/*
	 * Count number of active columns
	 */
	tupdesc = tg->tg_relation->rd_att;
	for (i = 0, attcnt = 0; i < tupdesc->natts; i++)
	{
		if (!tupdesc->attrs[i]->attisdropped)
			attcnt++;
	}

	/*
	 * look if last pkey column exists
	 */
	kpos = strrchr(ev->attkind, 'k');
	if (kpos == NULL)
		elog(ERROR, "need at least one key column");
	if (kpos - ev->attkind >= attcnt)
		elog(ERROR, "key column does not exist");
}

/*
 * parse trigger arguments.
 */
void pgq_prepare_event(struct PgqTriggerEvent *ev, TriggerData *tg, bool newstyle)
{
	memset(ev, 0, sizeof(*ev));

	/*
	 * Check trigger calling conventions
	 */
	if (!TRIGGER_FIRED_AFTER(tg->tg_event))
		/* dont care */;
	if (!TRIGGER_FIRED_FOR_ROW(tg->tg_event))
		elog(ERROR, "pgq trigger must be fired FOR EACH ROW");
	if (tg->tg_trigger->tgnargs < 1)
		elog(ERROR, "pgq trigger must have destination queue as argument");

	/*
	 * check operation type
	 */
	if (TRIGGER_FIRED_BY_INSERT(tg->tg_event))
		ev->op_type = 'I';
	else if (TRIGGER_FIRED_BY_UPDATE(tg->tg_event))
		ev->op_type = 'U';
	else if (TRIGGER_FIRED_BY_DELETE(tg->tg_event))
		ev->op_type = 'D';
	else
		elog(ERROR, "unknown event for pgq trigger");

	/*
	 * load table info
	 */
	ev->info = pgq_find_table_info(tg->tg_relation);
	ev->table_name = ev->info->table_name;
	ev->pkey_list = ev->info->pkey_list;

	/*
	 * parse args
	 */
	if (newstyle)
		parse_newstyle_args(ev, tg);
	else
		parse_oldstyle_args(ev, tg);

	/*
	 * init data
	 */
	ev->ev_type = pgq_init_varbuf();
	ev->ev_data = pgq_init_varbuf();
	ev->ev_extra1 = pgq_init_varbuf();
	
	/*
	 * Do the backup, if requested.
	 */
	if (ev->backup) {
		ev->ev_extra2 = pgq_init_varbuf();
		pgq_urlenc_row(ev, tg, tg->tg_trigtuple, ev->ev_extra2);
	}
}

/*
 * Check if column should be skipped
 */
bool pgqtriga_skip_col(PgqTriggerEvent *ev, TriggerData *tg, int i, int attkind_idx)
{
	TupleDesc tupdesc;
	const char *name;

	if (ev->attkind) {
		if (attkind_idx >= ev->attkind_len)
			return true;
		return ev->attkind[attkind_idx] == 'i';
	} else if (ev->ignore_list) {
		tupdesc = tg->tg_relation->rd_att;
		if (tupdesc->attrs[i]->attisdropped)
			return true;
		name = NameStr(tupdesc->attrs[i]->attname);
		return pgq_strlist_contains(ev->ignore_list, name);
	}
	return false;
}

/*
 * Check if column is pkey.
 */
bool pgqtriga_is_pkey(PgqTriggerEvent *ev, TriggerData *tg, int i, int attkind_idx)
{
	TupleDesc tupdesc;
	const char *name;

	if (ev->attkind) {
		if (attkind_idx >= ev->attkind_len)
			return false;
		return ev->attkind[attkind_idx] == 'k';
	} else if (ev->pkey_list) {
		tupdesc = tg->tg_relation->rd_att;
		if (tupdesc->attrs[i]->attisdropped)
			return false;
		name = NameStr(tupdesc->attrs[i]->attname);
		return pgq_strlist_contains(ev->pkey_list, name);
	}
	return false;
}


/*
 * Check if trigger action should be skipped.
 */

bool pgq_is_logging_disabled(void)
{
#if defined(PG_VERSION_NUM) && PG_VERSION_NUM >= 80300
	if (SessionReplicationRole != SESSION_REPLICATION_ROLE_ORIGIN)
		return true;
#endif
	return false;
}

