#include "pgqd.h"

#include <usual/string.h>

static bool fill_items(struct PgDatabase *db, PGresult *res)
{
	int i;
	if (db->maint_item_list)
		strlist_free(db->maint_item_list);
	db->maint_item_list = strlist_new();
	if (!db->maint_item_list)
		return false;
	for (i = 0; i < PQntuples(res); i++) {
		const char *item = PQgetvalue(res, i, 0);
		if (item)
			if (!strlist_append(db->maint_item_list, item))
				return false;
	}
	return true;
}

static void run_queue_list(struct PgDatabase *db)
{
	const char *q = "select queue_name from pgq.get_queue_info()";
	log_debug("%s: %s", db->name, q);
	db_send_query_simple(db->c_maint, q);
	db->maint_state = DB_MAINT_LOAD_QUEUES;
}

static void run_vacuum_list(struct PgDatabase *db)
{
	const char *q = "select * from pgq.maint_tables_to_vacuum()";
	log_debug("%s: %s", db->name, q);
	db_send_query_simple(db->c_maint, q);
	db->maint_state = DB_MAINT_VACUUM_LIST;
}

static void run_rotate_part1(struct PgDatabase *db)
{
	const char *q;
	const char *qname;
	qname = strlist_pop(db->maint_item_list);
	q = "select pgq.maint_rotate_part1($1)";
	log_debug("%s: %s [%s]", db->name, q, qname);
	db_send_query_params(db->c_maint, q, 1, qname);
	free(qname);
	db->maint_state = DB_MAINT_ROT1;
}

static void run_rotate_part2(struct PgDatabase *db)
{
	const char *q = "select pgq.maint_rotate_part2()";
	log_debug("%s: %s", db->name, q);
	db_send_query_simple(db->c_maint, q);
	db->maint_state = DB_MAINT_ROT2;
}

static void run_vacuum(struct PgDatabase *db)
{
	char qbuf[256];
	const char *table;
	table = strlist_pop(db->maint_item_list);
	snprintf(qbuf, sizeof(qbuf), "vacuum %s", table);
	log_debug("%s: %s", db->name, qbuf);
	db_send_query_simple(db->c_maint, qbuf);
	free(table);
	db->maint_state = DB_MAINT_DO_VACUUM;
}

static void close_maint(struct PgDatabase *db, double sleep_time)
{
	log_debug("%s: close_maint, %f", db->name, sleep_time);
	db->maint_state = DB_CLOSED;
	db_disconnect(db->c_maint);
	db_sleep(db->c_maint, sleep_time);
}

static void maint_handler(struct PgSocket *s, void *arg, enum PgEvent ev, PGresult *res)
{
	struct PgDatabase *db = arg;

	switch (ev) {
	case DB_CONNECT_OK:
		log_info("%s: starting maintenance", db->name);
		run_queue_list(db);
		break;
	case DB_RESULT_OK:
		switch (db->maint_state) {
		case DB_MAINT_LOAD_QUEUES:
			if (!fill_items(db, res))
				goto mem_err;
		case DB_MAINT_ROT1:
			PQclear(res);
			if (!strlist_empty(db->maint_item_list)) {
				run_rotate_part1(db);
			} else {
				run_rotate_part2(db);
			}
			break;
		case DB_MAINT_ROT2:
			PQclear(res);
			run_vacuum_list(db);
			break;
		case DB_MAINT_VACUUM_LIST:
			if (!fill_items(db, res))
				goto mem_err;
		case DB_MAINT_DO_VACUUM:
			PQclear(res);
			if (!strlist_empty(db->maint_item_list)) {
				run_vacuum(db);
			} else {
				close_maint(db, cf.maint_period);
			}
			break;
		default:
			fatal("bad state");
		}
		break;
	case DB_TIMEOUT:
		log_debug("%s: maint timeout", db->name);
		if (!db_connection_valid(db->c_maint))
			launch_maint(db);
		else
			run_queue_list(db);
		break;
	default:
		db_reconnect(db->c_maint);
	}
	return;
mem_err:
	if (db->maint_item_list) {
		strlist_free(db->maint_item_list);
		db->maint_item_list = NULL;
	}
	db_disconnect(db->c_maint);
	db_sleep(db->c_maint, 20);
}

void launch_maint(struct PgDatabase *db)
{
	log_debug("%s: launch_maint", db->name);

	if (!db->c_maint) {
		if (db->maint_item_list) {
			strlist_free(db->maint_item_list);
			db->maint_item_list = NULL;
		}
		db->c_maint = db_create(maint_handler, db);
	}

	if (!db_connection_valid(db->c_maint)) {
		const char *cstr = make_connstr(db->name);

		db_connect(db->c_maint, cstr);
		free(cstr);
	} else {
		/* Already have a connection, what are we doing here */
		log_error("%s: maint already initialized", db->name);
		return;
	}
}

