
#include "pgqd.h"


static void close_retry(struct PgDatabase *db, int sleep_time)
{
	log_debug("%s: close_retry, %d", db->name, sleep_time);
	db_disconnect(db->c_retry);
	db_sleep(db->c_retry, sleep_time);
}

static void run_retry(struct PgDatabase *db)
{
	const char *q = "select * from pgq.maint_retry_events()";
	log_debug("%s: %s", db->name, q);
	db_send_query_simple(db->c_retry, q);
}

static void parse_retry(struct PgDatabase *db, PGresult *res)
{
	if (PQntuples(res) == 1) {
		char *val = PQgetvalue(res, 0, 0);
		if (strcmp(val, "0") != 0) {
			run_retry(db);
		}
	}
	close_retry(db, 20);
}

static void retry_handler(struct PgSocket *s, void *arg, enum PgEvent ev, PGresult *res)
{
	struct PgDatabase *db = arg;

	switch (ev) {
	case DB_CONNECT_OK:
		run_retry(db);
		break;
	case DB_RESULT_OK:
		parse_retry(db, res);
		break;
	case DB_TIMEOUT:
		log_debug("%s: retry timeout", db->name);
		launch_retry(db);
		break;
	default:
		db_reconnect(db->c_retry);
	}
}

void launch_retry(struct PgDatabase *db)
{
	const char *cstr;
	if (db->c_retry) {
		log_debug("%s: retry already initialized", db->name);
	} else {
		log_debug("%s: launch_retry", db->name);
		db->c_retry = db_create(retry_handler, db);
	}
	cstr = make_connstr(db->name);
	db_connect(db->c_retry, cstr);
	free(cstr);
}

