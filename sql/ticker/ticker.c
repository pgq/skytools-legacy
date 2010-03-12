#include "pgqd.h"

static void run_pgq_check(struct PgDatabase *db)
{
	const char *q = "select 1 from pg_catalog.pg_namespace where nspname='pgq'";
	log_debug("%s: %s", db->name, q);
	pgs_send_query_simple(db->c_ticker, q);
	db->state = DB_TICKER_CHECK_PGQ;
}

static void run_version_check(struct PgDatabase *db)
{
	const char *q = "select pgq.version()";
	log_debug("%s: %s", db->name, q);
	pgs_send_query_simple(db->c_ticker, q);
	db->state = DB_TICKER_CHECK_VERSION;
}

static void run_ticker(struct PgDatabase *db)
{
	const char *q = "select pgq.ticker()";
	log_debug("%s: %s", db->name, q);
	pgs_send_query_simple(db->c_ticker, q);
	db->state = DB_TICKER_RUN;
}

static void close_ticker(struct PgDatabase *db, double sleep_time)
{
	log_debug("%s: close_ticker, %f", db->name, sleep_time);
	db->state = DB_CLOSED;
	pgs_reconnect(db->c_ticker, sleep_time);
}

static void parse_pgq_check(struct PgDatabase *db, PGresult *res)
{
	db->has_pgq = PQntuples(res) == 1;

	if (!db->has_pgq) {
		log_info("%s: no pgq", db->name);
		close_ticker(db, cf.check_period);
	} else {
		run_version_check(db);
	}
}

static void parse_version_check(struct PgDatabase *db, PGresult *res)
{
	char *ver;
	if (PQntuples(res) != 1) {
		log_debug("%s: calling pgq.version() failed", db->name);
		goto badpgq;
	}
	ver = PQgetvalue(res, 0, 0);
	if (ver[0] < '3') {
		log_debug("%s: bad pgq version: %s", db->name, ver);
		goto badpgq;
	}
	log_info("%s: pgq version ok: %s", db->name, ver);

	run_ticker(db);
	if (!db->c_maint)
		launch_maint(db);
	if (!db->c_retry)
		launch_retry(db);
	return;

badpgq:
	db->has_pgq = false;
	log_info("%s: bad pgq version, ignoring", db->name);
	close_ticker(db, cf.check_period);
}

static void parse_ticker_result(struct PgDatabase *db, PGresult *res)
{
	if (PQntuples(res) != 1) {
		log_debug("%s: calling pgq.ticker() failed", db->name);
	}

	pgs_sleep(db->c_ticker, cf.ticker_period);
}

static void tick_handler(struct PgSocket *s, void *arg, enum PgEvent ev, PGresult *res)
{
	struct PgDatabase *db = arg;

	switch (ev) {
	case PGS_CONNECT_OK:
		run_pgq_check(db);
		break;
	case PGS_RESULT_OK:
		switch (db->state) {
		case DB_TICKER_CHECK_PGQ:
			parse_pgq_check(db, res);
			break;
		case DB_TICKER_CHECK_VERSION:
			parse_version_check(db, res);
			break;
		case DB_TICKER_RUN:
			parse_ticker_result(db, res);
			break;
		default:
			fatal("bad state");
		}
		break;
	case PGS_TIMEOUT:
		log_debug("%s: tick timeout", db->name);
		if (!pgs_connection_valid(db->c_ticker))
			launch_ticker(db);
		else
			run_ticker(db);
		break;
	default:
		log_warning("%s: default timeout", db->name);
		pgs_reconnect(db->c_ticker, 60);
	}
}

void launch_ticker(struct PgDatabase *db)
{
	log_debug("%s: launch_ticker", db->name);
	if (!db->c_ticker) {
		const char *cstr = make_connstr(db->name);
		db->c_ticker = pgs_create(cstr, tick_handler, db);
	}
	pgs_connect(db->c_ticker);
}

