#include "pgqd.h"

#include <getopt.h>

#include <usual/event.h>
#include <usual/string.h>
#include <usual/signal.h>
#include <usual/daemon.h>
#include <usual/cfparser.h>
#include <usual/time.h>

static void detect_dbs(void);
static void recheck_dbs(void);

static const char *usage_str =
"usage: pgq-ticker [switches] config.file\n"
"Switches:\n"
"  -H        Show help\n"
"  -v        Increase verbosity\n"
"  -q        No output to console\n"
"  -V        Show version\n"
"  -d        Daemonize\n"
"";

struct Config cf;

static struct PgSocket *db_template;

static STATLIST(database_list);

static int got_sigint;

#define CF_REL_BASE struct Config
static const struct CfKey conf_params[] = {
	{ "logfile", CF_ABS_STR(cf_logfile) },
	{ "pidfile", CF_REL_STR(pidfile) },
	{ "initial_database", CF_REL_STR(initial_database), "template1" },
	{ "base_connstr", CF_REL_STR(base_connstr), "" },
	{ "database_list", CF_REL_STR(database_list) },
	{ "syslog", CF_REL_INT(syslog) },
	{ "check_period", CF_REL_TIME_DOUBLE(check_period), "60" },
	{ "maint_period", CF_REL_TIME_DOUBLE(maint_period), "120" },
	{ "retry_period", CF_REL_TIME_DOUBLE(retry_period), "30" },
	{ "ticker_period", CF_REL_TIME_DOUBLE(ticker_period), "1" },
	{ NULL },
};

static void *get_cf_target(void *arg, const char *name) { return &cf; }

static const struct CfSect conf_sects[] = {
	{ "pgqd", get_cf_target, conf_params },
	{ NULL }
};

static void load_config(bool reload)
{
	bool ok = load_ini_file(cf.config_file, conf_sects, NULL);
	if (!ok) {
		if (reload) {
			log_warning("failed to read config");
		} else {
			fatal("failed to read config");
		}
	}

	/* fixme */
	cf_syslog_ident = cf.syslog ? "pgqd" : NULL;
	reset_logging();
}

static void handle_sigterm(int sock, short flags, void *arg)
{
	log_info("Got SIGTERM, fast exit");
	/* pidfile cleanup happens via atexit() */
	exit(1);
}

static void handle_sigint(int sock, short flags, void *arg)
{
	log_info("Got SIGINT, shutting down");
	/* notify main loop to exit */
	got_sigint = 1;
}

static void handle_sighup(int sock, short flags, void *arg)
{
	log_info("Got SIGHUP re-reading config");
	load_config(true);
	recheck_dbs();
}

static void signal_setup(void)
{
	static struct event ev_sighup;
	static struct event ev_sigterm;
	static struct event ev_sigint;

	int err;

#ifdef SIGPIPE
	sigset_t set;

	/* block SIGPIPE */
	sigemptyset(&set);
	sigaddset(&set, SIGPIPE);
	err = sigprocmask(SIG_BLOCK, &set, NULL);
	if (err < 0)
		fatal_perror("sigprocmask");
#endif

#ifdef SIGHUP
	/* catch signals */
	signal_set(&ev_sighup, SIGHUP, handle_sighup, NULL);
	err = signal_add(&ev_sighup, NULL);
	if (err < 0)
		fatal_perror("signal_add");
#endif

	signal_set(&ev_sigterm, SIGTERM, handle_sigterm, NULL);
	err = signal_add(&ev_sigterm, NULL);
	if (err < 0)
		fatal_perror("signal_add");

	signal_set(&ev_sigint, SIGINT, handle_sigint, NULL);
	err = signal_add(&ev_sigint, NULL);
	if (err < 0)
		fatal_perror("signal_add");
}

const char *make_connstr(const char *dbname)
{
	static char buf[512];
	snprintf(buf, sizeof(buf), "%s dbname=%s ", cf.base_connstr, dbname);
	return buf;
}

static void launch_db(const char *dbname)
{
	struct PgDatabase *db;
	struct List *elem;

	/* check of already exists */
	statlist_for_each(elem, &database_list) {
		db = container_of(elem, struct PgDatabase, head);
		if (strcmp(db->name, dbname) == 0) {
			db->dropped = false;
			return;
		}
	}

	/* create new db entry */
	db = calloc(1, sizeof(*db));
	db->name = strdup(dbname);
	list_init(&db->head);
	statlist_append(&database_list, &db->head);

	/* start working on it */
	launch_ticker(db);
}

static void drop_db(struct PgDatabase *db)
{
	statlist_remove(&database_list, &db->head);
	pgs_free(db->c_ticker);
	pgs_free(db->c_maint);
	pgs_free(db->c_retry);
	strlist_free(db->maint_item_list);
	free(db->name);
	free(db);
}

static void detect_handler(struct PgSocket *db, void *arg, enum PgEvent ev, PGresult *res)
{
	int i;
	const char *s;

	switch (ev) {
	case PGS_CONNECT_OK:
		pgs_send_query_simple(db, "select datname from pg_database"
				     	 " where not datistemplate and datallowconn");
		break;
	case PGS_RESULT_OK:
		for (i = 0; i < PQntuples(res); i++) {
			s = PQgetvalue(res, i, 0);
			launch_db(s);
		}
		pgs_disconnect(db);
		pgs_sleep(db, cf.check_period);
		break;
	case PGS_TIMEOUT:
		detect_dbs();
		break;
	default:
		pgs_disconnect(db);
		pgs_sleep(db, cf.check_period);
	}
}

static void detect_dbs(void)
{
	if (!db_template) {
		const char *cstr = make_connstr(cf.initial_database);
		db_template = pgs_create(cstr, detect_handler, NULL);
	}
	pgs_connect(db_template);
}

static bool launch_db_cb(void *arg, const char *db)
{
	launch_db(db);
	return true;
}

static void recheck_dbs(void)
{
	struct PgDatabase *db;
	struct List *el, *tmp;
	if (cf.database_list && cf.database_list[0]) {
		statlist_for_each(el, &database_list) {
			db = container_of(el, struct PgDatabase, head);
			db->dropped = true;
		}
		if (!parse_word_list(cf.database_list, launch_db_cb, NULL)) {
			log_warning("database_list parsing failed: %s", strerror(errno));
			return;
		}
		statlist_for_each_safe(el, &database_list, tmp) {
			db = container_of(el, struct PgDatabase, head);
			if (db->dropped)
				drop_db(db);
		}
		if (db_template) {
			pgs_free(db_template);
			db_template = NULL;
		}
	} else if (!db_template) {
		log_info("auto-detecting dbs ...");
		detect_dbs();
	}
}

static void cleanup(void)
{
	struct PgDatabase *db;
	struct List *elem, *tmp;

	statlist_for_each_safe(elem, &database_list, tmp) {
		db = container_of(elem, struct PgDatabase, head);
		drop_db(db);
	}
	pgs_free(db_template);

	event_base_free(NULL);
}

static void main_loop_once(void)
{
	reset_time_cache();
	if (event_loop(EVLOOP_ONCE) != 0) {
		log_error("event_loop failed: %s", strerror(errno));
	}
}

int main(int argc, char *argv[])
{
	int c;
	bool daemon = false;

	while ((c = getopt(argc, argv, "dqvhV")) != -1) {
		switch (c) {
		case 'd':
			daemon = true;
			break;
		case 'v':
			cf_verbose++;
			break;
		case 'q':
			cf_quiet = 1;
			break;
		case 'h':
			printf(usage_str);
			return 0;
		default:
			printf("bad switch: ");
			printf(usage_str);
			return 1;
		}
	}
	if (optind + 1 != argc) {
		printf("pgqd requires config file\n");
		return 1;
	}

	cf.config_file = argv[optind];

	load_config(false);

	daemonize(cf.pidfile, daemon);

	if (!event_init())
		fatal("event_init failed");

	signal_setup();

	recheck_dbs();

	while (!got_sigint)
		main_loop_once();

	cleanup();

	return 0;
}
