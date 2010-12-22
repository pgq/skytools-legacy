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

static const char usage_str[] =
"usage: pgq-ticker [switches] config.file\n"
"Switches:\n"
"  -v        Increase verbosity\n"
"  -q        No output to console\n"
"  -d        Daemonize\n"
"  -h        Show help\n"
"  -V        Show version\n"
" --ini      Show sample config file\n"
"  -s        Stop - send SIGINT to running process\n"
"  -k        Kill - send SIGTERM to running process\n"
#ifdef SIGHUP
"  -r        Reload - send SIGHUP to running process\n"
#endif
"";

static const char *sample_ini =
#include "pgqd.ini.h"
;

struct Config cf;

struct Stats stats;

static struct PgSocket *db_template;

static STATLIST(database_list);

static int got_sigint;

#define CF_REL_BASE struct Config
static const struct CfKey conf_params[] = {
	CF_ABS("logfile", CF_FILE, cf_logfile, 0, NULL),
	CF_REL("pidfile", CF_FILE, pidfile, 0, NULL),
	CF_REL("initial_database", CF_STR, initial_database, 0, "template1"),
	CF_REL("base_connstr", CF_STR, base_connstr, 0, ""),
	CF_REL("database_list", CF_STR, database_list, 0, NULL),
	CF_ABS("syslog", CF_INT, cf_syslog, 0, "1"),
	CF_ABS("syslog_ident", CF_STR, cf_syslog_ident, 0, "pgqd"),
	CF_REL("check_period", CF_TIME_DOUBLE, check_period, 0, "60"),
	CF_REL("maint_period", CF_TIME_DOUBLE, maint_period, 0, "120"),
	CF_REL("retry_period", CF_TIME_DOUBLE, retry_period, 0, "30"),
	CF_REL("ticker_period", CF_TIME_DOUBLE, ticker_period, 0, "1"),
	CF_REL("stats_period", CF_TIME_DOUBLE, stats_period, 0, "30"),
	CF_REL("connection_lifetime", CF_TIME_DOUBLE, connection_lifetime, 0, "3600"),
	{ NULL },
};

static const struct CfSect conf_sects[] = {
	{ "pgqd", conf_params },
	{ NULL }
};

static struct CfContext conf_info = {
	.sect_list = conf_sects,
	.base = &cf,
};

static void load_config(void)
{
	bool ok = cf_load_file(&conf_info, cf.config_file);
	if (!ok)
		fatal("failed to read config");
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
	log_info("Got SIGHUP, re-reading config");
	load_config();
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
	statlist_init(&db->maint_op_list, "maint_op_list");
	statlist_append(&database_list, &db->head);

	/* start working on it */
	launch_ticker(db);
}

static void drop_db(struct PgDatabase *db, bool log)
{
	if (log)
		log_info("Unregister database: %s", db->name);
	statlist_remove(&database_list, &db->head);
	pgs_free(db->c_ticker);
	pgs_free(db->c_maint);
	pgs_free(db->c_retry);
	free_maint(db);
	free(db->name);
	free(db);
}

static void detect_handler(struct PgSocket *sk, void *arg, enum PgEvent ev, PGresult *res)
{
	int i;
	const char *s;
	struct List *el, *tmp;
	struct PgDatabase *db;

	switch (ev) {
	case PGS_CONNECT_OK:
		pgs_send_query_simple(sk, "select datname from pg_database"
				     	 " where not datistemplate and datallowconn");
		break;
	case PGS_RESULT_OK:
		/* tag old dbs as dead */
		statlist_for_each(el, &database_list) {
			db = container_of(el, struct PgDatabase, head);
			db->dropped = true;
		}
		/* process new dbs */
		for (i = 0; i < PQntuples(res); i++) {
			s = PQgetvalue(res, i, 0);
			launch_db(s);
		}
		/* drop old dbs */
		statlist_for_each_safe(el, &database_list, tmp) {
			db = container_of(el, struct PgDatabase, head);
			if (db->dropped)
				drop_db(db, true);
		}
		pgs_disconnect(sk);
		pgs_sleep(sk, cf.check_period);
		break;
	case PGS_TIMEOUT:
		detect_dbs();
		break;
	default:
		pgs_disconnect(sk);
		pgs_sleep(sk, cf.check_period);
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
		/* tag old dbs as dead */
		statlist_for_each(el, &database_list) {
			db = container_of(el, struct PgDatabase, head);
			db->dropped = true;
		}
		/* process new ones */
		if (!parse_word_list(cf.database_list, launch_db_cb, NULL)) {
			log_warning("database_list parsing failed: %s", strerror(errno));
			return;
		}
		/* drop old ones */
		statlist_for_each_safe(el, &database_list, tmp) {
			db = container_of(el, struct PgDatabase, head);
			if (db->dropped)
				drop_db(db, true);
		}

		/* done with template for the moment */
		if (db_template) {
			pgs_free(db_template);
			db_template = NULL;
		}
	} else if (!db_template) {
		log_info("auto-detecting dbs ...");
		detect_dbs();
	}
}

static struct event stats_ev;

static void stats_handler(int fd, short flags, void *arg)
{
	struct timeval tv = { cf.stats_period, 0 };

	log_info("{ticks: %d, maint: %d, retry: %d}",
		 stats.n_ticks, stats.n_maint, stats.n_retry);
	memset(&stats, 0, sizeof(stats));

	if (evtimer_add(&stats_ev, &tv) < 0)
		fatal_perror("evtimer_add");
}

static void stats_setup(void)
{
	struct timeval tv = { cf.stats_period, 0 };
	evtimer_set(&stats_ev, stats_handler, NULL);
	if (evtimer_add(&stats_ev, &tv) < 0)
		fatal_perror("evtimer_add");
}

static void cleanup(void)
{
	struct PgDatabase *db;
	struct List *elem, *tmp;

	statlist_for_each_safe(elem, &database_list, tmp) {
		db = container_of(elem, struct PgDatabase, head);
		drop_db(db, false);
	}
	pgs_free(db_template);

	event_base_free(NULL);
	reset_logging();
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
	int sig = 0;
	const char *signame = NULL;

	for (c = 1; c < argc; c++) {
		if (!strcmp(argv[c], "--ini")) {
			printf("%s", sample_ini);
			exit(0);
		}
		if (!strcmp(argv[c], "--help")) {
			printf(usage_str);
			exit(0);
		}
	}

	while ((c = getopt(argc, argv, "dqvhVrsk")) != -1) {
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
#ifdef SIGHUP
		case 'r':
			sig = SIGHUP;
			signame = "SIGHUP";
			break;
#endif
		case 's':
			sig = SIGINT;
			signame = "SIGINT";
			break;
		case 'k':
			sig = SIGTERM;
			signame = "SIGTERM";
			break;
		default:
			printf("bad switch: ");
			printf(usage_str);
			return 1;
		}
	}
	if (optind + 1 != argc) {
		fprintf(stderr, "pgqd requires config file\n");
		return 1;
	}

	cf.config_file = argv[optind];

	load_config();
	conf_info.loaded = true;

	if (sig) {
		if (!cf.pidfile || !cf.pidfile[0]) {
			fprintf(stderr, "No pidfile configured\n");
			return 1;
		}
		if (signal_pidfile(cf.pidfile, sig))
			fprintf(stderr, "%s sent\n", signame);
		else
			fprintf(stderr, "Old process is not running\n");
		return 0;
	}

	log_info("Starting pgqd " PACKAGE_VERSION);

	daemonize(cf.pidfile, daemon);

	if (!event_init())
		fatal("event_init failed");

	signal_setup();

	stats_setup();

	recheck_dbs();

	while (!got_sigint)
		main_loop_once();

	cleanup();

	return 0;
}
