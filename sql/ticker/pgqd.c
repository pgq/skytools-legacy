#include "pgqd.h"

#include <getopt.h>
#include <errno.h>
#include <signal.h>

#include <usual/event.h>
#include <usual/string.h>
#include <usual/alloc.h>
#include <usual/daemon.h>
#include <usual/cfparser.h>
#include <usual/time.h>

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

#define DEFSTR(name, def) { #name, cf_set_str, offsetof(struct Config, name), def }
#define DEFINT(name, def) { #name, cf_set_int, offsetof(struct Config, name), def }

static const struct CfKey conf_params[] = {
	DEFSTR(logfile, NULL),
	DEFSTR(pidfile, NULL),
	DEFSTR(initial_database, "template1"),
	DEFSTR(base_connstr, ""),
	DEFSTR(database_list, NULL),
	DEFINT(syslog, "0"),
	DEFINT(check_period, "60"),
	DEFINT(maint_period, "120"),
	DEFINT(retry_period, "30"),
	DEFINT(ticker_period, "1"),
	{ NULL },
};

static void *get_cf_target(void *arg) { return &cf; }

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
	cf_logfile = cf.logfile;
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
	/* pidfile cleanup happens via atexit() */
	exit(1);
}

static void handle_sighup(int sock, short flags, void *arg)
{
	log_info("Got SIGHUP re-reading config");
	load_config(true);
}

static void signal_setup(void)
{
	static struct event ev_sighup;
	static struct event ev_sigterm;
	static struct event ev_sigint;

	sigset_t set;
	int err;

	/* block SIGPIPE */
	sigemptyset(&set);
	sigaddset(&set, SIGPIPE);
	err = sigprocmask(SIG_BLOCK, &set, NULL);
	if (err < 0)
		fatal_perror("sigprocmask");

	/* catch signals */
	signal_set(&ev_sighup, SIGHUP, handle_sighup, NULL);
	err = signal_add(&ev_sighup, NULL);
	if (err < 0)
		fatal_perror("signal_add");

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
	char buf[512];
	snprintf(buf, sizeof(buf), "%s dbname=%s ", cf.base_connstr, dbname);
	return strdup(buf);
}

static void launch_db(const char *dbname)
{
	struct PgDatabase *db;

	db = calloc(1, sizeof(*db));
	db->name = strdup(dbname);
	statlist_init(&db->maint_item_list, "maint_item_list");
	list_init(&db->head);
	statlist_append(&database_list, &db->head);

	launch_ticker(db);
}

static void detect_handler(struct PgSocket *db, void *arg, enum PgEvent ev, PGresult *res)
{
	int i;
	const char *s;

	switch (ev) {
	case DB_CONNECT_OK:
		db_send_query_simple(db, "select datname from pg_database"
				     	 " where not datistemplate and datallowconn");
		break;
	case DB_RESULT_OK:
		for (i = 0; i < PQntuples(res); i++) {
			s = PQgetvalue(res, i, 0);
			launch_db(s);
		}
		PQclear(res);
		db_free(db_template);
		db_template = NULL;
		break;
	default:
		fatal("failure");
	}
}

static void detect_dbs(void)
{
	const char *cstr = make_connstr(cf.initial_database);
	db_template = db_create(detect_handler, NULL);
	db_connect(db_template, cstr);
	free(cstr);
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

	if (!cf.database_list || !cf.database_list[0]) {
		log_info("auto-detecting dbs ...");
		detect_dbs();
	} else {
		fatal("fixed list not implemented yet: '%s'", cf.database_list);
	}

	while (1)
		main_loop_once();

	return 0;
}
