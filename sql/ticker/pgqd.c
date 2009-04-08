#include "pgqd.h"

#include <getopt.h>
#include <event.h>

static const char *usage_str =
"usage: pgq-ticker [switches] [db ..]\n"
"Switches:\n"
"  -T db     Set initial db name to connect to (default: template1)\n"
"  -p port   port\n"
"  -U user   Username to use\n"
"  -h host   Host to use\n"
"  -H        Show help\n"
"Not implemented:\n"
"  -v        Increase verbosity\n"
"  -V        Show version\n"
"  -d        Daemonize\n"
"";

struct Config cf = {
	.db_template = "template1",
	.verbose = 1,
};

static struct PgSocket *db_template;

static STATLIST(database_list);

const char *make_connstr(const char *dbname)
{
	char buf[512];

	snprintf(buf, sizeof(buf), "dbname=%s", dbname);
	if (cf.db_host) {
		strlcat(buf, " host=", sizeof(buf));
		strlcat(buf, cf.db_host, sizeof(buf));
	}
	if (cf.db_port) {
		strlcat(buf, " port=", sizeof(buf));
		strlcat(buf, cf.db_port, sizeof(buf));
	}
	if (cf.db_username) {
		strlcat(buf, " user=", sizeof(buf));
		strlcat(buf, cf.db_username, sizeof(buf));
	}
	return strdup(buf);
}

static void launch_db(const char *dbname)
{
	struct PgDatabase *db;

	db = calloc(1, sizeof(*db));
	db->name = strdup(dbname);
	statlist_init(&db->maint_item_list, "maint_item_list");
	list_init(&db->head);
	statlist_append(&db->head, &database_list);

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
		printf("failure\n");
		exit(1);
	}
}

static void detect_dbs(void)
{
	const char *cstr = make_connstr(cf.db_template);
	db_template = db_create(detect_handler, NULL);
	db_connect(db_template, cstr);
	free(cstr);
}

static void main_loop_once(void)
{
	reset_time_cache();
	event_loop(EVLOOP_ONCE);
}

int main(int argc, char *argv[])
{
	int c, i;

	while ((c = getopt(argc, argv, "T:p:U:h:HvVd")) != -1) {
		switch (c) {
		case 'T':
			cf.db_template = optarg;
			break;
		case 'p':
			cf.db_port = optarg;
			break;
		case 'U':
			cf.db_username = optarg;
			break;
		case 'h':
			cf.db_host = optarg;
			break;
		case 'H':
			printf(usage_str);
			return 0;
		default:
			printf(usage_str);
			return 1;
		}
	}

	event_init();

	if (optind == argc) {
		printf("auto-detecting dbs ...\n");
		detect_dbs();
	} else {
		for (i = optind; i < argc; i++) {
			launch_db(argv[i]);
		}
	}

	while (1)
		main_loop_once();

	return 0;
}
