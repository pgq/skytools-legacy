#include "pgqd.h"

struct MaintItem {
	List head;
	const char *name;
};

static void add_maint_item(struct PgDatabase *db, const char *name)
{
	struct MaintItem *item = calloc(1, sizeof(*item));
	if (!item)
		return;
	list_init(&item->head);
	item->name = strdup(name);
	if (!item->name) {
		free(item);
		return;
	}
	statlist_append(&item->head, &db->maint_item_list);
}

static const char *pop_maint_item(struct PgDatabase *db)
{
	struct MaintItem *item;
	struct List *el;
	const char *name;

	el = statlist_pop(&db->maint_item_list);
	if (!el)
		return NULL;

	item = container_of(el, struct MaintItem, head);
	name = item->name;
	free(item);
	return name;
}

static void free_maint_list(struct PgDatabase *db)
{
	const char *name;
	while (1) {
		name = pop_maint_item(db);
		if (!name)
			break;
		free(name);
	}
}


static void fill_items(struct PgDatabase *db, PGresult *res)
{
	int i;
	for (i = 0; i < PQntuples(res); i++) {
		const char *item = PQgetvalue(res, i, 0);
		if (item)
			add_maint_item(db, item);
	}
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
	qname = pop_maint_item(db);
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
	table = pop_maint_item(db);
	snprintf(qbuf, sizeof(qbuf), "vacuum %s", table);
	log_debug("%s: %s", db->name, qbuf);
	db_send_query_simple(db->c_maint, qbuf);
	free(table);
	db->maint_state = DB_MAINT_DO_VACUUM;
}

static void close_maint(struct PgDatabase *db, int sleep_time)
{
	log_debug("%s: close_maint, %d", db->name, sleep_time);
	db->maint_state = DB_CLOSED;
	db_disconnect(db->c_maint);
	db_sleep(db->c_maint, sleep_time);
}

static void maint_handler(struct PgSocket *s, void *arg, enum PgEvent ev, PGresult *res)
{
	struct PgDatabase *db = arg;

	switch (ev) {
	case DB_CONNECT_OK:
		run_queue_list(db);
		break;
	case DB_RESULT_OK:
		switch (db->maint_state) {
		case DB_MAINT_LOAD_QUEUES:
			fill_items(db, res);
		case DB_MAINT_ROT1:
			PQclear(res);
			if (!statlist_empty(&db->maint_item_list)) {
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
			fill_items(db, res);
		case DB_MAINT_DO_VACUUM:
			PQclear(res);
			if (!statlist_empty(&db->maint_item_list)) {
				run_vacuum(db);
			} else {
				close_maint(db, 2*60);
			}
			break;
		default:
			printf("bad state\n");
			exit(1);
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
}

void launch_maint(struct PgDatabase *db)
{
	log_debug("%s: launch_maint", db->name);

	if (!db->c_maint) {
		free_maint_list(db);
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

