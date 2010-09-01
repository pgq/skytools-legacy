#include "pgqd.h"

#include <usual/string.h>
#include <usual/pgutil.h>

#include <ctype.h>

struct MaintOp {
	struct List head;
	const char *func_name;
	const char *func_arg;
};

static struct MaintOp *next_op(struct PgDatabase *db)
{
	struct List *el = statlist_pop(&db->maint_op_list);
	if (!el)
		return NULL;
	return container_of(el, struct MaintOp, head);
}

static void free_op(struct MaintOp *op)
{
	if (op) {
		free(op->func_name);
		free(op->func_arg);
		free(op);
	}
}

void free_maint(struct PgDatabase *db)
{
	struct MaintOp *op;

	strlist_free(db->maint_item_list);
	db->maint_item_list = NULL;

	while ((op = next_op(db)) != NULL) {
		free_op(op);
	}
	free_op(db->cur_maint);
	db->cur_maint = NULL;
}

static void close_maint(struct PgDatabase *db, double sleep_time)
{
	log_debug("%s: close_maint, %f", db->name, sleep_time);
	db->maint_state = DB_CLOSED;
	pgs_reconnect(db->c_maint, sleep_time);
}

static void run_test_version(struct PgDatabase *db)
{
	const char *q = "select 1 from pg_proc p, pg_namespace n"
			" where p.pronamespace = n.oid"
			"   and p.proname = 'maint_operations'"
			"   and n.nspname = 'pgq'";
	log_debug("%s: %s", db->name, q);
	pgs_send_query_simple(db->c_maint, q);
	db->maint_state = DB_MAINT_TEST_VERSION;
}

static bool has_ops(PGresult *res)
{
	if (PQntuples(res) == 1 && atoi(PQgetvalue(res, 0, 0)) == 1)
		return true;
	return false;
}

static bool fill_op_list(struct PgDatabase *db, PGresult *res)
{
	int i;
	struct MaintOp *op = NULL;
	const char *fname, *farg;

	free_maint(db);

	for (i = 0; i < PQntuples(res); i++) {
		op = calloc(1, sizeof(*op));
		if (!op)
			return false;
		list_init(&op->head);
		fname = PQgetvalue(res, i, 0);
		farg = PQgetvalue(res, i, 1);
		op->func_name = strdup(fname);
		if (!op->func_name)
			goto failed;
		if (farg) {
			op->func_arg = strdup(farg);
			if (!op->func_arg)
				goto failed;
		}
		statlist_append(&db->maint_op_list, &op->head);
	}
	return true;
failed:
	free_op(op);
	return false;
}

static void run_op_list(struct PgDatabase *db)
{
	const char *q = "select queue_name from pgq.maint_operations()";
	log_debug("%s: %s", db->name, q);
	pgs_send_query_simple(db->c_maint, q);
	db->maint_state = DB_MAINT_LOAD_QUEUES;
}

static const char *stmt_names[] = {
	"vacuum",
	"vacuum analyze",
	NULL
};

static inline bool idstart(unsigned char c)
{
	return isalpha(c) || c == '_';
}

static inline bool idbody(unsigned char c)
{
	return idstart(c) || isdigit(c);
}

static int copy_ident(const char *src, int srclen, char *dst, int dstlen)
{
	int i, j;

	if (dstlen <= 2)
		return -1;

	if (!idstart(src[0]))
		goto needs_quote;

	for (i = 0; i < srclen; i++) {
		if (!idbody(i))
			goto needs_quote;
		if (i >= dstlen)
			return -1;
		dst[i] = src[i];
	}
	if (i >= dstlen)
		return -1;
	dst[i] = 0;
	return i;

needs_quote:
	dst[0] = '"';
	for (i = 0, j = 1; i < srclen; i++) {
		if (j >= dstlen)
			return -1;
		dst[j++] = src[i];
		if (src[i] == '"') {
			if (j >= dstlen)
				return -1;
			dst[j++] = src[i];
		}
	}
	if (j >= dstlen - 2)
		return -1;
	dst[j++] = '"';
	dst[j] = 0;
	return j;
}

static bool quote_fqname(const char *name, char *dst, int dstlen)
{
	const char *dot;
	const char *scm = "public";
	int scmlen = strlen(scm);
	int res;

	dot = strchr(name, '.');
	if (dot) {
		scm = name;
		scmlen = dot - name;
		name = dot + 1;
	}

	res = copy_ident(scm, scmlen, dst, dstlen);
	if (res < 0)
		return false;
	dst[res] = '.';
	res = copy_ident(name, strlen(name), dst + res + 1, dstlen - res - 1);
	return res > 0;
}

static void run_op(struct PgDatabase *db, PGresult *res)
{
	struct MaintOp *op;
	char buf[1024];
	char namebuf[256];
	const char **np;

	if (db->cur_maint) {
		if (res && PQntuples(res) > 0) {
			const char *val = PQgetvalue(res, 0, 0);
			if (val && atoi(val)) {
				op = db->cur_maint;
				goto repeat;
			}
		}
next:
		free_op(db->cur_maint);
		db->cur_maint = NULL;
	}
	op = next_op(db);
	if (!op) {
		close_maint(db, cf.maint_period);
		return;
	}
	db->cur_maint = op;
repeat:
	/* check if its magic statement */
	for (np = stmt_names; *np; np++) {
		if (strcasecmp(op->func_name, *np) != 0)
			continue;
		if (!quote_fqname(op->func_arg, namebuf, sizeof(namebuf))) {
			log_error("Bad table name? - %s", op->func_arg);
			goto next;
		}
		/* run as a statement */
		snprintf(buf, sizeof(buf), "%s %s", op->func_name, namebuf);
		log_debug("%s: [%s]", db->name, buf);
		pgs_send_query_simple(db->c_maint, buf);
		goto done;
	}

	/* run as a function */
	if (!quote_fqname(op->func_name, namebuf, sizeof(namebuf))) {
		log_error("Bad func name? - %s", op->func_name);
		goto next;
	}
	snprintf(buf, sizeof(buf), "select %s($1)", namebuf);
	log_debug("%s: [%s]", db->name, buf);
	pgs_send_query_params(db->c_maint, buf, 1, op->func_arg);
done:
	db->maint_state = DB_MAINT_OP;
}

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
	pgs_send_query_simple(db->c_maint, q);
	db->maint_state = DB_MAINT_LOAD_QUEUES;
}

static void run_vacuum_list(struct PgDatabase *db)
{
	const char *q = "select * from pgq.maint_tables_to_vacuum()";
	log_debug("%s: %s", db->name, q);
	pgs_send_query_simple(db->c_maint, q);
	db->maint_state = DB_MAINT_VACUUM_LIST;
}

static void run_rotate_part1(struct PgDatabase *db)
{
	const char *q;
	const char *qname;
	qname = strlist_pop(db->maint_item_list);
	q = "select pgq.maint_rotate_tables_step1($1)";
	log_debug("%s: %s [%s]", db->name, q, qname);
	pgs_send_query_params(db->c_maint, q, 1, qname);
	free(qname);
	db->maint_state = DB_MAINT_ROT1;
}

static void run_rotate_part2(struct PgDatabase *db)
{
	const char *q = "select pgq.maint_rotate_tables_step2()";
	log_debug("%s: %s", db->name, q);
	pgs_send_query_simple(db->c_maint, q);
	db->maint_state = DB_MAINT_ROT2;
}

static void run_vacuum(struct PgDatabase *db)
{
	char qbuf[256];
	const char *table;
	table = strlist_pop(db->maint_item_list);
	snprintf(qbuf, sizeof(qbuf), "vacuum %s", table);
	log_debug("%s: %s", db->name, qbuf);
	pgs_send_query_simple(db->c_maint, qbuf);
	free(table);
	db->maint_state = DB_MAINT_DO_VACUUM;
}

static void maint_handler(struct PgSocket *s, void *arg, enum PgEvent ev, PGresult *res)
{
	struct PgDatabase *db = arg;

	switch (ev) {
	case PGS_CONNECT_OK:
		log_debug("%s: starting maintenance", db->name);
		run_test_version(db);
		break;
	case PGS_RESULT_OK:
		switch (db->maint_state) {
		case DB_MAINT_TEST_VERSION:
			if (has_ops(res))
				run_op_list(db);
			else
				run_queue_list(db);
			break;
		case DB_MAINT_LOAD_OPS:
			if (!fill_op_list(db, res))
				goto mem_err;
		case DB_MAINT_OP:
			run_op(db, res);
			break;
		case DB_MAINT_LOAD_QUEUES:
			if (!fill_items(db, res))
				goto mem_err;
		case DB_MAINT_ROT1:
			if (!strlist_empty(db->maint_item_list)) {
				run_rotate_part1(db);
			} else {
				run_rotate_part2(db);
			}
			break;
		case DB_MAINT_ROT2:
			run_vacuum_list(db);
			break;
		case DB_MAINT_VACUUM_LIST:
			if (!fill_items(db, res))
				goto mem_err;
		case DB_MAINT_DO_VACUUM:
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
	case PGS_TIMEOUT:
		log_debug("%s: maint timeout", db->name);
		if (!pgs_connection_valid(db->c_maint))
			launch_maint(db);
		else
			run_queue_list(db);
		break;
	default:
		log_warning("%s: default reconnect", db->name);
		pgs_reconnect(db->c_maint, 60);
	}
	return;
mem_err:
	if (db->maint_item_list) {
		strlist_free(db->maint_item_list);
		db->maint_item_list = NULL;
	}
	pgs_disconnect(db->c_maint);
	pgs_sleep(db->c_maint, 20);
}

void launch_maint(struct PgDatabase *db)
{
	const char *cstr;

	log_debug("%s: launch_maint", db->name);

	if (!db->c_maint) {
		if (db->maint_item_list) {
			strlist_free(db->maint_item_list);
			db->maint_item_list = NULL;
		}
		cstr = make_connstr(db->name);
		db->c_maint = pgs_create(cstr, maint_handler, db);
	}

	if (!pgs_connection_valid(db->c_maint)) {
		pgs_connect(db->c_maint);
	} else {
		/* Already have a connection, what are we doing here */
		log_error("%s: maint already initialized", db->name);
		return;
	}
}

