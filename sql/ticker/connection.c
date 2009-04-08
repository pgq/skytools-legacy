#include "connection.h"

#include <sys/types.h>
#include <unistd.h>
#include <stdarg.h>
#include <math.h>
#include <event.h>

#include "util.h"

#define W_NONE 0
#define W_SOCK 1
#define W_TIME 2

typedef void (*libev_cb)(int sock, short flags, void *arg);

struct PgSocket {
	struct event ev;

	unsigned wait_type:4; // 0 - no wait, 1 - socket, 2 - timeout

	PGconn *con;
	db_handler_f handler_func;
	void *handler_arg;
};

static void send_event(struct PgSocket *db, enum PgEvent ev)
{
	db->handler_func(db, db->handler_arg, ev, NULL);
}

static void wait_event(struct PgSocket *db, short ev, libev_cb fn)
{
	Assert(!db->wait_type);

	event_set(&db->ev, PQsocket(db->con), ev, fn, db);
	if (event_add(&db->ev, NULL) < 0)
		fatal_perror("event_add");

	db->wait_type = W_SOCK;
}

static void timeout_cb(int sock, short flags, void *arg)
{
	struct PgSocket *db = arg;

	db->wait_type = 0;

	send_event(db, DB_TIMEOUT);
}

void db_sleep(struct PgSocket *db, double timeout)
{
	struct timeval tv;

	Assert(!db->wait_type);
	Assert(!db->time_wait);

	tv.tv_sec = floor(timeout);
	tv.tv_usec = (timeout - tv.tv_sec) * USEC;

	evtimer_set(&db->ev, timeout_cb, db);
	if (evtimer_add(&db->ev, &tv) < 0)
		fatal_perror("event_add");

	db->wait_type = W_TIME;
}


/* some error happened */
static void conn_error(struct PgSocket *db, enum PgEvent ev, const char *desc)
{
	log_error("connection error: %s", desc);
	log_error("libpq: %s", PQerrorMessage(db->con));
	send_event(db, ev);
}

/*
 * Called when select() told that conn is avail for reading/writing.
 *
 * It should call postgres handlers and then change state if needed.
 */
static void result_cb(int sock, short flags, void *arg)
{
	struct PgSocket *db = arg;
	PGresult *res, *res_saved = NULL;

	db->wait_type = 0;

	if (!PQconsumeInput(db->con)) {
		conn_error(db, DB_RESULT_BAD, "PQconsumeInput");
		return;
	}

	/* loop until PQgetResult returns NULL */
	while (1) {
		/* incomplete result? */
		if (PQisBusy(db->con)) {
			wait_event(db, EV_READ, result_cb);
			return;
		}

		/* next result */
		res = PQgetResult(db->con);
		if (!res)
			break;

		if (res_saved) {
			printf("multiple results?\n");
			PQclear(res_saved);
		}
		res_saved = res;
	}

	db->handler_func(db, db->handler_arg, DB_RESULT_OK, res_saved);
}

static void send_cb(int sock, short flags, void *arg)
{
	int res;
	struct PgSocket *db = arg;

	db->wait_type = 0;

	res = PQflush(db->con);
	if (res > 0) {
		wait_event(db, EV_WRITE, send_cb);
	} else if (res == 0) {
		wait_event(db, EV_READ, result_cb);
	} else
		conn_error(db, DB_RESULT_BAD, "PQflush");
}


static void connect_cb(int sock, short flags, void *arg)
{
	struct PgSocket *db = arg;
	PostgresPollingStatusType poll_res;

	db->wait_type = 0;

	poll_res = PQconnectPoll(db->con);
	switch (poll_res) {
	case PGRES_POLLING_WRITING:
		wait_event(db, EV_WRITE, connect_cb);
		break;
	case PGRES_POLLING_READING:
		wait_event(db, EV_READ, connect_cb);
		break;
	case PGRES_POLLING_OK:
		//log_debug("login ok: fd=%d", PQsocket(db->con));
		send_event(db, DB_CONNECT_OK);
		break;
	default:
		conn_error(db, DB_CONNECT_FAILED, "PQconnectPoll");
	}
}

/*
 * Public API
 */

struct PgSocket *db_create(db_handler_f fn, void *handler_arg)
{
	struct PgSocket *db;
	
	db = calloc(1, sizeof(*db));
	if (!db)
		return NULL;

	db->handler_func = fn;
	db->handler_arg = handler_arg;

	return db;
}

void db_connect(struct PgSocket *db, const char *connstr)
{
	db->con = PQconnectStart(connstr);
	if (db->con == NULL) {
		conn_error(db, DB_CONNECT_FAILED, "PQconnectStart");
		return;
	}

	if (PQstatus(db->con) == CONNECTION_BAD) {
		conn_error(db, DB_CONNECT_FAILED, "PQconnectStart");
		return;
	}

	wait_event(db, EV_WRITE, connect_cb);
}


void db_disconnect(struct PgSocket *db)
{
	if (db->con) {
		PQfinish(db->con);
		db->con = NULL;
	}
}

void db_reconnect(struct PgSocket *db)
{
	db_disconnect(db);
	db_sleep(db, 60);
}

void db_free(struct PgSocket *db)
{
	if (db) {
		if (db->con)
			db_disconnect(db);
		free(db);
	}
}

void db_send_query_simple(struct PgSocket *db, const char *q)
{
	int res;

	log_debug("%s", q);
	res = PQsendQuery(db->con, q);
	if (!res) {
		conn_error(db, DB_RESULT_BAD, "PQsendQuery");
		return;
	}

	res = PQflush(db->con);
	if (res > 0) {
		wait_event(db, EV_WRITE, send_cb);
	} else if (res == 0) {
		wait_event(db, EV_READ, result_cb);
	} else
		conn_error(db, DB_RESULT_BAD, "PQflush");
}

void db_send_query_params(struct PgSocket *db, const char *q, int cnt, ...)
{
	int res, i;
	va_list ap;
	const char * args[10];

	if (cnt > 10) cnt = 10;

	va_start(ap, cnt);
	for (i = 0; i < cnt; i++)
		args[i] = va_arg(ap, char *);
	va_end(ap);

	res = PQsendQueryParams(db->con, q, cnt, NULL, args, NULL, NULL, 0);
	if (!res) {
		conn_error(db, DB_RESULT_BAD, "PQsendQueryParams");
		return;
	}

	res = PQflush(db->con);
	if (res > 0) {
		wait_event(db, EV_WRITE, send_cb);
	} else if (res == 0) {
		wait_event(db, EV_READ, result_cb);
	} else
		conn_error(db, DB_RESULT_BAD, "PQflush");
}

int db_connection_valid(struct PgSocket *db)
{
	return (db->con != NULL);
}

