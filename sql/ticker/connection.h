
#ifndef __CONNECTION_H__
#define __CONNECTION_H__

#include <libpq-fe.h>

enum PgEvent {
	DB_CONNECT_OK,
	DB_CONNECT_FAILED,
	DB_RESULT_OK,
	DB_RESULT_BAD,
	DB_TIMEOUT,
};

struct PgSocket;

typedef void (*db_handler_f)(struct PgSocket *pgs, void *arg, enum PgEvent dbev, PGresult *res);

struct PgSocket *db_create(db_handler_f fn, void *arg);
void db_free(struct PgSocket *db);

void db_connect(struct PgSocket *db, const char *cstr);
void db_disconnect(struct PgSocket *db);
void db_reconnect(struct PgSocket *db);

void db_send_query_simple(struct PgSocket *db, const char *query);
void db_send_query_params(struct PgSocket *db, const char *query, int args, ...);

void db_sleep(struct PgSocket *db, double timeout);

int db_connection_valid(struct PgSocket *db);

#endif

