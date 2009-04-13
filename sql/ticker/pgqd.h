
#ifndef __PGQD_H__
#define __PGQD_H__

#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#define Assert(x)

#include <usual/list.h>
#include <usual/alloc.h>
#include <usual/statlist.h>
#include <usual/logging.h>

#include "connection.h"

enum DbState {
	DB_CLOSED,
	DB_TICKER_CHECK_PGQ,
	DB_TICKER_CHECK_VERSION,
	DB_TICKER_RUN,
	DB_MAINT_LOAD_QUEUES,
	DB_MAINT_ROT1,
	DB_MAINT_ROT2,
	DB_MAINT_VACUUM_LIST,
	DB_MAINT_DO_VACUUM,
};

struct PgDatabase {
	struct List head;
	const char *name;
	struct PgSocket *c_ticker;
	struct PgSocket *c_maint;
	struct PgSocket *c_retry;
	bool has_pgq;
	enum DbState state;
	enum DbState maint_state;
	struct StatList maint_item_list;
};

struct Config {
	const char *config_file;
	const char *pidfile;
	const char *logfile;
	const char *base_connstr;
	const char *initial_database;
	const char *database_list;
	int syslog;
	int retry_period;
	int check_period;
	int maint_period;
	int ticker_period;
};

extern struct Config cf;


void launch_ticker(struct PgDatabase *db);
void launch_maint(struct PgDatabase *db);
void launch_retry(struct PgDatabase *db);

const char *make_connstr(const char *dbname);

#endif

