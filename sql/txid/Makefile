
PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)

PGVER := $(shell $(PG_CONFIG) --version | sed 's/PostgreSQL //')

ifeq ($(PGVER),)
$(error Failed to get Postgres version)
else
# postgres >= manages epoch itself, so skip epoch tables
pg83 = $(shell test $(PGVER) "<" "8.3" && echo "false" || echo "true")
pg82 = $(shell test $(PGVER) "<" "8.2" && echo "false" || echo "true")
endif

ifeq ($(pg83),true)  # we have 8.3 with internal txid

# install empty txid.sql
DATA_built = txid.sql
include $(PGXS)
txid.sql: txid.internal.sql
	cp $< $@

else # 8.2 or 8.1
#
# pg < 8.3 needs this module
#
MODULE_big = txid
SRCS = txid.c epoch.c
OBJS = $(SRCS:.c=.o)
REGRESS = txid
REGRESS_OPTS = --load-language=plpgsql
DATA = uninstall_txid.sql
DOCS = README.txid
DATA_built = txid.sql
EXTRA_CLEAN = txid.sql.in

# PGXS build procedure
include $(PGXS)

ifeq ($(pg82),true)
# 8.2 tracks epoch internally
TXID_SQL = txid.std.sql
else
# 8.1 needs epoch-tracking code
TXID_SQL = txid.std.sql txid.schema.sql
endif # ! 8.2

# additional deps
txid.o: txid.h
epoch.o: txid.h

txid.sql.in: $(TXID_SQL)
	cat $(TXID_SQL) > $@

endif # ! 8.3

test: install
	make installcheck || { less regression.diffs; exit 1; }

