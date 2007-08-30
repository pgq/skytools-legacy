
MODULE_big = txid
SRCS = txid.c epoch.c
OBJS = $(SRCS:.c=.o)

DATA_built = txid.sql
DATA = uninstall_txid.sql
DOCS = README.txid
EXTRA_CLEAN = txid.sql.in

REGRESS = txid
REGRESS_OPTS = --load-language=plpgsql

include ../../config.mak
include $(PGXS)

# additional deps
txid.o: txid.h
epoch.o: txid.h

# postgres >= manages epoch itself, so skip epoch tables
pgnew = $(shell test $(VERSION) "<" "8.2" && echo "false" || echo "true")
ifeq ($(pgnew),true)
TXID_SQL = txid.std.sql
else
TXID_SQL = txid.std.sql txid.schema.sql
endif
txid.sql.in: $(TXID_SQL)
	cat $(TXID_SQL) > $@

test: install
	make installcheck || { less regression.diffs; exit 1; }

