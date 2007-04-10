
DATA_built = londiste.sql londiste.upgrade.sql
DOCS = README.londiste

FUNCS = $(wildcard functions/*.sql)
SRCS = structure/tables.sql structure/types.sql $(FUNCS)

REGRESS = londiste_install londiste_denytrigger londiste_provider londiste_subscriber
REGRESS_OPTS = --load-language=plpythonu --load-language=plpgsql

include ../../config.mak

include $(PGXS)

londiste.sql: $(SRCS)
	cat $(SRCS) > $@

londiste.upgrade.sql: $(FUNCS)
	cat $(FUNCS) > $@

test: londiste.sql
	$(MAKE) installcheck || { less regression.diffs; exit 1; }

