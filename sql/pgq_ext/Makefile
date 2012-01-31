
DOCS = README.pgq_ext
DATA_built = pgq_ext.sql pgq_ext.upgrade.sql

SRCS = $(wildcard functions/*.sql structure/*.sql)

REGRESS = test_pgq_ext test_upgrade
REGRESS_OPTS = --load-language=plpgsql

CATSQL = ../../scripts/catsql.py
NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql

PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

pgq_ext.sql: $(SRCS)
	$(CATSQL) structure/install.sql > $@

pgq_ext.upgrade.sql: $(SRCS)
	$(CATSQL) structure/upgrade.sql > $@

test: pgq_ext.sql
	make installcheck || { less regression.diffs ; exit 1; }

ack:
	cp results/* expected/

cleandox:
	rm -rf docs/html docs/Data docs/sql

dox: cleandox $(SRCS)
	mkdir -p docs/html
	mkdir -p docs/sql
	$(CATSQL) --ndoc structure/tables.sql > docs/sql/schema.sql
	$(CATSQL) --ndoc structure/upgrade.sql > docs/sql/functions.sql
	$(NDOC) $(NDOCARGS)

