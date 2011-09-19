
DATA_built = londiste.sql londiste.upgrade.sql

SQLS  = $(shell sed -e 's/^[^\\].*//' -e 's/\\i //' structure/install.sql)
FUNCS = $(shell sed -e 's/^[^\\].*//' -e 's/\\i //' $(SQLS))
SRCS = $(SQLS) $(FUNCS)

NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql
CATSQL = ../../scripts/catsql.py

REGRESS = londiste_install londiste_provider londiste_subscriber \
	  londiste_fkeys londiste_execute londiste_seqs londiste_merge \
	  londiste_leaf
# londiste_denytrigger

REGRESS_OPTS = --dbname=regression
override CONTRIB_TESTDB=regression

PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

londiste.sql: $(SRCS)
	$(CATSQL) $(SQLS) > $@

londiste.upgrade.sql: $(SRCS)
	$(CATSQL) structure/functions.sql > $@

test: londiste.sql
	$(MAKE) installcheck || { filterdiff --format=unified regression.diffs | less; exit 1; }

ack:
	cp results/* expected/

dox: cleandox
	mkdir -p docs/html
	mkdir -p docs/sql
	$(CATSQL) --ndoc structure/tables.sql > docs/sql/schema.sql
	$(CATSQL) --ndoc structure/functions.sql > docs/sql/functions.sql
	$(NDOC) $(NDOCARGS)

cleandox:
	rm -rf docs/html docs/Data docs/sql

