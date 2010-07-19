
DATA_built = londiste.sql londiste.upgrade.sql

SQLS = structure/tables.sql structure/grants.sql structure/functions.sql
FUNCS = $(shell sed -e 's/^[^\\].*//' -e 's/\\i //' $(SQLS))
SRCS = $(SQLS) $(FUNCS)

REGRESS = londiste_install londiste_provider londiste_subscriber \
	  londiste_fkeys londiste_execute londiste_seqs londiste_merge
# londiste_denytrigger
REGRESS_OPTS = --dbname=regression

include ../../config.mak

include $(PGXS)

londiste.sql: $(SRCS)
	$(CATSQL) $(SQLS) > $@

londiste.upgrade.sql: $(SRCS)
	$(CATSQL) structure/functions.sql > $@

test: londiste.sql
	$(MAKE) installcheck || { less regression.diffs; exit 1; }

ack:
	cp results/* expected/

NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql
CATSQL = ../../scripts/catsql.py

dox: cleandox
	mkdir -p docs/html
	mkdir -p docs/sql
	$(CATSQL) --ndoc structure/tables.sql > docs/sql/schema.sql
	$(CATSQL) --ndoc structure/functions.sql > docs/sql/functions.sql
	$(NDOC) $(NDOCARGS)

cleandox:
	rm -rf docs/html docs/Data docs/sql

