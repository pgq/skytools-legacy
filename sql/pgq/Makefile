
DOCS = README.pgq
DATA_built = pgq.sql pgq.upgrade.sql
DATA = structure/uninstall_pgq.sql

SRCS = $(wildcard structure/*.sql) \
       $(wildcard functions/*.sql) \
       $(wildcard triggers/*.sql)

REGRESS = pgq_init pgq_core logutriga sqltriga
REGRESS_OPTS = --load-language=plpythonu --load-language=plpgsql

PGXS = $(shell pg_config --pgxs)
include $(PGXS)

NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql
CATSQL = ../../scripts/catsql.py

pgq.sql: $(SRCS)
	$(CATSQL) structure/install.sql > $@

pgq.upgrade.sql: $(SRCS)
	$(CATSQL) structure/func_internal.sql structure/func_public.sql > $@

dox: cleandox
	mkdir -p docs/html
	mkdir -p docs/sql
	$(CATSQL) --ndoc structure/tables.sql structure/types.sql > docs/sql/schema.sql
	$(CATSQL) --ndoc structure/func_public.sql > docs/sql/external.sql
	$(CATSQL) --ndoc structure/func_internal.sql > docs/sql/internal.sql
	$(CATSQL) --ndoc structure/triggers.sql > docs/sql/triggers.sql
	$(NDOC) $(NDOCARGS)

cleandox:
	rm -rf docs/html docs/Data docs/sql

clean: cleandox

test:
	#-dropdb pgq
	#createdb pgq
	#psql -f structure/pgq.sql pgq
	$(MAKE) installcheck || { less regression.diffs; exit 1; }

upload: dox
	rsync -az docs/html structure functions data1:public_html/pgq/
	$(MAKE) cleandox
	rsync -az catsql.py Makefile docs data1:public_html/pgq/
	$(MAKE) dox

