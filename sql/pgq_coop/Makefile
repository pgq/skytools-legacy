
DATA_built = pgq_coop.sql pgq_coop.upgrade.sql

SQL_FULL = structure/schema.sql structure/functions.sql structure/grants.sql

FUNCS = $(shell sed -n -e '/^\\/{s/\\i //;p}' structure/functions.sql)
SRCS = $(SQL_FULL) $(FUNCS)

REGRESS = pgq_coop_test
REGRESS_OPTS = --load-language=plpgsql

PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql
CATSQL = ../../scripts/catsql.py

#
# combined SQL files
#

pgq_coop.sql: $(SRCS)
	$(CATSQL) structure/install.sql > $@

pgq_coop.upgrade.sql: $(SRCS)
	$(CATSQL) structure/upgrade.sql > $@

#
# docs
#
dox: cleandox $(SRCS)
	mkdir -p docs/html
	mkdir -p docs/sql
	$(CATSQL) --ndoc structure/functions.sql > docs/sql/functions.sql
	$(NDOC) $(NDOCARGS)

cleandox:
	rm -rf docs/html docs/Data docs/sql

clean: cleandox

upload: dox
	rsync -az --delete docs/html/* data1:public_html/pgq-set/

#
# regtest shortcuts
#

test: pgq_coop.sql
	$(MAKE) installcheck || { less regression.diffs; exit 1; }

ack:
	cp results/*.out expected/

