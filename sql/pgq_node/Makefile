
DATA_built = pgq_node.sql pgq_node.upgrade.sql

LDRS = structure/functions.sql
FUNCS = $(shell sed -e 's/^[^\\].*//' -e 's/\\i //' $(LDRS))
SRCS = structure/tables.sql structure/functions.sql $(FUNCS)

REGRESS = pgq_node_test
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

pgq_node.sql: $(SRCS)
	$(CATSQL) structure/install.sql > $@

pgq_node.upgrade.sql: $(SRCS)
	$(CATSQL) structure/upgrade.sql > $@

#
# docs
#
dox: cleandox $(SRCS)
	mkdir -p docs/html
	mkdir -p docs/sql
	$(CATSQL) --ndoc structure/tables.sql > docs/sql/pgq_node.sql
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

test: pgq_node.sql
	$(MAKE) installcheck || { filterdiff --format=unified regression.diffs | less; exit 1; }

ack:
	cp results/*.out expected/

