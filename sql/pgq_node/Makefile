
DATA_built = pgq_node.sql pgq_node.upgrade.sql

FUNCS = $(shell sed -n -e '/^\\/{s/\\i //;p}' structure/functions.sql)
SRCS = structure/tables.sql structure/functions.sql $(FUNCS)

REGRESS = pgq_node_test
REGRESS_OPTS = --load-language=plpgsql

include ../../config.mak
include $(PGXS)

NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql
CATSQL = ../../scripts/catsql.py

#
# combined SQL files
#

pgq_node.sql: $(SRCS)
	$(CATSQL) structure/tables.sql structure/functions.sql > $@

pgq_node.upgrade.sql: $(SRCS)
	$(CATSQL) structure/functions.sql > $@

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
	$(MAKE) installcheck || { less regression.diffs; exit 1; }

ack:
	cp results/*.out expected/

