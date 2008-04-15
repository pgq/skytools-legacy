
DATA_built = pgq_set.sql pgq_set.upgrade.sql

FUNCS = $(shell sed -n -e '/^\\/{s/\\i //;p}' structure/functions.sql)
SRCS = structure/pgq_set.sql structure/functions.sql $(FUNCS)

REGRESS = pgq_set
REGRESS_OPTS = --load-language=plpgsql

include ../../config.mak
include $(PGXS)

NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql
CATSQL = ../../scripts/catsql.py

#
# combined SQL files
#

pgq_set.sql: $(SRCS)
	$(CATSQL) structure/pgq_set.sql structure/functions.sql > $@

pgq_set.upgrade.sql: $(SRCS)
	$(CATSQL) structure/functions.sql > $@

#
# docs
#
dox: cleandox $(SRCS)
	mkdir -p docs/html
	mkdir -p docs/sql
	$(CATSQL) --ndoc structure/pgq_set.sql > docs/sql/pgq_set.sql
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

test: pgq_set.sql
	$(MAKE) installcheck || { less regression.diffs; exit 1; }

ack:
	cp results/*.out expected/

