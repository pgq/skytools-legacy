
DOCS = README.pgq
DATA_built = pgq.sql pgq.upgrade.sql
DATA = structure/uninstall_pgq.sql

SRCS = $(wildcard structure/*.sql) \
       $(wildcard functions/*.sql) \
       $(wildcard triggers/*.sql) \
       lowlevel/pgq_lowlevel.sql

REGRESS = pgq_init pgq_core logutriga sqltriga
REGRESS_OPTS = --load-language=plpythonu --load-language=plpgsql

PGXS = $(shell pg_config --pgxs)
include $(PGXS)

NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql
CATSQL = ../../scripts/catsql.py

# PGXS does not have subdir support, thus hack to recurse into lowlevel/
all: low-all
install: low-install
clean: low-clean
distclean: low-distclean
low-all low-install low-clean low-distclean:
	$(MAKE) -C lowlevel $(subst low-,,$@) DESTDIR=$(DESTDIR) 

lowlevel/pgq_lowlevel.sql: low-all

#
# combined SQL files
#

pgq.sql: $(SRCS)
	$(CATSQL) structure/install.sql > $@

pgq.upgrade.sql: $(SRCS)
	$(CATSQL) structure/func_internal.sql structure/func_public.sql > $@

#
# docs
#
dox: cleandox $(SRCS)
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

upload: dox
	rsync -az --delete docs/html/* data1:public_html/pgq-new/

#
# regtest shortcuts
#

test: pgq.sql
	$(MAKE) installcheck || { less regression.diffs; exit 1; }

ack:
	cp results/*.out expected/

