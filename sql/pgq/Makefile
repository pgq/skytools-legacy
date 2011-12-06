
DOCS = README.pgq
DATA_built = pgq.sql pgq.upgrade.sql
DATA = structure/uninstall_pgq.sql

# scripts that load other sql files
LDRS = structure/func_internal.sql structure/func_public.sql structure/triggers.sql
FUNCS = $(shell sed -e 's/^[^\\].*//' -e 's/\\i //' $(LDRS))
SRCS = structure/tables.sql structure/grants.sql structure/install.sql \
       structure/uninstall_pgq.sql $(FUNCS)

REGRESS = pgq_init pgq_core logutriga sqltriga trunctrg
REGRESS_OPTS = --load-language=plpgsql

PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql
CATSQL = ../../scripts/catsql.py

SUBDIRS = lowlevel triggers

# PGXS does not have subdir support, thus hack to recurse into lowlevel/
all: sub-all
install: sub-install
clean: sub-clean
distclean: sub-distclean
sub-all sub-install sub-clean sub-distclean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $(subst sub-,,$@) DESTDIR=$(DESTDIR) PG_CONFIG=$(PG_CONFIG); \
	done

lowlevel/pgq_lowlevel.sql: sub-all
triggers/pgq_triggers.sql: sub-all

#
# combined SQL files
#

pgq.sql: $(SRCS)
	$(CATSQL) structure/install.sql > $@

# fixme: use LDRS here, to upgrade triggers
pgq.upgrade.sql: $(SRCS)
	$(CATSQL) structure/upgrade.sql > $@

#
# docs
#
dox: cleandox $(SRCS)
	mkdir -p docs/html
	mkdir -p docs/sql
	$(CATSQL) --ndoc structure/tables.sql > docs/sql/schema.sql
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
	$(MAKE) install installcheck || { filterdiff --format=unified regression.diffs | less; exit 1; }

ack:
	cp results/*.out expected/

