
EXTENSION = pgq

DOCS = README.pgq

Extension_data_built = pgq--3.1.sql pgq--unpackaged--3.1.sql

Contrib_data_built = pgq.sql pgq.upgrade.sql structure/oldgrants_pgq.sql structure/newgrants_pgq.sql
Contrib_data = structure/uninstall_pgq.sql

Contrib_regress   = pgq_init_noext pgq_core pgq_perms logutriga sqltriga $(trunc_test)
Extension_regress = pgq_init_ext   pgq_core pgq_perms logutriga sqltriga $(trunc_test)

Contrib_install_always = yes

# MAJORVERSION was defined in 8.4
trunc_test = $(if $(MAJORVERSION),trunctrg)

# scripts that load other sql files
LDRS = structure/func_internal.sql structure/func_public.sql structure/triggers.sql
FUNCS = $(shell sed -e 's/^[^\\].*//' -e 's/\\i //' $(LDRS))
SRCS = structure/tables.sql structure/grants.sql structure/install.sql \
       structure/uninstall_pgq.sql $(FUNCS)

include ../common-pgxs.mk

NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql
CATSQL = ../../scripts/catsql.py
GRANTFU = ../../scripts/grantfu.py

SUBDIRS = lowlevel triggers

# PGXS does not have subdir support, thus hack to recurse into lowlevel/
all: sub-all
install: sub-install
clean: sub-clean
distclean: sub-distclean
sub-all sub-install sub-clean sub-distclean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $(subst sub-,,$@) \
			DESTDIR=$(DESTDIR) \
			PG_CONFIG=$(PG_CONFIG) \
		|| exit 1; \
	done

lowlevel/pgq_lowlevel.sql: sub-all
triggers/pgq_triggers.sql: sub-all

#
# combined SQL files
#

pgq--3.1.sql: pgq.sql structure/ext_postproc.sql
	cat $^ > $@

pgq--unpackaged--3.1.sql: pgq.upgrade.sql structure/ext_unpackaged.sql structure/ext_postproc.sql
	cat $^ > $@

pgq.sql: $(SRCS)
	$(CATSQL) structure/install.sql > $@

# fixme: use LDRS here, to upgrade triggers
pgq.upgrade.sql: $(SRCS)
	$(CATSQL) structure/upgrade.sql > $@

structure/newgrants_pgq.sql: structure/grants.ini
	$(GRANTFU) -t -r -d $< > $@

structure/oldgrants_pgq.sql: structure/grants.ini structure/grants.sql
	echo "begin;" > $@
	$(GRANTFU) -R -o $< >> $@
	cat structure/grants.sql >> $@
	echo "commit;" >> $@

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

test: $(Contrib_data_built)

.PHONY: cleandox dox
