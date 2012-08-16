
EXTENSION = pgq_ext

Contrib_data_built = pgq_ext.sql pgq_ext.upgrade.sql \
	     structure/oldgrants_pgq_ext.sql \
	     structure/newgrants_pgq_ext.sql
Contrib_regress = init_noext test_pgq_ext test_upgrade

Extension_regress = init_ext test_pgq_ext
Extension_data_built = pgq_ext--3.1.sql pgq_ext--unpackaged--3.1.sql

Contrib_install_always = yes

DOCS = README.pgq_ext

SRCS = $(wildcard functions/*.sql structure/*.sql)

GRANTFU = ../../scripts/grantfu.py
CATSQL = ../../scripts/catsql.py
NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql

include ../common-pgxs.mk

pgq_ext--3.1.sql: pgq_ext.sql structure/ext_postproc.sql
	cat $^ > $@

pgq_ext--unpackaged--3.1.sql: pgq_ext.upgrade.sql structure/ext_unpackaged.sql structure/ext_postproc.sql
	cat $^ > $@

pgq_ext.sql: $(SRCS)
	$(CATSQL) structure/install.sql > $@

pgq_ext.upgrade.sql: $(SRCS)
	$(CATSQL) structure/upgrade.sql > $@

structure/newgrants_pgq_ext.sql: structure/grants.ini
	$(GRANTFU) -t -r -d $< > $@

structure/oldgrants_pgq_ext.sql: structure/grants.ini structure/grants.sql
	echo "begin;" > $@
	$(GRANTFU) -R -o $< >> $@
	cat structure/grants.sql >> $@
	echo "commit;" >> $@

cleandox:
	rm -rf docs/html docs/Data docs/sql

dox: cleandox $(SRCS)
	mkdir -p docs/html
	mkdir -p docs/sql
	$(CATSQL) --ndoc structure/tables.sql > docs/sql/schema.sql
	$(CATSQL) --ndoc structure/upgrade.sql > docs/sql/functions.sql
	$(NDOC) $(NDOCARGS)

