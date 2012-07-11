
EXTENSION = pgq_coop

Extension_data_built = pgq_coop--3.1.sql pgq_coop--unpackaged--3.1.sql
Contrib_data_built = pgq_coop.sql pgq_coop.upgrade.sql \
	     structure/newgrants_pgq_coop.sql \
	     structure/oldgrants_pgq_coop.sql

Contrib_regress   = pgq_coop_init_noext pgq_coop_test
Extension_regress = pgq_coop_init_ext   pgq_coop_test

Contrib_install_always = yes

SQL_FULL = structure/schema.sql structure/functions.sql structure/grants.sql
FUNCS = $(shell sed -n -e '/^\\/{s/\\i //;p}' structure/functions.sql)
SRCS = $(SQL_FULL) $(FUNCS)

include ../common-pgxs.mk

NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql
CATSQL = ../../scripts/catsql.py
GRANTFU = ../../scripts/grantfu.py


#
# combined SQL files
#

pgq_coop--3.1.sql: pgq_coop.sql
	cat $< > $@

#pgq_coop--unpackaged--3.1.sql: structure/ext_unpackaged.sql pgq_coop.upgrade.sql
pgq_coop--unpackaged--3.1.sql: pgq_coop.upgrade.sql structure/ext_unpackaged.sql
	cat $< > $@

pgq_coop.sql: $(SRCS)
	$(CATSQL) structure/install.sql > $@

pgq_coop.upgrade.sql: $(SRCS)
	$(CATSQL) structure/upgrade.sql > $@

structure/newgrants_pgq_coop.sql: structure/grants.ini
	$(GRANTFU) -t -r -d $< > $@

structure/oldgrants_pgq_coop.sql: structure/grants.ini structure/grants.sql
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
	$(CATSQL) --ndoc structure/functions.sql > docs/sql/functions.sql
	$(NDOC) $(NDOCARGS)

cleandox:
	rm -rf docs/html docs/Data docs/sql

clean: cleandox

