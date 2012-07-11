
EXTENSION = londiste

Contrib_data_built = londiste.sql londiste.upgrade.sql \
	     structure/oldgrants_londiste.sql \
	     structure/newgrants_londiste.sql

Extension_data_built = londiste--3.1.sql londiste--unpackaged--3.1.sql

base_regress = londiste_provider londiste_subscriber \
	       londiste_fkeys londiste_execute londiste_seqs londiste_merge \
	       londiste_leaf londiste_create_part

Contrib_regress = init_noext $(base_regress)
Extension_regress = init_ext $(base_regress)

Contrib_install_always = yes

SQLS  = $(shell sed -e 's/^[^\\].*//' -e 's/\\i //' structure/install.sql)
FUNCS = $(shell sed -e 's/^[^\\].*//' -e 's/\\i //' $(SQLS))
SRCS = $(SQLS) $(FUNCS)

NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql
CATSQL = ../../scripts/catsql.py
GRANTFU = ../../scripts/grantfu.py

include ../common-pgxs.mk

londiste--3.1.sql: londiste.sql structure/ext_postproc.sql
	cat $^ > $@

londiste--unpackaged--3.1.sql: londiste.upgrade.sql structure/ext_unpackaged.sql structure/ext_postproc.sql
	cat $^ > $@

londiste.sql: $(SRCS)
	$(CATSQL) $(SQLS) > $@

londiste.upgrade.sql: $(SRCS)
	$(CATSQL) structure/upgrade.sql > $@

structure/newgrants_londiste.sql: structure/grants.ini
	$(GRANTFU) -r -d -t $< > $@

structure/oldgrants_londiste.sql: structure/grants.ini
	echo "begin;" > $@
	$(GRANTFU) -R -o $< >> $@
	cat structure/grants.sql >> $@
	echo "commit;" >> $@

dox: cleandox
	mkdir -p docs/html
	mkdir -p docs/sql
	$(CATSQL) --ndoc structure/tables.sql > docs/sql/schema.sql
	$(CATSQL) --ndoc structure/functions.sql > docs/sql/functions.sql
	$(NDOC) $(NDOCARGS)

cleandox:
	rm -rf docs/html docs/Data docs/sql

