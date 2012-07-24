
EXTENSION = pgq_node

Extension_data_built = pgq_node--3.1.sql pgq_node--unpackaged--3.1.sql
Contrib_data_built = pgq_node.sql pgq_node.upgrade.sql \
	     structure/newgrants_pgq_node.sql \
	     structure/oldgrants_pgq_node.sql

Extension_regress = pgq_node_init_ext   pgq_node_test
Contrib_regress   = pgq_node_init_noext pgq_node_test

Contrib_install_always = yes

LDRS = structure/functions.sql
FUNCS = $(shell sed -e 's/^[^\\].*//' -e 's/\\i //' $(LDRS))
SRCS = structure/tables.sql structure/functions.sql structure/grants.sql \
       $(FUNCS)

include ../common-pgxs.mk

NDOC = NaturalDocs
NDOCARGS = -r -o html docs/html -p docs -i docs/sql
CATSQL = ../../scripts/catsql.py
GRANTFU = ../../scripts/grantfu.py

#
# combined SQL files
#

pgq_node--3.1.sql: pgq_node.sql structure/ext_postproc.sql
	cat $^ > $@

pgq_node--unpackaged--3.1.sql: pgq_node.upgrade.sql structure/ext_unpackaged.sql structure/ext_postproc.sql
	cat $^ > $@

pgq_node.sql: $(SRCS)
	$(CATSQL) structure/install.sql > $@

pgq_node.upgrade.sql: $(SRCS)
	$(CATSQL) structure/upgrade.sql > $@

structure/newgrants_pgq_node.sql: structure/grants.ini
	$(GRANTFU) -r -d -t $< > $@

structure/oldgrants_pgq_node.sql: structure/grants.ini structure/grants.sql
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
	$(CATSQL) --ndoc structure/tables.sql > docs/sql/pgq_node.sql
	$(CATSQL) --ndoc structure/functions.sql > docs/sql/functions.sql
	$(NDOC) $(NDOCARGS)

cleandox:
	rm -rf docs/html docs/Data docs/sql

clean: cleandox

