
EXTENSION = pgq_coop

EXT_VERSION = 3.1.1
EXT_OLD_VERSIONS = 3.1

Contrib_regress   = pgq_coop_init_noext pgq_coop_test
Extension_regress = pgq_coop_init_ext   pgq_coop_test

include ../common-pgxs.mk

#
# docs
#
dox: cleandox $(SRCS)
	mkdir -p docs/html
	mkdir -p docs/sql
	$(CATSQL) --ndoc structure/functions.sql > docs/sql/functions.sql
	$(NDOC) $(NDOCARGS)

