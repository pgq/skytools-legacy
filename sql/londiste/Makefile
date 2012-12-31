
EXTENSION = londiste

EXT_VERSION = 3.1.3
EXT_OLD_VERSIONS = 3.1 3.1.1

base_regress = londiste_provider londiste_subscriber \
	       londiste_fkeys londiste_execute londiste_seqs londiste_merge \
	       londiste_leaf londiste_create_part

Contrib_regress = init_noext $(base_regress)
Extension_regress = init_ext $(base_regress)

include ../common-pgxs.mk

dox: cleandox
	mkdir -p docs/html
	mkdir -p docs/sql
	$(CATSQL) --ndoc structure/tables.sql > docs/sql/schema.sql
	$(CATSQL) --ndoc structure/functions.sql > docs/sql/functions.sql
	$(NDOC) $(NDOCARGS)

