
#SQLS = v2.1.5_londiste.sql  v2.1.5_pgq_core.sql  v2.1.5_pgq_ext.sql
#SQLS = v2.1.6_londiste.sql  v2.1.6_pgq_ext.sql
#SQLS = v3.0_pgq_core.sql
#SQLS = londiste.upgrade_2.1_to_3.1.sql

SRCS = $(addprefix src/, $(SQLS))
DSTS = $(addprefix final/, $(SQLS))

CATSQL = $(PYTHON) ../scripts/catsql.py

all: $(DSTS)

final/%.sql: src/%.sql
	$(CATSQL) $< > $@

final/londiste.upgrade_2.1_to_3.1.sql: src/londiste.2to3.sql ../sql/londiste/londiste.sql
	echo "begin;" > $@
	cat src/londiste.2to3.sql >> $@
	grep -v 'create schema' ../sql/londiste/londiste.sql >> $@
	echo "commit;" >> $@

PSQL = psql -q

ltest: ../sql/pgq_node/pgq_node.sql
	$(PSQL) -d postgres -c "drop database if exists londiste_upgrade_test"
	$(PSQL) -d postgres -c "create database londiste_upgrade_test"
	$(PSQL) -d londiste_upgrade_test -f final/pgq_core_2.1.13.sql
	$(PSQL) -d londiste_upgrade_test -f final/londiste.2.1.12.sql
	$(PSQL) -d londiste_upgrade_test -f final/pgq.upgrade_2.1_to_3.0.sql
	$(PSQL) -d londiste_upgrade_test -f ../sql/pgq_node/pgq_node.sql
	$(PSQL) -d londiste_upgrade_test -f final/londiste.upgrade_2.1_to_3.1.sql

