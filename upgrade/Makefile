
#SQLS = v2.1.5_londiste.sql  v2.1.5_pgq_core.sql  v2.1.5_pgq_ext.sql
#SQLS = v2.1.6_londiste.sql  v2.1.6_pgq_ext.sql
#SQLS = v2.1.7_pgq_core.sql v2.1.7_londiste.sql
SQLS = v2.1.8_pgq_core.sql

SRCS = $(addprefix src/, $(SQLS))
DSTS = $(addprefix final/, $(SQLS))

CATSQL = python ../scripts/catsql.py

all: $(DSTS)

final/%.sql: src/%.sql
	$(CATSQL) $< > $@

