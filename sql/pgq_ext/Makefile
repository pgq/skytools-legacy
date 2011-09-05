
DOCS = README.pgq_ext
DATA_built = pgq_ext.sql

SRCS = structure/tables.sql functions/track_batch.sql functions/track_event.sql \
       functions/track_tick.sql functions/version.sql

REGRESS = test_pgq_ext
REGRESS_OPTS = --load-language=plpgsql

CATSQL = ../../scripts/catsql.py

include ../../config.mak

include $(PGXS)

pgq_ext.sql: $(SRCS)
	$(CATSQL) structure/install.sql > $@

test: pgq_ext.sql
	make installcheck || { less regression.diffs ; exit 1; }

ack:
	cp results/* expected/

