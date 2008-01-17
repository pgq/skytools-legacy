
DOCS = README.pgq_ext
DATA_built = pgq_ext.sql

SRCS = structure/tables.sql functions/track_batch.sql functions/track_event.sql \
       functions/track_tick.sql functions/version.sql

REGRESS = test_pgq_ext
REGRESS_OPTS = --load-language=plpgsql

include ../../config.mak

include $(PGXS)

pgq_ext.sql: $(SRCS)
	cat $(SRCS) > $@

test: pgq_ext.sql
	make installcheck || { less regression.diffs ; exit 1; }

ack:
	cp results/* expected/

