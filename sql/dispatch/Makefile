
REGRESS = test_create_part
REGRESS_OPTS = --load-language=plpgsql --load-language=plpythonu

PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

test:
	make installcheck || { less regression.diffs ; exit 1; }

ack:
	cp results/* expected/

