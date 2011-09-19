
MODULE_big = logtriga
SRCS = logtriga.c textbuf.c
OBJS = $(SRCS:.c=.o)
DATA_built = logtriga.sql

REGRESS = logtriga

PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

test: install
	make installcheck || { less regression.diffs; exit 1; }

	
