
include ../../config.mak

MODULE_big = logtriga
SRCS = logtriga.c textbuf.c
OBJS = $(SRCS:.c=.o)
DATA_built = logtriga.sql

REGRESS = logtriga

include $(PGXS)

test: install
	make installcheck || { less regression.diffs; exit 1; }

	
