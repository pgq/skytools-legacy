
include ../../../config.mak

MODULE_big = pgq_triggers
SRCS = common.c logtriga.c logutriga.c sqltriga.c makesql.c stringutil.c
OBJS = $(SRCS:.c=.o)
DATA_built = pgq_triggers.sql

include $(PGXS)

cs:
	cscope -b -f .cscope.out *.c

