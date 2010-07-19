
include ../../../config.mak

MODULE_big = pgq_triggers
SRCS = logtriga.c logutriga.c sqltriga.c \
       common.c makesql.c stringutil.c \
       parsesql.c qbuilder.c
OBJS = $(SRCS:.c=.o)
DATA_built = pgq_triggers.sql

include $(PGXS)

cs:
	cscope -b -f .cscope.out *.c

