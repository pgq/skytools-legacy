
include ../../config.mak

PROGRAM = pgqd

SRCS = connection.c pgqd.c util.c maint.c ticker.c retry.c
HDRS = connection.h pgqd.h util.h list.h

OBJS = $(SRCS:.c=.o)
PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport) -levent

include $(PGXS)

$(OBJS): $(HDRS)

