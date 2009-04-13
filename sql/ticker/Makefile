
include ../../config.mak

USUAL_DIR = ../../lib
USUAL_OBJDIR = .
USUAL_MODULES = logging daemon event aatree string time \
		cfparser fileutil socket
include $(USUAL_DIR)/Setup.mk

PROGRAM = pgqd

SRCS = connection.c pgqd.c maint.c ticker.c retry.c
HDRS = connection.h pgqd.h $(USUAL_HDRS)

OBJS = $(SRCS:.c=.o) $(USUAL_OBJS)
PG_CPPFLAGS = -I$(libpq_srcdir) $(USUAL_CPPFLAGS)
PG_LIBS = $(libpq_pgport)

include $(PGXS)

LIBS := $(filter-out -lreadline -ldl -lz, $(LIBS))

$(OBJS): $(HDRS)

%.o: $(USUAL_DIR)/usual/%.c $(USUAL_HDRS)
	$(CC) -c -o $@ $< $(DEFS) $(CPPFLAGS) $(CFLAGS)

