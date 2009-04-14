
include ../../config.mak

USUAL_DIR = ../../lib
USUAL_OBJDIR = .
USUAL_MODULES = logging daemon event aatree string time \
		cfparser fileutil socket
include $(USUAL_DIR)/Setup.mk

PROGRAM = pgqd

SRCS = connection.c pgqd.c maint.c ticker.c retry.c
HDRS = connection.h pgqd.h $(USUAL_HDRS)

PG_INCDIR = $(shell $(PG_CONFIG) --includedir)
PG_LIBDIR = $(shell $(PG_CONFIG) --libdir)
PG_CPPFLAGS = -I$(PG_INCDIR)
PG_LDFLAGS = -L$(PG_LIBDIR)
PG_LIBS = -lpq -lm

OBJS = $(SRCS:.c=.o) $(USUAL_OBJS)

CPPFLAGS += $(USUAL_CPPFLAGS) $(PG_CPPFLAGS)
LDFLAGS += $(PG_LDFLAGS)
LIBS += $(PG_LIBS)

all: $(PROGRAM)

$(PROGRAM): $(OBJS)
	$(CC) -o $@ $(OBJS) $(LDFLAGS) $(LIBS)

$(OBJS): $(HDRS)

%.o: $(USUAL_DIR)/usual/%.c $(USUAL_HDRS)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(DEFS) -c -o $@ $<

install: all
	mkdir -p '$(DESTDIR)$(bindir)'
	$(BININSTALL) -m 755 pgqd$(X) '$(DESTDIR)$(bindir)/pgqd'
	mkdir -p '$(DESTDIR)$(docdir)/conf'
	$(INSTALL) -m 644 pgqd.ini '$(DESTDIR)$(docdir)/conf/pgqd.ini.templ'

clean:
	rm -f $(PROGRAM) $(OBJS)

distclean: clean
installcheck:

