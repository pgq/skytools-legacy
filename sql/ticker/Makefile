
include ../../config.mak

SRCS = pgqd.c maint.c ticker.c retry.c
LOCAL_HDRS = pgqd.h

USUAL_DIR = ../../lib
USUAL_OBJDIR = .
USUAL_LOCAL_SRCS = $(SRCS) $(LOCAL_HDRS)
include $(USUAL_DIR)/Setup.mk

PROGRAM = pgqd

HDRS = $(LOCAL_HDRS) $(USUAL_HDRS)

PG_INCDIR = $(shell $(PG_CONFIG) --includedir)
PG_LIBDIR = $(shell $(PG_CONFIG) --libdir)

OBJS = $(SRCS:.c=.o) $(USUAL_OBJS)

CPPFLAGS += $(USUAL_CPPFLAGS) -I$(PG_INCDIR)
LDFLAGS += -L$(PG_LIBDIR)
LIBS += -lpq -lm

all: $(PROGRAM)

$(PROGRAM): $(OBJS)
	$(CC) -o $@ $(OBJS) $(LDFLAGS) $(LIBS)

$(OBJS): $(HDRS)

%.o: $(USUAL_DIR)/usual/%.c $(USUAL_HDRS)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(DEFS) -c -o $@ $<

pgqd.o: pgqd.ini.h
pgqd.ini.h: pgqd.ini
	sed -e 's/.*/"&\\n"/' $< > $@

install: all
	mkdir -p '$(DESTDIR)$(bindir)'
	$(BININSTALL) -m 755 pgqd$(X) '$(DESTDIR)$(bindir)/pgqd'
	mkdir -p '$(DESTDIR)$(docdir)/conf'
	$(INSTALL) -m 644 pgqd.ini '$(DESTDIR)$(docdir)/conf/pgqd.ini.templ'

clean:
	rm -f $(PROGRAM) $(OBJS) pgqd.ini.h

tags:
	ctags *.[ch] ../../lib/usual/*.[ch]

distclean: clean
installcheck:

