
include ../../config.mak

PG_INCDIR = $(shell $(PG_CONFIG) --includedir)
PG_LIBDIR = $(shell $(PG_CONFIG) --libdir)

bin_PROGRAMS = pgqd

pgqd_SOURCES = pgqd.c maint.c ticker.c retry.c pgqd.h
nodist_pgqd_SOURCES = pgqd.ini.h
pgqd_CPPFLAGS = -I$(PG_INCDIR)
pgqd_LDFLAGS = -L$(PG_LIBDIR)
pgqd_LIBS = -lpq -lm

pgqd_EMBED_LIBUSUAL = 1
USUAL_DIR = ../../lib
AM_FEATURES = libusual

EXTRA_DIST = pgqd.ini
CLEANFILES = pgqd.ini.h

include $(USUAL_DIR)/mk/antimake.mk

pgqd.ini.h: pgqd.ini
	sed -e 's/.*/"&\\n"/' $< > $@

install: install-conf
install-conf:
	mkdir -p '$(DESTDIR)$(docdir)/conf'
	$(INSTALL) -m 644 pgqd.ini '$(DESTDIR)$(docdir)/conf/pgqd.ini.templ'

tags:
	ctags *.[ch] ../../lib/usual/*.[ch]

