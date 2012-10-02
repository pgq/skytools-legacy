
include ../config.mak

SUBDIRS = londiste pgq pgq_coop pgq_ext pgq_node ticker txid

all install clean distclean installcheck test:
	for dir in $(SUBDIRS); do \
	  $(MAKE) -C $$dir $@ \
		DESTDIR="$(DESTDIR)" \
		PG_CONFIG="$(PG_CONFIG)" \
		PG_CPPFLAGS="$(PG_CPPFLAGS)" \
		PYTHON="$(PYTHON)" \
	  || exit $?; \
	done

