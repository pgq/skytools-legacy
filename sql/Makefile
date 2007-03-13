
include ../config.mak

SUBDIRS = logtriga londiste pgq pgq_ext txid

all install clean distclean installcheck:
	for dir in $(SUBDIRS); do \
	  make -C $$dir $@ DESTDIR=$(DESTDIR) || exit $?; \
	done

