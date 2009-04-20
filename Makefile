
-include config.mak

PYTHON ?= python

pyver = $(shell $(PYTHON) -V 2>&1 | sed 's/^[^ ]* \([0-9]*\.[0-9]*\).*/\1/')

SUBDIRS = sql doc

all: python-all sub-all config.mak

install: sub-install python-install
distclean: sub-distclean
sub-all sub-install sub-clean sub-distclean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $(subst sub-,,$@) DESTDIR=$(DESTDIR); \
	done

.PHONY: sub-all sub-clean sub-install sub-distclean

python-all: config.mak
	$(PYTHON) setup.py build

clean: sub-clean
	$(PYTHON) setup.py clean
	rm -rf build
	find python -name '*.py[oc]' -print | xargs rm -f
	rm -f python/skytools/installer_config.py source.list
	rm -rf tests/londiste/sys
	rm -rf tests/londiste/file_logs
	rm -rf tests/londiste/fix.*
	rm -rf tests/scripts/sys


installcheck:
	$(MAKE) -C sql installcheck

modules-install: config.mak
	$(MAKE) -C sql install DESTDIR=$(DESTDIR)
	test \! -d compat || $(MAKE) -C compat $@ DESTDIR=$(DESTDIR)

python-install: config.mak sub-all
	$(PYTHON) setup.py install --prefix=$(prefix) --root=$(DESTDIR)/ --record=tmp_files.lst
	grep '/bin/[a-z_0-9]*.py' tmp_files.lst \
	| $(PYTHON) misc/strip_ext.py $(if $(DESTDIR), $(DESTDIR), /)
	rm -f tmp_files.lst
	$(MAKE) -C doc DESTDIR=$(DESTDIR) install

python-install python-all: python/skytools/installer_config.py
python/skytools/installer_config.py: python/skytools/installer_config.py.in config.mak
	sed -e 's!@SQLDIR@!$(SQLDIR)!g' $< > $@

realclean: distclean
	$(MAKE) -C doc $@
	$(MAKE) distclean

distclean: sub-distclean
	rm -rf source.list dist skytools-*
	find python -name '*.pyc' | xargs rm -f
	rm -rf dist build
	rm -rf autom4te.cache config.log config.status config.mak

deb80:
	./configure --with-pgconfig=/usr/lib/postgresql/8.0/bin/pg_config
	sed -e s/PGVER/8.0/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb81:
	./configure --with-pgconfig=/usr/lib/postgresql/8.1/bin/pg_config
	sed -e s/PGVER/8.1/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb82:
	./configure --with-pgconfig=/usr/lib/postgresql/8.2/bin/pg_config
	sed -e s/PGVER/8.2/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb83:
	./configure --with-pgconfig=/usr/lib/postgresql/8.3/bin/pg_config
	sed -e s/PGVER/8.3/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb84:
	./configure --with-pgconfig=/usr/lib/postgresql/8.4/bin/pg_config
	sed -e s/PGVER/8.4/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

tgz: config.mak clean
	$(MAKE) -C doc man html
	rm -f source.list
	$(PYTHON) setup.py sdist -t source.cfg -m source.list

debclean: distclean
	rm -rf debian/tmp-* debian/build* debian/control debian/packages-tmp*
	rm -f debian/files debian/rules debian/sub* debian/packages

boot: configure

configure: configure.ac
	aclocal -I lib/m4
	autoheader
	autoconf

tags:
	ctags `find python -name '*.py'`

check:
	./misc/docheck.sh

.PHONY: all clean distclean install deb debclean tgz tags
.PHONY: python-all python-clean python-install check

