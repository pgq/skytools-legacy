
-include config.mak

PYTHON ?= python

pyver = $(shell $(PYTHON) -V 2>&1 | sed 's/^[^ ]* \([0-9]*\.[0-9]*\).*/\1/')

SUBDIRS = sql

all: python-all modules-all

modules-all: config.mak
	make -C sql all

python-all: config.mak
	$(PYTHON) setup.py build

clean:
	make -C sql clean
	make -C doc clean
	$(PYTHON) setup.py clean
	rm -rf build
	find python -name '*.py[oc]' -print | xargs rm -f
	rm -f python/skytools/installer_config.py

install: python-install modules-install

installcheck:
	make -C sql installcheck

modules-install: config.mak
	make -C sql install DESTDIR=$(DESTDIR)
	test \! -d compat || make -C compat $@ DESTDIR=$(DESTDIR)

python-install: config.mak
	$(PYTHON) setup.py install --prefix=$(prefix) --root=$(DESTDIR)/
	test \! -d compat || make -C compat $@ DESTDIR=$(DESTDIR)

distclean: clean
	for dir in $(SUBDIRS); do make -C $$dir $@ || exit 1; done
	make -C doc $@
	rm -rf source.list dist skytools-*
	find python -name '*.pyc' | xargs rm -f
	rm -rf dist build
	rm -rf autom4te.cache config.log config.status config.mak

deb80:
	./configure
	sed -e s/PGVER/8.0/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb81:
	./configure
	sed -e s/PGVER/8.1/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb82:
	./configure
	sed -e s/PGVER/8.2/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

tgz: config.mak clean
	$(PYTHON) setup.py sdist -t source.cfg -m source.list

debclean: distclean
	rm -rf debian/tmp-* debian/build* debian/control debian/packages-tmp*
	rm -f debian/files debian/rules debian/sub* debian/packages

boot: configure

configure: configure.ac
	autoconf


.PHONY: all clean distclean install deb debclean tgz
.PHONY: python-all python-clean python-install

