
-include config.mak

PYTHON ?= python

pyver = $(shell $(PYTHON) -V 2>&1 | sed 's/^[^ ]* \([0-9]*\.[0-9]*\).*/\1/')

SUBDIRS = sql doc

all: python-all modules-all

modules-all: config.mak
	$(MAKE) -C sql all

python-all: config.mak
	$(PYTHON) setup.py build

clean:
	$(PYTHON) setup.py clean
	$(MAKE) -C sql clean
	$(MAKE) -C doc clean
	rm -rf build
	find python -name '*.py[oc]' -print | xargs rm -f
	rm -f python/skytools/installer_config.py
	rm -rf tests/londiste/sys
	rm -rf tests/londiste/file_logs
	rm -rf tests/londiste/fix.*
	rm -rf tests/scripts/sys

install: python-install modules-install

installcheck:
	$(MAKE) -C sql installcheck

modules-install: config.mak
	$(MAKE) -C sql install DESTDIR=$(DESTDIR)
	test \! -d compat || $(MAKE) -C compat $@ DESTDIR=$(DESTDIR)

python-install: config.mak modules-all
	$(PYTHON) setup.py install --prefix=$(prefix) --root=$(DESTDIR)/ $(BROKEN_PYTHON)
	$(MAKE) -C doc DESTDIR=$(DESTDIR) install

python-install python-all: python/skytools/installer_config.py
python/skytools/installer_config.py: python/skytools/installer_config.py.in config.mak
	sed -e 's!@SQLDIR@!$(SQLDIR)!g' -e 's!@PACKAGE_VERSION@!$(PACKAGE_VERSION)!g' $< > $@

realclean:
	$(MAKE) -C doc $@
	$(MAKE) distclean

distclean: clean
	for dir in $(SUBDIRS); do $(MAKE) -C $$dir $@ || exit 1; done
	$(MAKE) -C doc $@
	rm -rf source.list dist skytools-*
	find python -name '*.pyc' | xargs rm -f
	rm -rf dist build
	rm -rf autom4te.cache config.log config.status config.mak

deb80:
	./configure --with-pgconfig=/usr/lib/postgresql/8.0/bin/pg_config --with-python=$(PYTHON)
	sed -e s/PGVER/8.0/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb81:
	./configure --with-pgconfig=/usr/lib/postgresql/8.1/bin/pg_config --with-python=$(PYTHON)
	sed -e s/PGVER/8.1/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb82:
	./configure --with-pgconfig=/usr/lib/postgresql/8.2/bin/pg_config --with-python=$(PYTHON)
	sed -e s/PGVER/8.2/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb83:
	./configure --with-pgconfig=/usr/lib/postgresql/8.3/bin/pg_config --with-python=$(PYTHON)
	sed -e s/PGVER/8.3/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb84:
	./configure --with-pgconfig=/usr/lib/postgresql/8.4/bin/pg_config --with-python=$(PYTHON)
	sed -e s/PGVER/8.4/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb90:
	./configure --with-pgconfig=/usr/lib/postgresql/9.0/bin/pg_config --with-python=$(PYTHON)
	sed -e s/PGVER/9.0/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb91:
	./configure --with-pgconfig=/usr/lib/postgresql/9.1/bin/pg_config --with-python=$(PYTHON)
	sed -e s/PGVER/9.1/g -e s/PYVER/$(pyver)/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

tgz: config.mak clean
	$(MAKE) -C doc man
	$(PYTHON) setup.py sdist -t source.cfg -m source.list

debclean: distclean
	rm -rf debian/tmp-* debian/build* debian/control debian/packages-tmp*
	rm -f debian/files debian/rules debian/sub* debian/packages

boot: configure

configure: configure.ac
	autoconf

# workaround for Debian's broken python
debfix:
	$(PYTHON) setup.py install --help | grep -q install-layout \
	&& echo BROKEN_PYTHON=--install-layout=deb || echo 'WORKING_PYTHON=found'

.PHONY: all clean distclean install deb debclean tgz
.PHONY: python-all python-clean python-install

