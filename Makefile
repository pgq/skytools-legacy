
-include config.mak

PYTHON ?= python

pyver = $(shell $(PYTHON) -V 2>&1 | sed 's/^[^ ]* \([0-9]*\.[0-9]*\).*/\1/')

SUBDIRS = sql doc

#SCRIPTS = python/londiste.py python/qadmin.py python/pgqadm.py python/walmgr.py \
#	  scripts/queue_loader.py scripts/queue_mover.py scripts/queue_splitter.py \
#	  scripts/scriptmgr.py scripts/skytools_upgrade.py

# add suffix
SFX_SCRIPTS = python/londiste.py python/walmgr.py scripts/scriptmgr.py scripts/queue_splitter.py
# dont add
NOSFX_SCRIPTS = python/qadmin.py

SCRIPT_SUFFIX = 3

all: python-all sub-all config.mak

install: sub-install python-install
distclean: sub-distclean
sub-all sub-install sub-clean sub-distclean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $(subst sub-,,$@) DESTDIR=$(DESTDIR) || exit $?; \
	done

.PHONY: sub-all sub-clean sub-install sub-distclean

python-all: config.mak
	$(PYTHON) setup_skytools.py build

clean: sub-clean
	$(PYTHON) setup_skytools.py clean
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
	mkdir -p $(DESTDIR)/$(bindir)
	$(PYTHON) setup_pkgloader.py install --prefix=$(prefix) --root=$(DESTDIR)/
	find build -name 'pkgloader*' | xargs rm
	$(PYTHON) setup_skytools.py install --prefix=$(prefix) --root=$(DESTDIR)/ --record=tmp_files.lst \
		--install-lib=$(prefix)/lib/python$(pyver)/site-packages/skytools-3.0
	for s in $(SFX_SCRIPTS); do \
		exe=`echo $$s|sed -e 's!.*/!!' -e 's/[.]py//'`; \
		install $$s $(DESTDIR)/$(bindir)/$${exe}$(SCRIPT_SUFFIX) || exit 1; \
	done
	for s in $(NOSFX_SCRIPTS); do \
		exe=`echo $$s|sed -e 's!.*/!!' -e 's/[.]py//'`; \
		install $$s $(DESTDIR)/$(bindir)/$$exe || exit 1; \
	done
	$(MAKE) -C doc DESTDIR=$(DESTDIR) install

python-install python-all: python/skytools/installer_config.py
python/skytools/installer_config.py: python/skytools/installer_config.py.in config.mak
	sed -e 's!@SQLDIR@!$(SQLDIR)!g' -e 's!@PACKAGE_VERSION@!$(PACKAGE_VERSION)!g' $< > $@

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
	$(PYTHON) setup_skytools.py sdist -t source.cfg -m source.list

debclean: distclean
	rm -rf debian/tmp-* debian/build* debian/control debian/packages-tmp*
	rm -f debian/files debian/rules debian/sub* debian/packages

boot: configure

configure: configure.ac lib/m4/usual.m4
	aclocal -I lib/m4
	autoheader
	autoconf

tags:
	ctags `find python -name '*.py'`

check:
	./misc/docheck.sh

.PHONY: all clean distclean install deb debclean tgz tags
.PHONY: python-all python-clean python-install check

