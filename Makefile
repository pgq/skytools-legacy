
-include config.mak

PYTHON ?= python

pyver = $(shell $(PYTHON) -V 2>&1 | sed 's/^[^ ]* \([0-9]*\.[0-9]*\).*/\1/')

SUBDIRS = sql doc

# modules that use doctest for regtests
DOCTESTMODS = skytools.quoting skytools.parsing skytools.timeutil \
	   skytools.sqltools skytools.querybuilder skytools.natsort \
	   skytools.utf8 skytools.sockutil skytools.fileutil \
	   londiste.exec_attrs


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
	rm -rf build build.sk3
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

SITEDIR = site-packages

python-install: config.mak
	$(PYTHON) setup_pkgloader.py install --prefix=$(prefix) --root=$(DESTDIR)/ $(BROKEN_PYTHON)
	$(PYTHON) setup_skytools.py install --prefix=$(prefix) --root=$(DESTDIR)/ $(BROKEN_PYTHON)
	$(MAKE) -C doc DESTDIR=$(DESTDIR) install

realclean: distclean
	$(MAKE) -C doc $@
	$(MAKE) distclean

distclean: sub-distclean
	rm -rf source.list dist skytools-*
	find python -name '*.pyc' | xargs rm -f
	rm -rf dist build
	rm -rf autom4te.cache config.log config.status config.mak

deb:
	rm -f debian/control
	make -f debian/rules debian/control
	debuild -uc -us -b

tgz: config.mak clean
	$(MAKE) -C doc man
	rm -f source.list
	$(PYTHON) setup_skytools.py sdist -t source.cfg -m source.list

debclean: distclean
	rm -rf debian/tmp-* debian/build* debian/control debian/packages-tmp*
	rm -f debian/files debian/rules debian/sub* debian/packages

boot: configure

configure: configure.ac lib/m4/usual.m4
	./autogen.sh

tags:
	ctags `find python -name '*.py'`

check:
	./misc/docheck.sh

# workaround for Debian's broken python
debfix:
	@$(PYTHON) setup_skytools.py install --help | grep -q install-layout \
	&& echo BROKEN_PYTHON=--install-layout=deb || echo 'WORKING_PYTHON=found'

.PHONY: all clean distclean install deb debclean tgz tags
.PHONY: python-all python-clean python-install check test

test:
	@cd python; for m in $(DOCTESTMODS); do \
		printf "%-22s ... " $$m; \
		$(PYTHON) -m $$m && echo "ok" || { echo "FAIL"; exit 1; }; \
	done

