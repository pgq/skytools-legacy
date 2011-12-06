
include ../config.mak

web = mkz@pgf:/home/pgfoundry.org/groups/skytools/htdocs/skytools-3.0

EPYDOC = epydoc
EPYARGS = --no-private --url="http://pgfoundry.org/projects/skytools/" \
	--name="Skytools" --html --no-private -v

HTMLS = README.html INSTALL.html \
	TODO.html pgq-sql.html pgq-nodupes.html \
	$(SCRIPT_HTMLS) faq.html set.notes.html skytools3.html devnotes.html

SCRIPT_TXTS = londiste3.txt walmgr3.txt qadmin.txt scriptmgr.txt skytools_upgrade.txt
SCRIPT_HTMLS = $(SCRIPT_TXTS:.txt=.html)

MAN5 =
MAN1_SFX = scriptmgr.1 skytools_upgrade.1 walmgr3.1 londiste3.1
MAN1 = qadmin.1 pgqd.1

FQHTML = $(addprefix html/, $(HTMLS))
FQMAN1 = $(addprefix man/, $(MAN1))
FQMAN1_SFX = $(addprefix man/, $(MAN1_SFX))
FQMAN5 = $(addprefix man/, $(MAN5))
FQMAN = $(FQMAN1) $(FQMAN1_SFX) $(FQMAN5)

COMMON = common.switches.txt common.config.txt

GETATTRS = python ../misc/getattrs.py
FIXMAN = python ../misc/fixman.py

#AFLAGS = -a linkcss
#AFLAGS = -a stylesheet=extra.css

all: $(FQMAN) $(FQHTML)

man: $(FQMAN)

html: $(FQHTML)

install: $(FQMAN) $(FQHTML)
	mkdir -p $(DESTDIR)/$(mandir)/man1
	mkdir -p $(DESTDIR)/$(mandir)/man5
	mkdir -p $(DESTDIR)/$(docdir)
	for m in $(FQMAN1_SFX); do \
	  xf="`basename $$m | sed 's/[.]/$(SUFFIX)./'`"; \
	  install -m 644 $$m $(DESTDIR)/$(mandir)/man1/$$xf || exit 1; \
	done
	for m in $(FQMAN1); do \
	  install -m 644 $$m $(DESTDIR)/$(mandir)/man1 || exit 1; \
	done
	for m in $(FQMAN5); do \
	  install -m 644 $$m $(DESTDIR)/$(mandir)/man5 || exit 1; \
	done
	for h in $(FQHTML); do \
	  install -m 644 $$h $(DESTDIR)/$(docdir) || exit 1; \
	done

PY_PKGS = skytools pgq londiste

#	skytools.config skytools.dbstruct skytools.gzlog \
#	skytools.quoting skytools.scripting skytools.sqltools \
#	pgq pgq.consumer pgq.event pgq.maint pgq.producer pgq.status pgq.ticker \
#	londiste londiste.compare londiste.file_read londiste.file_write \
#	londiste.installer londiste.playback londiste.repair londiste.setup \
#	londiste.syncer londiste.table_copy

apidoc:
	rm -rf api
	mkdir -p api
	cd ../python && $(EPYDOC) $(EPYARGS) -o ../doc/api $(PY_PKGS)
	cd ../sql/pgq && rm -rf docs/html && $(MAKE) dox
	cd ../sql/pgq_coop && rm -rf docs/html && $(MAKE) dox
	cd ../sql/pgq_node && rm -rf docs/html && $(MAKE) dox
	cd ../sql/londiste && rm -rf docs/html && $(MAKE) dox

apiupload: apidoc
	-rsync -rtlz api/* $(web)/api
	-rsync -rtlz ../sql/pgq/docs/html/* $(web)/pgq/
	-rsync -rtlz ../sql/pgq_coop/docs/html/* $(web)/pgq_coop/
	-rsync -rtlz ../sql/pgq_node/docs/html/* $(web)/pgq_node/
	-rsync -rtlz ../sql/londiste/docs/html/* $(web)/londiste/

clean:
	rm -rf api *.xml

distclean: clean
	rm -rf ../sql/pgq/docs/pgq

realclean: distclean
	rm -f *.[15] *.xml *.html

ifneq ($(ASCIIDOC),no)
ifneq ($(XMLTO),no)
man/%.xml: %.txt $(COMMON)
	@mkdir -p man
	$(ASCIIDOC) -b docbook -d manpage `$(GETATTRS) $<` -o - $< \
	| $(FIXMAN) > $@

man/%.1: man/%.xml
	@mkdir -p man
	$(XMLTO) -o man man $<
endif

html/%.html: %.txt $(COMMON)
	@mkdir -p html
	LANG=C cat $< \
	| sed -e '/^include/b;s,\([A-Za-z.0-9]*\)[.]txt,link:\1.html[],g' \
	| $(ASCIIDOC) $(AFLAGS) -a toc `$(GETATTRS) $<` -o - - \
	| sed -e "/^[/][*] Workarounds/r ../misc/extra.css" \
	> $@

html/README.html: ../README
	@mkdir -p html
	cat $< \
	| sed -e 's,doc/\([!-~]*\)[.]txt,link:\1.html[],g' \
	      -e 's,http:[!-~]*,&[],g' \
	| $(ASCIIDOC) $(AFLAGS) -o - - \
	| sed -e "/^[/][*] Workarounds/r ../misc/extra.css" \
	> $@

html/INSTALL.html: ../INSTALL
	@mkdir -p html
	$(ASCIIDOC) $(AFLAGS) -o - $< \
	| sed -e "/^[/][*] Workarounds/r ../misc/extra.css" \
	> $@
endif

web: $(FQHTMLS)
	rsync -avz $(FQHTML) $(web)/doc/



