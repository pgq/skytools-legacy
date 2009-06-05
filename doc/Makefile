
include ../config.mak

wiki = https://developer.skype.com/SkypeGarage/DbProjects/SkyTools

web = mkz@shell.pgfoundry.org:/home/pgfoundry.org/groups/skytools/htdocs/skytools-3.0

EPYDOC = epydoc
EPYARGS = --no-private --url="http://pgfoundry.org/projects/skytools/" \
	--name="Skytools" --html --no-private -v

HTMLS = README.html INSTALL.html \
	londiste.ref.html TODO.html pgq-sql.html pgqadm.html pgq-nodupes.html \
	$(SCRIPT_HTMLS) faq.html set.notes.html skytools3.html devnotes.html

SCRIPT_TXTS = walmgr.txt queue_mover.txt queue_splitter.txt qadmin.txt \
	      scriptmgr.txt skytools_upgrade.txt londiste.txt pgqadm.txt
SCRIPT_HTMLS = $(SCRIPT_TXTS:.txt=.html)

MAN1 = $(SCRIPT_TXTS:.txt=.1)
MAN5 = 

FQHTML = $(addprefix html/, $(HTMLS))
FQMAN1 = $(addprefix man/, $(MAN1))
FQMAN5 = $(addprefix man/, $(MAN5))
FQMAN = $(FQMAN1) $(FQMAN5)

COMMON = common.switches.txt common.config.txt common.logutriga.txt

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
	for m in $(FQMAN1); do \
	  install -m 644 $$m $(DESTDIR)/$(mandir)/man1 ; \
	done
	for m in $(FQMAN5); do \
	  install -m 644 $$m $(DESTDIR)/$(mandir)/man5 ; \
	done
	for h in $(FQHTML); do \
	  install -m 644 $$h $(DESTDIR)/$(docdir) ; \
	done

old.wiki.upload:
	devupload.sh overview.txt $(wiki)
	#devupload.sh TODO.txt $(wiki)/ToDo
	#devupload.sh londiste.txt $(wiki)/LondisteUsage
	#devupload.sh londiste.ref.txt $(wiki)/LondisteReference
	#devupload.sh pgq-sql.txt $(wiki)/PgQdocs
	#devupload.sh pgq-nodupes.txt $(wiki)/PgqNoDupes
	#devupload.sh walmgr.txt $(wiki)/WalMgr
	#devupload.sh pgq-admin.txt $(wiki)/PgqAdm

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

apiupload: apidoc
	rsync -rtlz api/* $(web)/api
	cd ../sql/pgq && rm -rf docs/html && $(MAKE) dox
	rsync -rtlz ../sql/pgq/docs/html/* $(web)/pgq/
	cd ../sql/pgq_coop && rm -rf docs/html && $(MAKE) dox
	rsync -rtlz ../sql/pgq_coop/docs/html/* $(web)/pgq_coop/
	cd ../sql/pgq_node && rm -rf docs/html && $(MAKE) dox
	rsync -rtlz ../sql/pgq_node/docs/html/* $(web)/pgq_node/
	cd ../sql/londiste && rm -rf docs/html && $(MAKE) dox
	rsync -rtlz ../sql/londiste/docs/html/* $(web)/londiste/

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

web: $(HTMLS)
	rsync -avz $(FQHTML) $(web)/doc/



