
include ../config.mak

web = mkz@pgf:/home/pgfoundry.org/groups/skytools/htdocs/skytools-3.0

EPYDOC = epydoc
EPYARGS = --no-private --url="http://pgfoundry.org/projects/skytools/" \
	--name="Skytools" --html --no-private -v

TOPHTML = README.html INSTALL.html index.html
DOCHTML = \
	TODO.html pgq-sql.html pgq-nodupes.html \
	faq.html set.notes.html skytools3.html devnotes.html pgqd.html \
	londiste3.html walmgr3.html qadmin.html scriptmgr.html \
	sql-grants.html \
	skytools_upgrade.html queue_mover3.html queue_splitter3.html \
	simple_consumer3.html simple_local_consumer3.html \
	howto/londiste3_cascaded_rep_howto.html \
	howto/londiste3_merge_howto.html \
	howto/londiste3_partitioning_howto.html \
	howto/londiste3_simple_rep_howto.html \
	howto/setup_walmgr_replication.html


MAN5 =
MAN1_SFX = scriptmgr.1 skytools_upgrade.1
MAN1 = qadmin.1 pgqd.1 walmgr3.1 londiste3.1 simple_consumer3.1 simple_local_consumer3.1 \
       queue_mover3.1 queue_splitter3.1

FQHTML = $(addprefix html/doc/, $(DOCHTML)) $(addprefix html/, $(TOPHTML))
FQMAN1 = $(addprefix man/, $(MAN1))
FQMAN1_SFX = $(addprefix man/, $(MAN1_SFX))
FQMAN5 = $(addprefix man/, $(MAN5))
FQMAN = $(FQMAN1) $(FQMAN1_SFX) $(FQMAN5)

COMMON = common.switches.txt common.config.txt

GETATTRS = $(PYTHON) ../misc/getattrs.py
FIXMAN = $(PYTHON) ../misc/fixman.py

#AFLAGS = -a linkcss
#AFLAGS = -a stylesheet=extra.css

all: $(FQMAN)

man: $(FQMAN)

html: $(FQHTML)

install: $(FQMAN)
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

htmlinstall: $(FQHTML)
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
	rm -rf html/api
	mkdir -p html/api
	cd ../python && $(EPYDOC) $(EPYARGS) -o ../doc/html/api $(PY_PKGS)
	cd ../sql/pgq && rm -rf docs/html && $(MAKE) dox && cp -rp docs/html ../../doc/html/pgq
	cd ../sql/pgq_coop && rm -rf docs/html && $(MAKE) dox && cp -rp docs/html ../../doc/html/pgq_coop
	cd ../sql/pgq_node && rm -rf docs/html && $(MAKE) dox && cp -rp docs/html ../../doc/html/pgq_node
	cd ../sql/londiste && rm -rf docs/html && $(MAKE) dox && cp -rp docs/html ../../doc/html/londiste
	cd ../sql/pgq_ext && rm -rf docs/html && $(MAKE) dox && cp -rp docs/html ../../doc/html/pgq_ext

apiupload: apidoc
	-rsync -rtlz api/* $(web)/api
	-rsync -rtlz ../sql/pgq/docs/html/* $(web)/pgq/
	-rsync -rtlz ../sql/pgq_coop/docs/html/* $(web)/pgq_coop/
	-rsync -rtlz ../sql/pgq_node/docs/html/* $(web)/pgq_node/
	-rsync -rtlz ../sql/londiste/docs/html/* $(web)/londiste/

clean:
	rm -rf html *.xml api

distclean: clean
	rm -rf ../sql/pgq/docs/pgq

realclean: distclean
	rm -rf man *.xml *.html

ifneq ($(ASCIIDOC),no)
ifneq ($(XMLTO),no)
man/%.xml: %.txt $(COMMON)
	@mkdir -p man
	$(ASCIIDOC) -b docbook -d manpage `$(GETATTRS) $<` -o $@ $<

man/%.1: man/%.xml
	@mkdir -p man
	$(XMLTO) -o man man $<
endif

html/doc/%.html: %.txt $(COMMON)
	@mkdir -p $(dir $@)
	LANG=C cat $< \
	| sed -e '/^include/b' -e 's,\([A-Za-z.0-9]*\)[.]txt,link:\1.html[],g' \
	| $(ASCIIDOC) $(AFLAGS) -a toc `$(GETATTRS) $<` -o - - \
	| sed -e "/^[/][*] Workarounds/r ../misc/extra.css" \
	> $@

html/README.html: ../README
	@mkdir -p $(dir $@)
	cat $< \
	| sed -e 's,doc/\([!-~]*\)[.]txt,link:\1.html[],g' \
	      -e 's,http:[!-~]*,&[],g' \
	| $(ASCIIDOC) $(AFLAGS) -a toc -o - - \
	| sed -e "/^[/][*] Workarounds/r ../misc/extra.css" \
	> $@

html/INSTALL.html: ../INSTALL
	@mkdir -p $(dir $@)
	$(ASCIIDOC) $(AFLAGS) -o - $< \
	| sed -e "/^[/][*] Workarounds/r ../misc/extra.css" \
	> $@

html/index.html: index.txt
	@mkdir -p $(dir $@)
	$(ASCIIDOC) $(AFLAGS) -o - $< \
	| sed -e "/^[/][*] Workarounds/r ../misc/extra.css" \
	> $@
endif

web: $(FQHTMLS)
	rsync -avz html/* $(web)/



