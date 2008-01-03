
include ../config.mak

wiki = https://developer.skype.com/SkypeGarage/DbProjects/SkyTools

web = mkz@shell.pgfoundry.org:/home/pgfoundry.org/groups/skytools/htdocs/

EPYDOC = epydoc-py25
EPYARGS = --no-private --url="http://pgfoundry.org/projects/skytools/" \
	--name="Skytools" --html --no-private

HTMLS = londiste.cmdline.html londiste.config.html README.html INSTALL.html \
	londiste.ref.html TODO.html pgq-sql.html pgq-admin.html pgq-nodupes.html \
	walmgr.html set.notes.html \
	upgrade.html

all: man

man: londiste.1 londiste.5 pgqadm.1 walmgr.1
html: $(HTMLS)

install: man
	mkdir -p $(DESTDIR)/$(mandir)/man1
	mkdir -p $(DESTDIR)/$(mandir)/man5
	install -m 644 londiste.1 $(DESTDIR)/$(mandir)/man1
	install -m 644 londiste.5 $(DESTDIR)/$(mandir)/man5
	install -m 644 pgqadm.1 $(DESTDIR)/$(mandir)/man1
	install -m 644 walmgr.1 $(DESTDIR)/$(mandir)/man1

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

clean:
	rm -rf api *.1 *.5 *.html *.xml

distclean:
	rm -rf ../sql/pgq/docs/pgq api

londiste.1: londiste.cmdline.xml
	xmlto man $<

londiste.5: londiste.config.xml
	xmlto man $<

pgqadm.1: pgq-admin.xml
	xmlto man $<

walmgr.1: walmgr.xml
	xmlto man $<

%.xml: %.txt
	#asciidoc -b docbook -d manpage -o - $< > $@
	asciidoc -b docbook -d manpage -o - $< \
	| python fixman.py > $@

%.html: %.txt
	asciidoc -a toc $<

README.html: ../README
	cat $< \
	| sed -e 's,doc/\([!-~]*\)[.]txt,link:\1.html[],g' \
	      -e 's,http:[!-~]*,&[],g' \
	| asciidoc -o $@ -

INSTALL.html: ../INSTALL
	asciidoc -o $@ $<

web: $(HTMLS)
	rsync -avz $(HTMLS) $(web)/doc/



