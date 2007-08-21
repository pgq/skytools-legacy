
include ../config.mak

wiki = https://developer.skype.com/SkypeGarage/DbProjects/SkyTools

web = mkz@shell.pgfoundry.org:/home/pgfoundry.org/groups/skytools/htdocs/

EPYDOC = epydoc-py25
EPYARGS = --no-private --url="http://pgfoundry.org/projects/skytools/" \
	--name="Skytools" --html --no-private

all: man

man: londiste.1 londiste.5
html: londiste.cmdline.html londiste.config.html

install: man
	mkdir -p $(mandir)/man1
	mkdir -p $(mandir)/man5
	install -m 644 londiste.1 $(mandir)/man1
	install -m 644 londiste.5 $(mandir)/man1

upload:
	devupload.sh overview.txt $(wiki)
	devupload.sh TODO.txt $(wiki)/ToDo
	#devupload.sh londiste.txt $(wiki)/LondisteUsage
	devupload.sh londiste.ref.txt $(wiki)/LondisteReference
	devupload.sh pgq-sql.txt $(wiki)/PgQdocs
	devupload.sh pgq-nodupes.txt $(wiki)/PgqNoDupes
	devupload.sh walmgr.txt $(wiki)/WalMgr
	devupload.sh pgq-admin.txt $(wiki)/PgqAdm

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

%.xml: %.txt
	asciidoc -d manpage -b docbook $<

%.html: %.txt
	asciidoc $<

manupload: londiste.1 londiste.1.html
	rsync -avz londiste.1 londiste.1.html londiste.1.txt pgf:web/skytools/htdocs/man/



