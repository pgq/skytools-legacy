
wiki = https://developer.skype.com/SkypeGarage/DbProjects/SkyTools

web = mkz@shell.pgfoundry.org:/home/pgfoundry.org/groups/skytools/htdocs/

EPYDOC = epydoc-py25
EPYARGS = --no-private --url="http://pgfoundry.org/projects/skytools/" \
	--name="Skytools" --html --no-private

all:

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
	rm -rf api

distclean:
	rm -rf ../sql/pgq/docs/pgq api

