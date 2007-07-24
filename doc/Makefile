
wiki = https://developer.skype.com/SkypeGarage/DbProjects/SkyTools

web = mkz@shell.pgfoundry.org:/home/pgfoundry.org/groups/skytools/htdocs/

EPYARGS = --no-private -u "http://pgfoundry.org/projects/skytools/" \
	-n "Skytools"

all:

upload:
	devupload.sh overview.txt $(wiki)
	#devupload.sh londiste.txt $(wiki)/LondisteUsage
	devupload.sh londiste.ref.txt $(wiki)/LondisteReference
	devupload.sh pgq-sql.txt $(wiki)/PgQdocs
	devupload.sh pgq-nodupes.txt $(wiki)/PgqNoDupes
	devupload.sh walmgr.txt $(wiki)/WalMgr
	devupload.sh pgq-admin.txt $(wiki)/PgqAdm

PY_PKGS = skytools skytools.config skytools.dbstruct skytools.gzlog \
	skytools.quoting skytools.scripting skytools.sqltools \
	pgq pgq.consumer pgq.event pgq.maint pgq.producer pgq.status pgq.ticker \
	londiste londiste.compare londiste.file_read londiste.file_write \
	londiste.installer londiste.playback londiste.repair londiste.setup \
	londiste.syncer londiste.table_copy

apidoc:
	rm -rf api
	mkdir -p api
	cd ../python && epydoc3 -o ../doc/api --html --no-private $(PY_PKGS)

apiupload: apidoc
	cd ../sql/pgq && rm -rf docs/pgq && $(MAKE) dox && mv docs/html docs/pgq
	rsync -rtlz api $(web)
	rsync -rtlz ../sql/pgq/docs/pgq $(web)

clean:
	rm -rf api

distclean:
	rm -rf ../sql/pgq/docs/pgq api

