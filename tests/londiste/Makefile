
PYTHONPATH := ../../python:$(PYTHONPATH)
PATH := ../../python:../../scripts:$(PATH)

contrib=/usr/share/postgresql/8.1/contrib
contrib=/opt/apps/pgsql-dev/share/contrib
contrib=/opt/pgsql/share/contrib


tests: gendb
	pgqadm.py conf/ticker_root.ini ticker -v -d
	pgqadm.py conf/ticker_branch.ini ticker -v -d
	pgqadm.py conf/ticker_leaf.ini ticker -v -d
	londiste.py conf/w_root.ini replay -d -v
	londiste.py conf/w_branch.ini replay -d -v
	londiste.py conf/w_leaf.ini replay -d -v
	echo everything is running now
	sleep 10
	grep -E 'WARN|ERR|CRIT' sys/log.*

gendb: init db_root db_branch db_leaf

init:
	mkdir -p sys
	./stop.sh
	sleep 1
	rm -rf file_logs sys
	mkdir -p sys

db_root:
	echo "creating database: $@"
	dropdb $@ && sleep 1 || true
	sleep 1
	createdb $@
	londiste.py conf/w_root.ini init-root n_root "dbname=$@"
	pgqadm.py -v conf/ticker_root.ini install
	psql -q $@ -f data.sql
	londiste.py -v conf/w_root.ini add data1
	#londiste.py -v conf/w_root.ini add data1
	#londiste.py -v conf/w_root.ini remove data1
	#londiste.py -v conf/w_root.ini remove data1
	#londiste.py -v conf/w_root.ini add data1
	londiste.py -v conf/w_root.ini tables

db_branch:
	echo "creating database: $@"
	dropdb $@ && sleep 1 || true
	createdb $@
	pgqadm.py conf/ticker_branch.ini install
	londiste.py conf/w_branch.ini init-branch n_branch "dbname=$@" --provider="dbname=db_root"
	psql -q $@ -f data.sql
	londiste.py conf/w_branch.ini add data1
	#londiste.py conf/w_branch.ini add data1
	#londiste.py conf/w_branch.ini remove data1
	#londiste.py conf/w_branch.ini remove data1
	#londiste.py conf/w_branch.ini add data1
	londiste.py conf/w_branch.ini tables

db_leaf:
	echo "creating database: $@"
	dropdb $@ && sleep 1 || true
	createdb $@
	pgqadm.py conf/ticker_leaf.ini install
	londiste.py conf/w_leaf.ini init-leaf n_leaf "dbname=$@" --provider="dbname=db_root"
	psql -q $@ -f data.sql
	londiste.py conf/w_leaf.ini add data1
	#londiste.py conf/w_leaf.ini add data1
	#londiste.py conf/w_leaf.ini remove data1
	#londiste.py conf/w_leaf.ini remove data1
	#londiste.py conf/w_leaf.ini add data1
	londiste.py conf/w_leaf.ini tables

