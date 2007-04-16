#! /bin/sh

. ./env.sh

contrib=/opt/pgsql/share/contrib

mkdir -p sys
./stop.sh
sleep 1

rm -f sys/*


db=scriptsrc
echo "creating database: $db"
dropdb $db
sleep 1
createdb $db

pgqadm.py conf/ticker.ini install

#createlang plpgsql $db
#createlang plpythonu $db
#psql -q $db -f $contrib/txid.sql
#psql -q $db -f $contrib/pgq.sql
psql -q $db -f $contrib/pgq_ext.sql
psql -q $db -f data.sql
psql -q $db -f install.sql

db=scriptdst
echo "creating database: $db"
dropdb $db
sleep 1
createdb $db
createlang plpgsql $db
psql -q $db -f data.sql
psql -q $db -f $contrib/pgq_ext.sql

echo "done, testing"

#pgqmgr.py -d conf/ticker.ini ticker
#./run-tests.sh

