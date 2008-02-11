#! /bin/sh

. ../env.sh


old=./sql

db=upgradedb
echo "creating database: $db"
dropdb $db
sleep 1
createdb $db

sver=`psql -At $db -c "show server_version" | sed 's/\([0-9]*[.][0-9]*\).*/\1/'`
echo "server version: $sver"
psql -q $db -c "create language plpgsql"
psql -q $db -c "create language plpythonu"
psql -q $db -f $old/v2.1.4_txid82.sql
psql -q $db -f $old/v2.1.4_pgq.sql
psql -q $db -f $old/v2.1.4_pgq_ext.sql
psql -q $db -f $old/v2.1.4_londiste.sql

