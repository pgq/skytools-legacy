#! /bin/sh

. ../env.sh

./gendb.sh

rm -rf upgrade
cp -rp ../../upgrade .

skytools_upgrade.py "dbname=upgradedb"

./gendb.sh
psql -q upgradedb -f upgrade/final/v2.1.5_pgq_core.sql
psql -q upgradedb -f upgrade/final/v2.1.5_pgq_ext.sql
psql -q upgradedb -f upgrade/final/v2.1.5_londiste.sql

echo "update from 2.1.5 to 2.1.6"
skytools_upgrade.py  "dbname=upgradedb"
echo " no update"
skytools_upgrade.py  "dbname=upgradedb"

