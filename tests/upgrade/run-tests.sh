#! /bin/sh

. ../env.sh

./gendb.sh

cp -rp ../../upgrade .

skytools_upgrade.py -v "dbname=upgradedb"

