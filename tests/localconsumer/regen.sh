#! /bin/sh

. ../testlib.sh

for db in qdb; do
  cleardb $db
done

rm -f log/*.log
mkdir -p state
rm -f state/*

set -e

title LocalConsumer test

title2 Initialization

msg Install PgQ

run_qadmin qdb "install pgq;"
run_qadmin qdb "create queue test_queue;"

msg Run ticker

cat_file conf/pgqd.ini <<EOF
[pgqd]
database_list = qdb
logfile = log/pgqd.log
pidfile = pid/pgqd.pid
EOF

run pgqd -d conf/pgqd.ini

msg Run consumer

cat_file conf/testconsumer_qdb.ini <<EOF
[testconsumer]
queue_name = test_queue
db = dbname=qdb
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
local_tracking_file = state/%(job_name)s.tick
EOF

run ./testconsumer.py -v conf/testconsumer_qdb.ini

