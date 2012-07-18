#! /bin/sh

. ../testlib.sh

for db in qdb; do
  cleardb $db
done

rm -f log/*.log pid/*.pid
mkdir -p state
rm -f state/*

set -e

title SimpleConsumer test

title2 Initialization

msg Install PgQ

run_qadmin qdb "install pgq;"
run psql -d qdb -f schema.sql

msg Run ticker

cat_file conf/pgqd.ini <<EOF
[pgqd]
database_list = qdb
logfile = log/pgqd.log
pidfile = pid/pgqd.pid
EOF

run pgqd -d conf/pgqd.ini

msg Run consumer

cat_file conf/simple1_qdb.ini <<EOF
[simple_consumer3]
queue_name = testqueue
src_db = dbname=qdb
dst_db = dbname=qdb
dst_query = insert into logtable (script, event_id, data) values ('simplecons', %%(pgq.ev_id)s, %%(data)s);
table_filter = qtable
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
local_tracking_file = state/%(job_name)s.tick
EOF

cat_file conf/simple2_qdb.ini <<EOF
[simple_local_consumer3]
queue_name = testqueue
src_db = dbname=qdb
dst_db = dbname=qdb
dst_query = insert into logtable (script, event_id, data) values ('simplelocalcons', %%(pgq.ev_id)s, %%(data)s);
table_filter = qtable
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
local_tracking_file = state/%(job_name)s.tick
EOF


run simple_consumer3 -v conf/simple1_qdb.ini --register
run simple_consumer3 -v -d conf/simple1_qdb.ini
run simple_local_consumer3 -v -d conf/simple2_qdb.ini

run_sql qdb "insert into qtable values ('data1')"

run sleep 10
run cat log/*
