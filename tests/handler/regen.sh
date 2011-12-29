#! /bin/bash

. ../testlib.sh

../zstop.sh

v='-v'

# bulkloader method
meth=0

db_list="hsrc hdst"

kdb_list=`echo $db_list | sed 's/ /,/g'`

#( cd ../..; make -s install )

echo " * create configs * "

# create ticker conf
cat > conf/pgqd.ini <<EOF
[pgqd]
database_list = $kdb_list
logfile = log/pgqd.log
pidfile = pid/pgqd.pid
EOF

# londiste3 configs
for db in $db_list; do
cat > conf/londiste_$db.ini <<EOF
[londiste3]
job_name = londiste_$db
db = dbname=$db
queue_name = replika
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid

handler_modules = londiste.handlers.bulk
EOF
done

for db in $db_list; do
  cleardb $db
done

clearlogs

set -e

msg "Basic config"
run cat conf/pgqd.ini
run cat conf/londiste_hsrc.ini

msg "Install londiste3 and initialize nodes"
run londiste3 $v conf/londiste_hsrc.ini create-root hsrc 'dbname=hsrc'
run londiste3 $v conf/londiste_hdst.ini create-leaf hdst 'dbname=hdst' --provider='dbname=hsrc'
for db in $db_list; do
  run_sql $db "update pgq.queue set queue_ticker_idle_period='5 secs'"
done

msg "Run ticker"
run pgqd -d conf/pgqd.ini
run sleep 5

msg "See topology"
run londiste3 $v conf/londiste_hsrc.ini status

msg "Run londiste3 daemon for each node"
for db in $db_list; do
  run londiste3 $v -d conf/londiste_$db.ini worker
done

msg "Create table on root node and fill couple of rows"
run_sql hsrc "create table mytable (id int4 primary key, data text, tstamp timestamptz default now())"
for n in 1 2 3; do
  run_sql hsrc "insert into mytable values ($n, 'row$n')"
done

msg "Insert row with encoding error"
run_sql hsrc "insert into mytable values(4, E'row\xab4')"

msg "Register table on root node"
run londiste3 $v conf/londiste_hsrc.ini add-table mytable

msg "Register table on other node with creation"
for db in hdst; do
  run londiste3 $v conf/londiste_$db.ini add-table mytable --create --handler=bulk_direct --handler-arg="method=$meth" --handler-arg="encoding=utf8"
done

msg "Wait until table is in sync"
cnt=0
while test $cnt -ne 1; do
  sleep 3
  cnt=`psql -A -t -d hdst -c "select count(*) from londiste.table_info where merge_state = 'ok'"`
  echo "  cnt=$cnt"
done

msg "Do some updates"
run_sql hsrc "insert into mytable values (5, 'row5')"
run_sql hsrc "update mytable set data = 'row5x' where id = 5"

run_sql hsrc "insert into mytable values (6, 'row6')"
run_sql hsrc "delete from mytable where id = 6"

run_sql hsrc "insert into mytable values (7, 'row7')"
run_sql hsrc "update mytable set data = 'row7x' where id = 7"
run_sql hsrc "delete from mytable where id = 7"

run_sql hsrc "delete from mytable where id = 1"
run_sql hsrc "update mytable set data = 'row2x' where id = 2"

# row with error
run_sql hsrc "insert into mytable values(8, E'row8\xaf')"

run sleep 5

msg "Check status"
run londiste3 $v conf/londiste_hsrc.ini status

run sleep 5

run_sql hdst 'select * from mytable order by id'

../zcheck.sh




msg "== dispatcher =="

msg "Create table on root node and fill couple of rows"
run_sql hsrc "create table logtable (id int4 primary key, data text, tstamp timestamptz default now())"
for n in 1 2 3; do
  run_sql hsrc "insert into logtable values ($n, 'row$n', '2011-0$n-01')"
done

msg "Register table on root node"
run londiste3 $v conf/londiste_hsrc.ini add-table logtable

msg "Register table on other node with creation"
run_sql hdst "create table logtable (id int4 primary key, data text, tstamp timestamptz default now())"
run_sql hdst "drop role if exists reporting"
run_sql hdst "create group reporting"
run_sql hdst "grant select on logtable to reporting"
run_sql hdst '\i ../../sql/dispatch/create_partition.sql'

for db in hdst; do
  run londiste3 $v conf/londiste_$db.ini add-table logtable --handler=monthly_field --handler-arg="part_field=tstamp" --handler-arg="encoding=utf8"
done

msg "Wait until table is in sync"
cnt=0
while test $cnt -ne 2; do
  sleep 3
  cnt=`psql -A -t -d hdst -c "select count(*) from londiste.table_info where merge_state = 'ok'"`
  echo "  cnt=$cnt"
done

msg "Do some updates"
for n in 4 5 6; do
  run_sql hsrc "insert into logtable values ($n, 'row$n', '2011-0$n-01')"
done

run sleep 10

msg "Check status"
run londiste3 $v conf/londiste_hsrc.ini status

run sleep 5

run_sql hdst 'select * from logtable order by id'

../zcheck.sh

