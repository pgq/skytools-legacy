#! /bin/bash

. ../testlib.sh

../zstop.sh

v='-q'
v=''

db_list="rendb1 rendb2"

kdb_list=`echo $db_list | sed 's/ /,/g'`

#( cd ../..; make -s install )

do_check() {
  test $nocheck = 1 || ../zcheck.sh
}

title Rename test

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

pgq_autocommit = 1
pgq_lazy_fetch = 0
EOF
done

for db in $db_list; do
  createdb $db
  cleardb $db
done

clearlogs

set -e

msg "Basic config"
run cat conf/pgqd.ini
run cat conf/londiste_rendb1.ini

msg "Install londiste3 and initialize nodes"
run londiste3 $v conf/londiste_rendb1.ini create-root node1 'dbname=rendb1'
run londiste3 $v conf/londiste_rendb2.ini create-branch node2 'dbname=rendb2' --provider='dbname=rendb1'

msg "Run londiste3 daemon for each node"
for db in $db_list; do
  run psql -d $db -c "update pgq.queue set queue_ticker_idle_period='5 secs'"
  run londiste3 $v -d conf/londiste_$db.ini worker
done

msg "Run ticker"
run pgqd $v -d conf/pgqd.ini

msg "See topology"
run londiste3 $v conf/londiste_rendb1.ini status
run sleep 2

msg "Create table on root node and fill couple of rows"
run psql -d rendb1 -c "create table mytable (id serial primary key, data text)"
for n in 1 2 3 4; do
  run psql -d rendb1 -c "insert into mytable (data) values ('row$n')"
done

msg "Register table"

run londiste3 $v conf/londiste_rendb1.ini add-table mytable
run londiste3 $v conf/londiste_rendb2.ini add-table mytable --dest-table=dtable --create

msg "Create another table on root node and fill couple of rows"
run psql -d rendb1 -c "create table rentable (id serial primary key, data text)"
for n in 1 2 3 4; do
  run psql -d rendb1 -c "insert into rentable (data) values ('rentable row $n')"
done

msg "Register table with rename on root"
run londiste3 $v conf/londiste_rendb1.ini add-table gtable --dest-table=rentable --force
run londiste3 $v conf/londiste_rendb2.ini add-table gtable --dest-table=gxtable --create

msg "Wait until tables are in sync"
cnt=0
while test $cnt -ne 2; do
  sleep 5
  cnt=`psql -A -t -d rendb2 -c "select count(*) from londiste.table_info where merge_state = 'ok'"`
  echo "  cnt=$cnt"
done

for n in 1 2 3 4; do
  run psql -d rendb1 -c "insert into mytable (data) values ('online row $n')"
  run psql -d rendb1 -c "insert into rentable (data) values ('online row $n')"
done

msg "Wait until all rows arrive"
cnt=0
while test $cnt -ne 8; do
  sleep 5
  cnt=`psql -A -t -d rendb2 -c "select count(*) from dtable"`
  echo "  cnt1=$cnt"
done
cnt=0
while test $cnt -ne 8; do
  sleep 1
  cnt=`psql -A -t -d rendb2 -c "select count(*) from dtable"`
  echo "  cnt2=$cnt"
done

msg "Testin compare"
run londiste3 $v conf/londiste_rendb2.ini compare gtable

msg "Testin repair"
run londiste3 $v conf/londiste_rendb2.ini repair gtable

msg "Done"

