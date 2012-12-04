#! /bin/bash

. ../testlib.sh

../zstop.sh

v='-q'
v=''
nocheck=1

db_list="db1 db2 db3 db4 db5"

kdb_list=`echo $db_list | sed 's/ /,/g'`

#( cd ../..; make -s install )

do_check() {
  test $nocheck = 1 || ../zcheck.sh
}

title "Merge + qnode test"

# create ticker conf
cat > conf/pgqd.ini <<EOF
[pgqd]
database_list = $kdb_list
logfile = log/pgqd.log
pidfile = pid/pgqd.pid
EOF


conf_londiste() {
  xqueue="$1"
  xdb="$2"
  xname="$3"
  cat > conf/${xname}.ini <<EOF
[londiste3]
db = dbname=${xdb}
queue_name = ${xqueue}
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
pgq_autocommit = 1
pgq_lazy_fetch = 0
EOF
}

for db in $db_list; do
  cleardb $db
done

clearlogs

set -e

conf_londiste   part0q   db1   l3_part0_db1
conf_londiste   part1q   db2   l3_part1_db2
conf_londiste   part2q   db3   l3_part2_db3

conf_londiste   part0q   db4   l3_part0_db4
conf_londiste   part1q   db4   l3_part1_db4

conf_londiste   part0q   db5   l3_part0_db5
conf_londiste   part1q   db5   l3_part1_db5
conf_londiste   part2q   db5   l3_part2_db5

msg "Install londiste3 and initialize nodes"
run londiste3 $v conf/l3_part0_db1.ini create-root node1 'dbname=db1'
run londiste3 $v conf/l3_part1_db2.ini create-root node2 'dbname=db2'
run londiste3 $v conf/l3_part2_db3.ini create-root node3 'dbname=db3'

run londiste3 $v conf/l3_part0_db4.ini create-branch node4 'dbname=db4' --provider='dbname=db1'
run londiste3 $v conf/l3_part1_db4.ini create-branch node4 'dbname=db4' --provider='dbname=db2'

run londiste3 $v conf/l3_part0_db5.ini create-leaf node5 'dbname=db5' --provider='dbname=db4'
run londiste3 $v conf/l3_part1_db5.ini create-leaf node5 'dbname=db5' --provider='dbname=db4'
run londiste3 $v conf/l3_part2_db5.ini create-leaf node5 'dbname=db5' --provider='dbname=db3'

for db in $db_list; do
  run psql -d $db -c "update pgq.queue set queue_ticker_idle_period='2 secs'"
done

msg "Run ticker"
run pgqd $v -d conf/pgqd.ini
run sleep 5

msg "See topology"
run londiste3 $v conf/l3_part0_db1.ini status
run londiste3 $v conf/l3_part1_db2.ini status
run londiste3 $v conf/l3_part2_db3.ini status

msg "Run londiste3 daemon for each node"
run londiste3 $v conf/l3_part0_db1.ini worker -d
run londiste3 $v conf/l3_part1_db2.ini worker -d
run londiste3 $v conf/l3_part2_db3.ini worker -d

run londiste3 $v conf/l3_part0_db4.ini worker -d
run londiste3 $v conf/l3_part1_db4.ini worker -d

run londiste3 $v conf/l3_part0_db5.ini worker -d
run londiste3 $v conf/l3_part1_db5.ini worker -d
run londiste3 $v conf/l3_part2_db5.ini worker -d

msg "Create table on root node and fill couple of rows"
run psql -d db1 -c "create table mytable (id int4 primary key, data text)"
run psql -d db2 -c "create table mytable (id int4 primary key, data text)"
run psql -d db3 -c "create table mytable (id int4 primary key, data text)"
run psql -d db5 -c "create table mytable (id int4 primary key, data text)"
for n in 1 2 3 4; do
  run psql -d db1 -c "insert into mytable (id, data) values ($n + 100, 'db1 row$n')"
  run psql -d db2 -c "insert into mytable (id, data) values ($n + 200, 'db2 row$n')"
  run psql -d db3 -c "insert into mytable (id, data) values ($n + 300, 'db3 row$n')"
done

msg "Register table on root node"
run londiste3 $v conf/l3_part0_db1.ini add-table mytable
run londiste3 $v conf/l3_part1_db2.ini add-table mytable
run londiste3 $v conf/l3_part2_db3.ini add-table mytable

msg "Make sure cascade is in sync"
run londiste3 $v conf/l3_part0_db5.ini wait-root
run londiste3 $v conf/l3_part1_db5.ini wait-root
run londiste3 $v conf/l3_part2_db5.ini wait-root

msg "Launch merge on db5"
run londiste3 $v conf/l3_part0_db5.ini add-table mytable --merge-all --find-copy-node
run londiste3 $v conf/l3_part0_db5.ini wait-sync

exit 0

