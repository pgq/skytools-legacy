#! /bin/bash

. ../testlib.sh

../zstop.sh

rm -f resurrect-lost-events.json

v='-q'
v=''
nocheck=1

db_list="db1 db2 db3 db4 db5"

kdb_list=`echo $db_list | sed 's/ /,/g'`

#( cd ../..; make -s install )

do_check() {
  test $nocheck = 1 || ../zcheck.sh
}

title Resurrect test

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

for n in 1 2 3; do
cat > conf/gen$n.ini <<EOF
[loadgen]
job_name = loadgen$n
db = dbname=db$n
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF
done

psql -d template1 -c 'drop database if exists db1x'
psql -d template1 -c 'drop database if exists db2x'
createdb db1
createdb db2

for db in $db_list; do
  cleardb $db
done

clearlogs

set -e

msg "Basic config"
run cat conf/pgqd.ini
run cat conf/londiste_db1.ini

msg "Install londiste3 and initialize nodes"
run londiste3 $v conf/londiste_db1.ini create-root node1 'dbname=db1'
run londiste3 $v conf/londiste_db2.ini create-branch node2 'dbname=db2' --provider='dbname=db1'
run londiste3 $v conf/londiste_db3.ini create-branch node3 'dbname=db3' --provider='dbname=db2'
run londiste3 $v conf/londiste_db4.ini create-branch node4 'dbname=db4' --provider='dbname=db2'
run londiste3 $v conf/londiste_db5.ini create-branch node5 'dbname=db5' --provider='dbname=db2'

msg "Run ticker"
run pgqd $v -d conf/pgqd.ini
run sleep 5

msg "See topology"
run londiste3 $v conf/londiste_db1.ini status

msg "Run londiste3 daemon for each node"
for db in $db_list; do
  run psql -d $db -c "update pgq.queue set queue_ticker_idle_period='2 secs'"
  run londiste3 $v -d conf/londiste_$db.ini worker
done

msg "Create table on root node and fill couple of rows"
run psql -d db1 -c "create table mytable (id serial primary key, data text)"
for n in 1 2 3 4; do
  run psql -d db1 -c "insert into mytable (data) values ('row$n')"
done

msg "Run loadgen on table"
run ./loadgen.py -d conf/gen1.ini

msg "Register table on root node"
run londiste3 $v conf/londiste_db1.ini add-table mytable
run londiste3 $v conf/londiste_db1.ini add-seq mytable_id_seq

msg "Register table on other node with creation"
for db in db2 db3 db4 db5; do
  run psql -d $db -c "create sequence mytable_id_seq"
  run londiste3 $v conf/londiste_$db.ini add-seq mytable_id_seq
  run londiste3 $v conf/londiste_$db.ini add-table mytable --create-full
done

msg "Wait until tables are in sync on db3"

run londiste3 conf/londiste_db2.ini wait-sync
run londiste3 conf/londiste_db3.ini wait-sync

run londiste3 conf/londiste_db3.ini status

###################

#msg "Stop Londiste on Node2"
#run londiste3 conf/londiste_db2.ini worker -s
#sleep 1

#msg "Wait a bit"
#run sleep 10
#############################
msg "Force lag on db2"
run londiste3 $v conf/londiste_db2.ini worker -s
run sleep 20

msg "Stop Londiste on Node1"
run londiste3 conf/londiste_db1.ini worker -s

msg "Stop loadgen"
run sleep 5
run ./loadgen.py -s conf/gen1.ini

#msg "Kill old root"
#ps aux | grep 'postgres[:].* db1 ' | awk '{print $2}' | xargs -r kill
#sleep 3
#ps aux | grep 'postgres[:].* db1 ' | awk '{print $2}' | xargs -r kill -9
#sleep 3

run londiste3 $v conf/londiste_db2.ini status --dead=node1

#msg "Stop Ticker"
run pgqd -s conf/pgqd.ini
run psql -d db2 -c 'alter database db1 rename to db1x'
run pgqd -d conf/pgqd.ini
#run londiste3 $v conf/londiste_db2.ini tag-dead node1
run londiste3 $v conf/londiste_db2.ini worker -d


msg "Take over root role"
run londiste3 $v conf/londiste_db2.ini takeover node1 --dead-root
run londiste3 $v conf/londiste_db2.ini tag-dead node1
run londiste3 $v conf/londiste_db2.ini status

msg "Move database back"
run psql -d db2 -c 'alter database db1x rename to db1'

msg "Do resurrection ritual"
run londiste3 conf/londiste_db1.ini resurrect

exit 0




##
## basic setup done
##

# test lagged takeover
if true; then

msg "Force lag on db2"
run londiste3 $v conf/londiste_db2.ini worker -s
run sleep 20

msg "Kill old root"
ps aux | grep 'postgres[:].* db1 ' | awk '{print $2}' | xargs -r kill
sleep 3
ps aux | grep 'postgres[:].* db1 ' | awk '{print $2}' | xargs -r kill -9
sleep 3
run psql -d db2 -c 'drop database db1'
run psql -d db2 -c 'create database db1'
run londiste3 $v conf/londiste_db2.ini status --dead=node1

msg "Change db2 to read from db3"
run londiste3 $v conf/londiste_db2.ini worker -d
run londiste3 $v conf/londiste_db2.ini change-provider --provider=node3 --dead=node1

msg "Wait until catchup"
run londiste3 $v conf/londiste_db2.ini wait-provider

msg "Promoting db2 to root"
run londiste3 $v conf/londiste_db2.ini takeover node1 --dead-root
run londiste3 $v conf/londiste_db2.ini tag-dead node1
run londiste3 $v conf/londiste_db2.ini status

run sleep 5

msg "Done"

do_check

exit 0
fi



msg "Change provider"
run londiste3 $v conf/londiste_db4.ini status
run londiste3 $v conf/londiste_db4.ini change-provider --provider=node3
run londiste3 $v conf/londiste_db4.ini status
run londiste3 $v conf/londiste_db5.ini change-provider --provider=node2
run londiste3 $v conf/londiste_db5.ini status

msg "Change topology"
run londiste3 $v conf/londiste_db1.ini status
run londiste3 $v conf/londiste_db3.ini takeover node2
run londiste3 $v conf/londiste_db2.ini status
run londiste3 $v conf/londiste_db2.ini takeover node1
run londiste3 $v conf/londiste_db2.ini status

msg "Restart loadgen"
run ./loadgen.py -s conf/gen1.ini
run ./loadgen.py -d conf/gen2.ini

run sleep 10
do_check

msg "Change topology / failover"
ps aux | grep 'postgres[:].* db2 ' | awk '{print $2}' | xargs -r kill
sleep 3
ps aux | grep 'postgres[:].* db2 ' | awk '{print $2}' | xargs -r kill -9
sleep 3
run psql -d db1 -c 'alter database db2 rename to db2x'
run londiste3 $v conf/londiste_db1.ini status --dead=node2
run londiste3 $v conf/londiste_db3.ini takeover db2 --dead-root || true
run londiste3 $v conf/londiste_db3.ini takeover node2 --dead-root
run londiste3 $v conf/londiste_db1.ini status

msg "Restart loadgen"
run ./loadgen.py -s conf/gen2.ini
run ./loadgen.py -d conf/gen3.ini


run sleep 10
do_check

msg Done
