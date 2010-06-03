#! /bin/bash

. ../testlib.sh

./zstop.sh

v='-q'

db_list="db1 db2 db3 db4 db5"

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
EOF
done

psql -d template1 -c 'drop database if exists db2x'
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
run londiste3 $v conf/londiste_db3.ini create-branch node3 'dbname=db3' --provider='dbname=db1'
run londiste3 $v conf/londiste_db4.ini create-leaf node4 'dbname=db4' --provider='dbname=db2'
run londiste3 $v conf/londiste_db5.ini create-branch node5 'dbname=db5' --provider='dbname=db3'

msg "Run ticker"
run pgqd -d conf/pgqd.ini
run sleep 5

msg "See topology"
run londiste3 $v conf/londiste_db4.ini status

msg "Run londiste3 daemon for each node"
for db in $db_list; do
  run psql -d $db -c "update pgq.queue set queue_ticker_idle_period='5 secs'"
  run londiste3 $v -d conf/londiste_$db.ini replay
done

msg "Create table on root node and fill couple of rows"
run psql -d db1 -c "create table mytable (id int4 primary key, data text)"
for n in 1 2 3 4; do
  run psql -d db1 -c "insert into mytable values ($n, 'row$n')"
done

msg "Register table on root node"
run londiste3 $v conf/londiste_db1.ini add-table mytable

msg "Register table on other node with creation"
for db in db2 db3 db4 db5; do
  run londiste3 $v conf/londiste_$db.ini add-table mytable --create
done
run sleep 20

if false; then

msg "Add column on root"
run cat ddl.sql
run londiste3 $v conf/londiste_db1.ini execute ddl.sql
msg "Insert data into new column"
for n in 5 6 7 8; do
  run psql -d db1 -c "insert into mytable values ($n, 'row$n', 'data2')"
done
msg "Wait a bit"
run sleep 20
msg "Check table structure"
run psql -d db5 -c '\d mytable'
run psql -d db5 -c 'select * from mytable'
run sleep 10
../zcheck.sh
fi

echo early quit
exit 0

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

run sleep 10
../zcheck.sh

msg "Change topology / failover"
ps aux | grep 'postgres[:].* db2 ' | awk '{print $2}' | xargs -r kill
sleep 3
ps aux | grep 'postgres[:].* db2 ' | awk '{print $2}' | xargs -r kill -9
sleep 3
run psql -d db1 -c 'alter database db2 rename to db2x'
run londiste3 $v conf/londiste_db1.ini status --dead=node2
run londiste3 $v conf/londiste_db3.ini takeover db2 --dead-root || true
run londiste3 $v conf/londiste_db3.ini takeover node2 --dead-root
run londiste3 $v conf/londiste_db1.ini status --dead=node2

run sleep 10
../zcheck.sh

