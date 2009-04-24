#! /bin/sh

. ../env.sh

mkdir -p log pid conf

./zstop.sh

v=
v=-q
v=-v

cleardb() {
  echo "Clearing database $1"
  psql -q -d $1 -c '
      set client_min_messages=warning;
      drop schema if exists londiste cascade;
      drop schema if exists pgq_ext cascade;
      drop schema if exists pgq_node cascade;
      drop schema if exists pgq cascade;
      drop table if exists mytable;
      drop table if exists footable;
      drop sequence if exists footable_id_seq;
  '
}

run() {
  echo "$ $*"
  "$@"
}

msg() {
  echo "##"
  echo "## $*"
  echo "##"
}

db_list="db1 db2 db3 db4 db5"

echo " * create configs * "

# create ticker conf
for db in $db_list; do
cat > conf/ticker_$db.ini << EOF
[pgqadm]
job_name = ticker_$db
db = dbname=$db
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF
done

# londiste configs
for db in $db_list; do
cat > conf/londiste_$db.ini << EOF
[londiste]
job_name = londiste_$db
db = dbname=$db
queue_name = replika
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF
done

for db in $db_list; do
  cleardb $db
done

echo "clean logs"
rm -f log/*.log

set -e

msg "Basic config"
run cat conf/ticker_db1.ini
run cat conf/londiste_db1.ini

msg "Install PgQ and run ticker on each db"
for db in $db_list; do
  run pgqadm $v conf/ticker_$db.ini install
done
for db in $db_list; do
  run pgqadm $v -d conf/ticker_$db.ini ticker
done

msg "Install Londiste and initialize nodes"
run londiste $v conf/londiste_db1.ini create-root node1 'dbname=db1'
run londiste $v conf/londiste_db2.ini create-branch node2 'dbname=db2' --provider='dbname=db1'
run londiste $v conf/londiste_db3.ini create-branch node3 'dbname=db3' --provider='dbname=db1'
run londiste $v conf/londiste_db4.ini create-leaf node4 'dbname=db4' --provider='dbname=db2'
run londiste $v conf/londiste_db5.ini create-branch node5 'dbname=db5' --provider='dbname=db3'

msg "See topology"
run londiste $v conf/londiste_db4.ini status

msg "Run Londiste daemon for each node"
for db in $db_list; do
  run londiste $v -d conf/londiste_$db.ini replay
done

msg "Create table on root node and fill couple of rows"
run psql -d db1 -c "create table mytable (id int4 primary key, data text)"
for n in 1 2 3 4; do
  run psql -d db1 -c "insert into mytable values ($n, 'row$n')"
done

msg "Register table on root node"
run londiste $v conf/londiste_db1.ini add-table mytable

msg "Register table on other node with creation"
for db in db2 db3 db4 db5; do
  run londiste $v conf/londiste_$db.ini add-table mytable --create
done
run sleep 20

msg "Add column on root"
run cat ddl.sql
run londiste $v conf/londiste_db1.ini execute ddl.sql

msg "Insert data into new column"
for n in 5 6 7 8; do
  run psql -d db1 -c "insert into mytable values ($n, 'row$n', 'data2')"
done
msg "Wait a bit"
run sleep 20

run psql -d db3 -c '\d mytable'
run psql -d db3 -c 'select * from mytable'

run sleep 10
./zcheck.sh
msg "Change topology"
run londiste $v conf/londiste_db1.ini status
run londiste $v conf/londiste_db3.ini change-provider --provider=node1
run londiste $v conf/londiste_db1.ini status
run londiste $v conf/londiste_db1.ini switchover --target=node2
run londiste $v conf/londiste_db1.ini status

run sleep 10
./zcheck.sh

msg "Change topology"
ps aux | grep "postres[:].* db2 " | awk '{print $2}' | xargs kill
run psql -d db1 -c 'alter database db2 rename to db2x'
run londiste $v conf/londiste_db4.ini takeover db2 --dead
run londiste $v conf/londiste_db1.ini status

run sleep 10
./zcheck.sh

