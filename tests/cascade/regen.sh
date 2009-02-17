#! /bin/sh

. ../env.sh

mkdir -p log pid

./zstop.sh

v=
v=-v
v=-q

cleardb() {
  psql -q -d $db -c '
      set client_min_messages=warning;
      drop schema if exists londiste cascade;
      drop schema if exists pgq_node cascade;
      drop schema if exists pgq cascade;
      drop table if exists mydata;
      drop table if exists footable;
      drop sequence if exists footable_id_seq;
  '
}

run() {
  echo "$ $*"
  "$@"
}

for db in db1 db2 db3; do
  pgqadm conf/ticker_$db.ini -k
  cleardb $db
done

run ./plainconsumer.py -s conf/nop_consumer.ini

rm -f log/*.log

set -e

run cat conf/ticker_db1.ini

#echo " # pgqadm install # "
run pgqadm $v conf/ticker_db1.ini install
run pgqadm $v conf/ticker_db2.ini install
run pgqadm $v conf/ticker_db3.ini install

#echo " # pgqadm ticker # "
run pgqadm $v -d conf/ticker_db1.ini ticker
run pgqadm $v -d conf/ticker_db2.ini ticker
run pgqadm $v -d conf/ticker_db3.ini ticker

if false; then 

#echo " # setadm create-node # "
run setadm $v --worker=node1_worker conf/setadm.ini create-root node1 'dbname=db1'
run setadm $v --worker=node2_worker conf/setadm.ini create-branch node2 'dbname=db2' --provider='dbname=db1'
run setadm $v --worker=node3_worker conf/setadm.ini create-branch node3 'dbname=db3' --provider='dbname=db2'

#echo " # setadm status # "
run setadm $v conf/setadm.ini status

#echo " # plainconsumer # "
run ./plainconsumer.py $v conf/nop_consumer.ini --register --provider='dbname=db1'
run ./plainconsumer.py $v -d conf/nop_consumer.ini

#echo " # plainworker # "
run ./plainworker.py $v -d conf/worker_db1.ini
run ./plainworker.py $v -d conf/worker_db2.ini
run ./plainworker.py $v -d conf/worker_db3.ini


#echo " # insert_event() # "
run psql db1 -c "select pgq.insert_event('fooqueue', 'tmp', 'data')"

fi

grep -E 'ERR|WARN|CRIT' log/*.log || true

#exit 0

# -- londiste

for db in db1 db2 db3; do
  run psql $db -c "create table mydata (id serial primary key, data text)"
done

run cat conf/londiste_db1.ini

run londiste $v conf/londiste_db1.ini create-root rnode1 'dbname=db1'
run londiste $v conf/londiste_db2.ini create-branch rnode2 'dbname=db2' --provider='dbname=db1'
run londiste $v conf/londiste_db3.ini create-branch rnode3 'dbname=db3' --provider='dbname=db2'

run londiste $v -d conf/londiste_db1.ini replay
run londiste $v -d conf/londiste_db2.ini replay
run londiste $v -d conf/londiste_db3.ini replay

run londiste $v conf/londiste_db1.ini add-table mydata
run londiste $v conf/londiste_db2.ini add-table mydata
run londiste $v conf/londiste_db3.ini add-table mydata

run cat addcol.sql
run londiste $v conf/londiste_db1.ini execute addcol.sql

run sleep 10
run psql db3 -c '\d mydata'

run londiste $v conf/londiste_db1.ini status
run londiste $v conf/londiste_db3.ini change-provider --provider=rnode1
run londiste $v conf/londiste_db1.ini status
run londiste $v conf/londiste_db1.ini switchover --target=rnode2
run londiste $v conf/londiste_db1.ini status

run cat footable.sql

run psql db2 -f footable.sql
run londiste $v conf/londiste_db2.ini add-seq footable_id_seq
run londiste $v conf/londiste_db2.ini add-table footable
run londiste $v conf/londiste_db1.ini add-seq footable_id_seq --create
run londiste $v conf/londiste_db1.ini add-table footable --create
run londiste $v conf/londiste_db3.ini add-seq footable_id_seq --create
run londiste $v conf/londiste_db3.ini add-table footable --create

#run psql db2 -f footable.sql
#run londiste $v conf/londiste_db2.ini add-seq footable_id_seq
#run londiste $v conf/londiste_db2.ini add-table footable
#run londiste $v conf/londiste_db2.ini add-seq footable_id_seq --create
#run londiste $v conf/londiste_db1.ini add-table footable --create
#run londiste $v conf/londiste_db2.ini add-seq footable_id_seq --create
#run londiste $v conf/londiste_db3.ini add-table footable --create

run psql db3 -c '\d footable'

sleep 3

grep -E 'ERR|WARN|CRIT' log/*.log || echo "All OK"

