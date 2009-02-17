#! /bin/sh

. ../env.sh

mkdir -p log pid conf

./zstop.sh

v=
v=-q
v=-v

(cd ../..; make -s python-install )

echo ""

cleardb() {
  echo "Clearing database $1"
  psql -q -d $1 -c '
      set client_min_messages=warning;
      drop schema if exists londiste cascade;
      drop schema if exists pgq_node cascade;
      drop schema if exists pgq cascade;
      drop schema if exists data cascade;
  '
}

run() {
  echo "$ $*"
  "$@"
}

db_list="loadersrc loaderdst"

for db in $db_list; do
  cleardb $db
done

echo "clean logs"
rm -f log/*.log

set -e

run setadm $v conf/setadm_loaderq.ini create-root ldr-src 'dbname=loadersrc' --worker=loader_src
run setadm $v conf/setadm_loaderq.ini create-leaf ldr-dst 'dbname=loaderdst' --worker=loader_dst --provider="dbname=loadersrc"

run pgqadm $v conf/ticker_loadersrc.ini -d ticker

run queue_loader $v -d conf/loader_src.ini
run queue_loader $v -d conf/loader_dst.ini

run psql -d loadersrc -f tables.sql
run psql -d loadersrc -f triggers.sql

run psql -d loaderdst -f tables.sql

run psql -d loadersrc -f send.data.sql
run psql -d loadersrc -f send.data.sql
run psql -d loadersrc -f send.data.sql

run sleep 2

run setadm $v conf/setadm_loaderq.ini status

./zcheck.sh

