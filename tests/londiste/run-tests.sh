#! /bin/sh

. ../env.sh

./gendb.sh

script=londiste.py

mwait () {
  echo "Waiting $1 seconds..."
  sleep $1
  shift
  echo "$@"
}

set -e

$script  conf/replic.ini provider install

psql -c "update pgq.queue set queue_ticker_idle_period = '15', queue_ticker_max_lag = '5'" provider

pgqadm.py -d conf/ticker.ini ticker

$script  conf/replic.ini subscriber install
$script  conf/replic.ini subscriber -v seqs
$script  conf/replic.ini subscriber -v fkeys

$script  conf/replic.ini subscriber register
$script  conf/replic.ini subscriber unregister

$script -v -d conf/replic.ini replay
#$script -v -d conf/fwrite.ini replay

sleep 2

$script  conf/replic.ini provider add data1 data2 inh_mid Table
$script  conf/replic.ini subscriber add data1
$script  conf/replic.ini subscriber add data2
$script  conf/replic.ini subscriber add inh_mid
$script  conf/replic.ini provider remove data2
$script  conf/replic.ini provider add Table
$script  conf/replic.ini provider remove Table
$script  conf/replic.ini provider add Table
$script  conf/replic.ini subscriber add Table
$script  conf/replic.ini subscriber remove Table
$script  conf/replic.ini subscriber add Table

sleep 2

$script  conf/replic.ini provider add data2

$script  conf/replic.ini provider add-seq data1_id_seq
$script  conf/replic.ini provider add-seq test_seq
$script  conf/replic.ini subscriber add-seq data1_id_seq
$script  conf/replic.ini subscriber add-seq test_seq

$script  conf/replic.ini provider seqs
$script  conf/replic.ini subscriber seqs

$script  conf/replic.ini subscriber fkeys

sleep 2

$script  conf/replic.ini subscriber tables
$script  conf/replic.ini subscriber missing

$script  conf/replic.ini subscriber remove data2
sleep 2
$script  conf/replic.ini subscriber add data2
sleep 2

echo "starting data gen script"

./testing.py -d conf/tester.ini

mwait 30 "vacuuming"
psql -c "vacuum analyze" provider
mwait 30 "vacuuming"
psql -c "vacuum analyze" provider

$script  conf/replic.ini provider add expect_test skip_test
$script  conf/replic.ini subscriber add --expect-sync expect_test
$script  conf/replic.ini subscriber add --skip-truncate skip_test

mwait 90 "stopping tester skript"
./testing.py -s conf/tester.ini

#exit 0

mwait 20 "comparing tables"

psql subscriber -c "select * from expect_test"
psql subscriber -c "select * from skip_test"

# those should give errors on expect_test and skip_test tables, thats expected
$script conf/replic.ini compare --force -v
$script conf/replic.ini repair --force -v

mwait 10 "stopping replica"
$script -v -s conf/replic.ini
#$script -v -s conf/fwrite.ini

mwait 10 "stopping ticker"
pgqadm.py -s -v conf/ticker.ini 

test -f sys/pid.replic.copy && {
  echo "copy failed, still running"
  kill `cat sys/pid.replic.copy`
  sleep 3
}

echo "done?"
ps aux|grep python

grep -E 'WARN|ERR' sys/*

