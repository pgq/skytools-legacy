#! /bin/sh

. ./env.sh

script=londiste.py

set -e

$script  conf/replic.ini provider install

psql -c "update pgq.queue set queue_ticker_idle_period = '3', queue_ticker_max_lag = '2'" provider

pgqadm.py -d conf/ticker.ini ticker

$script  conf/replic.ini subscriber install

$script  conf/replic.ini subscriber register
$script  conf/replic.ini subscriber unregister

$script -v -d conf/replic.ini replay
$script -v -d conf/fwrite.ini replay

sleep 2

$script  conf/replic.ini provider add data1
$script  conf/replic.ini subscriber add data1

sleep 2

$script  conf/replic.ini provider add data2
$script  conf/replic.ini subscriber add data2

sleep 2

$script  conf/replic.ini provider tables
$script  conf/replic.ini provider remove data2

sleep 2

$script  conf/replic.ini provider add data2

$script  conf/replic.ini provider add-seq data1_id_seq
$script  conf/replic.ini provider add-seq test_seq
$script  conf/replic.ini subscriber add-seq data1_id_seq
$script  conf/replic.ini subscriber add-seq test_seq

sleep 2

$script  conf/replic.ini subscriber tables
$script  conf/replic.ini subscriber missing

$script  conf/replic.ini subscriber remove data2
sleep 2
$script  conf/replic.ini subscriber add data2
sleep 2

./testing.py conf/tester.ini

