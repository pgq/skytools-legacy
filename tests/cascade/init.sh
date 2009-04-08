#! /bin/sh

. ../env.sh

mkdir -p log pid

dropdb db1
dropdb db2
dropdb db3

createdb db1
createdb db2
createdb db3

pgqadm conf/ticker_db1.ini install
pgqadm conf/ticker_db2.ini install
pgqadm conf/ticker_db3.ini install

