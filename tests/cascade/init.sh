#! /bin/sh

. ../env.sh

dropdb db1
dropdb db2
dropdb db3

createdb db1
createdb db2
createdb db3

pgqadm.py conf/ticker_db1.ini install
pgqadm.py conf/ticker_db2.ini install
pgqadm.py conf/ticker_db3.ini install

lst="part1 part2 part3 part4 full1 full2 full3 full4"

for db in $lst; do
  echo dropdb $db
  dropdb $db
done
for db in $lst; do
  echo createdb $db
  createdb $db
done
