#! /bin/sh

. ../env.sh

lst="db1 db2 db3 db4"

for db in $lst; do
  echo dropdb $db
  dropdb $db
done
for db in $lst; do
  echo createdb $db
  createdb $db
done
