#! /bin/sh

. ../env.sh

lst="loadersrc loaderdst"

for db in $lst; do
  echo dropdb $db
  dropdb $db
done
for db in $lst; do
  echo createdb $db
  createdb $db
done
