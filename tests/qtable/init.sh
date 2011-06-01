#! /bin/sh

. ../env.sh

db_list="hsrc hdst"

for db in $db_list; do
  echo dropdb $db
  dropdb $db
done


for db in $db_list; do
  echo createdb $db
  createdb $db
done
