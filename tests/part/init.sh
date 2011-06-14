#! /bin/sh

. ../env.sh

lst="full1 part1 part2 part3 part4"

../zstop.sh

for db in $lst; do
  echo dropdb $db
  dropdb $db
done
for db in $lst; do
  echo createdb $db
  createdb $db
done
