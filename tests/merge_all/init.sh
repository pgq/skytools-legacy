#! /bin/sh

. ../env.sh

lst="part1 part2 full"

../zstop.sh

for db in $lst; do
  echo dropdb $db
  dropdb $db
done
for db in $lst; do
  echo createdb $db
  createdb $db
done
