#! /bin/sh

. ../env.sh

lst="part1 part2 qn1 qn2 full"

../zstop.sh

for db in $lst; do
  echo dropdb $db
  psql postgres -q -c "drop database if exists \"$db\";"
done
for db in $lst; do
  echo createdb $db
  createdb $db
done
