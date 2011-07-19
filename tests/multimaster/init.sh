#! /bin/sh

. ../env.sh

lst="src1 src2 dst"

../zstop.sh

for db in $lst; do
  echo dropdb $db
  dropdb $db
done
for db in $lst; do
  echo createdb $db
  createdb $db
done
