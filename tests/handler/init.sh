#! /bin/sh

. ../env.sh

for db in hsrc hdst; do
  echo dropdb $db
  dropdb $db
done

echo createdb hsrc
createdb hsrc --encoding=sql_ascii --template=template0

echo createdb hdst
createdb hdst --encoding=utf-8 --template=template0
