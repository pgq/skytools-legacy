#! /bin/sh

. ../env.sh

mkdir -p log pid conf

../zstop.sh

v=
v=-v
v=-q

set -o pipefail

cleardb() {
  echo "Clearing database $1"
  psql -q -d $1 -c "
      set client_min_messages=warning;
      drop schema if exists londiste cascade;
      drop schema if exists pgq_node cascade;
      drop schema if exists pgq cascade;
  "
  psql -q -t -d db1 -c "
    select 'drop table ' || relname || ';' from pg_class c, pg_namespace n
    where relnamespace = n.oid and n.nspname = 'public' and c.relkind='r';
  " | psql -q -d $1
  psql -q -t -d db1 -c "
    select 'drop sequence ' || relname || ';' from pg_class c, pg_namespace n
    where relnamespace = n.oid and n.nspname = 'public' and c.relkind='S';
  " | psql -q -d $1
}

clearlogs() {
  echo "clean logs"
  rm -f log/*.log log/*.log.[0-9]
}

title() {
  echo ""
  echo "=" "$@" "="
  echo ""
}

run() {
  echo "    $ $*"
  "$@" 2>&1 | sed 's/^/    /'
}

msg() {
  echo ""
  echo "$@"
  echo ""
}

