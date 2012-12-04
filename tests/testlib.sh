#! /bin/sh

. ../env.sh

mkdir -p log pid conf

../zstop.sh

v=
v=-v
v=-q

#set -o pipefail

cleardb() {
  echo "// Clearing database $1"
  psql -q -d $1 -c "
      set client_min_messages=warning;
      drop schema if exists londiste cascade;
      drop schema if exists pgq_node cascade;
      drop schema if exists pgq cascade;
  "
  psql -q -t -d $1 -c "
    select 'drop table ' || relname || ' cascade;' from pg_class c, pg_namespace n
    where relnamespace = n.oid and n.nspname = 'public' and c.relkind='r';
  " | psql -q -d $1
  psql -q -t -d $1 -c "
    select 'drop sequence ' || relname || ';' from pg_class c, pg_namespace n
    where relnamespace = n.oid and n.nspname = 'public' and c.relkind='S';
  " | psql -q -d $1
}

clearlogs() {
  code_off
  echo "// clean logs"
  rm -f log/*.log log/*.log.[0-9]
}

code=0

code_on() {
  test $code = 1 || echo "----------"
  code=1
}

code_off() {
  test $code = 0 || echo "----------"
  code=0
}

title() {
  code_off
  echo ""
  echo "=" "$@" "="
  echo ""
}

title2() {
  code_off
  echo ""
  echo "==" "$@" "=="
  echo ""
}

title3() {
  code_off
  echo ""
  echo "==" "$@" "=="
  echo ""
}

run() {
  code_on
  echo "$ $*"
  "$@" 2>&1
}

run_sql() {
  code_on
  echo "$ psql -d \"$1\" -c \"$2\""
  psql -d "$1" -c "$2" 2>&1
}

run_qadmin() {
  code_on
  echo "$ qadmin -d \"$1\" -c \"$2\""
  qadmin -d "$1" -c "$2" 2>&1
}

msg() {
  code_off
  echo ""
  echo "$@"
  echo ""
}

cat_file() {
  code_off
  mkdir -p `dirname $1`
  echo ".File: $1"
  case "$1" in
    *Makefile) echo "[source,makefile]" ;;
    #*.[ch]) echo "[source,c]" ;;
    #*.ac) echo "[source,autoconf]" ;;
    #*.sh) echo "[source,shell]" ;;
    #*.sql) echo "[source,sql]" ;;
    *.*) printf "[source,%s]\n" `echo $1 | sed 's/.*\.//'` ;;
  esac
  echo "-----------------------------------"
  sed 's/^      //' > $1
  cat $1
  echo "-----------------------------------"
}

