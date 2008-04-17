#! /bin/sh

set -e

msg () {
  echo " *" "$@"
}

run () {
  echo "\$ $*"
  "$@"
}


# usage: makenode <set_name> <base_name> <type> <provider_base_name>
set_name="$1"
base_name="$2"
node_type="$3"
provider_base_name="$4"

db="db_$base_name"
connstr="dbname=$db host=127.0.0.1"
node_name="n_$base_name"
ticker_conf="sys/ticker_$base_name.ini"
londiste_conf="sys/worker_$base_name.ini"

for pf in sys/pid.ticker_$base_name \
  sys/pid.worker_$base_name \
  sys/pid.worker_$base_name.*
do
  test -f $pf || continue
  msg "Killing $pf"
  kill `cat $pf`
  sleep 1
done

msg "Creating $ticker_conf"
cat > "$ticker_conf" <<EOF
[pgqadm]
job_name = ticker_$base_name
db = $connstr
maint_delay_min = 1
loop_delay = 0.5
logfile = sys/log.%(job_name)s
pidfile = sys/pid.%(job_name)s
use_skylog = 0
connection_lifetime = 10
queue_refresh_period = 10
EOF

msg "Creating $londiste_conf"
cat > "$londiste_conf" <<EOF
[londiste]
job_name = worker_$base_name
set_name = $set_name
node_db = $connstr
pidfile = sys/pid.%(job_name)s
logfile = sys/log.%(job_name)s
loop_delay = 1
connection_lifetime = 10
parallel_copies = 4
EOF


msg "Dropping & Creating $db"
dropdb $db 2>&1 | grep -v 'not exist' || true
createdb $db

msg "Installing pgq"
pgqadm.py $ticker_conf install
msg "Launching ticker"
pgqadm.py $ticker_conf ticker -d

msg "Initializing node"
run londiste.py $londiste_conf "init-$node_type" "$node_name" "$connstr" -v \
  --provider="dbname=db_$provider_base_name host=127.0.0.1"

msg "Launching Londiste"
londiste.py $londiste_conf worker -d -v

for n in `seq 1 16`; do
  tbl="manytable$n"
  msg "Creating $tbl on n_$base_name"
  { psql -q $db 2>&1 | grep -v NOTICE || true ;  }<<EOF
create table $tbl (
  id serial primary key,
  txt text not null
);
insert into $tbl (txt)
select '$tbl-$base_name'
  from generate_series(1, 5 + $n);
EOF

  msg "Adding $tbl to n_$base_name"
  londiste.py $londiste_conf add $tbl

done


