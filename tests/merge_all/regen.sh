#! /bin/sh

. ../testlib.sh

title "Merge several shards into one database"

part_list="part1 part2"
full_list="full"
merge_list=""
for dst in $full_list; do
  for src in $part_list; do
    merge_list="$merge_list ${src}_${dst}"
  done
done
all_list="$part_list $full_list"
kdb_list="`echo $all_list|sed 's/ /,/g'`"

for db in $part_list $full_list; do
  cleardb $db
done

clearlogs

msg "Create configs"

# create ticker conf
cat > conf/pgqd.ini << EOF
[pgqd]
database_list = $kdb_list
logfile = log/pgqd.log
pidfile = pid/pgqd.pid
EOF

# partition replicas
for db in $part_list; do

# londiste on part node
cat > conf/londiste_$db.ini << EOF
[londiste3]
job_name = londiste_$db
db = dbname=$db
queue_name = replika_$db
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF

cat > conf/londiste_${db}_full.ini << EOF
[londiste3]
job_name = londiste_${db}_full
db = dbname=full
queue_name = replika_$db
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF

done

for f in conf/*.ini; do
  echo "$f"
  echo "------------------"
  cat "$f"
  echo "------------------"
  echo
done

set -e

msg "Create nodes for partition queues"

for src in $part_list; do
  run londiste3 $v conf/londiste_${src}.ini create-root ${src}_root "dbname=${src}"
done

msg "Create merge nodes for partition queues"

for src in $part_list; do
  run londiste3 $v conf/londiste_${src}_full.ini \
    create-leaf merge_${src}_full "dbname=full" --provider="dbname=$src" 
done

msg "Optimize pgq for testing to handle empty ticks faster"
for db in $all_list; do
  run_sql $db "update pgq.queue set queue_ticker_idle_period='3 secs'"
done

msg "Launch ticker"
run pgqd $v -d conf/pgqd.ini

msg "Launch londiste worker"
for db in $part_list; do
  run londiste3 $v -d conf/londiste_$db.ini worker
done

msg "Launch merge londiste"
for src in $part_list; do
  run londiste3 $v -d conf/londiste_${src}_full.ini worker
done

msg "Create table in partition nodes"
for db in $part_list; do
  run_sql "$db" "create table mydata (id int4 primary key, data text)"
done

msg "Register table in partition nodes"
for db in $part_list; do
  run londiste3 $v conf/londiste_$db.ini add-table mydata
done

msg "Wait for cascade sync (root->leaf)"
for src in $part_list; do
  run londiste3 $v conf/londiste_${src}_full.ini wait-root
done

msg "Insert few rows"
for n in 1 2; do
  run_sql part$n "insert into mydata values ($n, 'part$n')"
done

msg "Create table and register it in merge nodes"
run_sql full "create table mydata (id int4 primary key, data text)"
run londiste3 $v conf/londiste_part1_full.ini add-table mydata --merge-all

msg "Wait for replica to cach up"
for src in $part_list; do
  run londiste3 $v conf/londiste_${src}_full.ini wait-sync
done

msg "Insert few rows"
for n in 1 2; do
  run_sql part$n "insert into mydata values (2 + $n, 'part$n')"
done

msg "Now check if data apprered"
run_sql full "select * from mydata order by id"
run_sql full "select table_name, local, merge_state, table_attrs, dest_table from londiste.get_table_list('replika_part1')"
run_sql full "select table_name, local, merge_state, table_attrs, dest_table from londiste.get_table_list('replika_part2')"

../zcheck.sh

msg "Test EXECUTE through cascade"

for db in $part_list; do
  run londiste3 $v conf/londiste_$db.ini execute addcol-data2.sql
  # do one by one to avoid deadlock on merge side when several ddl's are received simultaneously
  run londiste3 $v conf/londiste_${src}_full.ini wait-root
done

msg "Insert more rows with more columns"
for n in 1 2; do
  run_sql part$n "insert into mydata values (4 + $n, 'part$n', 'x')"
done

msg "Wait for cascade sync (root->leaf)"
for src in $part_list; do
  run londiste3 $v conf/londiste_${src}_full.ini wait-root
done

psql -d part1 -c 'select * from mydata order by 1;'
psql -d part2 -c 'select * from mydata order by 1;'
psql -d full -c 'select * from mydata order by 1;'

../zcheck.sh
