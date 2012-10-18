#! /bin/sh

. ../testlib.sh

title "Merge several shards into one database"

part_list="part1 part2"
qn_list='qn1 qn2'
full_list="full"
all_list="$part_list $qn_list $full_list"
kdb_list="`echo $all_list|sed 's/ /,/g'`"

for db in $all_list; do
  cleardb $db
done

clearlogs

msg "Create configs for pgqd and londiste processes"

# create ticker conf
cat > conf/pgqd.ini << EOF
[pgqd]
database_list = $kdb_list
logfile = log/pgqd.log
pidfile = pid/pgqd.pid
EOF

# partition replicas
for n in 1 2; do

# londiste on part node
cat > conf/londiste_part$n.ini << EOF
[londiste3]
job_name = londiste_part$n
db = dbname=part$n
queue_name = replika_part$n
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF

cat > conf/londiste_part${n}_qn$n.ini << EOF
[londiste3]
job_name = londiste_qn$n
db = dbname=qn$n
queue_name = replika_part$n
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF

cat > conf/londiste_qn${n}_full.ini << EOF
[londiste3]
job_name = londiste_qn${n}_full
db = dbname=full
queue_name = replika_part$n
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

msg "Create both cascades root (shard) -> branch (qnode) -> leaf (merge). Also installs pgq and londiste into db modules"

for n in 1 2; do
  run londiste3 $v conf/londiste_part${n}.ini create-root part${n}_root "dbname=part${n}"
  run londiste3 $v conf/londiste_qn${n}.ini \
    create-branch qn${n} "dbname=qn${n}" --provider="dbname=part${n}"
  run londiste3 $v conf/londiste_qn${n}_full.ini \
    create-leaf merge_qn${n}_full "dbname=full" --provider="dbname=qn${n}"
done

msg "Optimize pgq for testing to handle empty ticks faster"
for db in $all_list; do
  run_sql $db "update pgq.queue set queue_ticker_idle_period='3 secs'"
done

msg "Launch ticker"
run pgqd $v -d conf/pgqd.ini

msg "Launch workers"

for n in 1 2; do
  run londiste3 $v -d conf/londiste_part${n}.ini worker
  run londiste3 $v -d conf/londiste_qn${n}.ini worker
  run londiste3 $v -d conf/londiste_qn${n}_full.ini worker
done

msg "Create table in partition nodes and in target database"
for db in $part_list; do
  run_sql "$db" "create table mydata (id int4 primary key, data text)"
done
run_sql full "create table mydata (id int4 primary key, data text)"

msg "Register table in partition nodes"
for db in $part_list; do
  run londiste3 $v conf/londiste_$db.ini add-table mydata
done

msg "Wait for cascade sync (root->leaf). Leaf must know that we have this table in root"
for src in $qn_list; do
  run londiste3 $v conf/londiste_${src}_full.ini wait-root
done

msg "Add table into merge node"
run londiste3 -q conf/londiste_qn1_full.ini add-table  public.mydata --find-copy-node --merge-all

msg "Insert few rows"
for n in 1 2; do
  run_sql part$n "insert into mydata values ($n, 'part$n')"
  run_sql part$n "insert into mydata values (2 + $n, 'part$n')"
done

msg "Wait for replica to cach up"
for src in $qn_list; do
  run londiste3 $v conf/londiste_${src}_full.ini wait-sync
done

msg "Now check if data apprered"
run_sql full "select * from mydata order by id"
run_sql full "select table_name, local, merge_state, table_attrs, dest_table from londiste.get_table_list('replika_part1')"
run_sql full "select table_name, local, merge_state, table_attrs, dest_table from londiste.get_table_list('replika_part2')"

../zcheck.sh

msg "Test EXECUTE through cascade"

for n in 1 2; do
  run londiste3 $v conf/londiste_part$n.ini execute addcol-data2.sql
  # do one by one to avoid deadlock on merge side when several ddl's are received simultaneously
  run londiste3 $v conf/londiste_qn${n}_full.ini wait-root
done

msg "Insert more rows with more columns"
for n in 1 2; do
  run_sql part$n "insert into mydata values (4 + $n, 'part$n', 'x')"
done

msg "Wait for cascade sync (root->leaf)"
for src in $qn_list; do
  run londiste3 $v conf/londiste_${src}_full.ini wait-root
done

psql -d part1 -c 'select * from mydata order by 1;'
psql -d part2 -c 'select * from mydata order by 1;'
psql -d full -c 'select * from mydata order by 1;'

../zcheck.sh
