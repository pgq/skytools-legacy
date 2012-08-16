#! /bin/sh

. ../testlib.sh

title "Merge"

part_list="part1 part2 part3 part4"
full_list="full1 full2 full3 full4"
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
msg "clean logs"
rm -f log/*.log

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

# londiste on combined nodes
for dst in full1 full2; do
cat > conf/londiste_${db}_${dst}.ini << EOF
[londiste3]
job_name = londiste_${db}_${dst}
db = dbname=$dst
queue_name = replika_$db
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF

done
done

# full replicas
for db in $full_list; do

cat > conf/londiste_$db.ini << EOF
[londiste3]
job_name = londiste_$db
db = dbname=$db
queue_name = replika
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF

done

set -e

msg "Create nodes for merged queue"

run londiste3 $v conf/londiste_full1.ini create-root fnode1 'dbname=full1'
run londiste3 $v conf/londiste_full2.ini create-branch fnode2 'dbname=full2' --provider='dbname=full1'
run londiste3 $v conf/londiste_full3.ini create-branch fnode3 'dbname=full3' --provider='dbname=full1'
run londiste3 $v conf/londiste_full4.ini create-leaf fnode4 'dbname=full4' --provider='dbname=full2'

msg "Create nodes for partition queues"

run londiste3 $v conf/londiste_part1.ini create-root p1root 'dbname=part1'
run londiste3 $v conf/londiste_part2.ini create-root p2root 'dbname=part2'
run londiste3 $v conf/londiste_part3.ini create-root p3root 'dbname=part3'
run londiste3 $v conf/londiste_part4.ini create-root p4root 'dbname=part4'

msg "Create merge nodes for partition queues"

for dst in full1 full2; do
  for src in $part_list; do
    run londiste3 $v conf/londiste_${src}_${dst}.ini \
                    create-leaf merge_${src}_${dst} "dbname=$dst" \
                    --provider="dbname=$src" --merge="replika"
  done
done


msg "Tune PgQ"

for db in part1 part2 part3 part4 full1; do
  run_sql $db "update pgq.queue set queue_ticker_idle_period='3 secs'"
done

msg "Launch ticker"
run pgqd $v -d conf/pgqd.ini

msg "Launch londiste worker"
for db in $all_list; do
  run londiste3 $v -d conf/londiste_$db.ini worker
done

msg "Launch merge londiste"
for dst in full1 full2; do
  for src in $part_list; do
    run londiste3 $v -d conf/londiste_${src}_${dst}.ini worker
  done
done

msg "Create table in partition nodes"
for db in $part_list; do
  run_sql "$db" "create table mydata (id int4 primary key, data text)"
done

msg "Register table in partition nodes"
for db in $part_list; do
  run londiste3 $v conf/londiste_$db.ini add-table mydata
done

msg "Wait until add-table events are distributed to leafs"
parts=$(echo "$part_list"|wc -w)
for db in full1 full2; do
cnt=0
while test $cnt -ne $parts; do
 sleep 5
 cnt=`psql ${db} -Atc "select count(*)-1 from londiste.table_info"`
 echo "$db cnt=$cnt parts=$parts"
done
done

msg "Insert few rows"
for n in 1 2 3 4; do
  run_sql part$n "insert into mydata values ($n, 'part$n')"
done

msg "Create table and register it in merge nodes"
run_sql full1 "create table mydata (id int4 primary key, data text)"
run londiste3 $v conf/londiste_full1.ini add-table mydata
run londiste3 $v conf/londiste_part1_full1.ini add-table mydata --merge-all

msg "Wait until table is in sync on combined-root"
cnt=0
while test $cnt -ne 5; do
  sleep 5
  cnt=`psql -A -t -d full1 -c "select count(*) from londiste.table_info where merge_state = 'ok'"`
  echo "cnt=$cnt"
done

msg "Create table and register it in full nodes"
for db in full2; do
  run londiste3 $v conf/londiste_$db.ini add-table mydata --create
  run londiste3 $v conf/londiste_part1_${db}.ini add-table mydata --merge-all
done
for db in full3 full4; do
  run londiste3 $v conf/londiste_$db.ini add-table mydata --create
done

msg "Sleep a bit"
run sleep 10

msg "Insert few rows"
for n in 1 2 3 4; do
  run_sql part$n "insert into mydata values (4 + $n, 'part$n')"
done

run sleep 10

msg "Now check if data apprered"
for db in full1; do
run_sql $db "select * from mydata order by id"
run_sql $db "select * from londiste.table_info order by queue_name"
done
run_sql full1 "select * from londiste.get_table_list('replika_part1')"
run_sql full2 "select * from londiste.get_table_list('replika_part2')"

../zcheck.sh

msg "Test EXECUTE through cascade"

for db in part1 part2 part3 part4; do
  run londiste3 $v conf/londiste_$db.ini execute addcol-data2.sql
done
msg "Sleep a bit"
run sleep 10

psql -d part1 -c '\d mydata'
psql -d full1 -c '\d mydata'
psql -d part1 -c '\d mydata'

../zcheck.sh

