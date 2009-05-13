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

for db in $part_list $full_list; do
  cleardb $db
done
msg "clean logs"
rm -f log/*.log

msg "Create configs"

# create ticker conf
for db in $all_list; do
cat > conf/ticker_$db.ini << EOF
[pgqadm]
job_name = ticker_$db
db = dbname=$db
loop_delay = 0.5
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF
done

# partition replicas
for db in $part_list; do

# londiste on part node
cat > conf/londiste_$db.ini << EOF
[londiste]
job_name = londiste_$db
db = dbname=$db
queue_name = replika_$db
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF

# londiste on combined nodes
for dst in full1 full2; do
cat > conf/londiste_${db}_${dst}.ini << EOF
[londiste]
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
[londiste]
job_name = londiste_$db
db = dbname=$db
queue_name = replika
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF

done

set -e

msg "Install PgQ"

for db in $all_list; do
  run pgqadm $v conf/ticker_$db.ini install
done

msg "Create nodes for merged queue"

run londiste $v conf/londiste_full1.ini create-root fnode1 'dbname=full1'
run londiste $v conf/londiste_full2.ini create-branch fnode2 'dbname=full2' --provider='dbname=full1'
run londiste $v conf/londiste_full3.ini create-branch fnode3 'dbname=full3' --provider='dbname=full1'
run londiste $v conf/londiste_full4.ini create-leaf fnode4 'dbname=full4' --provider='dbname=full2'

msg "Create nodes for partition queues"

run londiste $v conf/londiste_part1.ini create-root p1root 'dbname=part1'
run londiste $v conf/londiste_part2.ini create-root p2root 'dbname=part2'
run londiste $v conf/londiste_part3.ini create-root p3root 'dbname=part3'
run londiste $v conf/londiste_part4.ini create-root p4root 'dbname=part4'

msg "Create merge nodes for partition queues"

for dst in full1 full2; do
  for src in $part_list; do
    run londiste $v conf/londiste_${src}_${dst}.ini \
                    create-leaf merge_${src}_${dst} "dbname=$dst" \
                    --provider="dbname=$src" --merge="replika"
  done
done


msg "Launch tickers"
for db in $all_list; do
  run pgqadm $v -d conf/ticker_$db.ini ticker
done

msg "Launch londiste replay"
for db in $all_list; do
  run londiste $v -d conf/londiste_$db.ini replay
done

msg "Launch merge londiste"
for dst in full1 full2; do
  for src in $part_list; do
    run londiste $v -d conf/londiste_${src}_${dst}.ini replay
  done
done

msg "Create table in partition nodes"
for db in $part_list; do
  run psql $db -c "create table mydata (id int4 primary key, data text)"
done

msg "Register table in partition nodes"
for db in $part_list; do
  run londiste $v conf/londiste_$db.ini add-table mydata
done


msg "Insert few rows"
for n in 1 2 3 4; do
  run psql -d part$n -c "insert into mydata values ($n, 'part$n')"
done

msg "Create table and register it in merge nodes"
for db in full1; do
  run psql $db -c "create table mydata (id int4 primary key, data text)"
  run londiste $v conf/londiste_$db.ini add-table mydata
  for src in $part_list; do
    run londiste $v conf/londiste_${src}_${db}.ini add-table mydata
  done
done

msg "Sleep a bit"
run sleep 10

msg "Insert few rows"
for n in 1 2 3 4; do
  run psql -d part$n -c "insert into mydata values (4 + $n, 'part$n')"
done

run sleep 10

msg "Now check if data apprered"
for db in full1; do
run psql -d $db -c "select * from mydata order by id"
run psql -d $db -c "select * from londiste.table_info order by queue_name"
done
run psql -d full1 -c "select * from londiste.get_table_list('replika_part1')"
run psql -d full1 -c "select * from londiste.get_table_list('replika_part2')"

../zcheck.sh

