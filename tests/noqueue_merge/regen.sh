#! /bin/sh

. ../testlib.sh

v=-v

title "NoQueue Merge"

part_list="part1 part2 part3 part4"
full_list="full1 full2"

pnum=0
for p in $part_list; do
  pnum=$(($pnum + 1))
done

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

queue=l3_${db}_q
part_job=${queue}_${db}
# londiste on part node
cat > conf/$part_job.ini << EOF
[londiste3]
job_name = ${part_job}
db = dbname=$db
queue_name = ${queue}
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF

# londiste on combined nodes
for dst in $full_list; do
full_job=${queue}_$dst
cat > conf/${full_job}.ini << EOF
[londiste3]
job_name = ${full_job}
db = dbname=$dst
queue_name = $queue
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF

done
done

set -e

msg "Create nodes for partition queues"

# partition replicas
for db in $part_list; do
job=l3_${db}_q_${db}
run londiste3 $v conf/${job}.ini create-root ${db}_root "dbname=${db}"
done

msg "Create merge nodes for partition queues"

for dst in $full_list; do
  for src in $part_list; do
    job=l3_${src}_q_${dst}
    run londiste3 $v conf/${job}.ini \
                    create-leaf merge_${src}_${dst} "dbname=$dst" \
                    --provider="dbname=$src"
  done
done

msg "Tune PgQ"

for db in $all_list; do
  run_sql $db "update pgq.queue set queue_ticker_idle_period='3 secs'"
done

msg "Launch ticker"
run pgqd $v -d conf/pgqd.ini

msg "Launch londiste worker"

for db in $part_list; do
    queue=l3_${db}_q
    part_job=${queue}_${db}
    # londiste on part node
    run londiste3 $v -d conf/${part_job}.ini worker

    # londiste on combined nodes
    for dst in $full_list; do
        full_job=${queue}_$dst
        run londiste3 $v -d conf/${full_job}.ini worker
    done
done

msg "Create table in partition nodes"
for db in $part_list; do
  run_sql "$db" "create table mydata (id int4 primary key, data text)"
done

msg "Register table in partition nodes"
for db in $part_list; do
    job=l3_${db}_q_${db}
    run londiste3 $v conf/${job}.ini add-table mydata
done

msg "Wait until register reaches full1"
cnt=0
while test $cnt -ne $pnum; do
  sleep 5
  cnt=`psql -A -t -d full1 -c "select count(*) from londiste.table_info where merge_state is null"`
  echo "  cnt_tbl=$cnt"
done


msg "Insert few rows"
n=0
for p in $part_list; do
  n=$(($n + 1))
  run_sql $p "insert into mydata values ($n, '$p')"
done

msg "Create table and register it in full nodes"
for db in $full_list; do
    job=l3_part1_q_${db}
    run_sql $db "select * from londiste.table_info order by queue_name"
    run londiste3 $v conf/$job.ini add-table mydata --create --merge-all
    run_sql $db "select * from londiste.table_info order by queue_name"
    #for src in $part_list; do
    #    run londiste3 $v conf/l3_${src}_q_${db}.ini add-table mydata
    #done
done

msg "Wait until copy finishes on full1"
cnt=0
while test $cnt -ne $pnum; do
  sleep 5
  cnt=`psql -A -t -d full1 -c "select count(*) from londiste.table_info where merge_state = 'ok'"`
  echo "  cnt_ok=$cnt"
done


msg "Insert few rows"
for n in 1 2 3 4; do
  run_sql part$n "insert into mydata values (4 + $n, 'part$n')"
done

run sleep 10

msg "Now check if data apprered"
for db in $full_list; do
    run_sql $db "select * from mydata order by id"
    run_sql $db "select * from londiste.table_info order by queue_name"
    for src in $part_list; do
        run_sql $db "select * from londiste.get_table_list('l3_${src}_q')"
    done
done

../zcheck.sh

