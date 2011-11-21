#! /bin/sh

. ../testlib.sh

title "Part"
v=-v

part_list="part1 part2 part3 part4"
full_list="full1"
all_list="$part_list $full_list"
kdb_list="`echo $all_list|sed 's/ /,/g'`"

for db in $all_list; do
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

# part replicas
for dst in $part_list; do
cat > conf/londiste_${db}_${dst}.ini << EOF
[londiste3]
job_name = londiste_${db}_${dst}
db = dbname=$dst
queue_name = replika
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF
done
done

set -e

msg "Create nodes for full queue"
run londiste3 $v conf/londiste_full1.ini create-root root_full1 'dbname=full1'
#run londiste3 $v conf/londiste_full2.ini create-branch branch_full2 'dbname=full2' --provider='dbname=full1'

msg "Create nodes for replicas"
for dst in $part_list; do
  for src in $full_list; do
    run londiste3 $v conf/londiste_${src}_${dst}.ini \
                    create-leaf leaf_${src}_${dst} "dbname=$dst" \
                    --provider="dbname=$src"
  done
done

#msg "Create nodes for partition root queues"
#for db in $part_list; do
#run londiste3 $v conf/londiste_$db.ini create-root root_$db "dbname=$db"
#done

msg "Tune PgQ"
for db in $all_list; do
  run_sql $db "update pgq.queue set queue_ticker_idle_period='3 secs'"
done

msg "Launch ticker"
run pgqd $v -d conf/pgqd.ini

msg "Launch londiste worker"
for db in $full_list; do
  run londiste3 $v -d conf/londiste_$db.ini worker
done

msg "Launch merge londiste"
for src in $full_list; do
  for dst in $part_list; do
    run londiste3 $v -d conf/londiste_${src}_${dst}.ini worker
  done
done

msg "Create partconf in partition nodes"
part_count=$(echo $part_list|wc -w)
max_part=$(( $part_count-1 ))
i=0
for db in $part_list; do
run psql $db <<EOF
create schema partconf;
CREATE TABLE partconf.conf (
    part_nr integer,
    max_part integer,
    db_code bigint,
    is_primary boolean,
    max_slot integer,
    cluster_name text
);
insert into partconf.conf(part_nr, max_part) values($i, $max_part);
EOF
i=$(( $i+1 ))
done

msg "Create table in root node"
run_sql full1 "create table mydata (id int4 primary key, data text)"

msg "Insert few rows"
for n in 1 2 3 4; do
  run_sql full1 "insert into mydata values ($n, 'foo$n')"
done

msg "Register table in root node"
run londiste3 $v conf/londiste_full1.ini add-table mydata --handler=part --handler-arg="key=data"

#msg "Register and create table in branch node"
#run londiste3 $v conf/londiste_full2.ini add-table mydata --create --handler="part" --handler-arg="key=data"

msg "Wait until add-table events are distributed to leafs"
parts=$(echo "$part_list"|wc -w)
for db in $part_list; do
cnt=0
while test $cnt -ne 1; do
 #sleep 5
 cnt=`psql ${db} -Atc "select count(*) from londiste.table_info"`
 echo "$db: cnt=$cnt"
 if [ $cnt != 1 ]; then
     sleep 5
 fi
done
done

msg "Register table in partition nodes"
for src in $full_list; do
  for dst in $part_list; do
    run londiste3 $v -d conf/londiste_${src}_${dst}.ini add-table mydata --create --handler=part --handler-arg="key=data"
  done
done

msg "Wait until tables are sync in part nodes"
for db in $part_list; do
cnt=0
while test $cnt -ne 1; do
  #sleep 5
  cnt=`psql -A -t -d $db -c "select count(*) from londiste.table_info where merge_state = 'ok'"`
  echo "$db: cnt=$cnt"
   if [ $cnt != 1 ]; then
      sleep 5
   fi
done
done

msg "Sleep a bit"
run sleep 10

msg "Insert few rows"
for n in $(seq 5 10); do
  run_sql full1 "insert into mydata values ($n, 'foo$n')"
done

msg "Sleep a bit"
run sleep 10

msg "Now check if data apprered"
for db in $part_list; do
run_sql $db "select * from mydata order by id"
#run_sql $db "select * from londiste.table_info order by queue_name"
done

../zcheck.sh

