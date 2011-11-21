#! /bin/bash

. ../testlib.sh

../zstop.sh

v='-v'

# bulkloader method
meth=0

src_db_list="src1 src2"
dst_db_list="dst"
db_list="$src_db_list $dst_db_list"
kdb_list=`echo $db_list | sed 's/ /,/g'`

#( cd ../..; make -s install )

echo " * create configs * "

# create ticker conf
cat > conf/pgqd.ini <<EOF
[pgqd]
database_list = $kdb_list
logfile = log/pgqd.log
pidfile = pid/pgqd.pid
EOF

# londiste configurations
for db in $src_db_list; do

# londiste on source
cat > conf/londiste_$db.ini << EOF
[londiste3]
job_name = londiste_$db
db = dbname=$db
queue_name = replika_$db
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF

# londiste on source to target
for dst in $dst_db_list; do
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

for db in $db_list; do
  cleardb $db
done

clearlogs

set -e

msg "Install londiste3 and initialize nodes"

for db in $src_db_list; do
run londiste3 $v conf/londiste_$db.ini create-root $db "dbname=$db"
for dst in $dst_db_list; do
run londiste3 $v conf/londiste_${db}_${dst}.ini create-leaf $dst "dbname=$dst" --provider="dbname=$db"
done
done

for db in $db_list; do
  run_sql $db "update pgq.queue set queue_ticker_idle_period='5 secs'"
done

msg "Run ticker"
run pgqd -d conf/pgqd.ini
run sleep 5

msg "See topology"
for db in $src_db_list; do
run londiste3 $v conf/londiste_$db.ini status
done

msg "Run londiste3 daemon for each node"
for db in $src_db_list; do
run londiste3 $v -d conf/londiste_$db.ini worker
for dst in $dst_db_list; do
run londiste3 $v -d conf/londiste_${db}_${dst}.ini worker
done
done

for db in $dst_db_list; do
    run createlang -d $db plpythonu
    run psql $db -f ../../sql/conflicthandler/merge_on_time.sql
done

msg "Create table on root nodes, fill couple of rows and register"
for db in $src_db_list; do
run_sql $db "create table mytable (id int4 primary key, data text, tstamp timestamptz default now())"
for n in 1 2 3; do
  run_sql $db "insert into mytable values ($n, 'row$n')"
done
run londiste3 $v conf/londiste_$db.ini add-table mytable
done

sleep 10

msg "Register table on dst node with creation"
#run londiste3 $v conf/londiste_src1_dst.ini add-table mytable --create --no-merge --handler=applyfn --handler-arg="func_name=merge_on_time" --handler-arg="func_conf=timefield=tstamp"
run londiste3 $v conf/londiste_src1_dst.ini add-table mytable --create --handler=multimaster --handler-arg="timefield=tstamp"
sleep 10
#run londiste3 $v conf/londiste_src2_dst.ini add-table mytable --expect-sync --no-merge --handler=applyfn --handler-arg="func_name=merge_on_time" --handler-arg="func_conf=timefield=tstamp"
run londiste3 $v conf/londiste_src2_dst.ini add-table mytable --expect-sync --handler=multimaster --handler-arg="timefield=tstamp"


for db in $src_db_list; do
for n in 4 5 6; do
  run_sql $db "insert into mytable values ($n, 'row$n::$db')"
done
sleep 3
done


for n in 2 3 4; do
    run_sql src1 "update mytable set data = 'ok', tstamp = now() where id = $n"
done

for n in 1 5 6; do
    run_sql src2 "update mytable set data = 'ok', tstamp = now() where id = $n"
done

run sleep 10

for dst in $dst_db_list; do
run_sql $dst "select * from mytable"
done

../zcheck.sh

