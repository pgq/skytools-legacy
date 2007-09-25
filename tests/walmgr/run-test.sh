#! /bin/sh

set -e

. ../env.sh

tmp=/tmp/waltest
src=$PWD
walmgr=$src/../../python/walmgr.py

test -f $tmp/data.master/postmaster.pid \
&& kill `head -1 $tmp/data.master/postmaster.pid` || true
test -f $tmp/data.slave/postmaster.pid \
&& kill `head -1 $tmp/data.slave/postmaster.pid` || true

rm -rf $tmp
mkdir -p $tmp
cd $tmp

LANG=C
PATH=/usr/lib/postgresql/8.2/bin:$PATH
export PATH LANG

mkdir log slave slave/logs.complete slave/logs.partial

#
# Prepare configs
#

### wal.master.ini ###
cat > wal.master.ini <<EOF
[wal-master]
logfile              = $tmp/log/wal-master.log
master_db            = dbname=template1 port=7200 host=127.0.0.1
master_data          = $tmp/data.master
master_config        = %(master_data)s/postgresql.conf
slave_config         = $tmp/wal.slave.ini
slave = localhost:$tmp/slave
completed_wals       = %(slave)s/logs.complete
partial_wals         = %(slave)s/logs.partial
full_backup          = %(slave)s/data.master
# syncdaemon update frequency
loop_delay           = 10.0
EOF

### wal.slave.ini ###
cat > wal.slave.ini <<EOF
[wal-slave]
logfile              = $tmp/log/wal-slave.log
slave_data           = $tmp/data.slave
slave_stop_cmd       = $tmp/rc.slave stop
slave_start_cmd      = $tmp/rc.slave start
slave = $tmp/slave
completed_wals       = %(slave)s/logs.complete
partial_wals         = %(slave)s/logs.partial
full_backup          = %(slave)s/data.master
EOF

### rc.slave ###
cat > rc.slave <<EOF
#! /bin/sh
cd $tmp
test -f $tmp/data.slave/postgresql.conf \
|| cp $src/conf.slave/*.conf $tmp/data.slave
pg_ctl -l $tmp/log/pg.slave.log -D $tmp/data.slave "\$1"
EOF
chmod +x rc.slave

#
# Initialize master db
#
echo "### Running initdb for master ###"
initdb data.master > log/initdb.log 2>&1
cp $src/conf.master/*.conf  data.master/
pg_ctl -D data.master -l log/pg.master.log start
sleep 4
createdb -h /tmp/waltest -p 7200

echo '####' $walmgr $tmp/wal.master.ini setup
$walmgr wal.master.ini setup
echo '####' $walmgr $tmp/wal.master.ini backup
$walmgr wal.master.ini backup
psql -c "create table t as select * from now()" -p 7200 -h /tmp/waltest

echo '####' $walmgr $tmp/wal.slave.ini restore
$walmgr $tmp/wal.slave.ini restore
sleep 10
echo '####' $walmgr $tmp/wal.master.ini sync
$walmgr wal.master.ini sync
echo '####' $walmgr $tmp/wal.slave.ini boot
$walmgr $tmp/wal.slave.ini boot
sleep 4
psql -c "select * from t" -p 7201 -h /tmp/waltest 

pg_ctl -D data.master stop
pg_ctl -D data.slave stop
