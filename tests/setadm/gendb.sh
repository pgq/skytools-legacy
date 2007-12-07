#! /bin/sh

. ../env.sh
./stop.sh

dropdb zset_root
dropdb zset_branch
dropdb zset_leaf
createdb zset_root
createdb zset_branch
createdb zset_leaf

setadm.py conf/admin.ini  init-root   z-root   "dbname=zset_root"
setadm.py conf/admin.ini  init-branch z-branch "dbname=zset_branch" --provider=z-root
setadm.py conf/admin.ini  init-leaf   z-leaf   "dbname=zset_leaf" --provider=z-branch

./testconsumer.py -v -d conf/zroot.ini
./testconsumer.py -v -d conf/zbranch.ini
./testconsumer.py -v -d conf/zleaf.ini

