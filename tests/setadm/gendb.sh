#! /bin/sh

dropdb zset_root
dropdb zset_branch
dropdb zset_leaf
createdb zset_root
createdb zset_branch
createdb zset_leaf

./admset.py master.ini init-root z-root "dbname=zset_root"
./admset.py master.ini init-branch z-branch "dbname=zset_branch" --provider=z-root
./admset.py master.ini init-leaf z-leaf "dbname=zset_leaf" --provider=z-branch


