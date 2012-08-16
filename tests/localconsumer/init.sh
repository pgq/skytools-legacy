#! /bin/sh

. ../env.sh

mkdir -p log pid

dropdb qdb
createdb qdb

