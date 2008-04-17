#! /bin/sh

got=0
for pf in sys/pid.*; do
  test -f "$pf" || continue
  echo " * Killing $pf"
  kill `cat $pf`
  got=1
done
test got = 0 || sleep 1

