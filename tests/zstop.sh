#! /bin/sh

#. ../env.sh

for p in pid/*.pid*; do
  test -f "$p" || continue
  pid=`cat "$p"`
  test -d "/proc/$pid" || {
    rm -f "$p"
    continue
  }
  kill "$pid"
done

