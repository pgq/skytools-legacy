#! /bin/sh

#. ../env.sh

for p in pid/*.pid*; do
  test -f "$p" || continue
  pid=`cat "$p"`
  #test -d "/proc/$pid" || {
  #  rm -f "$p"
  #  continue
  #}
  kill "$pid"
done

killall pgqd
ps aux|grep londiste[3]|awk '{ print $2 }' | xargs -n 1 kill

