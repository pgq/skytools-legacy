#! /bin/sh

err=0

for s in pgq pgq_node pgq_coop londiste pgq_ext; do
  code_hash=$(git log --raw -n 1 sql/$s/functions | head -1)
  fn="sql/$s/functions/$s.version.sql"
  ver_hash=$(git log --raw -n 1 "$fn" | head -1)
  test "${code_hash}" = "${ver_hash}" || echo "$s has code changes, needs new version"

  ver_func=$(sed -n "s/.*return *'\(.*\)';/\1/;T;p" $fn)
  ver_control=$(sed -n "s/default_version = '\(.*\)'/\1/;T;p" sql/$s/$s.control)
  ver_make=$(sed -n "s/EXT_VERSION = \(.*\)/\1/;T;p" sql/$s/Makefile)

  if test "${ver_func}|${ver_control}" = "${ver_make}|${ver_make}"; then
    echo "$s: $ver_control"
  else
    echo "$s: version mismatch"
    echo "   Makefile:  $ver_make"
    echo "   version(): $ver_func"
    echo "   control:   $ver_control"
    err=1
  fi
done

exit $err

