#! /bin/sh

for s in pgq pgq_node pgq_coop londiste pgq_ext; do
  sver=$(git log --raw -n 1 sql/$s/functions | head -1)
  fn="sql/$s/functions/$s.version.sql"
  fver=$(git log --raw -n 1 "$fn" | head -1)
  test "$sver" = "$fver" && continue
  nver=`./misc/bumpver.py "$fn"`
  git commit -m "$s.version(): $nver" "$fn"
done

grep 'return ' sql/*/functions/*.version.sql
