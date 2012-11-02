#! /bin/sh

# generate per-version files

for v in 8.3 8.4 9.0 9.1 9.2; do

echo "usr/share/doc/postgresql-$v" > "postgresql-$v-pgq3.dirs"

cat > "postgresql-$v-pgq3.docs" <<EOF
sql/pgq/README.pgq
sql/pgq_ext/README.pgq_ext
EOF

cat > "postgresql-$v-pgq3.install" <<EOF
usr/lib/postgresql/$v/lib/pgq_triggers.so
usr/lib/postgresql/$v/lib/pgq_lowlevel.so
usr/share/postgresql/$v/contrib/pgq.upgrade.sql
usr/share/postgresql/$v/contrib/pgq_triggers.sql
usr/share/postgresql/$v/contrib/pgq_lowlevel.sql
usr/share/postgresql/$v/contrib/pgq_node.sql
usr/share/postgresql/$v/contrib/pgq_coop.upgrade.sql
usr/share/postgresql/$v/contrib/pgq_ext.sql
usr/share/postgresql/$v/contrib/londiste.sql
usr/share/postgresql/$v/contrib/pgq_node.upgrade.sql
usr/share/postgresql/$v/contrib/pgq.sql
usr/share/postgresql/$v/contrib/pgq_coop.sql
usr/share/postgresql/$v/contrib/londiste.upgrade.sql
usr/share/postgresql/$v/contrib/uninstall_pgq.sql
usr/share/postgresql/$v/contrib/newgrants_londiste.sql
usr/share/postgresql/$v/contrib/newgrants_pgq_coop.sql
usr/share/postgresql/$v/contrib/newgrants_pgq_ext.sql
usr/share/postgresql/$v/contrib/newgrants_pgq_node.sql
usr/share/postgresql/$v/contrib/newgrants_pgq.sql
usr/share/postgresql/$v/contrib/oldgrants_londiste.sql
usr/share/postgresql/$v/contrib/oldgrants_pgq_coop.sql
usr/share/postgresql/$v/contrib/oldgrants_pgq_ext.sql
usr/share/postgresql/$v/contrib/oldgrants_pgq_node.sql
usr/share/postgresql/$v/contrib/oldgrants_pgq.sql
EOF

getmkvar() {
  grep "^$1 =" ../sql/$2/Makefile | sed 's/.*= *//'
}

if test "$v" = "9.1" -o "$v" = "9.2"; then
  for mod in pgq pgq_node pgq_coop pgq_ext londiste; do
    (
      modver=`getmkvar EXT_VERSION $mod`
      oldvers=`getmkvar EXT_OLD_VERSIONS $mod`
      echo "usr/share/postgresql/$v/extension/${mod}.control"
      echo "usr/share/postgresql/$v/extension/${mod}--${modver}.sql"
      echo "usr/share/postgresql/$v/extension/${mod}--unpackaged--${modver}.sql"
      for old in ${oldvers}; do
        echo "usr/share/postgresql/$v/extension/${mod}--${old}--${modver}.sql"
      done
    ) >> "postgresql-$v-pgq3.install"
  done
fi

done
