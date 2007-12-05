dnl Process this file with autoconf to produce a configure script.

AC_INIT(skytools, 3.0-cvs)
AC_CONFIG_SRCDIR(python/pgqadm.py)

dnl Find Python interpreter
AC_ARG_WITH(python, [  --with-python=PYTHON    name of the Python executable (default: python)],
[ AC_MSG_CHECKING(for python)
  PYTHON=$withval
  AC_MSG_RESULT($PYTHON)],
[ AC_PATH_PROGS(PYTHON, python) ])
test -n "$PYTHON" || AC_MSG_ERROR([Cannot continue without Python])

dnl Find PostgreSQL pg_config
AC_ARG_WITH(pgconfig, [  --with-pgconfig=PG_CONFIG    path to pg_config (default: pg_config)],
[ AC_MSG_CHECKING(for pg_config)
  PG_CONFIG=$withval
  AC_MSG_RESULT($PG_CONFIG)],
[ AC_PATH_PROGS(PG_CONFIG, pg_config) ])
test -n "$PG_CONFIG" || AC_MSG_ERROR([Cannot continue without pg_config])

dnl Find GNU make
AC_MSG_CHECKING(for GNU make)
if test ! -n "$MAKE"; then
  for a in make gmake gnumake; do
    if "$a" --version 2>&1 | grep -q GNU; then
      MAKE="$a"
      break
    fi
  done
fi
if test -n "$MAKE"; then
  AC_MSG_RESULT($MAKE)
else
  AC_MSG_ERROR([GNU make is not found])
fi
AC_SUBST(MAKE)

dnl Write result
AC_OUTPUT([config.mak])

