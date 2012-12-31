dnl Process this file with autoconf to produce a configure script.

AC_INIT(skytools, 3.1.3)
AC_CONFIG_SRCDIR(python/londiste.py)
AC_CONFIG_HEADER(lib/usual/config.h)
AC_PREREQ([2.59])

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
    if "$a" --version 2>&1 | grep GNU > /dev/null; then
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

dnl name suffix
default_SUFFIX=3
SUFFIX="$default_SUFFIX"
AC_MSG_CHECKING(whether to add suffix to script names)
AC_ARG_WITH(suffix, [  --with-suffix[[=sfx]]  common package suffix (default: 3)],
[ if test "$withval" = "yes"; then
    SUFFIX="$default_SUFFIX"
  elif test "$withval" = "no"; then
    SUFFIX=""
  else
    SUFFIX="$withval"
  fi ])
if test "$SUFFIX" = ""; then
  AC_MSG_RESULT(no)
else
  AC_MSG_RESULT([yes, "$SUFFIX"])
fi
AC_SUBST(SUFFIX)

dnl asciidoc >= 8.2
AC_ARG_WITH(asciidoc, [  --with-asciidoc[[=prog]]  path to asciidoc 8.2 (default: asciidoc)],
[ if test "$withval" = "yes"; then
    AC_CHECK_PROGS(ASCIIDOC, [$ASCIIDOC asciidoc])
    test -n "$ASCIIDOC" || ASCIIDOC=no
  else
    AC_MSG_CHECKING(for asciidoc)
    ASCIIDOC=$withval
    AC_MSG_RESULT($ASCIIDOC)
  fi ],
[ ASCIIDOC="no" ])

if test "$ASCIIDOC" != "no"; then
  AC_MSG_CHECKING([whether asciidoc version >= 8.2])
  ver=`$ASCIIDOC --version 2>&1 | sed -e 's/asciidoc //'`
  case "$ver" in
dnl hack to make possible to use [, ] in regex
changequote({, })dnl
  [0-7].*|8.[01]|8.[01].*)
changequote([, ])dnl
      AC_MSG_RESULT([$ver, too old])
      ASCIIDOC="no"
      ;;
  *)  AC_MSG_RESULT([$ver, ok])
      ;;
  esac
fi

dnl Decide default skylog setting
SKYLOG=0
AC_MSG_CHECKING(for skylog default setting)
AC_ARG_WITH(skylog, [  --with-skylog  set default value for skylog (default: off)],
[ if test "$withval" = "yes"; then
    SKYLOG="1"
  elif test "$withval" = "no"; then
    SKYLOG="0"
  else
    SKYLOG="$withval"
  fi
  AC_MSG_RESULT([$SKYLOG])])
AC_MSG_RESULT([$SKYLOG])
AC_SUBST(SKYLOG)

dnl Decide module subdir
SK3_SUBDIR=0
AC_MSG_CHECKING([whether to install modules in /skytools-3.0/ subdir])
AC_ARG_WITH(sk3-subdir, AC_HELP_STRING([--with-sk3-subdir], [put modules into /skytools-3.0/ subdir]),
[ if test "$withval" = "yes"; then
    SK3_SUBDIR="1"
  elif test "$withval" = "no"; then
    SK3_SUBDIR="0"
  else
    SK3_SUBDIR="$withval"
  fi
  AC_MSG_RESULT([$SK3_SUBDIR])])
AC_MSG_RESULT([$SK3_SUBDIR])
AC_SUBST(SK3_SUBDIR)

dnl check for xmlto, but only if asciidoc is found
if test "$ASCIIDOC" != "no"; then
  AC_CHECK_PROGS(XMLTO, [$XMLTO xmlto])
  test -n "$XMLTO" || XMLTO=no
else
  XMLTO="no"
fi

# when in .git tree, turn asciidoc/xmlto uncondicionally on
if test -d .git; then
  if test "$ASCIIDOC" = "no"; then
    echo "*** Building from GIT requires asciidoc, enabling it ***"
    ASCIIDOC="asciidoc"
  fi
  if test "$XMLTO" = "no"; then
    echo "*** Building from GIT requires xmlto, enabling it ***"
    XMLTO="xmlto"
  fi
fi

AC_USUAL_PORT_CHECK
AC_USUAL_PROGRAM_CHECK
AC_USUAL_HEADER_CHECK
AC_USUAL_TYPE_CHECK
AC_USUAL_FUNCTION_CHECK

dnl Postres headers on Solaris define incompat unsetenv without that
AC_CHECK_FUNCS(unsetenv)

dnl Optional use of libevent
AC_USUAL_LIBEVENT_OPT

dnl Needed on SmartOS (Solaris)
AC_SEARCH_LIBS([socket],[socket])

AC_USUAL_DEBUG
AC_USUAL_CASSERT

dnl Write result
AC_CONFIG_FILES([config.mak])
AC_OUTPUT

