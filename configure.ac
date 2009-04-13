dnl Process this file with autoconf to produce a configure script.

AC_INIT(skytools, 3.0a1)
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

dnl check for xmlto, but only if asciidoc is found
if test "$ASCIIDOC" != "no"; then
  AC_CHECK_PROGS(XMLTO, [$XMLTO xmlto])
  test -n "$XMLTO" || XMLTO=no
else
  XMLTO="no"
fi

AC_PROG_CC
AC_PROG_CPP

dnl Additional gcc tuning
if test x"$GCC" = xyes; then
  AC_MSG_CHECKING([for working warning switches])
  good_CFLAGS="$CFLAGS"
  warnings=""
  flags="-Wall -Wextra"
  # turn off noise from Wextra
  flags="$flags -Wno-unused-parameter -Wno-missing-field-initializers"
  # Wextra does not turn those on?
  flags="$flags -Wmissing-prototypes -Wpointer-arith -Wendif-labels"
  flags="$flags -Wdeclaration-after-statement -Wold-style-definition"
  flags="$flags -Wstrict-prototypes -Wundef -Wformat -Wnonnull -Wstrict-overflow"
  for f in $flags; do
    CFLAGS="$good_CFLAGS $warnings $f"
    AC_COMPILE_IFELSE([void foo(void){}], [warnings="$warnings $f"])
  done
  CFLAGS="$good_CFLAGS $warnings"
  AC_MSG_RESULT([$warnings])
fi

# autoconf does not want to find 'install', if not using automake...
INSTALL=install

dnl Check if linker supports -Wl,--as-needed
dnl That helps to get rid of unnecessary -lrt
if test "$GCC" = "yes"; then
  old_LDFLAGS="$LDFLAGS"
  LDFLAGS="$LDFLAGS -Wl,--as-needed"
  AC_MSG_CHECKING([whether linker supports --as-needed])
  AC_LINK_IFELSE([int main(void) { return 0; }],
    [AC_MSG_RESULT([yes])],
    [AC_MSG_RESULT([no])
     LDFLAGS="$old_LDFLAGS"])
fi

dnl debault to debugging on
AC_ARG_ENABLE(debug,
  AC_HELP_STRING([--disable-debug],[strip binary]),
  [], [enable_debug=yes])
AC_MSG_CHECKING([whether to build debug binary])
AC_MSG_RESULT([$enable_debug])
if test "$enable_debug" = "yes"; then
  LDFLAGS="-g $LDFLAGS"
  BININSTALL="$INSTALL"
else
  BININSTALL="$INSTALL -s"
fi
AC_SUBST(enable_debug)
AC_SUBST(INSTALL)
AC_SUBST(BININSTALL)


dnl Postres headers on Solaris define incompat unsetenv without that
AC_CHECK_FUNCS(unsetenv)

dnl Write result
AC_CONFIG_FILES([config.mak])
AC_OUTPUT

