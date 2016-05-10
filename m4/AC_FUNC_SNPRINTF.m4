dnl @synopsis AC_FUNC_SNPRINTF
dnl
dnl Provides a test for a working snprintf function.
dnl unlike the original AC_FUNC_SNPRINTF test this one will also
dnl accept snprintf implementations which return -1 if the string does
dnl not fit in the buffer, e.g. snprintf on Solaris and glibc <= 2.0.6.
dnl defines HAVE_SNPRINTF if it is found, and
dnl sets ac_cv_func_snprintf to yes, otherwise to no.
dnl
dnl @version $Id: AC_FUNC_SNPRINTF.m4,v 1.2 2001/09/18 15:42:29 bastiaan Exp $
dnl @author Caolan McNamara <caolan@skynet.ie>
dnl
AC_DEFUN([AC_FUNC_SNPRINTF],
[AC_CACHE_CHECK(for working snprintf, ac_cv_func_snprintf,
[AC_TRY_RUN([#include <stdio.h>
int main () { int l = snprintf(NULL,0,"%d",100); exit (!((3 <= l) || (-1 == l))); }
], ac_cv_func_snprintf=yes, ac_cv_func_snprintf=no,
ac_cv_func_snprintf=no)])
if test $ac_cv_func_snprintf = yes; then
  AC_DEFINE(HAVE_SNPRINTF,,[define if the C library has snprintf])
fi
])
