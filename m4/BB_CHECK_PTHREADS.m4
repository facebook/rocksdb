dnl@synposis BB_CHECK_PTHREADS.m4
dnl
AC_DEFUN([BB_CHECK_PTHREADS],[
AC_REQUIRE([AC_PROG_CC])dnl
AC_REQUIRE([AC_PROG_CXX])dnl
AC_REQUIRE([AC_PROG_CPP])dnl
AC_REQUIRE([AC_PROG_CXXCPP])dnl

AC_ARG_WITH([pthreads], [  --with-pthreads         include pthreads support],
    [AC_SEARCH_LIBS([pthread_key_create], [pthread],
        [AC_DEFINE(HAVE_THREADING,,[define if threading is enabled])
         AC_DEFINE(USE_PTHREADS,,[define if pthread library is available])
         AC_DEFINE(_PTHREADS,,[define for STL if pthread library is used])],
    [AC_MSG_ERROR([pthreads not found])])])

])
