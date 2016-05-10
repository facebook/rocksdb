dnl $Id: acinclude.m4,v 1.0.0-0 2014/07/05 16:16:13 Cnangel Exp $

dnl -------------------------------------------------------------------------
dnl AC_CHECK_EXTRA_OPTIONS
dnl -------------------------------------------------------------------------
AC_DEFUN([AC_CHECK_EXTRA_OPTIONS],[

CFLAGS="-std=c++11 -DROCKSDB_PLATFORM_POSIX -fPIC"
CXXFLAGS="-std=c++11 -DROCKSDB_PLATFORM_POSIX -fPIC"

dnl -------------------------------------------------------------------------
dnl Enable debug with -g -Wall -D_DEBUG
dnl -------------------------------------------------------------------------
AC_MSG_CHECKING(for debugging)
AC_ARG_ENABLE([debug],
	[AS_HELP_STRING([--enable-debug],
		[compile for debugging @<:@default=no@:>@])],
	[
		AC_MSG_RESULT([yes])
		CFLAGS="${CFLAGS} -g -Wall -D_DEBUG"
		CXXFLAGS="${CXXFLAGS} -g -Wall -D_DEBUG"
	],
	[
		AC_MSG_RESULT([no])
		CFLAGS="${CFLAGS} -g -O2 -DNDEBUG"
		CXXFLAGS="${CXXFLAGS} -g -O2 -DNDEBUG"
	])


dnl -------------------------------------------------------------------------
dnl Enable profile with -pg
dnl -------------------------------------------------------------------------
AC_MSG_CHECKING([enable profile])
AC_ARG_ENABLE([profile],
	[AS_HELP_STRING([--enable-profile],
		[compile for profiling @<:@default=no@:>@])],
	[
		AC_MSG_RESULT([yes])
		CFLAGS="${CFLAGS} -pg"
		CXXFLAGS="${CXXFLAGS} -pg"
	],
	[
		AC_MSG_RESULT([no])
	])


dnl -------------------------------------------------------------------------
dnl Enable test options with -fstack-protector -fstack-protector-all -fprofile-arcs -ftest-coverage 
dnl -------------------------------------------------------------------------
AC_ARG_ENABLE([test],
		[AS_HELP_STRING([--enable-test],
			[enable test options with -fprofile-arcs -ftest-coverage @<:@default=no@:>@])],
		[],
		[enable_test=no]
		)
AC_MSG_CHECKING([whether do enable test options support])
AS_IF([test "x$enable_test" = "xyes"],
		[
			GCC_VERSION=`gcc -dumpversion | sed 's/\([[0-9]]\{1,\}\.[[0-9]]\{1,\}\)\.*\([[0-9]]\{1,\}\)\{0,1\}/\1\2/'`
			AS_IF([expr $GCC_VERSION '>=' 4 >/dev/null],
				[
					CFLAGS="${CFLAGS} -fstack-protector -fstack-protector-all -fprofile-arcs -ftest-coverage"
					CXXFLAGS="${CXXFLAGS} -fstack-protector -fstack-protector-all -fprofile-arcs -ftest-coverage"
				],
				[
					CFLAGS="${CFLAGS} -fprofile-arcs -ftest-coverage"
					CXXFLAGS="${CXXFLAGS} -fprofile-arcs -ftest-coverage"
				])
			AC_MSG_RESULT([yes])
		],
		[
			AC_MSG_RESULT([no])
		]
	 )
AM_CONDITIONAL(ENABLE_TEST_OPTIONS, test x$enable_test != xno)


dnl -------------------------------------------------------------------------
dnl Checks for gtest libraries.
dnl -------------------------------------------------------------------------
PKG_CHECK_MODULES([GTEST],[gtest >= 1.5.0],,AC_MSG_ERROR([
			*** gtest >= 1.5.0 is required to build.]))


dnl -------------------------------------------------------------------------
dnl Checks for gflags libraries.
dnl -------------------------------------------------------------------------
PKG_CHECK_MODULES([GFLAGS],[gflags >= 1.0.0],,AC_MSG_ERROR([
			*** gflags >= 1.0.0 is required to build.]))


dnl -------------------------------------------------------------------------
dnl Checks for snappy libraries.
dnl -------------------------------------------------------------------------
PKG_CHECK_MODULES([SNAPPY],[snappy >= 1.1.0],,AC_MSG_ERROR([
			*** snappy >= 1.1.0 is required to build.]))


dnl -------------------------------------------------------------------------
dnl Checks for zlib libraries.
dnl -------------------------------------------------------------------------
PKG_CHECK_MODULES([ZLIB],[zlib >= 1.2.3],,AC_MSG_ERROR([
			*** zlib >= 1.2.3 is required to build.]))


dnl -------------------------------------------------------------------------
dnl Checks for bzip2 libraries.
dnl -------------------------------------------------------------------------
PKG_CHECK_MODULES([BZIP2],[bzip2 >= 1.0.0],,AC_MSG_ERROR([
			*** bzip2 >= 1.0.0 is required to build.]))


dnl # Add cleanfiles for gcov
AC_SUBST([MOSTLYCLEANFILES], "*.bb *.bbg *.da *.gcov *.gcda *.gcno")


])

