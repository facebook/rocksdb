# $Id: Makefile.am,v 1.0.0-0 2015/05/26 18:48:00 Cnangel Exp $

AUTOMAKE_OPTIONS					= foreign

EXTRA_DIST							= $(NULL)

AM_LDFLAGS							= $(GTEST_LIBS) \
									  $(GFLAGS_LIBS) \
									  -fPIC -L/usr/lib@BITS@ \
									  -lrt \
									  -static \
									  $(NULL)

AM_CPPFLAGS							= -I$(top_srcdir)/include \
									  -I$(top_srcdir)/util \
									  $(GTEST_CFLAGS) \
									  $(GFLAGS_CFLAGS) \
									  -Wno-deprecated \
									  $(NULL)

noinst_LTLIBRARIES					= librocksdb_utiltest.la
librocksdb_utiltest_la_SOURCES		= testharness.cc \
									  testutil.cc \
									  transaction_test_util.cc \
									  db_test_util.cc \
									  db_impl_debug.cc \
									  xfunc_test_points.cc \
									  mock_table.cc \
									  $(NULL)
