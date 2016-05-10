# $Id: Makefile.am,v 1.0.0-0 2016/05/06 20:30:50 Cnangel Exp $

AUTOMAKE_OPTIONS					= foreign

EXTRA_DIST							= $(NULL)

AM_LDFLAGS							= $(top_builddir)/test/util/librocksdb_utiltest.la \
									  $(top_builddir)/librocksdb.la \
									  $(GTEST_LIBS) \
									  $(GFLAGS_LIBS) \
									  -fPIC -L/usr/lib@BITS@ \
									  -lrt \
									  -static \
									  $(NULL)

AM_CPPFLAGS							= -I$(top_srcdir)/include \
									  -I$(top_srcdir)/util \
									  -DGFLAGS=gflags \
									  $(GTEST_CFLAGS) \
									  $(GFLAGS_CFLAGS) \
									  -Wno-deprecated \
									  $(NULL)

bin_PROGRAMS						= db_repl_stress \
									  write_stress \
									  db_stress \
									  ldb_tool \
									  sst_dump_tool \
									  db_bench_tool \
									  $(NULL)
noinst_PROGRAMS						= cache_bench \
									  log_write_bench \
									  $(NULL)
db_repl_stress_SOURCES				= db_repl_stress.cc
write_stress_SOURCES				= write_stress.cc
db_stress_SOURCES					= db_stress.cc
ldb_tool_SOURCES					= ldb.cc ldb_tool.cc ldb_cmd.cc sst_dump_tool.cc
sst_dump_tool_SOURCES				= sst_dump_tool.cc sst_dump.cc ldb_cmd.cc ldb_tool.cc
db_bench_tool_SOURCES				= db_bench_tool.cc db_bench.cc

cache_bench_SOURCES					= cache_bench.cc
log_write_bench_SOURCES				= log_write_bench.cc
