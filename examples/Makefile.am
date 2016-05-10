# $Id: Makefile.am,v 1.0.0-0 2016/05/09 14:03:40 Cnangel Exp $

AUTOMAKE_OPTIONS					= foreign

EXTRA_DIST							= $(NULL)

AM_LDFLAGS							= $(top_builddir)/librocksdb.la \
									  $(SNAPPY_LIBS) \
									  $(ZLIB_LIBS) \
									  $(BZIP2_LIBS) \
									  -fPIC -L/usr/lib@BITS@ \
									  -lrt \
									  $(NULL)

AM_CPPFLAGS							= -I$(top_srcdir)/include \
									  -I$(top_srcdir)/util \
									  -DGFLAGS=gflags \
									  $(SNAPPY_CFLAGS) \
									  $(ZLIB_CFLAGS) \
									  $(BZIP2_CFLAGS) \
									  -Wno-deprecated \
									  $(NULL)

noinst_PROGRAMS						= c_simple_example \
									  column_families_example \
									  compact_files_example \
									  compaction_filter_example \
									  optimistic_transaction_example \
									  options_file_example \
									  simple_example \
									  transaction_example \
									  $(NULL)
c_simple_example_SOURCES			= c_simple_example.c
column_families_example_SOURCES		= column_families_example.cc
compact_files_example_SOURCES		= compact_files_example.cc
compaction_filter_example_SOURCES	= compaction_filter_example.cc
optimistic_transaction_example_SOURCES	= optimistic_transaction_example.cc
options_file_example_SOURCES		= options_file_example.cc
simple_example_SOURCES				= simple_example.cc
transaction_example_SOURCES			= transaction_example.cc
