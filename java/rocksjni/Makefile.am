# $Id: Makefile.am,v 1.0.0-0 2016/05/09 16:13:44 Cnangel Exp $

AUTOMAKE_OPTIONS						= foreign

dist_headers							= $(wildcard *.h)

AM_CPPFLAGS								= -I$(top_srcdir)/util \
										  -I$(top_srcdir)/include \
										  -I$(top_srcdir)/java \
										  -I$(JAVA_HOME)/include \
										  -I$(JAVA_HOME)/include/linux \
										  $(SNAPPY_CFLAGS) \
										  $(ZLIB_CFLAGS) \
										  $(BZIP2_CFLAGS) \
										  $(NULL)

AM_LDFLAGS								= $(NULL)

lib_LTLIBRARIES							= librocksdbjni-linux@ARCH@.la
# /* {{{ librocksdbjni_linux@ARCH@_la_SOURCES	= */
librocksdbjni_linux@ARCH@_la_SOURCES	= backupablejni.cc \
                                          backupenginejni.cc \
                                          checkpoint.cc \
                                          columnfamilyhandle.cc \
                                          compaction_filter.cc \
                                          comparator.cc \
                                          comparatorjnicallback.cc \
                                          env.cc \
                                          filter.cc \
                                          iterator.cc \
                                          loggerjnicallback.cc \
                                          memtablejni.cc \
                                          merge_operator.cc \
                                          options.cc \
                                          ratelimiterjni.cc \
                                          remove_emptyvalue_compactionfilterjni.cc \
                                          restorejni.cc \
                                          rocksjni.cc \
                                          slice.cc \
                                          snapshot.cc \
                                          statistics.cc \
                                          table.cc \
                                          transaction_log.cc \
                                          ttl.cc \
                                          write_batch.cc \
                                          writebatchhandlerjnicallback.cc \
                                          write_batch_with_index.cc \
										  $(dist_headers) \
										  $(NULL)
# /* }}} */
librocksdbjni_linux@ARCH@_la_LIBADD		= $(top_builddir)/port/librocksdb_port.la \
										  $(top_builddir)/util/librocksdb_util.la \
										  $(top_builddir)/table/librocksdb_tbl.la \
										  $(top_builddir)/db/librocksdb_db.la \
										  $(top_builddir)/memtable/librocksdb_memtbl.la \
										  $(top_builddir)/utilities/table_properties_collectors/librocksdb_tpc.la \
										  $(top_builddir)/utilities/redis/librocksdb_redis.la \
										  $(top_builddir)/utilities/backupable/librocksdb_bakable.la \
										  $(top_builddir)/utilities/ttl/librocksdb_ttl.la \
										  $(top_builddir)/utilities/compaction_filters/librocksdb_cf.la \
										  $(top_builddir)/utilities/transactions/librocksdb_trans.la \
										  $(top_builddir)/utilities/document/librocksdb_document.la \
										  $(top_builddir)/utilities/spatialdb/librocksdb_spatialdb.la \
										  $(top_builddir)/utilities/write_batch_with_index/librocksdb_wbwi.la \
										  $(top_builddir)/utilities/flashcache/librocksdb_flashcache.la \
										  $(top_builddir)/utilities/memory/librocksdb_memory.la \
										  $(top_builddir)/utilities/librocksdb_utilities.la \
										  $(top_builddir)/utilities/convenience/librocksdb_convenience.la \
										  $(top_builddir)/utilities/leveldb_options/librocksdb_leveldb_options.la \
										  $(top_builddir)/utilities/merge_operators/string_append/librocksdb_string_append.la \
										  $(top_builddir)/utilities/merge_operators/librocksdb_merge_operators.la \
										  $(top_builddir)/utilities/geodb/librocksdb_geodb.la \
										  $(top_builddir)/utilities/options/librocksdb_options.la \
										  $(top_builddir)/utilities/checkpoint/librocksdb_chkpoint.la \
										  $(NULL)
librocksdbjni_linux@ARCH@_la_LDFLAGS	= -module -avoid-version -shared -fpic

# /*
#  * Local variables:
#  * tab-width: 4
#  * c-basic-offset: 4
#  * End:
#  * vim600: noet sw=4 ts=4 fdm=marker
#  * vim<600: noet sw=4 ts=4
#  */
