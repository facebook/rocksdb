# $Id: Makefile.am,v 1.0.0-0 2016/05/07 08:41:30 Cnangel Exp $

AUTOMAKE_OPTIONS						= foreign

dist_headers							= $(wildcard *.h)

AM_CPPFLAGS								= -I$(top_srcdir)/util \
										  -I$(top_srcdir)/include \
										  $(SNAPPY_CFLAGS) \
										  $(ZLIB_CFLAGS) \
										  $(BZIP2_CFLAGS) \
										  $(NULL)

AM_LDFLAGS								= $(NULL)

SUBDIRS									= table_properties_collectors \
                                          redis \
                                          backupable \
                                          ttl \
                                          compaction_filters \
                                          transactions \
                                          document \
                                          spatialdb \
                                          write_batch_with_index \
                                          flashcache \
                                          memory \
                                          convenience \
                                          leveldb_options \
                                          merge_operators \
                                          geodb \
                                          options \
                                          checkpoint \
										  $(NULL)

noinst_LTLIBRARIES						= librocksdb_utilities.la
# /* {{{ librocksdb_utilities_la_SOURCES= */
librocksdb_utilities_la_SOURCES			= env_mirror.cc \
										  $(dist_headers) \
										  $(NULL)
# /* }}} */


# /*
#  * Local variables:
#  * tab-width: 4
#  * c-basic-offset: 4
#  * End:
#  * vim600: noet sw=4 ts=4 fdm=marker
#  * vim<600: noet sw=4 ts=4
#  */
