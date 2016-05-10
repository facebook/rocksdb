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

noinst_LTLIBRARIES						= librocksdb_trans.la
# /* {{{ librocksdb_trans_la_SOURCES	= */
librocksdb_trans_la_SOURCES				= optimistic_transaction_db_impl.cc \
                                          optimistic_transaction_impl.cc \
                                          transaction_base.cc \
                                          transaction_db_impl.cc \
                                          transaction_db_mutex_impl.cc \
                                          transaction_impl.cc \
                                          transaction_lock_mgr.cc \
                                          transaction_util.cc \
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
