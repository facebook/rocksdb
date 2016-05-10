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

noinst_LTLIBRARIES						= librocksdb_ttl.la
# /* {{{ librocksdb_ttl_la_SOURCES		= */
librocksdb_ttl_la_SOURCES				= db_ttl_impl.cc \
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
