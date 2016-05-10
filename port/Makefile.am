# $Id: Makefile.am,v 1.0.0-0 2015/07/22 10:27:43 Cnangel Exp $

AUTOMAKE_OPTIONS						= foreign

dist_headers							= $(wildcard *.h)

AM_CPPFLAGS								= -I$(top_srcdir)/port \
										  -I$(top_srcdir)/include \
										  $(NULL)

AM_LDFLAGS								= $(NULL)

noinst_LTLIBRARIES						= librocksdb_port.la
# /* {{{ librocksdb_port_la_SOURCES		= */
librocksdb_port_la_SOURCES				= port_posix.cc \
										  stack_trace.cc \
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
