# $Id: Makefile.am,v 1.0.0-0 2015/07/22 10:27:43 Cnangel Exp $

AUTOMAKE_OPTIONS						= foreign

dist_headers							= $(wildcard *.h)

AM_CPPFLAGS								= -I$(top_srcdir)/util \
										  -I$(top_srcdir)/include \
										  $(SNAPPY_CFLAGS) \
										  $(ZLIB_CFLAGS) \
										  $(BZIP2_CFLAGS) \
										  $(NULL)

AM_LDFLAGS								= $(NULL)

noinst_LTLIBRARIES						= librocksdb_tbl.la
# /* {{{ librocksdb_tbl_la_SOURCES		= */
librocksdb_tbl_la_SOURCES				= adaptive_table_factory.cc \
                                          block_based_filter_block.cc \
                                          block_based_table_builder.cc \
                                          block_based_table_factory.cc \
                                          block_based_table_reader.cc \
                                          block_builder.cc \
                                          block.cc \
                                          block_hash_index.cc \
                                          block_prefix_index.cc \
                                          bloom_block.cc \
                                          cuckoo_table_builder.cc \
                                          cuckoo_table_factory.cc \
                                          cuckoo_table_reader.cc \
                                          flush_block_policy.cc \
                                          format.cc \
                                          full_filter_block.cc \
                                          get_context.cc \
                                          iterator.cc \
                                          merger.cc \
                                          meta_blocks.cc \
                                          plain_table_builder.cc \
                                          plain_table_factory.cc \
                                          plain_table_index.cc \
                                          plain_table_key_coding.cc \
                                          plain_table_reader.cc \
                                          sst_file_writer.cc \
                                          table_properties.cc \
                                          two_level_iterator.cc \
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
