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

noinst_LTLIBRARIES						= librocksdb_db.la
# /* {{{ librocksdb_db_la_SOURCES		= */
librocksdb_db_la_SOURCES				= auto_roll_logger.cc \
                                          builder.cc \
                                          c.cc \
                                          column_family.cc \
                                          compacted_db_impl.cc \
                                          compaction.cc \
                                          compaction_iterator.cc \
                                          compaction_job.cc \
                                          compaction_picker.cc \
                                          convenience.cc \
                                          db_filesnapshot.cc \
                                          dbformat.cc \
                                          db_impl.cc \
                                          db_impl_experimental.cc \
                                          db_impl_readonly.cc \
                                          db_info_dumper.cc \
                                          db_iter.cc \
                                          event_helpers.cc \
                                          experimental.cc \
                                          file_indexer.cc \
                                          filename.cc \
                                          flush_job.cc \
                                          flush_scheduler.cc \
                                          forward_iterator.cc \
                                          internal_stats.cc \
                                          log_reader.cc \
                                          log_writer.cc \
                                          managed_iterator.cc \
                                          memtable_allocator.cc \
                                          memtable.cc \
                                          memtable_list.cc \
                                          merge_helper.cc \
                                          merge_operator.cc \
                                          repair.cc \
                                          slice.cc \
                                          snapshot_impl.cc \
                                          table_cache.cc \
                                          table_properties_collector.cc \
                                          transaction_log_impl.cc \
                                          version_builder.cc \
                                          version_edit.cc \
                                          version_set.cc \
                                          wal_manager.cc \
                                          write_batch_base.cc \
                                          write_batch.cc \
                                          write_controller.cc \
                                          write_thread.cc \
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
