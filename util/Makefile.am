# $Id: Makefile.am,v 1.0.0-0 2015/07/22 10:27:43 Cnangel Exp $

AUTOMAKE_OPTIONS						= foreign

dist_headers							= $(wildcard *.h)

AM_CPPFLAGS								= -I$(top_srcdir)/util \
										  -I$(top_srcdir)/include \
										  $(SNAPPY_CFLAGS) \
										  $(ZLIB_CFLAGS) \
										  $(BZIP2_CFLAGS) \
										  -DROCKSDB_LIB_IO_POSIX \
										  -DSNAPPY \
										  -DZLIB \
										  -DBZIP2 \
										  $(NULL)

AM_LDFLAGS								= $(NULL)

noinst_LTLIBRARIES						= librocksdb_util.la
# /* {{{ librocksdb_util_la_SOURCES		= */
librocksdb_util_la_SOURCES				= arena.cc \
                                          bloom.cc \
										  build_version.cc \
                                          cache.cc \
                                          coding.cc \
                                          compaction_job_stats_impl.cc \
                                          comparator.cc \
                                          concurrent_arena.cc \
                                          crc32c.cc \
                                          delete_scheduler.cc \
                                          dynamic_bloom.cc \
                                          env.cc \
										  env_chroot.cc \
                                          env_hdfs.cc \
                                          env_posix.cc \
										  event_logger.cc \
                                          file_reader_writer.cc \
                                          file_util.cc \
                                          filter_policy.cc \
                                          hash.cc \
                                          histogram.cc \
                                          histogram_windowing.cc \
                                          instrumented_mutex.cc \
                                          io_posix.cc \
                                          iostats_context.cc \
                                          log_buffer.cc \
                                          logging.cc \
                                          memenv.cc \
                                          mock_env.cc \
                                          murmurhash.cc \
                                          mutable_cf_options.cc \
                                          options_builder.cc \
                                          options.cc \
                                          options_helper.cc \
                                          options_parser.cc \
                                          options_sanity_check.cc \
                                          perf_context.cc \
                                          perf_level.cc \
                                          random.cc \
                                          rate_limiter.cc \
                                          slice.cc \
                                          sst_file_manager_impl.cc \
                                          statistics.cc \
                                          status.cc \
                                          status_message.cc \
                                          string_util.cc \
                                          sync_point.cc \
                                          thread_local.cc \
                                          thread_posix.cc \
                                          thread_status_impl.cc \
                                          thread_status_updater.cc \
                                          thread_status_updater_debug.cc \
                                          thread_status_util.cc \
                                          thread_status_util_debug.cc \
                                          xfunc.cc \
                                          xxhash.cc \
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
