# $Id: Makefile.am,v 1.0.0-0 2015/05/26 18:48:00 Cnangel Exp $

AUTOMAKE_OPTIONS					= foreign

EXTRA_DIST							= $(NULL)

SUBDIRS								= util \
									  . \
									  $(NULL)

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
									  -I$(top_srcdir)/utilities/redis \
									  $(GTEST_CFLAGS) \
									  $(GFLAGS_CFLAGS) \
									  -Wno-deprecated \
									  $(NULL)

# /* {{{ check_PROGRAMS				= */
check_PROGRAMS						= bloom_test \
                                      arena_test \
                                      autovector_test \
                                      cache_test \
                                      coding_test \
                                      crc32c_test \
                                      delete_scheduler_test \
                                      dynamic_bloom_test \
                                      env_test \
                                      event_logger_test \
                                      filelock_test \
                                      file_reader_writer_test \
                                      heap_test \
                                      histogram_test \
                                      iostats_context_test \
                                      memenv_test \
                                      mock_env_test \
                                      options_settable_test \
                                      options_test \
                                      rate_limiter_test \
                                      slice_transform_test \
                                      thread_list_test \
                                      thread_local_test \
                                      log_test \
                                      filename_test \
                                      manual_compaction_test \
                                      listener_test \
                                      perf_context_test \
                                      db_test2 \
                                      db_universal_compaction_test \
                                      compaction_picker_test \
                                      db_test \
                                      db_inplace_update_test \
                                      write_batch_test \
                                      flush_job_test \
                                      compaction_job_stats_test \
                                      plain_table_db_test \
                                      skiplist_test \
                                      db_iter_test \
                                      memtable_list_test \
                                      merge_test \
                                      file_indexer_test \
                                      db_properties_test \
                                      cuckoo_table_db_test \
                                      merge_helper_test \
                                      db_tailing_iter_test \
                                      db_dynamic_level_test \
                                      corruption_test \
                                      db_sst_test \
                                      options_file_test \
                                      write_callback_test \
                                      db_compaction_test \
                                      inlineskiplist_test \
                                      fault_injection_test \
                                      comparator_db_test \
                                      db_iterator_test \
                                      write_controller_test \
                                      db_block_cache_test \
                                      repair_test \
                                      compaction_job_test \
                                      wal_manager_test \
                                      version_set_test \
                                      column_family_test \
                                      db_table_properties_test \
                                      db_log_iter_test \
                                      compaction_iterator_test \
                                      db_bloom_filter_test \
                                      version_builder_test \
                                      dbformat_test \
                                      compact_files_test \
                                      db_compaction_filter_test \
                                      table_properties_collector_test \
                                      deletefile_test \
                                      version_edit_test \
                                      prefix_test \
                                      db_wal_test \
                                      auto_roll_logger_test \
                                      merger_test \
                                      table_test \
                                      block_test \
                                      cuckoo_table_reader_test \
                                      cuckoo_table_builder_test \
                                      block_hash_index_test \
                                      block_based_filter_block_test \
                                      full_filter_block_test \
                                      compact_on_deletion_collector_test \
                                      redis_lists_test \
                                      backupable_db_test \
                                      ttl_test \
                                      transaction_test \
                                      optimistic_transaction_test \
                                      document_db_test \
                                      json_document_test \
                                      spatial_db_test \
                                      write_batch_with_index_test \
                                      memory_test \
                                      env_mirror_test \
                                      stringappend_test \
                                      geodb_test \
                                      options_util_test \
                                      checkpoint_test \
                                      $(NULL)
# /* }}} */

bloom_test_SOURCES					= bloom_test.cc
arena_test_SOURCES					= arena_test.cc 
autovector_test_SOURCES				= autovector_test.cc
cache_test_SOURCES					= cache_test.cc
coding_test_SOURCES					= coding_test.cc
crc32c_test_SOURCES					= crc32c_test.cc
delete_scheduler_test_SOURCES		= delete_scheduler_test.cc
dynamic_bloom_test_SOURCES			= dynamic_bloom_test.cc
env_test_SOURCES					= env_test.cc
event_logger_test_SOURCES			= event_logger_test.cc
filelock_test_SOURCES				= filelock_test.cc
file_reader_writer_test_SOURCES		= file_reader_writer_test.cc
heap_test_SOURCES					= heap_test.cc
histogram_test_SOURCES				= histogram_test.cc
iostats_context_test_SOURCES		= iostats_context_test.cc
memenv_test_SOURCES					= memenv_test.cc
mock_env_test_SOURCES				= mock_env_test.cc
options_settable_test_SOURCES		= options_settable_test.cc
options_test_SOURCES				= options_test.cc
rate_limiter_test_SOURCES			= rate_limiter_test.cc
slice_transform_test_SOURCES		= slice_transform_test.cc
thread_list_test_SOURCES			= thread_list_test.cc
thread_local_test_SOURCES			= thread_local_test.cc
log_test_SOURCES					= log_test.cc
filename_test_SOURCES				= filename_test.cc
manual_compaction_test_SOURCES		= manual_compaction_test.cc
listener_test_SOURCES				= listener_test.cc
perf_context_test_SOURCES			= perf_context_test.cc
db_test2_SOURCES					= db_test2.cc
db_universal_compaction_test_SOURCES= db_universal_compaction_test.cc
compaction_picker_test_SOURCES		= compaction_picker_test.cc
db_test_SOURCES						= db_test.cc
db_inplace_update_test_SOURCES		= db_inplace_update_test.cc
write_batch_test_SOURCES			= write_batch_test.cc
flush_job_test_SOURCES				= flush_job_test.cc
compaction_job_stats_test_SOURCES	= compaction_job_stats_test.cc
plain_table_db_test_SOURCES			= plain_table_db_test.cc
skiplist_test_SOURCES				= skiplist_test.cc
db_iter_test_SOURCES				= db_iter_test.cc
memtable_list_test_SOURCES			= memtable_list_test.cc
merge_test_SOURCES					= merge_test.cc
file_indexer_test_SOURCES			= file_indexer_test.cc
db_properties_test_SOURCES			= db_properties_test.cc
cuckoo_table_db_test_SOURCES		= cuckoo_table_db_test.cc
merge_helper_test_SOURCES			= merge_helper_test.cc
db_tailing_iter_test_SOURCES		= db_tailing_iter_test.cc
db_dynamic_level_test_SOURCES		= db_dynamic_level_test.cc
corruption_test_SOURCES				= corruption_test.cc
db_sst_test_SOURCES					= db_sst_test.cc
options_file_test_SOURCES			= options_file_test.cc
write_callback_test_SOURCES			= write_callback_test.cc
db_compaction_test_SOURCES			= db_compaction_test.cc
inlineskiplist_test_SOURCES			= inlineskiplist_test.cc
fault_injection_test_SOURCES		= fault_injection_test.cc
comparator_db_test_SOURCES			= comparator_db_test.cc
db_iterator_test_SOURCES			= db_iterator_test.cc
write_controller_test_SOURCES		= write_controller_test.cc
db_block_cache_test_SOURCES			= db_block_cache_test.cc
repair_test_SOURCES					= repair_test.cc
compaction_job_test_SOURCES			= compaction_job_test.cc
wal_manager_test_SOURCES			= wal_manager_test.cc
version_set_test_SOURCES			= version_set_test.cc
column_family_test_SOURCES			= column_family_test.cc
db_table_properties_test_SOURCES	= db_table_properties_test.cc
db_log_iter_test_SOURCES			= db_log_iter_test.cc
compaction_iterator_test_SOURCES	= compaction_iterator_test.cc
db_bloom_filter_test_SOURCES		= db_bloom_filter_test.cc
version_builder_test_SOURCES		= version_builder_test.cc
dbformat_test_SOURCES				= dbformat_test.cc
compact_files_test_SOURCES			= compact_files_test.cc
db_compaction_filter_test_SOURCES	= db_compaction_filter_test.cc
table_properties_collector_test_SOURCES	= table_properties_collector_test.cc
deletefile_test_SOURCES				= deletefile_test.cc
version_edit_test_SOURCES			= version_edit_test.cc
prefix_test_SOURCES					= prefix_test.cc
db_wal_test_SOURCES					= db_wal_test.cc
auto_roll_logger_test_SOURCES		= auto_roll_logger_test.cc
merger_test_SOURCES					= merger_test.cc
table_test_SOURCES					= table_test.cc
block_test_SOURCES					= block_test.cc
cuckoo_table_reader_test_SOURCES	= cuckoo_table_reader_test.cc
cuckoo_table_builder_test_SOURCES	= cuckoo_table_builder_test.cc
block_hash_index_test_SOURCES		= block_hash_index_test.cc
block_based_filter_block_test_SOURCES	= block_based_filter_block_test.cc
full_filter_block_test_SOURCES		= full_filter_block_test.cc
compact_on_deletion_collector_test_SOURCES	= compact_on_deletion_collector_test.cc
redis_lists_test_SOURCES			= redis_lists_test.cc
backupable_db_test_SOURCES			= backupable_db_test.cc
ttl_test_SOURCES					= ttl_test.cc
transaction_test_SOURCES			= transaction_test.cc
optimistic_transaction_test_SOURCES	= optimistic_transaction_test.cc
document_db_test_SOURCES			= document_db_test.cc
json_document_test_SOURCES			= json_document_test.cc
spatial_db_test_SOURCES				= spatial_db_test.cc
write_batch_with_index_test_SOURCES	= write_batch_with_index_test.cc
memory_test_SOURCES					= memory_test.cc
env_mirror_test_SOURCES				= env_mirror_test.cc
stringappend_test_SOURCES			= stringappend_test.cc
geodb_test_SOURCES					= geodb_test.cc
options_util_test_SOURCES			= options_util_test.cc
checkpoint_test_SOURCES				= checkpoint_test.cc

TESTS_ENVIRONMENT					= LD_LIBRARY_PATH=/home/a/lib:/usr/local/lib:/usr/lib@BITS@ HEAPCHECK=normal
TESTS								= $(check_PROGRAMS)

# /*
#  * Local variables:
#  * tab-width: 4
#  * c-basic-offset: 4
#  * End:
#  * vim600: noet sw=4 ts=4 fdm=marker
#  * vim<600: noet sw=4 ts=4
#  */

