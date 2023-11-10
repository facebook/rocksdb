//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include "include/org_rocksdb_PerfContext.h"
#include "rocksdb/db.h"
#include "rocksdb/perf_context.h"

void Java_org_rocksdb_PerfContext_reset(JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  perf_context->Reset();
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getUserKeyComparisonCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getUserKeyComparisonCount(JNIEnv*, jobject,
                                                             jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->user_key_comparison_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlockCacheHitCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlockCacheHitCount(JNIEnv*, jobject,
                                                         jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->block_cache_hit_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlockReadCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlockReadCount(JNIEnv*, jobject,
                                                     jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->block_read_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlockCacheIndexHitCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlockCacheIndexHitCount(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->block_cache_index_hit_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlockCacheStandaloneHandleCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlockCacheStandaloneHandleCount(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->block_cache_standalone_handle_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlockCacheRealHandleCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlockCacheRealHandleCount(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->block_cache_real_handle_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getIndexBlockReadCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getIndexBlockReadCount(JNIEnv*, jobject,
                                                          jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->index_block_read_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlockCacheFilterHitCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlockCacheFilterHitCount(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->block_cache_filter_hit_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getFilterBlockReadCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getFilterBlockReadCount(JNIEnv*, jobject,
                                                           jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->filter_block_read_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getCompressionDictBlockReadCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getCompressionDictBlockReadCount(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->compression_dict_block_read_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlockReadByte
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlockReadByte(JNIEnv*, jobject,
                                                    jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->block_read_byte;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlockReadTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlockReadTime(JNIEnv*, jobject,
                                                    jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->block_read_time;
}

jlong Java_org_rocksdb_PerfContext_getBlockReadCpuTime(JNIEnv*, jobject,
                                                       jlong jpc_handler) {
  // reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handler);
  return perf_context->block_read_cpu_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getSecondaryCacheHitCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getSecondaryCacheHitCount(JNIEnv*, jobject,
                                                             jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->secondary_cache_hit_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getCompressedSecCacheInsertRealCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getCompressedSecCacheInsertRealCount(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->compressed_sec_cache_insert_real_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getCompressedSecCacheInsertDummyCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getCompressedSecCacheInsertDummyCount(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->compressed_sec_cache_insert_dummy_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getCompressedSecCacheUncompressedBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getCompressedSecCacheUncompressedBytes(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->compressed_sec_cache_uncompressed_bytes;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getCompressedSecCacheCompressedBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getCompressedSecCacheCompressedBytes(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->compressed_sec_cache_compressed_bytes;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlockChecksumTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlockChecksumTime(JNIEnv*, jobject,
                                                        jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->block_checksum_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlockDecompressTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlockDecompressTime(JNIEnv*, jobject,
                                                          jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->block_decompress_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getReadBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getReadBytes(JNIEnv*, jobject,
                                                jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->get_read_bytes;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getMultigetReadBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getMultigetReadBytes(JNIEnv*, jobject,
                                                        jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->multiget_read_bytes;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getIterReadBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getIterReadBytes(JNIEnv*, jobject,
                                                    jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->iter_read_bytes;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlobCacheHitCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlobCacheHitCount(JNIEnv*, jobject,
                                                        jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->blob_cache_hit_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlobReadCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlobReadCount(JNIEnv*, jobject,
                                                    jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->blob_read_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlobReadByte
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlobReadByte(JNIEnv*, jobject,
                                                   jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->blob_read_byte;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlobReadTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlobReadTime(JNIEnv*, jobject,
                                                   jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->blob_read_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlobChecksumTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlobChecksumTime(JNIEnv*, jobject,
                                                       jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->blob_checksum_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlobDecompressTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlobDecompressTime(JNIEnv*, jobject,
                                                         jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->blob_decompress_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getInternal_key_skipped_count
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getInternalKeySkippedCount(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->internal_key_skipped_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getInternalDeleteSkippedCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getInternalDeleteSkippedCount(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->internal_delete_skipped_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getInternalRecentSkippedCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getInternalRecentSkippedCount(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->internal_recent_skipped_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getInternalMergeCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getInternalMergeCount(JNIEnv*, jobject,
                                                         jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->internal_merge_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getInternalMergePointLookupCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getInternalMergePointLookupCount(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->internal_merge_point_lookup_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getInternalRangeDelReseekCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getInternalRangeDelReseekCount(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->internal_range_del_reseek_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getSnapshotTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getSnapshotTime(JNIEnv*, jobject,
                                                   jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->get_snapshot_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getFromMemtableTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getFromMemtableTime(JNIEnv*, jobject,
                                                       jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->get_from_memtable_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getFromMemtableCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getFromMemtableCount(JNIEnv*, jobject,
                                                        jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->get_from_memtable_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getPostProcessTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getPostProcessTime(JNIEnv*, jobject,
                                                      jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->get_post_process_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getFromOutputFilesTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getFromOutputFilesTime(JNIEnv*, jobject,
                                                          jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->get_from_output_files_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getSeekOnMemtableTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getSeekOnMemtableTime(JNIEnv*, jobject,
                                                         jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->seek_on_memtable_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getSeekOnMemtableCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getSeekOnMemtableCount(JNIEnv*, jobject,
                                                          jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->seek_on_memtable_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getNextOnMemtableCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getNextOnMemtableCount(JNIEnv*, jobject,
                                                          jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->next_on_memtable_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getPrevOnMemtableCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getPrevOnMemtableCount(JNIEnv*, jobject,
                                                          jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->prev_on_memtable_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getSeekChildSeekTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getSeekChildSeekTime(JNIEnv*, jobject,
                                                        jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->seek_child_seek_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getSeekChildSeekCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getSeekChildSeekCount(JNIEnv*, jobject,
                                                         jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->seek_child_seek_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getSeekMinHeapTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getSeekMinHeapTime(JNIEnv*, jobject,
                                                      jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->seek_min_heap_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getSeekMaxHeapTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getSeekMaxHeapTime(JNIEnv*, jobject,
                                                      jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->seek_max_heap_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getSeekInternalSeekTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getSeekInternalSeekTime(JNIEnv*, jobject,
                                                           jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->seek_internal_seek_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getFindNextUserEntryTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getFindNextUserEntryTime(JNIEnv*, jobject,
                                                            jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->find_next_user_entry_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getWriteWalTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getWriteWalTime(JNIEnv*, jobject,
                                                   jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->write_wal_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getWriteMemtableTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getWriteMemtableTime(JNIEnv*, jobject,
                                                        jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->write_memtable_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getWriteDelayTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getWriteDelayTime(JNIEnv*, jobject,
                                                     jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->write_delay_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getWriteSchedulingFlushesCompactionsTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getWriteSchedulingFlushesCompactionsTime(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->write_scheduling_flushes_compactions_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getWritePreAndPostProcessTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getWritePreAndPostProcessTime(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->write_pre_and_post_process_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getWriteThreadWaitNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getWriteThreadWaitNanos(JNIEnv*, jobject,
                                                           jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->write_thread_wait_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getDbMutexLockNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getDbMutexLockNanos(JNIEnv*, jobject,
                                                       jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->db_mutex_lock_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getDbConditionWaitNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getDbConditionWaitNanos(JNIEnv*, jobject,
                                                           jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->db_condition_wait_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getMergeOperatorTimeNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getMergeOperatorTimeNanos(JNIEnv*, jobject,
                                                             jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->merge_operator_time_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getReadIndexBlockNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getReadIndexBlockNanos(JNIEnv*, jobject,
                                                          jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->read_index_block_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getReadFilterBlockNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getReadFilterBlockNanos(JNIEnv*, jobject,
                                                           jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->read_filter_block_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getNewTableBlockIterNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getNewTableBlockIterNanos(JNIEnv*, jobject,
                                                             jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->new_table_block_iter_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getNewTableIteratorNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getNewTableIteratorNanos(JNIEnv*, jobject,
                                                            jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->new_table_iterator_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBlockSeekNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBlockSeekNanos(JNIEnv*, jobject,
                                                     jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->block_seek_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getFindTableNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getFindTableNanos(JNIEnv*, jobject,
                                                     jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->find_table_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBloomMemtableHitCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBloomMemtableHitCount(JNIEnv*, jobject,
                                                            jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->bloom_memtable_hit_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBloomMemtableMissCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBloomMemtableMissCount(JNIEnv*, jobject,
                                                             jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->bloom_memtable_miss_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBloomSstHitCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBloomSstHitCount(JNIEnv*, jobject,
                                                       jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->bloom_sst_hit_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getBloomSstMissCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getBloomSstMissCount(JNIEnv*, jobject,
                                                        jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->bloom_sst_miss_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getKeyLockWaitTime
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getKeyLockWaitTime(JNIEnv*, jobject,
                                                      jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->key_lock_wait_time;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getKeyLockWaitCount
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getKeyLockWaitCount(JNIEnv*, jobject,
                                                       jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->key_lock_wait_count;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvNewSequentialFileNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvNewSequentialFileNanos(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_new_sequential_file_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvNewRandomAccessFileNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvNewRandomAccessFileNanos(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_new_random_access_file_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvNewWritableFileNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvNewWritableFileNanos(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_new_writable_file_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvReuseWritableFileNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvReuseWritableFileNanos(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_reuse_writable_file_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvNewRandomRwFileNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvNewRandomRwFileNanos(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_new_random_rw_file_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvNewDirectoryNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvNewDirectoryNanos(JNIEnv*, jobject,
                                                           jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_new_directory_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvFileExistsNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvFileExistsNanos(JNIEnv*, jobject,
                                                         jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_file_exists_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvGetChildrenNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvGetChildrenNanos(JNIEnv*, jobject,
                                                          jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_get_children_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvGetChildrenFileAttributesNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvGetChildrenFileAttributesNanos(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_get_children_file_attributes_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvDeleteFileNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvDeleteFileNanos(JNIEnv*, jobject,
                                                         jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_delete_file_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvCreateDirNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvCreateDirNanos(JNIEnv*, jobject,
                                                        jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_create_dir_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvCreateDirIfMissingNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvCreateDirIfMissingNanos(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_create_dir_if_missing_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvDeleteDirNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvDeleteDirNanos(JNIEnv*, jobject,
                                                        jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_delete_dir_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvGetFileSizeNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvGetFileSizeNanos(JNIEnv*, jobject,
                                                          jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_get_file_size_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvGetFileModificationTimeNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvGetFileModificationTimeNanos(
    JNIEnv*, jobject, jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_get_file_modification_time_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvRenameFileNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvRenameFileNanos(JNIEnv*, jobject,
                                                         jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_rename_file_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvLinkFileNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvLinkFileNanos(JNIEnv*, jobject,
                                                       jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_link_file_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvLockFileNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvLockFileNanos(JNIEnv*, jobject,
                                                       jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_lock_file_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvUnlockFileNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvUnlockFileNanos(JNIEnv*, jobject,
                                                         jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_unlock_file_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEnvNewLoggerNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEnvNewLoggerNanos(JNIEnv*, jobject,
                                                        jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->env_new_logger_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getCpuNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getGetCpuNanos(JNIEnv*, jobject,
                                                  jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->get_cpu_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getIterNextCpuNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getIterNextCpuNanos(JNIEnv*, jobject,
                                                       jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->iter_next_cpu_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getIterPrevCpuNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getIterPrevCpuNanos(JNIEnv*, jobject,
                                                       jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->iter_prev_cpu_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getIterSeekCpuNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getIterSeekCpuNanos(JNIEnv*, jobject,
                                                       jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->iter_seek_cpu_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getEncryptDataNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getEncryptDataNanos(JNIEnv*, jobject,
                                                       jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->encrypt_data_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getDecryptDataNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getDecryptDataNanos(JNIEnv*, jobject,
                                                       jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->decrypt_data_nanos;
}

/*
 * Class:     org_rocksdb_PerfContext
 * Method:    getNumberAsyncSeek
 * Signature: (J)J
 */
jlong Java_org_rocksdb_PerfContext_getNumberAsyncSeek(JNIEnv*, jobject,
                                                      jlong jpc_handle) {
  ROCKSDB_NAMESPACE::PerfContext* perf_context =
      reinterpret_cast<ROCKSDB_NAMESPACE::PerfContext*>(jpc_handle);
  return perf_context->number_async_seek;
}
