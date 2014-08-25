// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ for rocksdb::Options.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>
#include <memory>

#include "include/org_rocksdb_Options.h"
#include "include/org_rocksdb_WriteOptions.h"
#include "include/org_rocksdb_ReadOptions.h"
#include "rocksjni/portal.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/table.h"
#include "rocksdb/slice_transform.h"

/*
 * Class:     org_rocksdb_Options
 * Method:    newOptions
 * Signature: ()V
 */
void Java_org_rocksdb_Options_newOptions(JNIEnv* env, jobject jobj) {
  rocksdb::Options* op = new rocksdb::Options();
  rocksdb::OptionsJni::setHandle(env, jobj, op);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Options_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  delete reinterpret_cast<rocksdb::Options*>(handle);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCreateIfMissing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setCreateIfMissing(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean flag) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->create_if_missing = flag;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    createIfMissing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_createIfMissing(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->create_if_missing;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteBufferSize
 * Signature: (JJ)I
 */
void Java_org_rocksdb_Options_setWriteBufferSize(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong jwrite_buffer_size) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->write_buffer_size =
          static_cast<size_t>(jwrite_buffer_size);
}


/*
 * Class:     org_rocksdb_Options
 * Method:    writeBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writeBufferSize(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->write_buffer_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxWriteBufferNumber
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxWriteBufferNumber(
    JNIEnv* env, jobject jobj, jlong jhandle, jint jmax_write_buffer_number) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_write_buffer_number =
          jmax_write_buffer_number;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    createStatistics
 * Signature: (J)V
 */
void Java_org_rocksdb_Options_createStatistics(
    JNIEnv* env, jobject jobj, jlong jOptHandle) {
  reinterpret_cast<rocksdb::Options*>(jOptHandle)->statistics =
      rocksdb::CreateDBStatistics();
}

/*
 * Class:     org_rocksdb_Options
 * Method:    statisticsPtr
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_statisticsPtr(
    JNIEnv* env, jobject jobj, jlong jOptHandle) {
  auto st = reinterpret_cast<rocksdb::Options*>(jOptHandle)->statistics.get();
  return reinterpret_cast<jlong>(st);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxWriteBufferNumber
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxWriteBufferNumber(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_write_buffer_number;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    errorIfExists
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_errorIfExists(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->error_if_exists;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setErrorIfExists
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setErrorIfExists(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean error_if_exists) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->error_if_exists =
      static_cast<bool>(error_if_exists);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    paranoidChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_paranoidChecks(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->paranoid_checks;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setParanoidChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setParanoidChecks(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean paranoid_checks) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->paranoid_checks =
      static_cast<bool>(paranoid_checks);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxOpenFiles
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxOpenFiles(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_open_files;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxOpenFiles
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxOpenFiles(
    JNIEnv* env, jobject jobj, jlong jhandle, jint max_open_files) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_open_files =
      static_cast<int>(max_open_files);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    disableDataSync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_disableDataSync(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->disableDataSync;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDisableDataSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setDisableDataSync(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean disableDataSync) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->disableDataSync =
      static_cast<bool>(disableDataSync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useFsync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useFsync(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->use_fsync;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseFsync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseFsync(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean use_fsync) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->use_fsync =
      static_cast<bool>(use_fsync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbLogDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_dbLogDir(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<rocksdb::Options*>(jhandle)->db_log_dir.c_str());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDbLogDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_Options_setDbLogDir(
    JNIEnv* env, jobject jobj, jlong jhandle, jstring jdb_log_dir) {
  const char* log_dir = env->GetStringUTFChars(jdb_log_dir, 0);
  reinterpret_cast<rocksdb::Options*>(jhandle)->db_log_dir.assign(log_dir);
  env->ReleaseStringUTFChars(jdb_log_dir, log_dir);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_walDir(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<rocksdb::Options*>(jhandle)->wal_dir.c_str());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_Options_setWalDir(
    JNIEnv* env, jobject jobj, jlong jhandle, jstring jwal_dir) {
  const char* wal_dir = env->GetStringUTFChars(jwal_dir, 0);
  reinterpret_cast<rocksdb::Options*>(jhandle)->wal_dir.assign(wal_dir);
  env->ReleaseStringUTFChars(jwal_dir, wal_dir);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    deleteObsoleteFilesPeriodMicros
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_deleteObsoleteFilesPeriodMicros(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->delete_obsolete_files_period_micros;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDeleteObsoleteFilesPeriodMicros
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setDeleteObsoleteFilesPeriodMicros(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong micros) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->delete_obsolete_files_period_micros =
          static_cast<int64_t>(micros);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBackgroundCompactions(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->max_background_compactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBackgroundCompactions(
    JNIEnv* env, jobject jobj, jlong jhandle, jint max) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->max_background_compactions = static_cast<int>(max);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBackgroundFlushes
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBackgroundFlushes(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_background_flushes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBackgroundFlushes
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBackgroundFlushes(
    JNIEnv* env, jobject jobj, jlong jhandle, jint max_background_flushes) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_background_flushes =
      static_cast<int>(max_background_flushes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxLogFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxLogFileSize(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_log_file_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxLogFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxLogFileSize(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong max_log_file_size) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_log_file_size =
      static_cast<size_t>(max_log_file_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    logFileTimeToRoll
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_logFileTimeToRoll(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->log_file_time_to_roll;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLogFileTimeToRoll
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setLogFileTimeToRoll(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong log_file_time_to_roll) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->log_file_time_to_roll =
      static_cast<size_t>(log_file_time_to_roll);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    keepLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_keepLogFileNum(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->keep_log_file_num;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setKeepLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setKeepLogFileNum(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong keep_log_file_num) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->keep_log_file_num =
      static_cast<size_t>(keep_log_file_num);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxManifestFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxManifestFileSize(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_manifest_file_size;
}

/*
 * Method:    memTableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_memTableFactoryName(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  rocksdb::MemTableRepFactory* tf = opt->memtable_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  // temporarly fix for the historical typo
  if (strcmp(tf->Name(), "HashLinkListRepFactory") == 0) {
    return env->NewStringUTF("HashLinkedListRepFactory");
  }

  return env->NewStringUTF(tf->Name());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxManifestFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxManifestFileSize(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong max_manifest_file_size) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_manifest_file_size =
      static_cast<int64_t>(max_manifest_file_size);
}

/*
 * Method:    setMemTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMemTableFactory(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong jfactory_handle) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->memtable_factory.reset(
      reinterpret_cast<rocksdb::MemTableRepFactory*>(jfactory_handle));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    tableCacheNumshardbits
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_tableCacheNumshardbits(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->table_cache_numshardbits;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTableCacheNumshardbits
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setTableCacheNumshardbits(
    JNIEnv* env, jobject jobj, jlong jhandle, jint table_cache_numshardbits) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->table_cache_numshardbits =
      static_cast<int>(table_cache_numshardbits);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    tableCacheRemoveScanCountLimit
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_tableCacheRemoveScanCountLimit(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->table_cache_remove_scan_count_limit;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTableCacheRemoveScanCountLimit
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setTableCacheRemoveScanCountLimit(
    JNIEnv* env, jobject jobj, jlong jhandle, jint limit) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->table_cache_remove_scan_count_limit = static_cast<int>(limit);
}

/*
 * Method:    useFixedLengthPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_useFixedLengthPrefixExtractor(
    JNIEnv* env, jobject jobj, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->prefix_extractor.reset(
      rocksdb::NewFixedPrefixTransform(static_cast<size_t>(jprefix_length)));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_walTtlSeconds(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->WAL_ttl_seconds;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalTtlSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalTtlSeconds(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong WAL_ttl_seconds) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->WAL_ttl_seconds =
      static_cast<int64_t>(WAL_ttl_seconds);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_walSizeLimitMB(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->WAL_size_limit_MB;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalSizeLimitMB
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalSizeLimitMB(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong WAL_size_limit_MB) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->WAL_size_limit_MB =
      static_cast<int64_t>(WAL_size_limit_MB);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    manifestPreallocationSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_manifestPreallocationSize(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->manifest_preallocation_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setManifestPreallocationSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setManifestPreallocationSize(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong preallocation_size) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->manifest_preallocation_size =
      static_cast<size_t>(preallocation_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowOsBuffer
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowOsBuffer(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->allow_os_buffer;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowOsBuffer
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowOsBuffer(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean allow_os_buffer) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->allow_os_buffer =
      static_cast<bool>(allow_os_buffer);
}

/*
 * Method:    setTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setTableFactory(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong jfactory_handle) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->table_factory.reset(
      reinterpret_cast<rocksdb::TableFactory*>(jfactory_handle));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowMmapReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowMmapReads(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->allow_mmap_reads;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowMmapReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowMmapReads(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean allow_mmap_reads) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->allow_mmap_reads =
      static_cast<bool>(allow_mmap_reads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowMmapWrites
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowMmapWrites(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->allow_mmap_writes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowMmapWrites
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowMmapWrites(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean allow_mmap_writes) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->allow_mmap_writes =
      static_cast<bool>(allow_mmap_writes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    isFdCloseOnExec
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_isFdCloseOnExec(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->is_fd_close_on_exec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setIsFdCloseOnExec
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setIsFdCloseOnExec(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean is_fd_close_on_exec) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->is_fd_close_on_exec =
      static_cast<bool>(is_fd_close_on_exec);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    skipLogErrorOnRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_skipLogErrorOnRecovery(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->skip_log_error_on_recovery;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSkipLogErrorOnRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setSkipLogErrorOnRecovery(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean skip) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->skip_log_error_on_recovery =
      static_cast<bool>(skip);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    statsDumpPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_statsDumpPeriodSec(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->stats_dump_period_sec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStatsDumpPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setStatsDumpPeriodSec(
    JNIEnv* env, jobject jobj, jlong jhandle, jint stats_dump_period_sec) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->stats_dump_period_sec =
      static_cast<int>(stats_dump_period_sec);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    adviseRandomOnOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_adviseRandomOnOpen(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->advise_random_on_open;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAdviseRandomOnOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAdviseRandomOnOpen(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean advise_random_on_open) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->advise_random_on_open =
      static_cast<bool>(advise_random_on_open);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useAdaptiveMutex
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useAdaptiveMutex(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->use_adaptive_mutex;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseAdaptiveMutex
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseAdaptiveMutex(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean use_adaptive_mutex) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->use_adaptive_mutex =
      static_cast<bool>(use_adaptive_mutex);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_bytesPerSync(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->bytes_per_sync;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setBytesPerSync(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong bytes_per_sync) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->bytes_per_sync =
      static_cast<int64_t>(bytes_per_sync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowThreadLocal
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowThreadLocal(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->allow_thread_local;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowThreadLocal
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowThreadLocal(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean allow_thread_local) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->allow_thread_local =
      static_cast<bool>(allow_thread_local);
}

/*
 * Method:    tableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_tableFactoryName(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  rocksdb::TableFactory* tf = opt->table_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  return env->NewStringUTF(tf->Name());
}


/*
 * Class:     org_rocksdb_Options
 * Method:    minWriteBufferNumberToMerge
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_minWriteBufferNumberToMerge(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->min_write_buffer_number_to_merge;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMinWriteBufferNumberToMerge
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMinWriteBufferNumberToMerge(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jmin_write_buffer_number_to_merge) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->min_write_buffer_number_to_merge =
          static_cast<int>(jmin_write_buffer_number_to_merge);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setCompressionType(
    JNIEnv* env, jobject jobj, jlong jhandle, jbyte compression) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->compression =
      static_cast<rocksdb::CompressionType>(compression);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_compressionType(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->compression;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionStyle
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setCompactionStyle(
    JNIEnv* env, jobject jobj, jlong jhandle, jbyte compaction_style) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->compaction_style =
      static_cast<rocksdb::CompactionStyle>(compaction_style);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compactionStyle
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_compactionStyle(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->compaction_style;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    numLevels
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_numLevels(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->num_levels;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setNumLevels
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setNumLevels(
    JNIEnv* env, jobject jobj, jlong jhandle, jint jnum_levels) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->num_levels =
      static_cast<int>(jnum_levels);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelZeroFileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_levelZeroFileNumCompactionTrigger(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelZeroFileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevelZeroFileNumCompactionTrigger(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->level0_file_num_compaction_trigger =
          static_cast<int>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelZeroSlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_levelZeroSlowdownWritesTrigger(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelSlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevelZeroSlowdownWritesTrigger(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->level0_slowdown_writes_trigger =
          static_cast<int>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelZeroStopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_levelZeroStopWritesTrigger(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelStopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevelZeroStopWritesTrigger(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->level0_stop_writes_trigger =
      static_cast<int>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxMemCompactionLevel
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxMemCompactionLevel(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->max_mem_compaction_level;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxMemCompactionLevel
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxMemCompactionLevel(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jmax_mem_compaction_level) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_mem_compaction_level =
      static_cast<int>(jmax_mem_compaction_level);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    targetFileSizeBase
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_targetFileSizeBase(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->target_file_size_base;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTargetFileSizeBase
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setTargetFileSizeBase(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jtarget_file_size_base) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->target_file_size_base =
      static_cast<int>(jtarget_file_size_base);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    targetFileSizeMultiplier
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_targetFileSizeMultiplier(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->target_file_size_multiplier;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTargetFileSizeMultiplier
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setTargetFileSizeMultiplier(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jtarget_file_size_multiplier) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->target_file_size_multiplier =
          static_cast<int>(jtarget_file_size_multiplier);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBytesForLevelBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxBytesForLevelBase(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->max_bytes_for_level_base;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBytesForLevelBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxBytesForLevelBase(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jlong jmax_bytes_for_level_base) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->max_bytes_for_level_base =
          static_cast<int64_t>(jmax_bytes_for_level_base);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBytesForLevelMultiplier
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBytesForLevelMultiplier(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->max_bytes_for_level_multiplier;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBytesForLevelMultiplier
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBytesForLevelMultiplier(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jmax_bytes_for_level_multiplier) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->max_bytes_for_level_multiplier =
          static_cast<int>(jmax_bytes_for_level_multiplier);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    expandedCompactionFactor
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_expandedCompactionFactor(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->expanded_compaction_factor;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setExpandedCompactionFactor
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setExpandedCompactionFactor(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jexpanded_compaction_factor) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->expanded_compaction_factor =
          static_cast<int>(jexpanded_compaction_factor);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    sourceCompactionFactor
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_sourceCompactionFactor(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->source_compaction_factor;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSourceCompactionFactor
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setSourceCompactionFactor(
    JNIEnv* env, jobject jobj, jlong jhandle,
        jint jsource_compaction_factor) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->source_compaction_factor =
          static_cast<int>(jsource_compaction_factor);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxGrandparentOverlapFactor
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxGrandparentOverlapFactor(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->max_grandparent_overlap_factor;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxGrandparentOverlapFactor
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxGrandparentOverlapFactor(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jmax_grandparent_overlap_factor) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->max_grandparent_overlap_factor =
          static_cast<int>(jmax_grandparent_overlap_factor);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    softRateLimit
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_Options_softRateLimit(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->soft_rate_limit;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSoftRateLimit
 * Signature: (JD)V
 */
void Java_org_rocksdb_Options_setSoftRateLimit(
    JNIEnv* env, jobject jobj, jlong jhandle, jdouble jsoft_rate_limit) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->soft_rate_limit =
      static_cast<double>(jsoft_rate_limit);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    hardRateLimit
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_Options_hardRateLimit(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->hard_rate_limit;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setHardRateLimit
 * Signature: (JD)V
 */
void Java_org_rocksdb_Options_setHardRateLimit(
    JNIEnv* env, jobject jobj, jlong jhandle, jdouble jhard_rate_limit) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->hard_rate_limit =
      static_cast<double>(jhard_rate_limit);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    rateLimitDelayMaxMilliseconds
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_rateLimitDelayMaxMilliseconds(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->rate_limit_delay_max_milliseconds;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setRateLimitDelayMaxMilliseconds
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setRateLimitDelayMaxMilliseconds(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jrate_limit_delay_max_milliseconds) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->rate_limit_delay_max_milliseconds =
          static_cast<int>(jrate_limit_delay_max_milliseconds);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    arenaBlockSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_arenaBlockSize(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->arena_block_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setArenaBlockSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setArenaBlockSize(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong jarena_block_size) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->arena_block_size =
      static_cast<size_t>(jarena_block_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    disableAutoCompactions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_disableAutoCompactions(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->disable_auto_compactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDisableAutoCompactions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setDisableAutoCompactions(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jboolean jdisable_auto_compactions) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->disable_auto_compactions =
          static_cast<bool>(jdisable_auto_compactions);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    purgeRedundantKvsWhileFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_purgeRedundantKvsWhileFlush(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->purge_redundant_kvs_while_flush;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setPurgeRedundantKvsWhileFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setPurgeRedundantKvsWhileFlush(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jboolean jpurge_redundant_kvs_while_flush) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->purge_redundant_kvs_while_flush =
          static_cast<bool>(jpurge_redundant_kvs_while_flush);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    verifyChecksumsInCompaction
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_verifyChecksumsInCompaction(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->verify_checksums_in_compaction;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setVerifyChecksumsInCompaction
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setVerifyChecksumsInCompaction(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jboolean jverify_checksums_in_compaction) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->verify_checksums_in_compaction =
          static_cast<bool>(jverify_checksums_in_compaction);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    filterDeletes
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_filterDeletes(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->filter_deletes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setFilterDeletes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setFilterDeletes(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean jfilter_deletes) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->filter_deletes =
      static_cast<bool>(jfilter_deletes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxSequentialSkipInIterations
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxSequentialSkipInIterations(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->max_sequential_skip_in_iterations;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxSequentialSkipInIterations
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxSequentialSkipInIterations(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jlong jmax_sequential_skip_in_iterations) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->max_sequential_skip_in_iterations =
          static_cast<int64_t>(jmax_sequential_skip_in_iterations);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    inplaceUpdateSupport
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_inplaceUpdateSupport(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->inplace_update_support;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setInplaceUpdateSupport
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setInplaceUpdateSupport(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jboolean jinplace_update_support) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->inplace_update_support =
          static_cast<bool>(jinplace_update_support);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    inplaceUpdateNumLocks
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_inplaceUpdateNumLocks(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->inplace_update_num_locks;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setInplaceUpdateNumLocks
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setInplaceUpdateNumLocks(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jlong jinplace_update_num_locks) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->inplace_update_num_locks =
          static_cast<size_t>(jinplace_update_num_locks);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    memtablePrefixBloomBits
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_memtablePrefixBloomBits(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->memtable_prefix_bloom_bits;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMemtablePrefixBloomBits
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMemtablePrefixBloomBits(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jmemtable_prefix_bloom_bits) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->memtable_prefix_bloom_bits =
          static_cast<int32_t>(jmemtable_prefix_bloom_bits);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    memtablePrefixBloomProbes
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_memtablePrefixBloomProbes(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->memtable_prefix_bloom_probes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMemtablePrefixBloomProbes
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMemtablePrefixBloomProbes(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jmemtable_prefix_bloom_probes) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->memtable_prefix_bloom_probes =
          static_cast<int32_t>(jmemtable_prefix_bloom_probes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bloomLocality
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_bloomLocality(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->bloom_locality;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBloomLocality
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setBloomLocality(
    JNIEnv* env, jobject jobj, jlong jhandle, jint jbloom_locality) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->bloom_locality =
      static_cast<int32_t>(jbloom_locality);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxSuccessiveMerges
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxSuccessiveMerges(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_successive_merges;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxSuccessiveMerges
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxSuccessiveMerges(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jlong jmax_successive_merges) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_successive_merges =
      static_cast<size_t>(jmax_successive_merges);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    minPartialMergeOperands
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_minPartialMergeOperands(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(
      jhandle)->min_partial_merge_operands;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMinPartialMergeOperands
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMinPartialMergeOperands(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint jmin_partial_merge_operands) {
  reinterpret_cast<rocksdb::Options*>(
      jhandle)->min_partial_merge_operands =
          static_cast<int32_t>(jmin_partial_merge_operands);
}

//////////////////////////////////////////////////////////////////////////////
// WriteOptions

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    newWriteOptions
 * Signature: ()V
 */
void Java_org_rocksdb_WriteOptions_newWriteOptions(
    JNIEnv* env, jobject jwrite_options) {
  rocksdb::WriteOptions* op = new rocksdb::WriteOptions();
  rocksdb::WriteOptionsJni::setHandle(env, jwrite_options, op);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    disposeInternal
 * Signature: ()V
 */
void Java_org_rocksdb_WriteOptions_disposeInternal(
    JNIEnv* env, jobject jwrite_options, jlong jhandle) {
  auto write_options = reinterpret_cast<rocksdb::WriteOptions*>(jhandle);
  delete write_options;

  rocksdb::WriteOptionsJni::setHandle(env, jwrite_options, nullptr);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setSync(
  JNIEnv* env, jobject jwrite_options, jlong jhandle, jboolean jflag) {
  reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->sync = jflag;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    sync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_sync(
    JNIEnv* env, jobject jwrite_options, jlong jhandle) {
  return reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->sync;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setDisableWAL
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setDisableWAL(
    JNIEnv* env, jobject jwrite_options, jlong jhandle, jboolean jflag) {
  reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->disableWAL = jflag;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    disableWAL
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_disableWAL(
    JNIEnv* env, jobject jwrite_options, jlong jhandle) {
  return reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->disableWAL;
}

/////////////////////////////////////////////////////////////////////
// rocksdb::ReadOptions

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    newReadOptions
 * Signature: ()V
 */
void Java_org_rocksdb_ReadOptions_newReadOptions(
    JNIEnv* env, jobject jobj) {
  auto read_opt = new rocksdb::ReadOptions();
  rocksdb::ReadOptionsJni::setHandle(env, jobj, read_opt);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ReadOptions_disposeInternal(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  delete reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  rocksdb::ReadOptionsJni::setHandle(env, jobj, nullptr);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    verifyChecksums
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_verifyChecksums(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::ReadOptions*>(
      jhandle)->verify_checksums;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setVerifyChecksums
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setVerifyChecksums(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jboolean jverify_checksums) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->verify_checksums =
      static_cast<bool>(jverify_checksums);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    fillCache
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_fillCache(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->fill_cache;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setFillCache
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setFillCache(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean jfill_cache) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->fill_cache =
      static_cast<bool>(jfill_cache);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    tailing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_tailing(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  return reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->tailing;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setTailing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setTailing(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean jtailing) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->tailing =
      static_cast<bool>(jtailing);
}
