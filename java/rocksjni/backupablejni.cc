// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::BackupableDB and rocksdb::BackupableDBOptions methods
// from Java side.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>
#include <vector>

#include "include/org_rocksdb_BackupableDBOptions.h"
#include "rocksjni/portal.h"
#include "rocksdb/utilities/backupable_db.h"

///////////////////////////////////////////////////////////////////////////
// BackupDBOptions

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    newBackupableDBOptions
 * Signature: (Ljava/lang/String;)J
 */
jlong Java_org_rocksdb_BackupableDBOptions_newBackupableDBOptions(
    JNIEnv* env, jclass jcls, jstring jpath) {
  const char* cpath = env->GetStringUTFChars(jpath, NULL);
  auto bopt = new rocksdb::BackupableDBOptions(cpath);
  env->ReleaseStringUTFChars(jpath, cpath);
  return reinterpret_cast<jlong>(bopt);
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    backupDir
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_BackupableDBOptions_backupDir(
    JNIEnv* env, jobject jopt, jlong jhandle) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return env->NewStringUTF(bopt->backup_dir.c_str());
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setShareTableFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setShareTableFiles(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean flag) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->share_table_files = flag;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    shareTableFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupableDBOptions_shareTableFiles(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->share_table_files;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setSync(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean flag) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->sync = flag;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    sync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupableDBOptions_sync(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->sync;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setDestroyOldData
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setDestroyOldData(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean flag) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->destroy_old_data = flag;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    destroyOldData
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupableDBOptions_destroyOldData(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->destroy_old_data;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setBackupLogFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setBackupLogFiles(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean flag) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->backup_log_files = flag;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    backupLogFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupableDBOptions_backupLogFiles(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->backup_log_files;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setBackupRateLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setBackupRateLimit(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong jbackup_rate_limit) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->backup_rate_limit = jbackup_rate_limit;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    backupRateLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_BackupableDBOptions_backupRateLimit(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->backup_rate_limit;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setRestoreRateLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setRestoreRateLimit(
    JNIEnv* env, jobject jobj, jlong jhandle, jlong jrestore_rate_limit) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->restore_rate_limit = jrestore_rate_limit;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    restoreRateLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_BackupableDBOptions_restoreRateLimit(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->restore_rate_limit;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setShareFilesWithChecksum
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setShareFilesWithChecksum(
    JNIEnv* env, jobject jobj, jlong jhandle, jboolean flag) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->share_files_with_checksum = flag;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    shareFilesWithChecksum
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupableDBOptions_shareFilesWithChecksum(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->share_files_with_checksum;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_BackupableDBOptions_disposeInternal(
    JNIEnv* env, jobject jopt, jlong jhandle) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  assert(bopt);
  delete bopt;
}
