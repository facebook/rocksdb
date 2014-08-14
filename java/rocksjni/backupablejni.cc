// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
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

#include "include/org_rocksdb_BackupableDB.h"
#include "include/org_rocksdb_BackupableDBOptions.h"
#include "rocksjni/portal.h"
#include "rocksdb/utilities/backupable_db.h"

/*
 * Class:     org_rocksdb_BackupableDB
 * Method:    open
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupableDB_open(
    JNIEnv* env, jobject jbdb, jlong jdb_handle, jlong jopt_handle) {
  auto db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto opt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jopt_handle);
  auto bdb = new rocksdb::BackupableDB(db, *opt);

  // as BackupableDB extends RocksDB on the java side, we can reuse
  // the RocksDB portal here.
  rocksdb::RocksDBJni::setHandle(env, jbdb, bdb);
}

/*
 * Class:     org_rocksdb_BackupableDB
 * Method:    createNewBackup
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupableDB_createNewBackup(
    JNIEnv* env, jobject jbdb, jlong jhandle, jboolean jflag) {
  rocksdb::Status s =
      reinterpret_cast<rocksdb::BackupableDB*>(jhandle)->CreateNewBackup(jflag);
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_BackupableDB
 * Method:    purgeOldBackups
 * Signature: (JI)V
 */
void Java_org_rocksdb_BackupableDB_purgeOldBackups(
    JNIEnv* env, jobject jbdb, jlong jhandle, jboolean jnumBackupsToKeep) {
  rocksdb::Status s =
      reinterpret_cast<rocksdb::BackupableDB*>(jhandle)->
      PurgeOldBackups(jnumBackupsToKeep);
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

///////////////////////////////////////////////////////////////////////////
// BackupDBOptions

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    newBackupableDBOptions
 * Signature: (Ljava/lang/String;)V
 */
void Java_org_rocksdb_BackupableDBOptions_newBackupableDBOptions(
    JNIEnv* env, jobject jobj, jstring jpath, jboolean jshare_table_files,
    jboolean jsync, jboolean jdestroy_old_data, jboolean jbackup_log_files,
    jlong jbackup_rate_limit, jlong jrestore_rate_limit) {
  jbackup_rate_limit = (jbackup_rate_limit <= 0) ? 0 : jbackup_rate_limit;
  jrestore_rate_limit = (jrestore_rate_limit <= 0) ? 0 : jrestore_rate_limit;

  const char* cpath = env->GetStringUTFChars(jpath, 0);

  auto bopt = new rocksdb::BackupableDBOptions(cpath, nullptr,
      jshare_table_files, nullptr, jsync, jdestroy_old_data, jbackup_log_files,
      jbackup_rate_limit, jrestore_rate_limit);

  env->ReleaseStringUTFChars(jpath, cpath);

  rocksdb::BackupableDBOptionsJni::setHandle(env, jobj, bopt);
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    backupDir
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_BackupableDBOptions_backupDir(
    JNIEnv* env, jobject jopt, jlong jhandle, jstring jpath) {
  auto bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return env->NewStringUTF(bopt->backup_dir.c_str());
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

  rocksdb::BackupableDBOptionsJni::setHandle(env, jopt, nullptr);
}
