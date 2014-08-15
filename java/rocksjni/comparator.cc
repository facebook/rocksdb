// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::Comparator.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>
#include <functional>

#include "include/org_rocksdb_AbstractComparator.h"
#include "include/org_rocksdb_Comparator.h"
#include "include/org_rocksdb_DirectComparator.h"
#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/portal.h"

//<editor-fold desc="org.rocksdb.ComparatorOptions">

void Java_org_rocksdb_ComparatorOptions_newComparatorOptions(
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

//</editor-fold>

//<editor-fold desc="org.rocksdb.AbstractComparator>

/*
 * Class:     org_rocksdb_AbstractComparator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_AbstractComparator_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  delete reinterpret_cast<rocksdb::BaseComparatorJniCallback*>(handle);
}

//</editor-fold>

//<editor-fold desc="org.rocksdb.Comparator>

/*
 * Class:     org_rocksdb_Comparator
 * Method:    createNewComparator0
 * Signature: ()V
 */
void Java_org_rocksdb_Comparator_createNewComparator0(
    JNIEnv* env, jobject jobj, jlong copt_handle) {
  const rocksdb::ComparatorJniCallbackOptions* copt = reinterpret_cast<rocksdb::ComparatorJniCallbackOptions*>(copt_handle);
  const rocksdb::ComparatorJniCallback* c = new rocksdb::ComparatorJniCallback(env, jobj, copt);
  rocksdb::AbstractComparatorJni::setHandle(env, jobj, c);
}

//</editor-fold>

//<editor-fold desc="org.rocksdb.DirectComparator>

/*
 * Class:     org_rocksdb_DirectComparator
 * Method:    createNewDirectComparator0
 * Signature: ()V
 */
void Java_org_rocksdb_DirectComparator_createNewDirectComparator0(
    JNIEnv* env, jobject jobj, jlong copt_handle) {
  const rocksdb::ComparatorJniCallbackOptions* copt = reinterpret_cast<rocksdb::ComparatorJniCallbackOptions*>(copt_handle);
  const rocksdb::DirectComparatorJniCallback* c = new rocksdb::DirectComparatorJniCallback(env, jobj, copt);
  rocksdb::AbstractComparatorJni::setHandle(env, jobj, c);
}

//</editor-fold>

