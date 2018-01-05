// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::CompactionFilter.

#include <jni.h>

#include "include/org_rocksdb_AbstractCompactionFilter.h"
#include "include/org_rocksdb_AbstractJavaCompactionFilter.h"
#include "rocksdb/compaction_filter.h"
#include "rocksjni/compaction_filter_jnicallback.h"

// <editor-fold desc="org.rocksdb.AbstractCompactionFilter">

/*
 * Class:     org_rocksdb_AbstractCompactionFilter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_AbstractCompactionFilter_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto* cf = reinterpret_cast<rocksdb::CompactionFilter*>(handle);
  assert(cf != nullptr);
  delete cf;
}

/*
 * Class:     org_rocksdb_AbstractJavaCompactionFilter
 * Method:    newCompactionFilter
 * Signature: ()J
 */
jlong Java_org_rocksdb_AbstractJavaCompactionFilter_newCompactionFilter(JNIEnv* env, jclass jcls) {
  auto* compaction_filter = new rocksdb::CompactionFilterJniCallback();
  return reinterpret_cast<jlong>(compaction_filter);
}

/*
 * Class:     org_rocksdb_AbstractJavaCompactionFilter
 * Method:    initializeFilter
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_AbstractJavaCompactionFilter_initializeFilter(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto* compaction_filter = reinterpret_cast<rocksdb::CompactionFilterJniCallback*>(handle);
  return compaction_filter->Initialize(env, jobj);
}

// </editor-fold>
