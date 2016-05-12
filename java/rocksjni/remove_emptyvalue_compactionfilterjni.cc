// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <jni.h>

#include "include/org_rocksdb_RemoveEmptyValueCompactionFilter.h"
#include "utilities/compaction_filters/remove_emptyvalue_compactionfilter.h"


/*
 * Class:     org_rocksdb_RemoveEmptyValueCompactionFilter
 * Method:    createNewRemoveEmptyValueCompactionFilter0
 * Signature: ()J
 */
jlong Java_org_rocksdb_RemoveEmptyValueCompactionFilter_createNewRemoveEmptyValueCompactionFilter0(
    JNIEnv* env, jclass jcls) {
  auto* compaction_filter =
      new rocksdb::RemoveEmptyValueCompactionFilter();

  // set the native handle to our native compaction filter
  return reinterpret_cast<jlong>(compaction_filter);
}
