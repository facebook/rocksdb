// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

//
// This file implements the "bridge" between Java and C++ for
// rocksdb::CompactionFilter.

#include <jni.h>

#include "include/org_rocksdb_AbstractCompactionFilter.h"
#include "rocksdb/compaction_filter.h"

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
// </editor-fold>
