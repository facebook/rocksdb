// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::FilterPolicy.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>

#include "include/org_rocksdb_Filter.h"
#include "include/org_rocksdb_BloomFilter.h"
#include "rocksjni/portal.h"
#include "rocksdb/filter_policy.h"

/*
 * Class:     org_rocksdb_BloomFilter
 * Method:    createBloomFilter
 * Signature: (IZ)J
 */
jlong Java_org_rocksdb_BloomFilter_createNewBloomFilter(
    JNIEnv* env, jclass jcls, jint bits_per_key,
    jboolean use_block_base_builder) {
  auto* fp = const_cast<rocksdb::FilterPolicy *>(
      rocksdb::NewBloomFilterPolicy(bits_per_key, use_block_base_builder));
  auto* pFilterPolicy =
      new std::shared_ptr<rocksdb::FilterPolicy>;
  *pFilterPolicy = std::shared_ptr<rocksdb::FilterPolicy>(fp);
  return reinterpret_cast<jlong>(pFilterPolicy);
}

/*
 * Class:     org_rocksdb_Filter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Filter_disposeInternal(
    JNIEnv* env, jobject jobj, jlong jhandle) {

  std::shared_ptr<rocksdb::FilterPolicy> *handle =
      reinterpret_cast<std::shared_ptr<rocksdb::FilterPolicy> *>(jhandle);
  handle->reset();
}
