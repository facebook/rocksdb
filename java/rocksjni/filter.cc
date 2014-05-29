// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
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
 * Method:    createNewFilter0
 * Signature: (I)V
 */
void Java_org_rocksdb_BloomFilter_createNewFilter0(
    JNIEnv* env, jobject jobj, jint bits_per_key) {
  const rocksdb::FilterPolicy* fp = rocksdb::NewBloomFilterPolicy(bits_per_key);
  rocksdb::FilterJni::setHandle(env, jobj, fp);
}

/*
 * Class:     org_rocksdb_Filter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Filter_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  delete reinterpret_cast<rocksdb::FilterPolicy*>(handle);
}
