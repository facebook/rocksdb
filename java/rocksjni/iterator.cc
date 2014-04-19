// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::Iterator methods from Java side.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>

#include "include/org_rocksdb_Iterator.h"
#include "rocksjni/portal.h"
#include "rocksdb/iterator.h"

jboolean Java_org_rocksdb_Iterator_isValid0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto st = reinterpret_cast<rocksdb::Iterator*>(handle);
  assert(st != nullptr);
  
  return st->Valid();
}

void Java_org_rocksdb_Iterator_seekToFirst0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto st = reinterpret_cast<rocksdb::Iterator*>(handle);
  assert(st != nullptr);
  
  st->SeekToFirst();
}

void Java_org_rocksdb_Iterator_close0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto st = reinterpret_cast<rocksdb::Iterator*>(handle);
  assert(st != nullptr);
  
  delete st;
  
  rocksdb::IteratorJni::setHandle(env, jobj, nullptr);
}