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

void Java_org_rocksdb_Iterator_seekToLast0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto st = reinterpret_cast<rocksdb::Iterator*>(handle);
  assert(st != nullptr);
  
  st->SeekToLast();
}

void Java_org_rocksdb_Iterator_next0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto st = reinterpret_cast<rocksdb::Iterator*>(handle);
  assert(st != nullptr);
  
  st->Next();
}

void Java_org_rocksdb_Iterator_prev0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto st = reinterpret_cast<rocksdb::Iterator*>(handle);
  assert(st != nullptr);
  
  st->Prev();
}

jbyteArray Java_org_rocksdb_Iterator_key0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto st = reinterpret_cast<rocksdb::Iterator*>(handle);
  assert(st != nullptr);
  
  rocksdb::Slice key_slice = st->key();
  
  jbyteArray jkey = env->NewByteArray(key_slice.size());
    env->SetByteArrayRegion(
        jkey, 0, key_slice.size(),
        reinterpret_cast<const jbyte*>(key_slice.data()));
  return jkey;
}

jbyteArray Java_org_rocksdb_Iterator_value0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto st = reinterpret_cast<rocksdb::Iterator*>(handle);
  assert(st != nullptr);
  
  rocksdb::Slice value_slice = st->value();
  
  jbyteArray jvalue = env->NewByteArray(value_slice.size());
    env->SetByteArrayRegion(
        jvalue, 0, value_slice.size(),
        reinterpret_cast<const jbyte*>(value_slice.data()));
  return jvalue;
}

void Java_org_rocksdb_Iterator_seek0(
    JNIEnv* env, jobject jobj, jlong handle,
    jbyteArray jtarget, jint jtarget_len) {
  auto st = reinterpret_cast<rocksdb::Iterator*>(handle);
  assert(st != nullptr);
  
  jbyte* target = env->GetByteArrayElements(jtarget, 0);
  rocksdb::Slice target_slice(
      reinterpret_cast<char*>(target), jtarget_len);
  
  st->Seek(target_slice);
  
  env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

void Java_org_rocksdb_Iterator_status0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto st = reinterpret_cast<rocksdb::Iterator*>(handle);
  assert(st != nullptr);
  
  rocksdb::Status s = st->status();
  
  if (s.ok()) {
    return;
  }
  
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

void Java_org_rocksdb_Iterator_close0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto st = reinterpret_cast<rocksdb::Iterator*>(handle);
  assert(st != nullptr);
  
  delete st;
  
  rocksdb::IteratorJni::setHandle(env, jobj, nullptr);
}