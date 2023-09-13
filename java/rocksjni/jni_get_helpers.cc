//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksjni/jni_get_helpers.h"
#include "rocksjni/portal.h"

namespace ROCKSDB_NAMESPACE {

jbyteArray rocksjni_get_helper(
    JNIEnv* env,
    const ROCKSDB_NAMESPACE::FnGet& fn_get,
    jbyteArray jkey, jint jkey_off, jint jkey_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return nullptr;
  }

  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  ROCKSDB_NAMESPACE::PinnableSlice pinnable_value;
  ROCKSDB_NAMESPACE::Status s = fn_get(key_slice, &pinnable_value);

  // cleanup
  delete[] key;

  if (s.IsNotFound()) {
    return nullptr;
  }

  if (s.ok()) {
    jbyteArray jret_value =
        ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, pinnable_value);
    pinnable_value.Reset();
    if (jret_value == nullptr) {
      // exception occurred
      return nullptr;
    }
    return jret_value;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  return nullptr;
}

jint rocksjni_get_helper(
    JNIEnv* env, 
    const ROCKSDB_NAMESPACE::FnGet& fn_get,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jbyteArray jval,
    jint jval_off, jint jval_len, bool* has_exception) {
  static const int kNotFound = -1;
  static const int kStatusError = -2;

  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    delete[] key;
    *has_exception = true;
    return kStatusError;
  }

  ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  ROCKSDB_NAMESPACE::PinnableSlice pinnable_value;
  ROCKSDB_NAMESPACE::Status s = fn_get(key_slice, &pinnable_value);
  
  // cleanup
  delete[] key;

  if (s.IsNotFound()) {
    *has_exception = false;
    return kNotFound;
  } else if (!s.ok()) {
    *has_exception = true;
    // Here since we are throwing a Java exception from c++ side.
    // As a result, c++ does not know calling this function will in fact
    // throwing an exception.  As a result, the execution flow will
    // not stop here, and codes after this throw will still be
    // executed.
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);

    // Return a dummy const value to avoid compilation error, although
    // java side might not have a chance to get the return value :)
    return kStatusError;
  }

  const jint pinnable_value_len = static_cast<jint>(pinnable_value.size());
  const jint length = std::min(jval_len, pinnable_value_len);

  env->SetByteArrayRegion(jval, jval_off, length,
                          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(
                              pinnable_value.data())));
  pinnable_value.Reset();
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    *has_exception = true;
    return kStatusError;
  }

  *has_exception = false;
  return pinnable_value_len;
}

}; // namespace ROCKSDB_NAMESPACE
