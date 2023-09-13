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

}; // namespace ROCKSDB_NAMESPACE
