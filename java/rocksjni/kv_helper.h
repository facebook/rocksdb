// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines helper methods for Java API write methods
//

#pragma once

#include <jni.h>

#include <cstring>
#include <functional>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class JByteArraySlice {
 public:
  JByteArraySlice(JNIEnv* env, const jbyteArray& jarray, const jint jarray_off,
                  const jint jarray_len)
      : array_(new jbyte[jarray_len]),
        slice_(reinterpret_cast<char*>(array_), jarray_len) {
    env->GetByteArrayRegion(jarray, jarray_off, jarray_len, array_);
    if (env->ExceptionCheck()) {
      slice_.clear();
      delete[] array_;
    }
  };

  ~JByteArraySlice() {
    slice_.clear();
    delete[] array_;
  };

  Slice& slice() { return slice_; }

 private:
  jbyte* array_;
  Slice slice_;
};

class KVHelperJNI {
 public:
  static bool IfEnvOK(JNIEnv* env, std::function<Status()> fn) {
    if (env->ExceptionCheck()) {
      return false;
    }
    auto status = fn();
    if (status.ok()) {
      return true;
    }
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
    return false;
  }
};

}  // namespace ROCKSDB_NAMESPACE
