//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <jni.h>

#include <functional>

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {

typedef std::function<ROCKSDB_NAMESPACE::Status(
    // const ROCKSDB_NAMESPACE::ReadOptions&,
    const ROCKSDB_NAMESPACE::Slice&, ROCKSDB_NAMESPACE::PinnableSlice*)>
    FnGet;

jbyteArray rocksjni_get_helper(JNIEnv* env,
                               const ROCKSDB_NAMESPACE::FnGet& fn_get,
                               jbyteArray jkey, jint jkey_off, jint jkey_len);

jint rocksjni_get_helper(JNIEnv* env, const ROCKSDB_NAMESPACE::FnGet& fn_get,
                         jbyteArray jkey, jint jkey_off, jint jkey_len,
                         jbyteArray jval, jint jval_off, jint jval_len,
                         bool* has_exception);

/**
 * @brief keys and key conversions for MultiGet
 *
 */
class MultiGetJNIKeys {
 private:
  std::vector<ROCKSDB_NAMESPACE::Slice> slices;
  std::vector<jbyte*> key_bufs_to_free;

 public:
  ~MultiGetJNIKeys();

  bool fromByteArrays(JNIEnv* env, jobjectArray jkeys, jintArray jkey_offs,
                      jintArray jkey_lens);

  bool fromByteBuffers(JNIEnv* env, jobjectArray jkeys, jintArray jkey_offs,
                       jintArray jkey_lens);

  ROCKSDB_NAMESPACE::Slice* data();
  std::vector<ROCKSDB_NAMESPACE::Slice>::size_type size();
};

/**
 * @brief values and value conversions for MultiGet
 *
 */
class MultiGetJNIValues {
 public:
  template <class TValue>
  static jobjectArray byteArrays(JNIEnv*, std::vector<TValue>&,
                                 std::vector<ROCKSDB_NAMESPACE::Status>&);

  template <class TValue>
  static void fillValuesStatusObjects(JNIEnv*, std::vector<TValue>&,
                                      std::vector<ROCKSDB_NAMESPACE::Status>&,
                                      jobjectArray jvalues,
                                      jintArray jvalue_sizes,
                                      jobjectArray jstatuses);
};

class ColumnFamilyJNIHelpers {
 public:
  /**
   * @brief create a native array of cf handles from java handles
   *
   * @param env
   * @param jcolumn_family_handles
   * @return unique ptr to vector of handles on success, reset() unique ptr on
   * failure (and a JNI exception will be generated)
   */
  static std::unique_ptr<std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>>
  handlesFromJLongArray(JNIEnv* env, jlongArray jcolumn_family_handles);
};

};  // namespace ROCKSDB_NAMESPACE
