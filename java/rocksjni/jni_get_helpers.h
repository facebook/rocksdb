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

class GetJNIKey {
 private:
  ROCKSDB_NAMESPACE::Slice slice_;
  std::unique_ptr<jbyte[]> key_buf;

 public:
  bool fromByteArray(JNIEnv* env, jbyteArray jkey, jint jkey_off,
                     jint jkey_len);

  inline ROCKSDB_NAMESPACE::Slice slice() { return slice_; }
};

/**
 * @brief keys and key conversions for MultiGet
 *
 */
class MultiGetJNIKeys {
 private:
  std::vector<ROCKSDB_NAMESPACE::Slice> slices_;
  std::vector<std::unique_ptr<jbyte[]>> key_bufs;

 public:
  bool fromByteArrays(JNIEnv* env, jobjectArray jkeys, jintArray jkey_offs,
                      jintArray jkey_lens);

  bool fromByteArrays(JNIEnv* env, jobjectArray jkeys);

  bool fromByteBuffers(JNIEnv* env, jobjectArray jkeys, jintArray jkey_offs,
                       jintArray jkey_lens);

  ROCKSDB_NAMESPACE::Slice* data();
  inline std::vector<ROCKSDB_NAMESPACE::Slice>& slices() { return slices_; }
  std::vector<ROCKSDB_NAMESPACE::Slice>::size_type size();
};

class GetJNIValue {
 public:
  static const int kNotFound = -1;
  static const int kStatusError = -2;

  /**
   * @brief allocate and fill a byte array from the value in a pinnable slice
   * If the supplied status is faulty, raise an exception instead
   *
   * @param env JNI environment in which to raise any exception
   * @param s status to check before copying the result
   * @param value pinnable slice containing a value
   * @return jbyteArray
   */
  static jbyteArray byteArray(JNIEnv* env, ROCKSDB_NAMESPACE::Status& s,
                              ROCKSDB_NAMESPACE::PinnableSlice& value);

  /**
   * @brief fill an existing byte array from the value in a pinnable slice
   *
   * If the supplied status is faulty, raise an exception instead
   *
   * @param env JNI environment in which to raise any exception
   * @param s status to check before copying the result
   * @param value pinnable slice containing a value
   * @param jval byte array target for value
   * @param jval_off offset in the array at which to place the value
   * @param jval_len length of byte array into which to copy
   * @return jint length copied, or a -ve status code
   */
  static jint fillValue(JNIEnv* env, ROCKSDB_NAMESPACE::Status& s,
                        ROCKSDB_NAMESPACE::PinnableSlice& value,
                        jbyteArray jval, jint jval_off, jint jval_len);
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

  static ROCKSDB_NAMESPACE::ColumnFamilyHandle* handleFromJLong(
      JNIEnv* env, jlong jcolumn_family_handle);
};

};  // namespace ROCKSDB_NAMESPACE
