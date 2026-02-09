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

/**
 * @brief Encapsulate keys and key conversions from Java/JNI objects for
 * MultiGet
 *
 */
class MultiGetJNIKeys {
 private:
  std::vector<ROCKSDB_NAMESPACE::Slice> slices_;
  std::vector<std::unique_ptr<jbyte[]>> key_bufs;

 public:
  /**
   * @brief Construct helper multiget keys object from array of java keys
   *
   * @param env JNI environment
   * @param jkeys Array of `byte[]`, each of which contains a key
   * @param jkey_offs array of offsets into keys, at which each key starts
   * @param jkey_lens array of key lengths
   * @return true if the keys were copied successfully from the parameters
   * @return false if a Java exception was raised (memory problem, or array
   * indexing problem)
   */
  bool fromByteArrays(JNIEnv* env, jobjectArray jkeys, jintArray jkey_offs,
                      jintArray jkey_lens);

  /**
   * @brief Construct helper multiget keys object from array of java keys
   *
   * @param env env JNI environment
   * @param jkeys jkeys Array of byte[], each of which is a key
   * @return true if the keys were copied successfully from the parameters
   * @return false if a Java exception was raised (memory problem, or array
   * indexing problem)
   */
  bool fromByteArrays(JNIEnv* env, jobjectArray jkeys);

  /**
   * @brief Construct helper multiget keys object from array of java ByteBuffers
   *
   * @param env JNI environment
   * @param jkeys Array of `java.nio.ByteBuffer`, each of which contains a key
   * @param jkey_offs array of offsets into buffers, at which each key starts
   * @param jkey_lens array of key lengths
   * @return `true` if the keys were copied successfully from the parameters
   * @return `false` if a Java exception was raised (memory problem, or array
   * indexing problem)
   */
  bool fromByteBuffers(JNIEnv* env, jobjectArray jkeys, jintArray jkey_offs,
                       jintArray jkey_lens);

  /**
   * @brief Used when the keys need to be passed to a RocksDB function which
   * takes keys as an array of slice pointers
   *
   * @return ROCKSDB_NAMESPACE::Slice* an array of slices, the n-th slice
   * contains the n-th key created by `fromByteArrays()` or `fromByteBuffers()`
   */
  ROCKSDB_NAMESPACE::Slice* data();

  /**
   * @brief Used when the keys need to be passed to a RocksDB function which
   * takes keys as a vector of slices
   *
   * @return std::vector<ROCKSDB_NAMESPACE::Slice>& a vector of slices, the n-th
   * slice contains the n-th key created by `fromByteArrays()` or
   * `fromByteBuffers()`
   */
  inline std::vector<ROCKSDB_NAMESPACE::Slice>& slices() { return slices_; }

  /**
   * @brief
   *
   * @return std::vector<ROCKSDB_NAMESPACE::Slice>::size_type the number of keys
   * in this object
   */
  std::vector<ROCKSDB_NAMESPACE::Slice>::size_type size();
};

/**
 * @brief Class with static helpers for returning java objects from RocksDB data
 * returned by MultiGet
 *
 */
class MultiGetJNIValues {
 public:
  /**
   * @brief create an array of `byte[]` containing the result values from
   * `MultiGet`
   *
   * @tparam TValue a `std::string` or a `PinnableSlice` containing the result
   * for a single key
   * @return jobjectArray an array of `byte[]`, one per value in the input
   * vector
   */
  template <class TValue>
  static jobjectArray byteArrays(JNIEnv*, std::vector<TValue>&,
                                 std::vector<ROCKSDB_NAMESPACE::Status>&);

  /**
   * @brief fill a supplied array of `byte[]` with the result values from
   * `MultiGet`
   *
   * @tparam TValue a `std::string` or a `PinnableSlice` containing the result
   * for a single key
   * @param jvalues the array of `byte[]` to instantiate
   * @param jvalue_sizes the offsets at which to place the results in `jvalues`
   * @param jstatuses the status for every individual key/value get
   */
  template <class TValue>
  static void fillByteBuffersAndStatusObjects(
      JNIEnv*, std::vector<TValue>&, std::vector<ROCKSDB_NAMESPACE::Status>&,
      jobjectArray jvalues, jintArray jvalue_sizes, jobjectArray jstatuses);
};

/**
 * @brief class with static helper for arrays of column family handles
 *
 */
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

  /**
   * @brief create a column family handle from a raw pointer, or raise an
   * appropriate JNI exception
   *
   * @param env
   * @param jcolumn_family_handle the raw pointer to convert
   * @return ROCKSDB_NAMESPACE::ColumnFamilyHandle* or raises a java exception
   */
  static ROCKSDB_NAMESPACE::ColumnFamilyHandle* handleFromJLong(
      JNIEnv* env, jlong jcolumn_family_handle);
};

};  // namespace ROCKSDB_NAMESPACE
