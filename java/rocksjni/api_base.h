// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines the "bridge" object between Java and C++ for
// ROCKSDB_NAMESPACE::ColumnFamilyHandle.

#pragma once

#include <iostream>

#include "rocksdb/db.h"

class APIBase {
  /**
   * @brief control deletion of the underlying pointer.
   * We do not own, and should not delete, the pointer if it is
   * the default instance of a pointer class in RocksDB.
   * For instance, the default ColumnFamilyHandle behaves like this.
   *
   */

 public:
  template <class THandle>
  class SharedPtrHolder {
    const THandle* handle;
    const bool isNotOwned;

   public:
    virtual ~SharedPtrHolder() {
      if (!isNotOwned) {
        delete handle;
      }
    }

    SharedPtrHolder(THandle* handle, bool isNotOwned)
        : handle(handle), isNotOwned(isNotOwned){};
  };

 public:
  std::vector<long> use_counts();

  static jlongArray longArrayFromVector(JNIEnv* env, std::vector<long>& vec) {
    jlongArray jLongArray = env->NewLongArray(static_cast<jsize>(vec.size()));
    if (jLongArray == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }
    env->SetLongArrayRegion(jLongArray, 0, static_cast<jsize>(vec.size()),
                            &vec[0]);
    return jLongArray;
  }

  template <class APIHandle>
  static jlongArray getReferenceCounts(JNIEnv* env, jlong jhandle) {
    std::unique_ptr<APIHandle> apiHandle(reinterpret_cast<APIHandle*>(jhandle));
    std::vector<long> counts = apiHandle->use_counts();
    apiHandle.release();
    return longArrayFromVector(env, counts);
  }

  /**
   * @brief create a THandle wrapped with a SharedPtrHolder which will delete
   * the THandle at delete() time only if the handle is NOT a special default.
   *
   * This uses the distinction between the owned and stored pointers in
   * std::shared_ptr
   *
   * This usage was devised in order not to delete the default CFH,
   * which shouldn't be deleted by us, as it is owned by the core RocksDB C++
   * layer. It also gets used to hold callback instances of the DB, the
   * lifecycle of which must be strictly nested within that of the "real"
   * handle.
   *
   * @param handle
   * @param isNotOwned - don't delete the underlying object when closing the
   * shared pointer
   * @return std::shared_ptr<ROCKSDB_NAMESPACE::THandle> (most probably a
   * ColumnFamilyHandle)
   */
  template <class THandle>
  static std::shared_ptr<THandle> createSharedPtr(THandle* handle,
                                                  bool isNotOwned) {
    return std::shared_ptr<THandle>(
        std::make_shared<SharedPtrHolder<THandle>>(handle, isNotOwned), handle);
  };
};