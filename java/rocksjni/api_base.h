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
   * @brief control deletion of the underllying pointer.
   * We do not own, and should not delete, the pointer if it is
   * the default instance of a pointer class in RocksDB.
   * For instance, ColumnFamilyHandle behaves llike this.
   *
   */

 public:
  template <class THandle>
  class SharedPtrHolder {
    const THandle* handle;
    const bool isDefault;

   public:
    virtual ~SharedPtrHolder() {
      if (!isDefault) {
        delete handle;
      }
    }

    SharedPtrHolder(THandle* handle, bool isDefault)
        : handle(handle), isDefault(isDefault){};
  };

 public:
  void check(std::string message);

  /**
   * @brief create a THandle wrapped with a SharedPtrHolder which will delete
   * the THandle at delete() time only if the handle is NOT a special default.
   * This was devised in order not to delete the default CFH,
   * which shouldn't be deleted by us, as it is owned by the C++ layer.
   *
   * @param handle
   * @return std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>
   */
  template <class THandle>
  static std::shared_ptr<THandle> createSharedPtr(THandle* handle,
                                                  bool isDefault) {
    std::shared_ptr<SharedPtrHolder<THandle>> holder(
        new SharedPtrHolder<THandle>(handle, isDefault));
    return std::shared_ptr<THandle>(holder, handle);
  };
};