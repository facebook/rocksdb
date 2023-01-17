// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

/*
 * This macro is used for 32 bit OS. In 32 bit OS, the result number is a
 negative number if we use reinterpret_cast<jlong>(pointer).
 * For example, jlong ptr = reinterpret_cast<jlong>(pointer), ptr is a negative
 number in 32 bit OS.
 * If we check ptr using ptr > 0, it fails. For example, the following code is
 not correct.
 * if (jblock_cache_handle > 0) {
      std::shared_ptr<ROCKSDB_NAMESPACE::Cache> *pCache =
          reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache> *>(
              jblock_cache_handle);
      options.block_cache = *pCache;
    }
 * But the result number is positive number if we do
 reinterpret_cast<size_t>(pointer) first and then cast it to jlong. size_t is 4
 bytes long in 32 bit OS and 8 bytes long in 64 bit OS.
 static_cast<jlong>(reinterpret_cast<size_t>(_pointer)) is also working in 64
 bit OS.
 *
 * We don't need an opposite cast because it works from jlong to c++ pointer in
 both 32 bit and 64 bit OS.
 * For example, the following code is working in both 32 bit and 64 bit OS.
 jblock_cache_handle is jlong.
 *   std::shared_ptr<ROCKSDB_NAMESPACE::Cache> *pCache =
          reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::Cache> *>(
              jblock_cache_handle);
*/

#define GET_CPLUSPLUS_POINTER(_pointer) \
  static_cast<jlong>(reinterpret_cast<size_t>(_pointer))
