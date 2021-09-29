//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>

#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {

// Computes a stable unique identifier for an SST file from TableProperties.
// This is supported for block-based table files created with RocksDB 6.24
// and later. NotSupported will be returned for other cases.
//
// Up to 192 bits (out_len <= 3 with uint64_t*, out_len <= 24 with char*) can
// be returned in out_array, but at least 128 bits is highly recommended for
// strong uniqueness properties. Shorter lengths are usable as a hash of the
// full unique id. The char* overload saves the data as little-endian
// (binary). No human-readable format is provided.
//
// More detail: the first 128 bits are *guaranteed* unique for SST files
// generated in the same process (even different DBs, RocksDB >= 6.26).
// Assuming one generates many SST files in the lifetime of each process,
// the probability of collision between processes is "better than
// random": if processes generate n SST files on average, we expect to
// generate roughly 2^64 * sqrt(n) files before first collision in the
// first 128 bits. See https://github.com/pdillinger/unique_id
// Using the full 192 bits, we expect to generate roughly 2^96 * sqrt(n)
// files before first collision.
Status GetUniqueIdFromTableProperties(const TableProperties& props,
                                      uint64_t* out_array, size_t out_len);
Status GetUniqueIdFromTableProperties(const TableProperties& props,
                                      char* out_array, size_t out_len);

template <typename T, size_t N>
inline Status GetUniqueIdFromTableProperties(const TableProperties& props,
                                             std::array<T, N>* out_array) {
  static_assert(N > 0U, "Array must not be empty");
  static_assert(sizeof(std::array<T, N>) <= 24U,
                "Maximum of 192 bits supported");
  return GetUniqueIdFromTableProperties(props, out_array->data(),
                                        out_array->size());
}

template <>
Status GetUniqueIdFromTableProperties(const TableProperties& props,
                                      std::array<uint64_t, 3>* out_array);

}  // namespace ROCKSDB_NAMESPACE
