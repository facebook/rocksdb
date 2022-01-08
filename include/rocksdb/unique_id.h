//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL: This API is subject to change
//
// Computes a stable, universally unique 192-bit (24 binary char) identifier
// for an SST file from TableProperties. This is supported for table (SST)
// files created with RocksDB 6.24 and later. NotSupported will be returned
// for other cases. The first 16 bytes (128 bits) is of sufficient quality
// for almost all applications, and shorter prefixes are usable as a
// hash of the full unique id.
//
// Note: .c_str() is not compatible with binary char strings, so using
// .c_str() on the result will often result in information loss and very
// poor uniqueness probability.
//
// More detail: the first 128 bits are *guaranteed* unique for SST files
// generated in the same process (even different DBs, RocksDB >= 6.26),
// and first 128 bits are guaranteed not "all zeros" (RocksDB >= 6.26)
// so that the "all zeros" value can be used reliably for a null ID.
// Assuming one generates many SST files in the lifetime of each process,
// the probability of collision between processes is "better than
// random": if processes generate n SST files on average, we expect to
// generate roughly 2^64 * sqrt(n) files before first collision in the
// first 128 bits. See https://github.com/pdillinger/unique_id
// Using the full 192 bits, we expect to generate roughly 2^96 * sqrt(n)
// files before first collision.
Status GetUniqueIdFromTableProperties(const TableProperties &props,
                                      std::string *out_id);

// EXPERIMENTAL: This API is subject to change
//
// Converts a binary string (unique id) to hexadecimal, with each 64 bits
// separated by '-', e.g. 6474DF650323BDF0-B48E64F3039308CA-17284B32E7F7444B
// Also works on unique id prefix.
std::string UniqueIdToHumanString(const std::string &id);

}  // namespace ROCKSDB_NAMESPACE
