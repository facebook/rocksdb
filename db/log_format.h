//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.txt for more detail.

#pragma once

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
namespace log {

enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,
  kFullType = 1,

  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4,

  // For recycled log files
  kRecyclableFullType = 5,
  kRecyclableFirstType = 6,
  kRecyclableMiddleType = 7,
  kRecyclableLastType = 8,

  // Compression Type
  kSetCompressionType = 9,

  // User-defined timestamp sizes
  kUserDefinedTimestampSizeType = 10,
  kRecyclableUserDefinedTimestampSizeType = 11,
};
constexpr int kMaxRecordType = kRecyclableUserDefinedTimestampSizeType;

constexpr unsigned int kBlockSize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte)
constexpr int kHeaderSize = 4 + 2 + 1;

// Recyclable header is checksum (4 bytes), length (2 bytes), type (1 byte),
// log number (4 bytes).
constexpr int kRecyclableHeaderSize = 4 + 2 + 1 + 4;

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE
