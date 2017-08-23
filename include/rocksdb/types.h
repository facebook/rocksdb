// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef STORAGE_ROCKSDB_INCLUDE_TYPES_H_
#define STORAGE_ROCKSDB_INCLUDE_TYPES_H_

#include <stdint.h>

namespace rocksdb {

// Define all public custom types here.

// Represents a sequence number in a WAL file.
typedef uint64_t SequenceNumber;

}  //  namespace rocksdb

#endif //  STORAGE_ROCKSDB_INCLUDE_TYPES_H_
