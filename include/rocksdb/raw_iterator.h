// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE


#include "rocksdb/advanced_options.h"
namespace ROCKSDB_NAMESPACE {

class RawIterator {
 public:
  virtual ~RawIterator() {}
  virtual bool has_next() const = 0;
  virtual Slice getKey() const = 0;
  virtual Slice getValue() const = 0;
  virtual uint64_t getSequenceNumber() const = 0;
  virtual uint32_t getType() const = 0;
  virtual void next() = 0;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
