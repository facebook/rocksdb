// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
// IteratorBatch holds a collection of reads to apply sequentially to a DB.
// Similar to write_batch

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include "rocksdb/status.h"

namespace rocksdb {

class DBImpl;
class ColumnFamilyHandle;

class ReadBatchBase {
 public:
  static const size_t kHeader = 4;
  explicit ReadBatchBase(size_t reserved_bytes);
  explicit ReadBatchBase(const std::string& rep) : rep_(rep) {}
  virtual ~ReadBatchBase() = 0;

 protected:
  virtual int Count();
  virtual void SetCount(int n);
  std::string rep_;
};

class IteratorBatch : public ReadBatchBase {
 public:
  explicit IteratorBatch(size_t reserved_bytes = 0)
      : ReadBatchBase(reserved_bytes) {}
  explicit IteratorBatch(const std::string& rep) : ReadBatchBase(rep) {}
  ~IteratorBatch();
  virtual void Valid(uint64_t timestamp);
  virtual void SeekToFirst(uint64_t timestamp);
  virtual void SeekToLast(uint64_t timestamp);
  virtual void Seek(uint64_t timestamp);
  virtual void SeekForPrev(uint64_t timestamp);
  virtual void Next(uint64_t timestamp);
  virtual void Prev(uint64_t timestamp);
  virtual void Key(uint64_t timestamp);
  virtual void Value(uint64_t timestamp);
  virtual void Status(uint64_t timestamp);
  virtual void GetProperty(uint64_t timestamp);
};

class ReadBatch : public ReadBatchBase {
 public:
  explicit ReadBatch(size_t reserved_bytes = 0)
      : ReadBatchBase(reserved_bytes) {}
  explicit ReadBatch(const std::string& rep) : ReadBatchBase(rep) {}
  ~ReadBatch() {}

  virtual void Get(uint32_t column_family_id, const Slice& key);

  virtual void NewIterator(uint32_t column_family_id, uint32_t iter_id);

  // Retrieve the serialized version of this batch.
  const std::string& Data() const { return rep_; }

  Status Execute(
      DBImpl* db,
      std::unordered_map<uint32_t, ColumnFamilyHandle*>& handle_map) const;
};

}  // namespace rocksdb
