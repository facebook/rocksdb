// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include "db/merge_context.h"
#include "utilities/write_batch_with_index/write_batch_interval_map.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

namespace ROCKSDB_NAMESPACE {

// Implement a record of the deleted ranges within an indexed write batch.
// Holds a reference to the write batch where the key parts of the
// index entries are stored.
// Use these write batch index entries as our keys when we store intervals.
// The class is otherwise just a concrete instance of an interval map.
class DeletedRangeMap : IntervalMap<const struct WriteBatchIndexEntry,
                                    const class WriteBatchEntryComparator&> {
 public:
  DeletedRangeMap(const class WriteBatchEntryComparator& cmp,
                  Allocator* allocator, WriteBatch* write_batch)
      : IntervalMap(cmp, allocator), indexed_write_batch(write_batch) {}

  // key parameters should refer to slices within the write batch
  void AddInterval(const uint32_t cf_id, const Slice& from_key,
                   const Slice& to_key);

  bool IsInInterval(const uint32_t cf_id, const Slice& key);

 private:
  WriteBatch* indexed_write_batch;
};

};  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
