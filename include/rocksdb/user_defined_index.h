//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************

#pragma once

#include <string>

#include "rocksdb/advanced_iterator.h"
#include "rocksdb/customizable.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Prefix for user-defined index block names
inline const std::string kUserDefinedIndexPrefix =
    "rocksdb.user_defined_index.";

// This is a public API for user-defined index builders.
// It allows users to define their own index format and build custom
// indexes during table building. Currently, only a monolithic index
// block is supported (no partitioned index).

// The interface for building user-defined index.
class UserDefinedIndexBuilder {
 public:
  // Right now, we only support Puts. In the future, we may support merges,
  // deletions etc.
  enum ValueType {
    kValue,
    kTypeMax,
  };

  // File offset and size of the data block
  struct BlockHandle {
    uint64_t offset;
    uint64_t size;
  };

  virtual ~UserDefinedIndexBuilder() = default;

  // Add a new index entry to index block. The key for the new index entry
  // should be >= last_key_in_current_block and < first_key_in_next_block.
  // The previous index entry key and the new index entry key cover
  // all the keys in the data block associated with the new index entry.
  //
  // Called before the OnKeyAdded() call for first_key_in_next_block.
  // @last_key_in_current_block: The last key in the current data block
  // @first_key_in_next_block: it will be nullptr if the entry being added is
  //                           the last one in the table
  // @block_handle: offset/size of the data block referenced by this index
  //                entry. This should be stored along with the index entry
  //                key
  // @separator_scratch: a scratch buffer to back a computed separator between
  //                     those, as needed. May be modified on each call.
  // @return: the key or separator stored in the index, which could be
  //          last_key_in_current_block or a computed separator backed by
  //          separator_scratch.
  virtual Slice AddIndexEntry(const Slice& last_key_in_current_block,
                              const Slice* first_key_in_next_block,
                              const BlockHandle& block_handle,
                              std::string* separator_scratch) = 0;

  // This method will be called whenever a key is added. The subclasses may
  // override OnKeyAdded() if they need to collect additional information.
  // The type argument indicates whether the value is a full value or partial.
  // At the moment, only full values are supported.
  virtual void OnKeyAdded(const Slice& /*key*/, ValueType /*type*/,
                          const Slice& /*value*/) {}

  // Finish building the index.
  // Returns a Status and the serialized index contents.
  // The memory backing the contents should not be freed until this builder
  // object is destructed.
  virtual Status Finish(Slice* index_contents) = 0;
};

// The interface for iterating the user defined index. This will be
// instantiated and used by a scan to iterate through the index entries
// covered by the scan.
class UserDefinedIndexIterator {
 public:
  virtual ~UserDefinedIndexIterator() = default;

  // Prepare the iterator for a series of scans. The iterator should use
  // this as an opportunity to do any prefetching and buffering of results.
  virtual void Prepare(const ScanOptions scan_opts[], size_t num_opts) = 0;

  // Given the target key, position the index iterator at the index entry
  // with the smallest key >= target. The result must be updated with the
  // index key, and the bound_check_result. The bound_check_result should
  // be set to kOutOfBound if no block satisfies the target key and
  // termination criteria, kInbound if the data block is definitely fully
  // within bounds, or kUnknown if the data block could be partially
  // within bounds.
  virtual Status SeekAndGetResult(const Slice& target,
                                  IterateResult* result) = 0;

  // Advance to the next index entry. The result must be populated similar
  // to SeekAndGetResult.
  virtual Status NextAndGetResult(IterateResult* result) = 0;

  // Return the BlockHandle in the current index entry
  virtual UserDefinedIndexBuilder::BlockHandle value() = 0;
};

// A reader interface for the user defined index
class UserDefinedIndexReader {
 public:
  virtual ~UserDefinedIndexReader() = default;

  // Allocate an iterator that will be used by RocksDB to perform scans
  virtual std::unique_ptr<UserDefinedIndexIterator> NewIterator(
      const ReadOptions& read_options) = 0;

  // The memory usage of the index, including the size of the raw contents and
  // any other heap data structures allocated by the reader
  virtual size_t ApproximateMemoryUsage() const = 0;
};

// Factory for creating user-defined index builders.
class UserDefinedIndexFactory : public Customizable {
 public:
  virtual ~UserDefinedIndexFactory() = default;

  // Create a new builder for user-defined index.
  virtual UserDefinedIndexBuilder* NewBuilder() const = 0;

  // Create a new user defined index reader given the contents of the index
  // block
  virtual std::unique_ptr<UserDefinedIndexReader> NewReader(
      Slice& index_block) const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
