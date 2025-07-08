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

#include "rocksdb/customizable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Prefix for user-defined index block names
inline const std::string kUserDefinedIndexPrefix =
    "rocksdb.user_defined_index.";

// This is a public API for user-defined index builders.
// It allows users to define their own index format and build custom
// indexes during table building.

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

// Factory for creating user-defined index builders.
class UserDefinedIndexFactory : public Customizable {
 public:
  virtual ~UserDefinedIndexFactory() = default;

  // Create a new builder for user-defined index.
  virtual UserDefinedIndexBuilder* NewBuilder() const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
