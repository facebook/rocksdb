//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stddef.h>
#include <stdint.h>

#include "rocksdb/iterator.h"
#include "rocksdb/options.h"

namespace rocksdb {

struct BlockContents;
class Comparator;
class BlockHashIndex;

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  ~Block();

  size_t size() const { return size_; }
  const char* data() const { return data_; }
  bool cachable() const { return cachable_; }
  uint32_t NumRestarts() const;
  CompressionType compression_type() const { return compression_type_; }

  // If hash index lookup is enabled and `use_hash_index` is true. This block
  // will do hash lookup for the key prefix.
  //
  // NOTE: for the hash based lookup, if a key prefix doesn't match any key,
  // the iterator will simply be set as "invalid", rather than returning
  // the key that is just pass the target key.
  Iterator* NewIterator(const Comparator* comparator);
  void SetBlockHashIndex(BlockHashIndex* hash_index);

 private:
  const char* data_;
  size_t size_;
  uint32_t restart_offset_;     // Offset in data_ of restart array
  bool owned_;                  // Block owns data_[]
  bool cachable_;
  CompressionType compression_type_;
  std::unique_ptr<BlockHashIndex> hash_index_;

  // No copying allowed
  Block(const Block&);
  void operator=(const Block&);

  class Iter;
};

}  // namespace rocksdb
