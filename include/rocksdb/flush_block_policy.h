//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <string>

namespace rocksdb {

class Slice;
class BlockBuilder;

// FlushBlockPolicy provides a configurable way to determine when to flush a
// block in the block based tables,
class FlushBlockPolicy {
 public:
  // Keep track of the key/value sequences and return the boolean value to
  // determine if table builder should flush current data block.
  virtual bool Update(const Slice& key,
                      const Slice& value) = 0;

  virtual ~FlushBlockPolicy() { }
};

class FlushBlockPolicyFactory {
 public:
  // Return the name of the flush block policy.
  virtual const char* Name() const = 0;

  // Return a new block flush policy that flushes data blocks by data size.
  // FlushBlockPolicy may need to access the metadata of the data block
  // builder to determine when to flush the blocks.
  //
  // Callers must delete the result after any database that is using the
  // result has been closed.
  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBuilder& data_block_builder) const = 0;

  virtual ~FlushBlockPolicyFactory() { }
};

class FlushBlockBySizePolicyFactory : public FlushBlockPolicyFactory {
 public:
  FlushBlockBySizePolicyFactory(const uint64_t block_size,
                                const uint64_t block_size_deviation) :
      block_size_(block_size),
      block_size_deviation_(block_size_deviation) {
  }

  virtual const char* Name() const override {
    return "FlushBlockBySizePolicyFactory";
  }

  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBuilder& data_block_builder) const override;

 private:
  const uint64_t block_size_;
  const uint64_t block_size_deviation_;
};

}  // rocksdb
