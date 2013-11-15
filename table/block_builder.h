//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <vector>

#include <stdint.h>
#include "rocksdb/slice.h"

namespace rocksdb {

struct Options;
class Comparator;

class BlockBuilder {
 public:
  BlockBuilder(int block_restart_interval, const Comparator* comparator);
  explicit BlockBuilder(const Options* options);

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been callled since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Returns an estimated block size after appending key and value.
  size_t EstimateSizeAfterKV(const Slice& key, const Slice& value) const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const {
    return buffer_.empty();
  }

 private:
  const int          block_restart_interval_;
  const Comparator*  comparator_;

  std::string           buffer_;    // Destination buffer
  std::vector<uint32_t> restarts_;  // Restart points
  int                   counter_;   // Number of entries emitted since restart
  bool                  finished_;  // Has Finish() been called?
  std::string           last_key_;

  // No copying allowed
  BlockBuilder(const BlockBuilder&);
  void operator=(const BlockBuilder&);
};

}  // namespace rocksdb
