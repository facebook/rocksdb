//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "rocksdb/iterator.h"
#include "rocksdb/env.h"

namespace rocksdb {

struct ReadOptions;

// Return a new two level iterator.  A two-level iterator contains an
// index iterator whose values point to a sequence of blocks where
// each block is itself a sequence of key,value pairs.  The returned
// two-level iterator yields the concatenation of all key/value pairs
// in the sequence of blocks.  Takes ownership of "index_iter" and
// will delete it when no longer needed.
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.
extern Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    Iterator* (*block_function)(
        void* arg,
        const ReadOptions& options,
        const EnvOptions& soptions,
        const Slice& index_value,
        bool for_compaction),
    void* arg,
    const ReadOptions& options,
    const EnvOptions& soptions,
    bool for_compaction = false);

}  // namespace rocksdb
