//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#pragma once

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "util/hash.h"

namespace rocksdb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const Options& opt);

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  bool SamePrefix(const Slice &key1, const Slice &key2) const;
  void GenerateFilter();

  const FilterPolicy* policy_;
  const SliceTransform* prefix_extractor_;
  bool whole_key_filtering_;
  const Comparator* comparator_;

  std::string entries_;         // Flattened entry contents
  std::vector<size_t> start_;   // Starting index in entries_ of each entry
  std::string result_;          // Filter data computed so far
  std::vector<Slice> tmp_entries_; // policy_->CreateFilter() argument
  std::vector<uint32_t> filter_offsets_;

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

class FilterBlockReader {
 public:
 // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(
    const Options& opt,
    const Slice& contents,
    bool delete_contents_after_use = false);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);
  bool PrefixMayMatch(uint64_t block_offset, const Slice& prefix);

 private:
  const FilterPolicy* policy_;
  const SliceTransform* prefix_extractor_;
  bool whole_key_filtering_;
  const char* data_;    // Pointer to filter data (at block-start)
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
  std::unique_ptr<const char[]> filter_data;


  bool MayMatch(uint64_t block_offset, const Slice& entry);
};

}
