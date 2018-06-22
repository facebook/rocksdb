// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef BLOCK_SUFFIX_INDEX_H
#define BLOCK_SUFFIX_INDEX_H

#include<string>
#include<vector>

namespace rocksdb {

class BlockSuffixIndexBuilder {
 public:

  BlockSuffixIndexBuilder(uint16_t n): num_buckets_(n), bucket_(n) { }

  void Add(const Slice& suffix, const uint16_t& pos);

  void Finish(std::string&);

 private:
  uint16_t num_buckets_;
  std::vector<std::vector<uint16_t>> bucket_;
};

class BlockSuffixIndex {
 public:

  BlockSuffixIndex(Slice* s): suffix_index_(s) {}

  bool Seek(const Slice& target, uint16_t* pos) const;

 private:
  Slice* suffix_index_;
  uint16_t num_buckets_;
};

}  // namespace rocksdb
#endif
