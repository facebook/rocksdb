//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "port/port_posix.h"
#include "rocksdb/db.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class KeyGenerator {
 public:
  // Generate next key
  // buff: the caller needs to make sure there's enough space for generated key
  // offset: to control the group of the key, 0 means normal key, 1 means
  //  non-existing key, 2 is reserved
  // prefix_only: only return a prefix
  Slice Next(char* buff, int8_t offset = 0, bool prefix_only = false);

  // use internal buffer for generated key, make sure there's only one caller in
  // single thread
  Slice Next() { return Next(buff_); }

  // user internal buffer for generated prefix
  Slice NextPrefix() {
    assert(prefix_num_ > 0);
    return Next(buff_, 0, true);
  }

  // helper function to get non exist key
  Slice NextNonExist() { return Next(buff_, 1); }

  Slice MaxKey(char* buff) const {
    memset(buff, 0xff, key_size_);
    return {buff, key_size_};
  }

  Slice MinKey(char* buff) const {
    memset(buff, 0, key_size_);
    return {buff, key_size_};
  }

  // max_key: the max key that it could generate
  // prefix_num: the max prefix number
  // key_size: in bytes
  explicit KeyGenerator(Random* rnd, uint64_t max_key = 100 * 1024 * 1024,
                        size_t prefix_num = 0, size_t key_size = 10);

  // generate sequential keys
  explicit KeyGenerator(uint64_t max_key = 100 * 1024 * 1024,
                        size_t key_size = 10);

 private:
  Random* rnd_;
  size_t prefix_num_ = 0;
  size_t prefix_size_ = 0;
  size_t key_size_;
  uint64_t max_key_;
  bool is_sequential_ = false;
  uint32_t next_sequential_key_ = 0;
  char buff_[256] = {0};
  const int MULTIPLIER = 3;

  void static Encode(char* buf, uint32_t value);
};

} // namespace ROCKSDB_NAMESPACE
