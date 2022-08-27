//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "bench_util.h"

#include "port/port_posix.h"
#include "rocksdb/db.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

Slice KeyGenerator::Next(char* buff, int8_t offset, bool prefix_only) {
  assert(max_key_ < std::numeric_limits<uint32_t>::max() /
                        MULTIPLIER);  // TODO: add large key support

  uint32_t k;
  if (is_sequential_) {
    assert(next_sequential_key_ < max_key_);
    k = (next_sequential_key_ % max_key_) * MULTIPLIER + offset;
    if (next_sequential_key_ + 1 == max_key_) {
      next_sequential_key_ = 0;
    } else {
      next_sequential_key_++;
    }
  } else {
    k = (rnd_->Next() % max_key_) * MULTIPLIER + offset;
  }
  // TODO: make sure the buff is large enough
  memset(buff, 0, key_size_);
  if (prefix_num_ > 0) {
    uint32_t prefix = (k % prefix_num_) * MULTIPLIER + offset;
    Encode(buff, prefix);
    if (prefix_only) {
      return {buff, prefix_size_};
    }
  }
  // encode `k` into buff
  Encode(buff + prefix_size_, k);
  return {buff, key_size_};
}

void KeyGenerator::Encode(char* buf, uint32_t value) {
  if (port::kLittleEndian) {
    buf[0] = static_cast<char>((value >> 24) & 0xff);
    buf[1] = static_cast<char>((value >> 16) & 0xff);
    buf[2] = static_cast<char>((value >> 8) & 0xff);
    buf[3] = static_cast<char>(value & 0xff);
  } else {
    memcpy(buf, &value, sizeof(value));
  }
}
KeyGenerator::KeyGenerator(Random* rnd, uint64_t max_key, size_t prefix_num,
                           size_t key_size) {
  prefix_num_ = prefix_num;
  key_size_ = key_size;
  max_key_ = max_key;
  rnd_ = rnd;
  if (prefix_num > 0) {
    prefix_size_ = 4;  // TODO: support different prefix_size
  }
}

KeyGenerator::KeyGenerator(uint64_t max_key, size_t key_size) {
  key_size_ = key_size;
  max_key_ = max_key;
  rnd_ = nullptr;
  is_sequential_ = true;
}

}  // ROCKSDB_NAMESPACE