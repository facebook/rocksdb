// Copyright (c) 2013, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <atomic>
#include <memory>

namespace rocksdb {

class Slice;

class DynamicBloom {

 public:

  // total_bits: fixed total bits for the bloom
  // hash_func:  customized hash function
  // num_probes: number of hash probes for a single key
  DynamicBloom(uint32_t total_bits,
               uint32_t (*hash_func)(const Slice& key),
               uint32_t num_probes = 6);

  explicit DynamicBloom(uint32_t total_bits, uint32_t num_probes = 6);

  // Assuming single threaded access to Add
  void Add(const Slice& key);

  // Assuming single threaded access to Add
  void AddHash(uint32_t hash);

  // Multithreaded access to MayContain is OK
  bool MayContain(const Slice& key);

  // Multithreaded access to MayContain is OK
  bool MayContainHash(uint32_t hash);

 private:
  uint32_t (*hash_func_)(const Slice& key);
  uint32_t total_bits_;
  uint32_t num_probes_;
  std::unique_ptr<unsigned char[]> data_;
};

}
