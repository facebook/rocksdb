// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include "util/murmurhash.h"

namespace rocksdb {

static const uint32_t kMaxNumHashTable  = 64;

uint64_t GetSliceMurmurHash(const Slice& s, uint32_t index,
    uint64_t max_num_buckets) {
  static constexpr uint32_t seeds[kMaxNumHashTable] = {
    816922183, 506425713, 949485004, 22513986, 421427259, 500437285,
    888981693, 847587269, 511007211, 722295391, 934013645, 566947683,
    193618736, 428277388, 770956674, 819994962, 755946528, 40807421,
    263144466, 241420041, 444294464, 731606396, 304158902, 563235655,
    968740453, 336996831, 462831574, 407970157, 985877240, 637708754,
    736932700, 205026023, 755371467, 729648411, 807744117, 46482135,
    847092855, 620960699, 102476362, 314094354, 625838942, 550889395,
    639071379, 834567510, 397667304, 151945969, 443634243, 196618243,
    421986347, 407218337, 964502417, 327741231, 493359459, 452453139,
    692216398, 108161624, 816246924, 234779764, 618949448, 496133787,
    156374056, 316589799, 982915425, 553105889 };
  return MurmurHash(s.data(), s.size(), seeds[index]) % max_num_buckets;
}
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
