//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
#pragma once
#include "db/version_edit.h"
#include "util/random.h"

namespace rocksdb {
static const uint32_t kFileReadSampleRate = 1024;
extern bool should_sample_file_read();
extern void sample_file_read_inc(FileMetaData*);

inline bool should_sample_file_read() {
  return (Random::GetTLSInstance()->Next() % kFileReadSampleRate == 307);
}

inline void sample_file_read_inc(FileMetaData* meta) {
  meta->stats.num_reads_sampled.fetch_add(kFileReadSampleRate,
                                          std::memory_order_relaxed);
}
}
