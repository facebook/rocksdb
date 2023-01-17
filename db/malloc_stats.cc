//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/malloc_stats.h"

#ifndef ROCKSDB_LITE
#include <string.h>

#include <memory>

#include "port/jemalloc_helper.h"

namespace ROCKSDB_NAMESPACE {

#ifdef ROCKSDB_JEMALLOC

struct MallocStatus {
  char* cur;
  char* end;
};

static void GetJemallocStatus(void* mstat_arg, const char* status) {
  MallocStatus* mstat = reinterpret_cast<MallocStatus*>(mstat_arg);
  size_t status_len = status ? strlen(status) : 0;
  size_t buf_size = (size_t)(mstat->end - mstat->cur);
  if (!status_len || status_len > buf_size) {
    return;
  }

  snprintf(mstat->cur, buf_size, "%s", status);
  mstat->cur += status_len;
}
void DumpMallocStats(std::string* stats) {
  if (!HasJemalloc()) {
    return;
  }
  MallocStatus mstat;
  const unsigned int kMallocStatusLen = 1000000;
  std::unique_ptr<char[]> buf{new char[kMallocStatusLen + 1]};
  mstat.cur = buf.get();
  mstat.end = buf.get() + kMallocStatusLen;
  malloc_stats_print(GetJemallocStatus, &mstat, "");
  stats->append(buf.get());
}
#else
void DumpMallocStats(std::string*) {}
#endif  // ROCKSDB_JEMALLOC
}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
