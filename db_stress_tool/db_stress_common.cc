//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#ifdef GFLAGS
#include "db_stress_tool/db_stress_common.h"

std::shared_ptr<rocksdb::Env> FLAGS_env = rocksdb::Env::Default();
enum rocksdb::CompressionType FLAGS_compression_type_e =
    rocksdb::kSnappyCompression;
enum rocksdb::ChecksumType FLAGS_checksum_type_e = rocksdb::kCRC32c;
enum RepFactory FLAGS_rep_factory = kSkipList;

namespace rocksdb {
void PoolSizeChangeThread(void* v) {
  assert(FLAGS_compaction_thread_pool_adjust_interval > 0);
  ThreadState* thread = reinterpret_cast<ThreadState*>(v);
  SharedState* shared = thread->shared;

  while (true) {
    {
      MutexLock l(shared->GetMutex());
      if (shared->ShoudStopBgThread()) {
        shared->SetBgThreadFinish();
        shared->GetCondVar()->SignalAll();
        return;
      }
    }

    auto thread_pool_size_base = FLAGS_max_background_compactions;
    auto thread_pool_size_var = FLAGS_compaction_thread_pool_variations;
    int new_thread_pool_size =
        thread_pool_size_base - thread_pool_size_var +
        thread->rand.Next() % (thread_pool_size_var * 2 + 1);
    if (new_thread_pool_size < 1) {
      new_thread_pool_size = 1;
    }
    FLAGS_env->SetBackgroundThreads(new_thread_pool_size);
    // Sleep up to 3 seconds
    FLAGS_env->SleepForMicroseconds(
        thread->rand.Next() % FLAGS_compaction_thread_pool_adjust_interval *
            1000 +
        1);
  }
}

void PrintKeyValue(int cf, uint64_t key, const char* value, size_t sz) {
  if (!FLAGS_verbose) {
    return;
  }
  std::string tmp;
  tmp.reserve(sz * 2 + 16);
  char buf[4];
  for (size_t i = 0; i < sz; i++) {
    snprintf(buf, 4, "%X", value[i]);
    tmp.append(buf);
  }
  fprintf(stdout, "[CF %d] %" PRIi64 " == > (%" ROCKSDB_PRIszt ") %s\n", cf,
          key, sz, tmp.c_str());
}

int64_t GenerateOneKey(ThreadState* thread, uint64_t iteration) {
  const double completed_ratio =
      static_cast<double>(iteration) / FLAGS_ops_per_thread;
  const int64_t base_key = static_cast<int64_t>(
      completed_ratio * (FLAGS_max_key - FLAGS_active_width));
  return base_key + thread->rand.Next() % FLAGS_active_width;
}

std::vector<int64_t> GenerateNKeys(ThreadState* thread, int num_keys,
                                   uint64_t iteration) {
  const double completed_ratio =
      static_cast<double>(iteration) / FLAGS_ops_per_thread;
  const int64_t base_key = static_cast<int64_t>(
      completed_ratio * (FLAGS_max_key - FLAGS_active_width));
  std::vector<int64_t> keys;
  keys.reserve(num_keys);
  int64_t next_key = base_key + thread->rand.Next() % FLAGS_active_width;
  keys.push_back(next_key);
  for (int i = 1; i < num_keys; ++i) {
    // This may result in some duplicate keys
    next_key = next_key + thread->rand.Next() %
                              (FLAGS_active_width - (next_key - base_key));
    keys.push_back(next_key);
  }
  return keys;
}

size_t GenerateValue(uint32_t rand, char* v, size_t max_sz) {
  size_t value_sz =
      ((rand % kRandomValueMaxFactor) + 1) * FLAGS_value_size_mult;
  assert(value_sz <= max_sz && value_sz >= sizeof(uint32_t));
  (void)max_sz;
  *((uint32_t*)v) = rand;
  for (size_t i = sizeof(uint32_t); i < value_sz; i++) {
    v[i] = (char)(rand ^ i);
  }
  v[value_sz] = '\0';
  return value_sz;  // the size of the value set.
}
}  // namespace rocksdb
#endif  // GFLAGS
