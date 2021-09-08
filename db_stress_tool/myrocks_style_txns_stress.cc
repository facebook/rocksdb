//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS
#include "db_stress_tool/db_stress_common.h"
#ifndef NDEBUG
#include "utilities/fault_injection_fs.h"
#endif  // NDEBUG

namespace ROCKSDB_NAMESPACE {
class MyRocksStyleTxnsStressTest : public StressTest {
 public:
  MyRocksStyleTxnsStressTest() {}

  ~MyRocksStyleTxnsStressTest() override {}

  Status TestGet(ThreadState* thread, const ReadOptions& read_opts,
                 const std::vector<int>& rand_column_families,
                 const std::vector<int64_t>& rand_keys) override;

  std::vector<Status> TestMultiGet(
      ThreadState* thread, const ReadOptions& read_opts,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) override;

  Status TestPrefixScan(ThreadState* thread, const ReadOptions& read_opts,
                        const std::vector<int>& rand_column_families,
                        const std::vector<int64_t>& rand_keys) override;

  // Given a key K, this creates an iterator which scans to K and then
  // does a random sequence of Next/Prev operations.
  Status TestIterate(ThreadState* thread, const ReadOptions& read_opts,
                     const std::vector<int>& rand_column_families,
                     const std::vector<int64_t>& rand_keys) override;

  Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                 const ReadOptions& read_opts, const std::vector<int>& cf_ids,
                 const std::vector<int64_t>& keys, char (&value)[100],
                 std::unique_ptr<MutexLock>& lock) override;

  Status TestDelete(ThreadState* thread, WriteOptions& write_opts,
                    const std::vector<int>& rand_column_families,
                    const std::vector<int64_t>& rand_keys,
                    std::unique_ptr<MutexLock>& lock) override;

  Status TestDeleteRange(ThreadState* thread, WriteOptions& write_opts,
                         const std::vector<int>& rand_column_families,
                         const std::vector<int64_t>& rand_keys,
                         std::unique_ptr<MutexLock>& lock) override;

  void TestIngestExternalFile(ThreadState* thread,
                              const std::vector<int>& rand_column_families,
                              const std::vector<int64_t>& rand_keys,
                              std::unique_ptr<MutexLock>& lock) override;

  void VerifyDb(ThreadState* thread) const override;
};

// Used for point-lookup transaction
Status MyRocksStyleTxnsStressTest::TestGet(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  (void)thread;
  (void)read_opts;
  (void)rand_column_families;
  (void)rand_keys;
  return Status::OK();
}

// Not used
std::vector<Status> MyRocksStyleTxnsStressTest::TestMultiGet(
    ThreadState* /*thread*/, const ReadOptions& /*read_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/) {
  return std::vector<Status>{Status::NotSupported()};
}

Status MyRocksStyleTxnsStressTest::TestPrefixScan(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  (void)thread;
  (void)read_opts;
  (void)rand_column_families;
  (void)rand_keys;
  return Status::OK();
}

// Given a key K, this creates an iterator which scans to K and then
// does a random sequence of Next/Prev operations.
Status MyRocksStyleTxnsStressTest::TestIterate(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  (void)thread;
  (void)read_opts;
  (void)rand_column_families;
  (void)rand_keys;

  return Status::OK();
}

// Not intended for use.
Status MyRocksStyleTxnsStressTest::TestPut(
    ThreadState* /*thread*/, WriteOptions& /*write_opts*/,
    const ReadOptions& /*read_opts*/, const std::vector<int>& /*cf_ids*/,
    const std::vector<int64_t>& /*keys*/, char (&value)[100],
    std::unique_ptr<MutexLock>& /*lock*/) {
  (void)value;
  return Status::NotSupported();
}

// Not intended for use.
Status MyRocksStyleTxnsStressTest::TestDelete(
    ThreadState* /*thread*/, WriteOptions& /*write_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/,
    std::unique_ptr<MutexLock>& /*lock*/) {
  return Status::NotSupported();
}

// Not intended for use.
Status MyRocksStyleTxnsStressTest::TestDeleteRange(
    ThreadState* /*thread*/, WriteOptions& /*write_opts*/,
    const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/,
    std::unique_ptr<MutexLock>& /*lock*/) {
  return Status::NotSupported();
}

// Not intended for use.
void MyRocksStyleTxnsStressTest::TestIngestExternalFile(
    ThreadState* /*thread*/, const std::vector<int>& /*rand_column_families*/,
    const std::vector<int64_t>& /*rand_keys*/,
    std::unique_ptr<MutexLock>& /*lock*/) {}

void MyRocksStyleTxnsStressTest::VerifyDb(ThreadState* thread) const {
  (void)thread;
}

StressTest* CreateMyRocksStyleTxnsStressTest() {
  return new MyRocksStyleTxnsStressTest();
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
