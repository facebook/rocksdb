//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"

namespace ROCKSDB_NAMESPACE {

namespace {
std::string Key1(uint64_t key) {
  std::string ret;
  PutFixed64(&ret, key);
  std::reverse(ret.begin(), ret.end());
  return ret;
}

std::string Timestamp(uint64_t ts) {
  std::string ret;
  PutFixed64(&ret, ts);
  return ret;
}
}  // anonymous namespace

class TimestampCompatibleCompactionTest : public DBTestBase {
 public:
  TimestampCompatibleCompactionTest()
      : DBTestBase("/ts_compatible_compaction_test", /*env_do_fsync=*/true) {}

  std::string Get(const std::string& key, uint64_t ts) {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(ts);
    Slice ts_slice = ts_str;
    read_opts.timestamp = &ts_slice;
    std::string value;
    Status s = db_->Get(read_opts, key, &value);
    if (s.IsNotFound()) {
      value.assign("NOT_FOUND");
    } else if (!s.ok()) {
      value.assign(s.ToString());
    }
    return value;
  }
};

TEST_F(TimestampCompatibleCompactionTest, UserKeyCrossFileBoundary) {
  Options options = CurrentOptions();
  options.env = env_;
  options.compaction_style = kCompactionStyleLevel;
  options.comparator = test::ComparatorWithU64Ts();
  options.level0_file_num_compaction_trigger = 3;
  constexpr size_t kNumKeysPerFile = 101;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        const auto* compaction = reinterpret_cast<Compaction*>(arg);
        ASSERT_NE(nullptr, compaction);
        ASSERT_EQ(0, compaction->start_level());
        ASSERT_EQ(1, compaction->num_input_levels());
        // Check that all 3 L0 ssts are picked for level compaction.
        ASSERT_EQ(3, compaction->num_input_files(0));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  // Write a L0 with keys 0, 1, ..., 99 with ts from 100 to 199.
  uint64_t ts = 100;
  uint64_t key = 0;
  WriteOptions write_opts;
  for (; key < kNumKeysPerFile - 1; ++key, ++ts) {
    std::string ts_str = Timestamp(ts);
    Slice ts_slice = ts_str;
    write_opts.timestamp = &ts_slice;
    ASSERT_OK(db_->Put(write_opts, Key1(key), "foo_" + std::to_string(key)));
  }
  // Write another L0 with keys 99 with newer ts.
  ASSERT_OK(Flush());
  uint64_t saved_read_ts1 = ts++;
  key = 99;
  for (int i = 0; i < 4; ++i, ++ts) {
    std::string ts_str = Timestamp(ts);
    Slice ts_slice = ts_str;
    write_opts.timestamp = &ts_slice;
    ASSERT_OK(db_->Put(write_opts, Key1(key), "bar_" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  uint64_t saved_read_ts2 = ts++;
  // Write another L0 with keys 99, 100, 101, ..., 150
  for (; key <= 150; ++key, ++ts) {
    std::string ts_str = Timestamp(ts);
    Slice ts_slice = ts_str;
    write_opts.timestamp = &ts_slice;
    ASSERT_OK(db_->Put(write_opts, Key1(key), "foo1_" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  // Wait for compaction to finish
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  uint64_t read_ts = ts;
  ASSERT_EQ("foo_99", Get(Key1(99), saved_read_ts1));
  ASSERT_EQ("bar_99", Get(Key1(99), saved_read_ts2));
  ASSERT_EQ("foo1_99", Get(Key1(99), read_ts));
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
