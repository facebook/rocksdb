//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_manager.h"
#include "util/sst_file_manager_impl.h"
#include "util/bit_corruption_injection_test_env.h"

namespace rocksdb {

class CompactionCorruptionTest : public DBTestBase {
 public:
  CompactionCorruptionTest() : DBTestBase("/compaction_corruption_test") {}
};

class BackgroundErrorListener : public EventListener {
   public:
       explicit BackgroundErrorListener() {}
       ~BackgroundErrorListener() override{};

       void OnSpecialBackgroundError(BackgroundErrorReason reason, Status* bg_error,
           BackgroundErrorInfo *info) override {
         assert(info->beginKeys.size() >= 1);
         assert(info->endKeys.size() >= 1);
         assert(info->beginKeys.size() == info->endKeys.size());
         assert(info->beginKeys[0] == DBTestBase::Key(0));
         assert(info->endKeys[0] == Slice("l", 1));
       }
};

TEST_F(CompactionCorruptionTest, CancellingCompactionsWorks) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  //auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Options options = CurrentOptions();
  options.disable_auto_compactions=true;
  options.level0_file_num_compaction_trigger = 2;
  options.new_table_reader_for_compaction_inputs=true;
  options.env = new BitCorruptionInjectionTestEnv(rocksdb::Env::Default(), -1);

  auto listener = std::make_shared<BackgroundErrorListener>();
  options.listeners.push_back(listener);
  DestroyAndReopen(options);

  int num_corruptions=0;
  int injected_corruptions = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::run()::EncounteredCorruption",
      [&](void* arg) { num_corruptions++;});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CorruptedRandomAccessFile::Read():SuccessfulCorruption",
      [&](void* arg) { injected_corruptions++;});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CorruptedRandomAccessFile::Read():CheckIfCompactionTest",
      [&](void* arg) {
        (*static_cast<bool*>(arg)) = true;});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);

  // Generate a file containing 10 keys.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 50)));
  }
  ASSERT_OK(Flush());

  // Generate another file to trigger compaction.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 50)));
  }
  ASSERT_OK(Flush());

  reinterpret_cast<BitCorruptionInjectionTestEnv *>(options.env)->SetUber(400);
  dbfull()->SetOptions({{"disable_auto_compactions", "false"}});
  dbfull()->TEST_WaitForCompact();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  if (injected_corruptions >= 1) { // might not be the case because probabilistic
    assert(num_corruptions >= 1);
  }
}
} // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
