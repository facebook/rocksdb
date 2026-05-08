//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"

namespace ROCKSDB_NAMESPACE {

class DBEtc3Test : public DBTestBase {
 public:
  DBEtc3Test() : DBTestBase("db_etc3_test", /*env_do_fsync=*/true) {}
};

TEST_F(DBEtc3Test, ManifestRollOver) {
  do {
    Options options;
    // Force new manifest on each manifest write
    options.max_manifest_file_size = 0;
    options.max_manifest_space_amp_pct = 0;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    {
      ASSERT_OK(Put(1, "key1", std::string(1000, '1')));
      ASSERT_OK(Put(1, "key2", std::string(1000, '2')));
      ASSERT_OK(Put(1, "key3", std::string(1000, '3')));
      uint64_t manifest_before_flush = dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_OK(Flush(1));  // This should trigger LogAndApply.
      uint64_t manifest_after_flush = dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_GT(manifest_after_flush, manifest_before_flush);
      // Re-open should always re-create manifest file
      ReopenWithColumnFamilies({"default", "pikachu"}, options);
      ASSERT_GT(dbfull()->TEST_Current_Manifest_FileNo(), manifest_after_flush);
      ASSERT_EQ(std::string(1000, '1'), Get(1, "key1"));
      ASSERT_EQ(std::string(1000, '2'), Get(1, "key2"));
      ASSERT_EQ(std::string(1000, '3'), Get(1, "key3"));
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBEtc3Test, AutoTuneManifestSize) {
  // Ensure we have auto-tuning beyond max_manifest_file_size by default
  ASSERT_EQ(DBOptions{}.max_manifest_space_amp_pct, 500);

  Options options = CurrentOptions();
  ASSERT_OK(db_->SetOptions({{"level0_file_num_compaction_trigger", "20"}}));

  // Use large column family names to essentially control the amount of payload
  // data needed for the manifest file. Drop manifest entries don't include the
  // CF name so are small.
  uint64_t prev_manifest_num = 0, cur_manifest_num = 0;
  std::deque<ColumnFamilyHandle*> handles;
  int counter = 5;
  auto AddCfFn = [&]() {
    std::string name = "cf" + std::to_string(counter++);
    name.resize(1000, 'a');
    ASSERT_OK(db_->CreateColumnFamily(options, name, &handles.emplace_back()));
    prev_manifest_num = cur_manifest_num;
    cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
  };
  auto DropCfFn = [&]() {
    ASSERT_OK(db_->DropColumnFamily(handles.front()));
    ASSERT_OK(db_->DestroyColumnFamilyHandle(handles.front()));
    handles.pop_front();
    prev_manifest_num = cur_manifest_num;
    cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
  };
  auto TrivialManifestWriteFn = [&]() {
    ASSERT_OK(Put("x", std::to_string(counter++)));
    ASSERT_OK(Flush());
    prev_manifest_num = cur_manifest_num;
    cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
  };

  options.max_manifest_file_size = 1000000;
  options.max_manifest_space_amp_pct = 0;  // no auto-tuning yet
  DestroyAndReopen(options);

  // With the generous (minimum) maximum manifest size, should not be rotated
  AddCfFn();
  AddCfFn();
  AddCfFn();
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // Change options for small max and (still) no auto-tuning
  ASSERT_OK(db_->SetDBOptions({{"max_manifest_file_size", "3000"}}));

  // Takes effect on the next manifest write
  TrivialManifestWriteFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);

  // Now we have to rewrite the whole manifest on each write because the
  // compacted size exceeds the "max" size.
  AddCfFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  DropCfFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  AddCfFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  TrivialManifestWriteFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);

  // Enabling auto-tuning should fix this, immediately for next manifest writes.
  // This will allow up to double-ish the size of the compacted manifest,
  // which last should have been 4000 + some bytes.
  ASSERT_EQ(handles.size(), 4U);
  ASSERT_OK(db_->SetDBOptions({{"max_manifest_space_amp_pct", "105"}}));

  // After 9 CF names should be enough to rotate the manifest
  for (int i = 1; i <= 5; ++i) {
    if ((i % 2) == 1) {
      DropCfFn();
    }
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }
  TrivialManifestWriteFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);

  // We now have a different last compacted manifest size, should be
  // able to go beyond 9 CFs named in manifest this time.
  ASSERT_EQ(handles.size(), 6U);

  DropCfFn();
  DropCfFn();
  for (int i = 1; i <= 4; ++i) {
    DropCfFn();
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }
  // We've written 10 named CFs to the manifest. We should be able to
  // dynamically change the auto-tuning still based on the last "compacted"
  // manifest size of 7000 + some bytes.
  ASSERT_OK(db_->SetDBOptions({{"max_manifest_space_amp_pct", "51"}}));
  TrivialManifestWriteFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  // And the "compacted" manifest size has reset again, so should be changed
  // again sooner.
  ASSERT_EQ(handles.size(), 4U);
  for (int i = 1; i <= 2; ++i) {
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }
  // Enough for manifest change
  AddCfFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);

  // ---- Verify persisted compacted manifest size survives close/reopen ----
  // Close with CFs still live. Reopen with reuse_manifest_on_open so the
  // manifest is NOT rewritten from scratch. The persisted compacted size
  // should be loaded and used for auto-tuning.
  // At this point we have 7 CF handles plus default.
  ASSERT_EQ(handles.size(), 7U);

  // Collect CF names for reopen, then release handles (Close needs this)
  std::vector<std::string> cf_names = {"default"};
  for (auto* h : handles) {
    cf_names.push_back(h->GetName());
  }
  for (auto* h : handles) {
    ASSERT_OK(db_->DestroyColumnFamilyHandle(h));
  }
  handles.clear();

  Close();
  // Use a large max_manifest_file_size so the reused manifest (which is
  // already ~10KB) does NOT trigger rotation on the first few writes.
  // Auto-tuning with the persisted compacted size (~5KB) at 200% amp
  // gives a tuned threshold of ~15KB. Without persistence, the threshold
  // would be max(max_manifest_file_size, 0 * anything) =
  // max_manifest_file_size.
  //
  // We set max_manifest_file_size to two values to distinguish:
  // - 3000: if persisted compacted size is NOT loaded, tuned = 3000,
  //   and the first AddCf will rotate (manifest is already ~10KB > 3000)
  // - With persisted compacted size loaded, tuned = max(3000, 5000*3) = 15000,
  //   so no rotation until we exceed 15KB
  Close();
  options.max_manifest_file_size = 3000;
  options.max_manifest_space_amp_pct = 200;
  options.reuse_manifest_on_open = true;
  uint64_t manifest_num_before_reopen = cur_manifest_num;
  ReopenWithColumnFamilies(cf_names, options);
  cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();

  // With persistence: the manifest file number should NOT have changed
  // during reopen, because the persisted compacted size keeps the tuned
  // threshold high enough. Without persistence, last_compacted = 0, so
  // tuned = max_manifest_file_size = 3000, and the first LogAndApply
  // during Open rotates the manifest because it's already ~10KB > 3000.
  ASSERT_EQ(manifest_num_before_reopen, cur_manifest_num);

  // Adding CFs should still not trigger rotation because the tuned
  // threshold (~15KB) exceeds the current manifest size (~10KB + adds).
  for (int i = 1; i <= 4; ++i) {
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }
  for (int i = 1; i <= 4; ++i) {
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }

  // Wrap up
  while (!handles.empty()) {
    DropCfFn();
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
