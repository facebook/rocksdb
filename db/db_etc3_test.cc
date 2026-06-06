//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "rocksdb/convenience.h"
#include "rocksdb/metadata.h"
#include "rocksdb/sst_file_writer.h"

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
  // Test setup: create many flushed files. Keep level compaction semantics so
  // DeleteFilesInRanges can remove non-L0 files, but prevent automatic
  // compactions and write stalls from adding unrelated behavior.
  options.disable_auto_compactions = true;
  options.level0_slowdown_writes_trigger = 100;
  options.level0_stop_writes_trigger = 200;

  // Test strategy: use large column family names to control the rough amount
  // of payload added to the MANIFEST. Drop manifest entries do not include the
  // CF name, so they are small.
  //
  // Most CF helper calls piggy-back a background manifest write so the main
  // auto-tuning phases continue to test the unrelaxed background threshold
  // even though CF manipulation itself is foreground. Phase-specific foreground
  // checks disable that piggy-backed background write.
  uint64_t prev_manifest_num = 0, cur_manifest_num = 0;
  std::deque<ColumnFamilyHandle*> handles;
  int counter = 5;
  auto UpdateManifestNumsFrom = [&](uint64_t before_manifest_num) {
    prev_manifest_num = before_manifest_num;
    cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
  };
  auto BackgroundManifestWriteFn = [&]() {
    uint64_t before_manifest_num = cur_manifest_num;
    ASSERT_OK(Put("x", std::to_string(counter++)));
    ASSERT_OK(Flush());
    UpdateManifestNumsFrom(before_manifest_num);
  };
  auto AddCfFn = [&](bool include_background_manifest_write = true) {
    uint64_t before_manifest_num = cur_manifest_num;
    std::string name = "cf" + std::to_string(counter++);
    name.resize(1000, 'a');
    ASSERT_OK(db_->CreateColumnFamily(options, name, &handles.emplace_back()));
    cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
    if (include_background_manifest_write) {
      BackgroundManifestWriteFn();
    }
    UpdateManifestNumsFrom(before_manifest_num);
  };
  auto DropCfFn = [&](bool include_background_manifest_write = true) {
    uint64_t before_manifest_num = cur_manifest_num;
    ASSERT_OK(db_->DropColumnFamily(handles.front()));
    ASSERT_OK(db_->DestroyColumnFamilyHandle(handles.front()));
    handles.pop_front();
    cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
    if (include_background_manifest_write) {
      BackgroundManifestWriteFn();
    }
    UpdateManifestNumsFrom(before_manifest_num);
  };

  // ---- Phase 1: foreground threshold relaxation is bounded ----
  //
  // Foreground operations should only get about 25% extra headroom, not an
  // unbounded threshold. With 1000-byte CF names and a 3000-byte normal limit,
  // the relaxed limit should allow the first four foreground-only CF additions
  // but require rotation on the fifth.
  options.max_manifest_file_size = 3000;
  options.max_manifest_space_amp_pct = 0;
  DestroyAndReopen(options);
  cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
  prev_manifest_num = cur_manifest_num;
  for (int i = 1; i <= 4; ++i) {
    AddCfFn(/*include_background_manifest_write=*/false);
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }
  AddCfFn(/*include_background_manifest_write=*/false);
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  while (!handles.empty()) {
    ASSERT_OK(db_->DestroyColumnFamilyHandle(handles.front()));
    handles.pop_front();
  }

  // ---- Phase 2: no auto-tuning means frequent rotation ----
  //
  options.max_manifest_file_size = 1000000;
  options.max_manifest_space_amp_pct = 0;  // no auto-tuning yet
  DestroyAndReopen(options);
  cur_manifest_num = dbfull()->TEST_Current_Manifest_FileNo();
  prev_manifest_num = cur_manifest_num;

  // With the generous minimum manifest size, should not be rotated.
  AddCfFn();
  AddCfFn();
  AddCfFn();
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // Lower the minimum while still disabling auto-tuning.
  ASSERT_OK(db_->SetDBOptions({{"max_manifest_file_size", "3000"}}));

  // Takes effect on the next manifest write
  BackgroundManifestWriteFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);

  // Now we have to rewrite the whole manifest on each write because the
  // compacted size exceeds the "max" size.
  AddCfFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  DropCfFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  AddCfFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  BackgroundManifestWriteFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);

  // ---- Phase 3: auto-tuning raises the background threshold ----
  //
  // Enabling auto-tuning should fix this, immediately for next manifest writes.
  // This will allow up to roughly double the size of the compacted manifest,
  // which now includes CF entries plus the piggy-backed background writes.
  ASSERT_EQ(handles.size(), 4U);
  ASSERT_OK(db_->SetDBOptions({{"max_manifest_space_amp_pct", "95"}}));

  // Auto-tuning lets several more CF names accumulate in the MANIFEST before
  // the piggy-backed background write crosses the threshold.
  for (int i = 1; i <= 3; ++i) {
    if ((i % 2) == 1) {
      DropCfFn();
    }
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }
  AddCfFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);

  // We now have a different last compacted manifest size, so the next
  // threshold should be based on the newly compacted MANIFEST.
  ASSERT_EQ(handles.size(), 6U);

  DropCfFn();
  DropCfFn();
  for (int i = 1; i <= 4; ++i) {
    DropCfFn();
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }
  // We should be able to dynamically change the auto-tuning still based on
  // the last "compacted" manifest size.
  ASSERT_OK(db_->SetDBOptions({{"max_manifest_space_amp_pct", "51"}}));
  BackgroundManifestWriteFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);
  // And the "compacted" manifest size has reset again, so should be changed
  // again sooner.
  ASSERT_EQ(handles.size(), 4U);
  for (int i = 1; i <= 2; ++i) {
    AddCfFn();
    ASSERT_EQ(prev_manifest_num, cur_manifest_num);
  }

  // ---- Phase 4: foreground operations use relaxed threshold ----
  //
  // The current MANIFEST is now large enough for the next background manifest
  // write to rotate it, but still small enough for foreground operations to use
  // their 25% extra headroom. Assert that each foreground operation stays on
  // the same MANIFEST, then verify the next background write rotates.
  const std::string external_files_dir = dbname_ + "/external_files";
  ASSERT_OK(env_->CreateDirIfMissing(external_files_dir));
  auto WriteExternalSstFile = [&](const std::string& file_name,
                                  const std::string& key,
                                  const std::string& value) {
    const std::string file_path = external_files_dir + "/" + file_name;
    Status ignored = env_->DeleteFile(file_path);
    ignored.PermitUncheckedError();

    SstFileWriter sst_file_writer(EnvOptions(), options);
    ASSERT_OK(sst_file_writer.Open(file_path));
    ASSERT_OK(sst_file_writer.Put(key, value));
    ASSERT_OK(sst_file_writer.Finish());
  };
  auto IngestExternalFileForegroundManifestWriteFn =
      [&](std::string* ingested_key) {
        uint64_t before_manifest_num = cur_manifest_num;
        const std::string key = "z" + std::to_string(counter++);
        const std::string value = "v" + std::to_string(counter++);
        const std::string file_name =
            "ingest_" + std::to_string(counter++) + ".sst";
        const std::string file_path = external_files_dir + "/" + file_name;
        WriteExternalSstFile(file_name, key, value);

        ASSERT_OK(
            db_->IngestExternalFile({file_path}, IngestExternalFileOptions()));
        ASSERT_EQ(value, Get(key));
        *ingested_key = key;
        Status ignored = env_->DeleteFile(file_path);
        ignored.PermitUncheckedError();
        UpdateManifestNumsFrom(before_manifest_num);
      };
  auto CreateColumnFamilyWithImportForegroundManifestWriteFn = [&]() {
    uint64_t before_manifest_num = cur_manifest_num;
    const std::string cf_name = "import_cf" + std::to_string(counter++);
    const std::string key = "import" + std::to_string(counter++);
    const std::string value = "v" + std::to_string(counter++);
    const std::string file_name =
        "import_" + std::to_string(counter++) + ".sst";
    const std::string file_path = external_files_dir + "/" + file_name;
    WriteExternalSstFile(file_name, key, value);

    LiveFileMetaData file_metadata;
    file_metadata.name = file_name;
    file_metadata.db_path = external_files_dir;
    file_metadata.smallest_seqno = 0;
    file_metadata.largest_seqno = 0;
    file_metadata.level = 0;
    ExportImportFilesMetaData metadata;
    metadata.files.push_back(file_metadata);
    metadata.db_comparator_name = options.comparator->Name();

    ColumnFamilyHandle* import_handle = nullptr;
    ASSERT_OK(db_->CreateColumnFamilyWithImport(options, cf_name,
                                                ImportColumnFamilyOptions(),
                                                metadata, &import_handle));
    ASSERT_NE(import_handle, nullptr);
    handles.push_back(import_handle);

    std::string result;
    ASSERT_OK(db_->Get(ReadOptions(), import_handle, key, &result));
    ASSERT_EQ(value, result);
    Status ignored = env_->DeleteFile(file_path);
    ignored.PermitUncheckedError();
    UpdateManifestNumsFrom(before_manifest_num);
  };
  auto DeleteFilesInRangesForegroundManifestWriteFn =
      [&](const std::string& key) {
        uint64_t before_manifest_num = cur_manifest_num;
        const std::string limit = key + "\xff";
        std::vector<RangeOpt> ranges;
        ranges.emplace_back(key, limit);
        ASSERT_OK(DeleteFilesInRanges(db_.get(), db_->DefaultColumnFamily(),
                                      ranges.data(), ranges.size(),
                                      /*include_end=*/false));

        std::string result;
        ASSERT_TRUE(db_->Get(ReadOptions(), key, &result).IsNotFound());
        UpdateManifestNumsFrom(before_manifest_num);
      };

  // Column family manipulation.
  AddCfFn(/*include_background_manifest_write=*/false);
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // SetOptions should not write to the MANIFEST. If that regresses, the write
  // should be treated as a background write and rotate here.
  {
    ASSERT_OK(db_->SetOptions({{"level0_slowdown_writes_trigger", "101"}}));
    UpdateManifestNumsFrom(cur_manifest_num);
  }
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // External file ingestion.
  std::string ingested_key;
  IngestExternalFileForegroundManifestWriteFn(&ingested_key);
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // Imported column family creation.
  CreateColumnFamilyWithImportForegroundManifestWriteFn();
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // Physical file deletion by range.
  DeleteFilesInRangesForegroundManifestWriteFn(ingested_key);
  ASSERT_EQ(prev_manifest_num, cur_manifest_num);

  // Background flush.
  BackgroundManifestWriteFn();
  ASSERT_LT(prev_manifest_num, cur_manifest_num);

  // ---- Phase 5: persisted compacted manifest size survives close/reopen ----
  // Close with CFs still live. Reopen with reuse_manifest_on_open so the
  // manifest is NOT rewritten from scratch. The persisted compacted size
  // should be loaded and used for auto-tuning.
  // At this point we have 8 CF handles plus default.
  ASSERT_EQ(handles.size(), 8U);

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
  // Use a max_manifest_file_size below the reused manifest size. Auto-tuning
  // with the persisted compacted size at 200% amp keeps the tuned threshold
  // high enough. Without persistence, the threshold would be
  // max(max_manifest_file_size, 0 * anything) = max_manifest_file_size.
  //
  // With max_manifest_file_size set to 3000, missing persisted compacted size
  // would keep tuned = 3000 and the first AddCf would rotate because the
  // reused manifest is already larger. With persisted compacted size loaded,
  // the tuned threshold is based on the compacted size, so no rotation until
  // the manifest grows well beyond the minimum.
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
  // during Open rotates the manifest because it is already larger than 3000.
  ASSERT_EQ(manifest_num_before_reopen, cur_manifest_num);

  // Adding CFs should still not trigger rotation because the tuned threshold
  // from the persisted compacted size exceeds the current manifest size.
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
    DropCfFn(/*include_background_manifest_write=*/false);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
