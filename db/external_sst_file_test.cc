//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <table/block_based/block_based_table_factory.h>

#include <functional>
#include <memory>

#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "file/filename.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/thread_guard.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

// A test environment that can be configured to fail the Link operation.
class ExternalSSTTestFS : public FileSystemWrapper {
 public:
  ExternalSSTTestFS(const std::shared_ptr<FileSystem>& t, bool fail_link)
      : FileSystemWrapper(t), fail_link_(fail_link) {}
  static const char* kClassName() { return "ExternalSSTTestFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus LinkFile(const std::string& s, const std::string& t,
                    const IOOptions& options, IODebugContext* dbg) override {
    if (fail_link_) {
      return IOStatus::NotSupported("Link failed");
    }
    return target()->LinkFile(s, t, options, dbg);
  }

  void set_fail_link(bool fail_link) { fail_link_ = fail_link; }

 private:
  bool fail_link_;
};

class ExternalSSTFileTestBase : public DBTestBase {
 public:
  ExternalSSTFileTestBase()
      : DBTestBase("external_sst_file_test", /*env_do_fsync=*/true) {
    sst_files_dir_ = dbname_ + "/sst_files/";
    DestroyAndRecreateExternalSSTFilesDir();
  }

  void DestroyAndRecreateExternalSSTFilesDir() {
    ASSERT_OK(DestroyDir(env_, sst_files_dir_));
    ASSERT_OK(env_->CreateDir(sst_files_dir_));
  }

  ~ExternalSSTFileTestBase() override {
    DestroyDir(env_, sst_files_dir_).PermitUncheckedError();
  }

 protected:
  std::string sst_files_dir_;
};

class ExternSSTFileLinkFailFallbackTest
    : public ExternalSSTFileTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool, bool>> {
 public:
  ExternSSTFileLinkFailFallbackTest() {
    fs_ = std::make_shared<ExternalSSTTestFS>(env_->GetFileSystem(), true);
    test_env_.reset(new CompositeEnvWrapper(env_, fs_));
    options_ = CurrentOptions();
    options_.disable_auto_compactions = true;
    options_.env = test_env_.get();
  }

  void TearDown() override {
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options_));
  }

 protected:
  Options options_;
  std::shared_ptr<ExternalSSTTestFS> fs_;
  std::unique_ptr<Env> test_env_;
};

class ExternalSSTFileTest
    : public ExternalSSTFileTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  ExternalSSTFileTest() = default;

  Status GenerateOneExternalFile(
      const Options& options, ColumnFamilyHandle* cfh,
      std::vector<std::pair<std::string, std::string>>& data, int file_id,
      bool sort_data, std::string* external_file_path,
      std::map<std::string, std::string>* true_data) {
    // Generate a file id if not provided
    if (-1 == file_id) {
      file_id = (++last_file_id_);
    }
    // Sort data if asked to do so
    if (sort_data) {
      std::sort(data.begin(), data.end(),
                [&](const std::pair<std::string, std::string>& e1,
                    const std::pair<std::string, std::string>& e2) {
                  return options.comparator->Compare(e1.first, e2.first) < 0;
                });
      auto uniq_iter = std::unique(
          data.begin(), data.end(),
          [&](const std::pair<std::string, std::string>& e1,
              const std::pair<std::string, std::string>& e2) {
            return options.comparator->Compare(e1.first, e2.first) == 0;
          });
      data.resize(uniq_iter - data.begin());
    }
    std::string file_path = sst_files_dir_ + std::to_string(file_id);
    SstFileWriter sst_file_writer(EnvOptions(), options, cfh);
    Status s = sst_file_writer.Open(file_path);
    if (!s.ok()) {
      return s;
    }
    for (const auto& entry : data) {
      s = sst_file_writer.Put(entry.first, entry.second);
      if (!s.ok()) {
        sst_file_writer.Finish().PermitUncheckedError();
        return s;
      }
    }
    s = sst_file_writer.Finish();
    if (s.ok() && external_file_path != nullptr) {
      *external_file_path = file_path;
    }
    if (s.ok() && nullptr != true_data) {
      for (const auto& entry : data) {
        true_data->insert({entry.first, entry.second});
      }
    }
    return s;
  }

  Status GenerateAndAddExternalFile(
      const Options options,
      std::vector<std::pair<std::string, std::string>> data, int file_id = -1,
      bool allow_global_seqno = false, bool write_global_seqno = false,
      bool verify_checksums_before_ingest = true, bool ingest_behind = false,
      bool sort_data = false,
      std::map<std::string, std::string>* true_data = nullptr,
      ColumnFamilyHandle* cfh = nullptr, bool fill_cache = false) {
    // Generate a file id if not provided
    if (file_id == -1) {
      file_id = last_file_id_ + 1;
      last_file_id_++;
    }

    // Sort data if asked to do so
    if (sort_data) {
      std::sort(data.begin(), data.end(),
                [&](const std::pair<std::string, std::string>& e1,
                    const std::pair<std::string, std::string>& e2) {
                  return options.comparator->Compare(e1.first, e2.first) < 0;
                });
      auto uniq_iter = std::unique(
          data.begin(), data.end(),
          [&](const std::pair<std::string, std::string>& e1,
              const std::pair<std::string, std::string>& e2) {
            return options.comparator->Compare(e1.first, e2.first) == 0;
          });
      data.resize(uniq_iter - data.begin());
    }
    std::string file_path = sst_files_dir_ + std::to_string(file_id);
    SstFileWriter sst_file_writer(EnvOptions(), options, cfh);

    Status s = sst_file_writer.Open(file_path);
    if (!s.ok()) {
      return s;
    }
    for (auto& entry : data) {
      s = sst_file_writer.Put(entry.first, entry.second);
      if (!s.ok()) {
        sst_file_writer.Finish().PermitUncheckedError();
        return s;
      }
    }
    s = sst_file_writer.Finish();

    if (s.ok()) {
      IngestExternalFileOptions ifo;
      ifo.allow_global_seqno = allow_global_seqno;
      ifo.write_global_seqno = allow_global_seqno ? write_global_seqno : false;
      ifo.verify_checksums_before_ingest = verify_checksums_before_ingest;
      ifo.ingest_behind = ingest_behind;
      ifo.fill_cache = fill_cache;
      if (cfh) {
        s = db_->IngestExternalFile(cfh, {file_path}, ifo);
      } else {
        s = db_->IngestExternalFile({file_path}, ifo);
      }
    }

    if (s.ok() && true_data) {
      for (auto& entry : data) {
        (*true_data)[entry.first] = entry.second;
      }
    }

    return s;
  }

  Status GenerateAndAddExternalFiles(
      const Options& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      const std::vector<IngestExternalFileOptions>& ifos,
      std::vector<std::vector<std::pair<std::string, std::string>>>& data,
      int file_id, bool sort_data,
      std::vector<std::map<std::string, std::string>>& true_data) {
    if (-1 == file_id) {
      file_id = (++last_file_id_);
    }
    // Generate external SST files, one for each column family
    size_t num_cfs = column_families.size();
    assert(ifos.size() == num_cfs);
    assert(data.size() == num_cfs);
    std::vector<IngestExternalFileArg> args(num_cfs);
    for (size_t i = 0; i != num_cfs; ++i) {
      std::string external_file_path;
      Status s = GenerateOneExternalFile(
          options, column_families[i], data[i], file_id, sort_data,
          &external_file_path,
          true_data.size() == num_cfs ? &true_data[i] : nullptr);
      if (!s.ok()) {
        return s;
      }
      ++file_id;

      args[i].column_family = column_families[i];
      args[i].external_files.push_back(external_file_path);
      args[i].options = ifos[i];
    }
    return db_->IngestExternalFiles(args);
  }

  Status GenerateAndAddExternalFile(
      const Options options, std::vector<std::pair<int, std::string>> data,
      int file_id = -1, bool allow_global_seqno = false,
      bool write_global_seqno = false,
      bool verify_checksums_before_ingest = true, bool ingest_behind = false,
      bool sort_data = false,
      std::map<std::string, std::string>* true_data = nullptr,
      ColumnFamilyHandle* cfh = nullptr) {
    std::vector<std::pair<std::string, std::string>> file_data;
    for (auto& entry : data) {
      file_data.emplace_back(Key(entry.first), entry.second);
    }
    return GenerateAndAddExternalFile(options, file_data, file_id,
                                      allow_global_seqno, write_global_seqno,
                                      verify_checksums_before_ingest,
                                      ingest_behind, sort_data, true_data, cfh);
  }

  Status GenerateAndAddExternalFile(
      const Options options, std::vector<int> keys, int file_id = -1,
      bool allow_global_seqno = false, bool write_global_seqno = false,
      bool verify_checksums_before_ingest = true, bool ingest_behind = false,
      bool sort_data = false,
      std::map<std::string, std::string>* true_data = nullptr,
      ColumnFamilyHandle* cfh = nullptr, bool fill_cache = false) {
    std::vector<std::pair<std::string, std::string>> file_data;
    for (auto& k : keys) {
      file_data.emplace_back(Key(k), Key(k) + std::to_string(file_id));
    }
    return GenerateAndAddExternalFile(
        options, file_data, file_id, allow_global_seqno, write_global_seqno,
        verify_checksums_before_ingest, ingest_behind, sort_data, true_data,
        cfh, fill_cache);
  }

  Status DeprecatedAddFile(const std::vector<std::string>& files,
                           bool move_files = false,
                           bool skip_snapshot_check = false,
                           bool skip_write_global_seqno = false) {
    IngestExternalFileOptions opts;
    opts.move_files = move_files;
    opts.snapshot_consistency = !skip_snapshot_check;
    opts.allow_global_seqno = false;
    opts.allow_blocking_flush = false;
    opts.write_global_seqno = !skip_write_global_seqno;
    return db_->IngestExternalFile(files, opts);
  }

 protected:
  int last_file_id_ = 0;
};

TEST_F(ExternalSSTFileTest, ComparatorMismatch) {
  Options options = CurrentOptions();
  Options options_diff_ucmp = options;

  options.comparator = BytewiseComparator();
  options_diff_ucmp.comparator = ReverseBytewiseComparator();

  SstFileWriter sst_file_writer(EnvOptions(), options_diff_ucmp);

  std::string file = sst_files_dir_ + "file.sst";
  ASSERT_OK(sst_file_writer.Open(file));
  ASSERT_OK(sst_file_writer.Put("foo", "val"));
  ASSERT_OK(sst_file_writer.Put("bar", "val1"));
  ASSERT_OK(sst_file_writer.Finish());

  DestroyAndReopen(options);
  ASSERT_NOK(DeprecatedAddFile({file}));
}

TEST_F(ExternalSSTFileTest, NoBlockCache) {
  LRUCacheOptions co;
  co.capacity = 32 << 20;
  std::shared_ptr<Cache> cache = NewLRUCache(co);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  table_options.cache_index_and_filter_blocks = true;
  Options options = CurrentOptions();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  size_t usage_before_ingestion = cache->GetUsage();
  std::map<std::string, std::string> true_data;
  // Ingest with fill_cache = true
  ASSERT_OK(GenerateAndAddExternalFile(options, {1, 2}, -1, false, false, true,
                                       false, false, &true_data, nullptr,
                                       /*fill_cache=*/true));
  ASSERT_EQ(FilesPerLevel(), "0,0,0,0,0,0,1");
  EXPECT_GT(cache->GetUsage(), usage_before_ingestion);

  TablePropertiesCollection tp;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&tp));
  for (const auto& entry : tp) {
    EXPECT_GT(entry.second->index_size, 0);
    EXPECT_GT(entry.second->filter_size, 0);
  }

  usage_before_ingestion = cache->GetUsage();
  // Ingest with fill_cache = false
  ASSERT_OK(GenerateAndAddExternalFile(options, {3, 4}, -1, false, false, true,
                                       false, false, &true_data, nullptr,
                                       /*fill_cache=*/false));
  EXPECT_EQ(usage_before_ingestion, cache->GetUsage());

  tp.clear();
  ASSERT_OK(db_->GetPropertiesOfAllTables(&tp));
  for (const auto& entry : tp) {
    EXPECT_GT(entry.second->index_size, 0);
    EXPECT_GT(entry.second->filter_size, 0);
  }
}

TEST_F(ExternalSSTFileTest, Basic) {
  do {
    Options options = CurrentOptions();

    SstFileWriter sst_file_writer(EnvOptions(), options);

    // Current file size should be 0 after sst_file_writer init and before open
    // a file.
    ASSERT_EQ(sst_file_writer.FileSize(), 0);

    // file1.sst (0 => 99)
    std::string file1 = sst_files_dir_ + "file1.sst";
    ASSERT_OK(sst_file_writer.Open(file1));
    for (int k = 0; k < 100; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
    }
    ExternalSstFileInfo file1_info;
    ASSERT_OK(sst_file_writer.Finish(&file1_info));

    // Current file size should be non-zero after success write.
    ASSERT_GT(sst_file_writer.FileSize(), 0);

    ASSERT_EQ(file1_info.file_path, file1);
    ASSERT_EQ(file1_info.num_entries, 100);
    ASSERT_EQ(file1_info.smallest_key, Key(0));
    ASSERT_EQ(file1_info.largest_key, Key(99));
    ASSERT_EQ(file1_info.num_range_del_entries, 0);
    ASSERT_EQ(file1_info.smallest_range_del_key, "");
    ASSERT_EQ(file1_info.largest_range_del_key, "");
    // sst_file_writer already finished, cannot add this value
    ASSERT_NOK(sst_file_writer.Put(Key(100), "bad_val"));

    // file2.sst (100 => 199)
    std::string file2 = sst_files_dir_ + "file2.sst";
    ASSERT_OK(sst_file_writer.Open(file2));
    for (int k = 100; k < 200; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
    }
    // Cannot add this key because it's not after last added key
    ASSERT_NOK(sst_file_writer.Put(Key(99), "bad_val"));
    ExternalSstFileInfo file2_info;
    ASSERT_OK(sst_file_writer.Finish(&file2_info));
    ASSERT_EQ(file2_info.file_path, file2);
    ASSERT_EQ(file2_info.num_entries, 100);
    ASSERT_EQ(file2_info.smallest_key, Key(100));
    ASSERT_EQ(file2_info.largest_key, Key(199));

    // file3.sst (195 => 299)
    // This file values overlap with file2 values
    std::string file3 = sst_files_dir_ + "file3.sst";
    ASSERT_OK(sst_file_writer.Open(file3));
    for (int k = 195; k < 300; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
    }
    ExternalSstFileInfo file3_info;
    ASSERT_OK(sst_file_writer.Finish(&file3_info));

    // Current file size should be non-zero after success finish.
    ASSERT_GT(sst_file_writer.FileSize(), 0);
    ASSERT_EQ(file3_info.file_path, file3);
    ASSERT_EQ(file3_info.num_entries, 105);
    ASSERT_EQ(file3_info.smallest_key, Key(195));
    ASSERT_EQ(file3_info.largest_key, Key(299));

    // file4.sst (30 => 39)
    // This file values overlap with file1 values
    std::string file4 = sst_files_dir_ + "file4.sst";
    ASSERT_OK(sst_file_writer.Open(file4));
    for (int k = 30; k < 40; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
    }
    ExternalSstFileInfo file4_info;
    ASSERT_OK(sst_file_writer.Finish(&file4_info));
    ASSERT_EQ(file4_info.file_path, file4);
    ASSERT_EQ(file4_info.num_entries, 10);
    ASSERT_EQ(file4_info.smallest_key, Key(30));
    ASSERT_EQ(file4_info.largest_key, Key(39));

    // file5.sst (400 => 499)
    std::string file5 = sst_files_dir_ + "file5.sst";
    ASSERT_OK(sst_file_writer.Open(file5));
    for (int k = 400; k < 500; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
    }
    ExternalSstFileInfo file5_info;
    ASSERT_OK(sst_file_writer.Finish(&file5_info));
    ASSERT_EQ(file5_info.file_path, file5);
    ASSERT_EQ(file5_info.num_entries, 100);
    ASSERT_EQ(file5_info.smallest_key, Key(400));
    ASSERT_EQ(file5_info.largest_key, Key(499));

    // file6.sst (delete 400 => 500)
    std::string file6 = sst_files_dir_ + "file6.sst";
    ASSERT_OK(sst_file_writer.Open(file6));
    ASSERT_OK(sst_file_writer.DeleteRange(Key(400), Key(500)));
    ExternalSstFileInfo file6_info;
    ASSERT_OK(sst_file_writer.Finish(&file6_info));
    ASSERT_EQ(file6_info.file_path, file6);
    ASSERT_EQ(file6_info.num_entries, 0);
    ASSERT_EQ(file6_info.smallest_key, "");
    ASSERT_EQ(file6_info.largest_key, "");
    ASSERT_EQ(file6_info.num_range_del_entries, 1);
    ASSERT_EQ(file6_info.smallest_range_del_key, Key(400));
    ASSERT_EQ(file6_info.largest_range_del_key, Key(500));

    // file7.sst (delete 500 => 570, put 520 => 599 divisible by 2)
    std::string file7 = sst_files_dir_ + "file7.sst";
    ASSERT_OK(sst_file_writer.Open(file7));
    ASSERT_OK(sst_file_writer.DeleteRange(Key(500), Key(550)));
    for (int k = 520; k < 560; k += 2) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
    }
    ASSERT_OK(sst_file_writer.DeleteRange(Key(525), Key(575)));
    for (int k = 560; k < 600; k += 2) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
    }
    ExternalSstFileInfo file7_info;
    ASSERT_OK(sst_file_writer.Finish(&file7_info));
    ASSERT_EQ(file7_info.file_path, file7);
    ASSERT_EQ(file7_info.num_entries, 40);
    ASSERT_EQ(file7_info.smallest_key, Key(520));
    ASSERT_EQ(file7_info.largest_key, Key(598));
    ASSERT_EQ(file7_info.num_range_del_entries, 2);
    ASSERT_EQ(file7_info.smallest_range_del_key, Key(500));
    ASSERT_EQ(file7_info.largest_range_del_key, Key(575));

    // file8.sst (delete 600 => 700)
    std::string file8 = sst_files_dir_ + "file8.sst";
    ASSERT_OK(sst_file_writer.Open(file8));
    ASSERT_OK(sst_file_writer.DeleteRange(Key(600), Key(700)));
    ExternalSstFileInfo file8_info;
    ASSERT_OK(sst_file_writer.Finish(&file8_info));
    ASSERT_EQ(file8_info.file_path, file8);
    ASSERT_EQ(file8_info.num_entries, 0);
    ASSERT_EQ(file8_info.smallest_key, "");
    ASSERT_EQ(file8_info.largest_key, "");
    ASSERT_EQ(file8_info.num_range_del_entries, 1);
    ASSERT_EQ(file8_info.smallest_range_del_key, Key(600));
    ASSERT_EQ(file8_info.largest_range_del_key, Key(700));

    // Cannot create an empty sst file
    std::string file_empty = sst_files_dir_ + "file_empty.sst";
    ExternalSstFileInfo file_empty_info;
    ASSERT_NOK(sst_file_writer.Finish(&file_empty_info));

    DestroyAndReopen(options);
    // Add file using file path
    ASSERT_OK(DeprecatedAddFile({file1}));
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
    for (int k = 0; k < 100; k++) {
      ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
    }

    // Add file while holding a snapshot will fail
    const Snapshot* s1 = db_->GetSnapshot();
    if (s1 != nullptr) {
      ASSERT_NOK(DeprecatedAddFile({file2}));
      db_->ReleaseSnapshot(s1);
    }
    // We can add the file after releaseing the snapshot
    ASSERT_OK(DeprecatedAddFile({file2}));

    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
    for (int k = 0; k < 200; k++) {
      ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
    }

    // This file has overlapping values with the existing data
    ASSERT_NOK(DeprecatedAddFile({file3}));

    // This file has overlapping values with the existing data
    ASSERT_NOK(DeprecatedAddFile({file4}));

    // Overwrite values of keys divisible by 5
    for (int k = 0; k < 200; k += 5) {
      ASSERT_OK(Put(Key(k), Key(k) + "_val_new"));
    }
    ASSERT_NE(db_->GetLatestSequenceNumber(), 0U);

    // Key range of file5 (400 => 499) don't overlap with any keys in DB
    ASSERT_OK(DeprecatedAddFile({file5}));

    // This file has overlapping values with the existing data
    ASSERT_NOK(DeprecatedAddFile({file6}));

    // Key range of file7 (500 => 598) don't overlap with any keys in DB
    ASSERT_OK(DeprecatedAddFile({file7}));

    // Key range of file7 (600 => 700) don't overlap with any keys in DB
    ASSERT_OK(DeprecatedAddFile({file8}));

    // Make sure values are correct before and after flush/compaction
    for (int i = 0; i < 2; i++) {
      for (int k = 0; k < 200; k++) {
        std::string value = Key(k) + "_val";
        if (k % 5 == 0) {
          value += "_new";
        }
        ASSERT_EQ(Get(Key(k)), value);
      }
      for (int k = 400; k < 500; k++) {
        std::string value = Key(k) + "_val";
        ASSERT_EQ(Get(Key(k)), value);
      }
      for (int k = 500; k < 600; k++) {
        std::string value = Key(k) + "_val";
        if (k < 520 || k % 2 == 1) {
          value = "NOT_FOUND";
        }
        ASSERT_EQ(Get(Key(k)), value);
      }
      ASSERT_OK(Flush());
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }

    Close();
    options.disable_auto_compactions = true;
    Reopen(options);

    // Delete keys in range (400 => 499)
    for (int k = 400; k < 500; k++) {
      ASSERT_OK(Delete(Key(k)));
    }
    // We deleted range (400 => 499) but cannot add file5 because
    // of the range tombstones
    ASSERT_NOK(DeprecatedAddFile({file5}));

    // Compacting the DB will remove the tombstones
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    // Now we can add the file
    ASSERT_OK(DeprecatedAddFile({file5}));

    // Verify values of file5 in DB
    for (int k = 400; k < 500; k++) {
      std::string value = Key(k) + "_val";
      ASSERT_EQ(Get(Key(k)), value);
    }
    DestroyAndRecreateExternalSSTFilesDir();
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction |
                         kRangeDelSkipConfigs));
}

TEST_F(ExternalSSTFileTest, BasicWideColumn) {
  do {
    Options options = CurrentOptions();

    SstFileWriter sst_file_writer(EnvOptions(), options);

    // Current file size should be 0 after sst_file_writer init and before open
    // a file.
    ASSERT_EQ(sst_file_writer.FileSize(), 0);

    std::string file = sst_files_dir_ + "wide_column_file.sst";
    ASSERT_OK(sst_file_writer.Open(file));
    for (int k = 0; k < 10; k++) {
      std::string val1 = Key(k) + "_attr_1_val";
      std::string val2 = Key(k) + "_attr_2_val";
      WideColumns columns{{"attr_1", val1}, {"attr_2", val2}};
      ASSERT_OK(sst_file_writer.PutEntity(Key(k), columns));
    }
    ExternalSstFileInfo file_info;
    ASSERT_OK(sst_file_writer.Finish(&file_info));

    // Current file size should be non-zero after success write.
    ASSERT_GT(sst_file_writer.FileSize(), 0);

    ASSERT_EQ(file_info.file_path, file);
    ASSERT_EQ(file_info.num_entries, 10);
    ASSERT_EQ(file_info.smallest_key, Key(0));
    ASSERT_EQ(file_info.largest_key, Key(9));
    ASSERT_EQ(file_info.num_range_del_entries, 0);
    ASSERT_EQ(file_info.smallest_range_del_key, "");
    ASSERT_EQ(file_info.largest_range_del_key, "");

    DestroyAndReopen(options);
    // Add file using file path
    ASSERT_OK(DeprecatedAddFile({file}));
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
    for (int k = 0; k < 10; k++) {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               Key(k), &result));
      std::string val1 = Key(k) + "_attr_1_val";
      std::string val2 = Key(k) + "_attr_2_val";
      WideColumns expected_columns{{"attr_1", val1}, {"attr_2", val2}};
      ASSERT_EQ(result.columns(), expected_columns);
    }

  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction |
                         kRangeDelSkipConfigs));
}

TEST_F(ExternalSSTFileTest, BasicMixed) {
  do {
    Options options = CurrentOptions();

    SstFileWriter sst_file_writer(EnvOptions(), options);

    // Current file size should be 0 after sst_file_writer init and before open
    // a file.
    ASSERT_EQ(sst_file_writer.FileSize(), 0);

    std::string file = sst_files_dir_ + "mixed_file.sst";
    ASSERT_OK(sst_file_writer.Open(file));
    for (int k = 0; k < 100; k++) {
      if (k % 5 == 0) {
        std::string val1 = Key(k) + "_attr_1_val";
        std::string val2 = Key(k) + "_attr_2_val";
        WideColumns columns{{"attr_1", val1}, {"attr_2", val2}};
        ASSERT_OK(sst_file_writer.PutEntity(Key(k), columns));
      } else {
        ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
      }
    }
    ExternalSstFileInfo file_info;
    ASSERT_OK(sst_file_writer.Finish(&file_info));

    // Current file size should be non-zero after success write.
    ASSERT_GT(sst_file_writer.FileSize(), 0);

    ASSERT_EQ(file_info.file_path, file);
    ASSERT_EQ(file_info.num_entries, 100);
    ASSERT_EQ(file_info.smallest_key, Key(0));
    ASSERT_EQ(file_info.largest_key, Key(99));
    ASSERT_EQ(file_info.num_range_del_entries, 0);
    ASSERT_EQ(file_info.smallest_range_del_key, "");
    ASSERT_EQ(file_info.largest_range_del_key, "");

    DestroyAndReopen(options);
    // Add file using file path
    ASSERT_OK(DeprecatedAddFile({file}));
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
    for (int k = 0; k < 10; k++) {
      if (k % 5 == 0) {
        PinnableWideColumns result;
        ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                                 Key(k), &result));
        std::string val1 = Key(k) + "_attr_1_val";
        std::string val2 = Key(k) + "_attr_2_val";
        WideColumns expected_columns{{"attr_1", val1}, {"attr_2", val2}};
        ASSERT_EQ(result.columns(), expected_columns);
      } else {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
      }
    }
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction |
                         kRangeDelSkipConfigs));
}

class SstFileWriterCollector : public TablePropertiesCollector {
 public:
  explicit SstFileWriterCollector(const std::string prefix) : prefix_(prefix) {
    name_ = prefix_ + "_SstFileWriterCollector";
  }

  const char* Name() const override { return name_.c_str(); }

  Status Finish(UserCollectedProperties* properties) override {
    std::string count = std::to_string(count_);
    properties->insert({prefix_ + "_SstFileWriterCollector", "YES"});
    properties->insert({prefix_ + "_Count", count});
    return Status::OK();
  }

  Status AddUserKey(const Slice& /*user_key*/, const Slice& /*value*/,
                    EntryType /*type*/, SequenceNumber /*seq*/,
                    uint64_t /*file_size*/) override {
    ++count_;
    return Status::OK();
  }

  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties{};
  }

 private:
  uint32_t count_ = 0;
  std::string prefix_;
  std::string name_;
};

class SstFileWriterCollectorFactory : public TablePropertiesCollectorFactory {
 public:
  explicit SstFileWriterCollectorFactory(std::string prefix)
      : prefix_(prefix), num_created_(0) {}
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /*context*/) override {
    num_created_++;
    return new SstFileWriterCollector(prefix_);
  }
  const char* Name() const override { return "SstFileWriterCollectorFactory"; }

  std::string prefix_;
  uint32_t num_created_;
};

TEST_F(ExternalSSTFileTest, AddList) {
  do {
    Options options = CurrentOptions();

    auto abc_collector = std::make_shared<SstFileWriterCollectorFactory>("abc");
    auto xyz_collector = std::make_shared<SstFileWriterCollectorFactory>("xyz");

    options.table_properties_collector_factories.emplace_back(abc_collector);
    options.table_properties_collector_factories.emplace_back(xyz_collector);

    SstFileWriter sst_file_writer(EnvOptions(), options);

    // file1.sst (0 => 99)
    std::string file1 = sst_files_dir_ + "file1.sst";
    ASSERT_OK(sst_file_writer.Open(file1));
    for (int k = 0; k < 100; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
    }
    ExternalSstFileInfo file1_info;
    ASSERT_OK(sst_file_writer.Finish(&file1_info));
    ASSERT_EQ(file1_info.file_path, file1);
    ASSERT_EQ(file1_info.num_entries, 100);
    ASSERT_EQ(file1_info.smallest_key, Key(0));
    ASSERT_EQ(file1_info.largest_key, Key(99));
    // sst_file_writer already finished, cannot add this value
    ASSERT_NOK(sst_file_writer.Put(Key(100), "bad_val"));

    // file2.sst (100 => 199)
    std::string file2 = sst_files_dir_ + "file2.sst";
    ASSERT_OK(sst_file_writer.Open(file2));
    for (int k = 100; k < 200; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
    }
    // Cannot add this key because it's not after last added key
    ASSERT_NOK(sst_file_writer.Put(Key(99), "bad_val"));
    ExternalSstFileInfo file2_info;
    ASSERT_OK(sst_file_writer.Finish(&file2_info));
    ASSERT_EQ(file2_info.file_path, file2);
    ASSERT_EQ(file2_info.num_entries, 100);
    ASSERT_EQ(file2_info.smallest_key, Key(100));
    ASSERT_EQ(file2_info.largest_key, Key(199));

    // file3.sst (195 => 199)
    // This file values overlap with file2 values
    std::string file3 = sst_files_dir_ + "file3.sst";
    ASSERT_OK(sst_file_writer.Open(file3));
    for (int k = 195; k < 200; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
    }
    ExternalSstFileInfo file3_info;
    ASSERT_OK(sst_file_writer.Finish(&file3_info));
    ASSERT_EQ(file3_info.file_path, file3);
    ASSERT_EQ(file3_info.num_entries, 5);
    ASSERT_EQ(file3_info.smallest_key, Key(195));
    ASSERT_EQ(file3_info.largest_key, Key(199));

    // file4.sst (30 => 39)
    // This file values overlap with file1 values
    std::string file4 = sst_files_dir_ + "file4.sst";
    ASSERT_OK(sst_file_writer.Open(file4));
    for (int k = 30; k < 40; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
    }
    ExternalSstFileInfo file4_info;
    ASSERT_OK(sst_file_writer.Finish(&file4_info));
    ASSERT_EQ(file4_info.file_path, file4);
    ASSERT_EQ(file4_info.num_entries, 10);
    ASSERT_EQ(file4_info.smallest_key, Key(30));
    ASSERT_EQ(file4_info.largest_key, Key(39));

    // file5.sst (200 => 299)
    std::string file5 = sst_files_dir_ + "file5.sst";
    ASSERT_OK(sst_file_writer.Open(file5));
    for (int k = 200; k < 300; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
    }
    ExternalSstFileInfo file5_info;
    ASSERT_OK(sst_file_writer.Finish(&file5_info));
    ASSERT_EQ(file5_info.file_path, file5);
    ASSERT_EQ(file5_info.num_entries, 100);
    ASSERT_EQ(file5_info.smallest_key, Key(200));
    ASSERT_EQ(file5_info.largest_key, Key(299));

    // file6.sst (delete 0 => 100)
    std::string file6 = sst_files_dir_ + "file6.sst";
    ASSERT_OK(sst_file_writer.Open(file6));
    ASSERT_OK(sst_file_writer.DeleteRange(Key(0), Key(75)));
    ASSERT_OK(sst_file_writer.DeleteRange(Key(25), Key(100)));
    ExternalSstFileInfo file6_info;
    ASSERT_OK(sst_file_writer.Finish(&file6_info));
    ASSERT_EQ(file6_info.file_path, file6);
    ASSERT_EQ(file6_info.num_entries, 0);
    ASSERT_EQ(file6_info.smallest_key, "");
    ASSERT_EQ(file6_info.largest_key, "");
    ASSERT_EQ(file6_info.num_range_del_entries, 2);
    ASSERT_EQ(file6_info.smallest_range_del_key, Key(0));
    ASSERT_EQ(file6_info.largest_range_del_key, Key(100));

    // file7.sst (delete 99 => 201)
    std::string file7 = sst_files_dir_ + "file7.sst";
    ASSERT_OK(sst_file_writer.Open(file7));
    ASSERT_OK(sst_file_writer.DeleteRange(Key(99), Key(201)));
    ExternalSstFileInfo file7_info;
    ASSERT_OK(sst_file_writer.Finish(&file7_info));
    ASSERT_EQ(file7_info.file_path, file7);
    ASSERT_EQ(file7_info.num_entries, 0);
    ASSERT_EQ(file7_info.smallest_key, "");
    ASSERT_EQ(file7_info.largest_key, "");
    ASSERT_EQ(file7_info.num_range_del_entries, 1);
    ASSERT_EQ(file7_info.smallest_range_del_key, Key(99));
    ASSERT_EQ(file7_info.largest_range_del_key, Key(201));

    // list 1 has internal key range conflict
    std::vector<std::string> file_list0({file1, file2});
    std::vector<std::string> file_list1({file3, file2, file1});
    std::vector<std::string> file_list2({file5});
    std::vector<std::string> file_list3({file3, file4});
    std::vector<std::string> file_list4({file5, file7});
    std::vector<std::string> file_list5({file6, file7});

    DestroyAndReopen(options);

    // These lists of files have key ranges that overlap with each other
    ASSERT_NOK(DeprecatedAddFile(file_list1));
    // Both of the following overlap on the range deletion tombstone.
    ASSERT_NOK(DeprecatedAddFile(file_list4));
    ASSERT_NOK(DeprecatedAddFile(file_list5));

    // Add files using file path list
    ASSERT_OK(DeprecatedAddFile(file_list0));
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
    for (int k = 0; k < 200; k++) {
      ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
    }

    TablePropertiesCollection props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
    ASSERT_EQ(props.size(), 2);
    for (const auto& file_props : props) {
      auto user_props = file_props.second->user_collected_properties;
      ASSERT_EQ(user_props["abc_SstFileWriterCollector"], "YES");
      ASSERT_EQ(user_props["xyz_SstFileWriterCollector"], "YES");
      ASSERT_EQ(user_props["abc_Count"], "100");
      ASSERT_EQ(user_props["xyz_Count"], "100");
    }

    // Add file while holding a snapshot will fail
    const Snapshot* s1 = db_->GetSnapshot();
    if (s1 != nullptr) {
      ASSERT_NOK(DeprecatedAddFile(file_list2));
      db_->ReleaseSnapshot(s1);
    }
    // We can add the file after releaseing the snapshot
    ASSERT_OK(DeprecatedAddFile(file_list2));
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
    for (int k = 0; k < 300; k++) {
      ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
    }

    ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
    ASSERT_EQ(props.size(), 3);
    for (const auto& file_props : props) {
      auto user_props = file_props.second->user_collected_properties;
      ASSERT_EQ(user_props["abc_SstFileWriterCollector"], "YES");
      ASSERT_EQ(user_props["xyz_SstFileWriterCollector"], "YES");
      ASSERT_EQ(user_props["abc_Count"], "100");
      ASSERT_EQ(user_props["xyz_Count"], "100");
    }

    // This file list has overlapping values with the existing data
    ASSERT_NOK(DeprecatedAddFile(file_list3));

    // Overwrite values of keys divisible by 5
    for (int k = 0; k < 200; k += 5) {
      ASSERT_OK(Put(Key(k), Key(k) + "_val_new"));
    }
    ASSERT_NE(db_->GetLatestSequenceNumber(), 0U);

    // Make sure values are correct before and after flush/compaction
    for (int i = 0; i < 2; i++) {
      for (int k = 0; k < 200; k++) {
        std::string value = Key(k) + "_val";
        if (k % 5 == 0) {
          value += "_new";
        }
        ASSERT_EQ(Get(Key(k)), value);
      }
      for (int k = 200; k < 300; k++) {
        std::string value = Key(k) + "_val";
        ASSERT_EQ(Get(Key(k)), value);
      }
      ASSERT_OK(Flush());
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }

    // Delete keys in range (200 => 299)
    for (int k = 200; k < 300; k++) {
      ASSERT_OK(Delete(Key(k)));
    }
    // We deleted range (200 => 299) but cannot add file5 because
    // of the range tombstones
    ASSERT_NOK(DeprecatedAddFile(file_list2));

    // Compacting the DB will remove the tombstones
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    // Now we can add the file
    ASSERT_OK(DeprecatedAddFile(file_list2));

    // Verify values of file5 in DB
    for (int k = 200; k < 300; k++) {
      std::string value = Key(k) + "_val";
      ASSERT_EQ(Get(Key(k)), value);
    }
    DestroyAndRecreateExternalSSTFilesDir();
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction |
                         kRangeDelSkipConfigs));
}

TEST_F(ExternalSSTFileTest, AddListAtomicity) {
  do {
    Options options = CurrentOptions();

    SstFileWriter sst_file_writer(EnvOptions(), options);

    // files[0].sst (0 => 99)
    // files[1].sst (100 => 199)
    // ...
    // file[8].sst (800 => 899)
    int n = 9;
    std::vector<std::string> files(n);
    std::vector<ExternalSstFileInfo> files_info(n);
    for (int i = 0; i < n; i++) {
      files[i] = sst_files_dir_ + "file" + std::to_string(i) + ".sst";
      ASSERT_OK(sst_file_writer.Open(files[i]));
      for (int k = i * 100; k < (i + 1) * 100; k++) {
        ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
      }
      ASSERT_OK(sst_file_writer.Finish(&files_info[i]));
      ASSERT_EQ(files_info[i].file_path, files[i]);
      ASSERT_EQ(files_info[i].num_entries, 100);
      ASSERT_EQ(files_info[i].smallest_key, Key(i * 100));
      ASSERT_EQ(files_info[i].largest_key, Key((i + 1) * 100 - 1));
    }
    files.push_back(sst_files_dir_ + "file" + std::to_string(n) + ".sst");
    ASSERT_NOK(DeprecatedAddFile(files));
    for (int k = 0; k < n * 100; k++) {
      ASSERT_EQ("NOT_FOUND", Get(Key(k)));
    }
    files.pop_back();
    ASSERT_OK(DeprecatedAddFile(files));
    for (int k = 0; k < n * 100; k++) {
      std::string value = Key(k) + "_val";
      ASSERT_EQ(Get(Key(k)), value);
    }
    DestroyAndRecreateExternalSSTFilesDir();
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction));
}
// This test reporduce a bug that can happen in some cases if the DB started
// purging obsolete files when we are adding an external sst file.
// This situation may result in deleting the file while it's being added.
TEST_F(ExternalSSTFileTest, PurgeObsoleteFilesBug) {
  Options options = CurrentOptions();
  SstFileWriter sst_file_writer(EnvOptions(), options);

  // file1.sst (0 => 500)
  std::string sst_file_path = sst_files_dir_ + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(sst_file_path));
  for (int i = 0; i < 500; i++) {
    std::string k = Key(i);
    ASSERT_OK(sst_file_writer.Put(k, k + "_val"));
  }

  ExternalSstFileInfo sst_file_info;
  ASSERT_OK(sst_file_writer.Finish(&sst_file_info));

  options.delete_obsolete_files_period_micros = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::Prepare:FileAdded", [&](void* /* arg */) {
        ASSERT_OK(Put("aaa", "bbb"));
        ASSERT_OK(Flush());
        ASSERT_OK(Put("aaa", "xxx"));
        ASSERT_OK(Flush());
        ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(DeprecatedAddFile({sst_file_path}));

  for (int i = 0; i < 500; i++) {
    std::string k = Key(i);
    std::string v = k + "_val";
    ASSERT_EQ(Get(k), v);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(ExternalSSTFileTest, SkipSnapshot) {
  Options options = CurrentOptions();

  SstFileWriter sst_file_writer(EnvOptions(), options);

  // file1.sst (0 => 99)
  std::string file1 = sst_files_dir_ + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  ASSERT_OK(sst_file_writer.Finish(&file1_info));
  ASSERT_EQ(file1_info.file_path, file1);
  ASSERT_EQ(file1_info.num_entries, 100);
  ASSERT_EQ(file1_info.smallest_key, Key(0));
  ASSERT_EQ(file1_info.largest_key, Key(99));

  // file2.sst (100 => 299)
  std::string file2 = sst_files_dir_ + "file2.sst";
  ASSERT_OK(sst_file_writer.Open(file2));
  for (int k = 100; k < 300; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file2_info;
  ASSERT_OK(sst_file_writer.Finish(&file2_info));
  ASSERT_EQ(file2_info.file_path, file2);
  ASSERT_EQ(file2_info.num_entries, 200);
  ASSERT_EQ(file2_info.smallest_key, Key(100));
  ASSERT_EQ(file2_info.largest_key, Key(299));

  ASSERT_OK(DeprecatedAddFile({file1}));

  // Add file will fail when holding snapshot and use the default
  // skip_snapshot_check to false
  const Snapshot* s1 = db_->GetSnapshot();
  if (s1 != nullptr) {
    ASSERT_NOK(DeprecatedAddFile({file2}));
  }

  // Add file will success when set skip_snapshot_check to true even db holding
  // snapshot
  if (s1 != nullptr) {
    ASSERT_OK(DeprecatedAddFile({file2}, false, true));
    db_->ReleaseSnapshot(s1);
  }

  // file3.sst (300 => 399)
  std::string file3 = sst_files_dir_ + "file3.sst";
  ASSERT_OK(sst_file_writer.Open(file3));
  for (int k = 300; k < 400; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file3_info;
  ASSERT_OK(sst_file_writer.Finish(&file3_info));
  ASSERT_EQ(file3_info.file_path, file3);
  ASSERT_EQ(file3_info.num_entries, 100);
  ASSERT_EQ(file3_info.smallest_key, Key(300));
  ASSERT_EQ(file3_info.largest_key, Key(399));

  // check that we have change the old key
  ASSERT_EQ(Get(Key(300)), "NOT_FOUND");
  const Snapshot* s2 = db_->GetSnapshot();
  ASSERT_OK(DeprecatedAddFile({file3}, false, true));
  ASSERT_EQ(Get(Key(300)), Key(300) + ("_val"));
  ASSERT_EQ(Get(Key(300), s2), Key(300) + ("_val"));

  db_->ReleaseSnapshot(s2);
}

TEST_F(ExternalSSTFileTest, MultiThreaded) {
  env_->skip_fsync_ = true;
  // Bulk load 10 files every file contain 1000 keys
  int num_files = 10;
  int keys_per_file = 1000;

  // Generate file names
  std::vector<std::string> file_names;
  for (int i = 0; i < num_files; i++) {
    std::string file_name = "file_" + std::to_string(i) + ".sst";
    file_names.push_back(sst_files_dir_ + file_name);
  }

  do {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    std::atomic<int> thread_num(0);
    std::function<void()> write_file_func = [&]() {
      int file_idx = thread_num.fetch_add(1);
      int range_start = file_idx * keys_per_file;
      int range_end = range_start + keys_per_file;

      SstFileWriter sst_file_writer(EnvOptions(), options);

      ASSERT_OK(sst_file_writer.Open(file_names[file_idx]));

      for (int k = range_start; k < range_end; k++) {
        ASSERT_OK(sst_file_writer.Put(Key(k), Key(k)));
      }

      ASSERT_OK(sst_file_writer.Finish());
    };
    // Write num_files files in parallel
    std::vector<port::Thread> sst_writer_threads;
    for (int i = 0; i < num_files; ++i) {
      sst_writer_threads.emplace_back(write_file_func);
    }

    for (auto& t : sst_writer_threads) {
      t.join();
    }

    fprintf(stderr, "Wrote %d files (%d keys)\n", num_files,
            num_files * keys_per_file);

    thread_num.store(0);
    std::atomic<int> files_added(0);
    // Thread 0 -> Load {f0,f1}
    // Thread 1 -> Load {f0,f1}
    // Thread 2 -> Load {f2,f3}
    // Thread 3 -> Load {f2,f3}
    // Thread 4 -> Load {f4,f5}
    // Thread 5 -> Load {f4,f5}
    // ...
    std::function<void()> load_file_func = [&]() {
      // We intentionally add every file twice, and assert that it was added
      // only once and the other add failed
      int thread_id = thread_num.fetch_add(1);
      int file_idx = (thread_id / 2) * 2;
      // sometimes we use copy, sometimes link .. the result should be the same
      bool move_file = (thread_id % 3 == 0);

      std::vector<std::string> files_to_add;

      files_to_add = {file_names[file_idx]};
      if (static_cast<size_t>(file_idx + 1) < file_names.size()) {
        files_to_add.push_back(file_names[file_idx + 1]);
      }

      Status s = DeprecatedAddFile(files_to_add, move_file);
      if (s.ok()) {
        files_added += static_cast<int>(files_to_add.size());
      }
    };

    // Bulk load num_files files in parallel
    std::vector<port::Thread> add_file_threads;
    DestroyAndReopen(options);
    for (int i = 0; i < num_files; ++i) {
      add_file_threads.emplace_back(load_file_func);
    }

    for (auto& t : add_file_threads) {
      t.join();
    }
    ASSERT_EQ(files_added.load(), num_files);
    fprintf(stderr, "Loaded %d files (%d keys)\n", num_files,
            num_files * keys_per_file);

    // Overwrite values of keys divisible by 100
    for (int k = 0; k < num_files * keys_per_file; k += 100) {
      std::string key = Key(k);
      ASSERT_OK(Put(key, key + "_new"));
    }

    for (int i = 0; i < 2; i++) {
      // Make sure the values are correct before and after flush/compaction
      for (int k = 0; k < num_files * keys_per_file; ++k) {
        std::string key = Key(k);
        std::string value = (k % 100 == 0) ? (key + "_new") : key;
        ASSERT_EQ(Get(key), value);
      }
      ASSERT_OK(Flush());
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }

    fprintf(stderr, "Verified %d values\n", num_files * keys_per_file);
    DestroyAndRecreateExternalSSTFilesDir();
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction));
}

TEST_F(ExternalSSTFileTest, OverlappingRanges) {
  env_->skip_fsync_ = true;
  Random rnd(301);
  SequenceNumber assigned_seqno = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::Run", [&assigned_seqno](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        assigned_seqno = *(static_cast<SequenceNumber*>(arg));
      });
  bool need_flush = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFile:NeedFlush", [&need_flush](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        need_flush = *(static_cast<bool*>(arg));
      });
  bool overlap_with_db = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::AssignLevelAndSeqnoForIngestedFile",
      [&overlap_with_db](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        overlap_with_db = *(static_cast<bool*>(arg));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  do {
    Options options = CurrentOptions();
    env_->skip_fsync_ = true;
    DestroyAndReopen(options);

    SstFileWriter sst_file_writer(EnvOptions(), options);

    printf("Option config = %d\n", option_config_);
    std::vector<std::pair<int, int>> key_ranges;
    for (int i = 0; i < 100; i++) {
      int range_start = rnd.Uniform(20000);
      int keys_per_range = 10 + rnd.Uniform(41);

      key_ranges.emplace_back(range_start, range_start + keys_per_range);
    }

    int memtable_add = 0;
    int success_add_file = 0;
    int failed_add_file = 0;
    std::map<std::string, std::string> true_data;
    for (size_t i = 0; i < key_ranges.size(); i++) {
      int range_start = key_ranges[i].first;
      int range_end = key_ranges[i].second;

      Status s;
      std::string range_val = "range_" + std::to_string(i);

      // For 20% of ranges we use DB::Put, for 80% we use DB::AddFile
      if (i && i % 5 == 0) {
        // Use DB::Put to insert range (insert into memtable)
        range_val += "_put";
        for (int k = range_start; k <= range_end; k++) {
          s = Put(Key(k), range_val);
          ASSERT_OK(s);
        }
        memtable_add++;
      } else {
        // Use DB::AddFile to insert range
        range_val += "_add_file";

        // Generate the file containing the range
        std::string file_name = sst_files_dir_ + env_->GenerateUniqueId();
        s = sst_file_writer.Open(file_name);
        ASSERT_OK(s);
        for (int k = range_start; k <= range_end; k++) {
          s = sst_file_writer.Put(Key(k), range_val);
          ASSERT_OK(s);
        }
        ExternalSstFileInfo file_info;
        s = sst_file_writer.Finish(&file_info);
        ASSERT_OK(s);

        // Insert the generated file
        s = DeprecatedAddFile({file_name});
        auto it = true_data.lower_bound(Key(range_start));
        if (option_config_ != kUniversalCompaction &&
            option_config_ != kUniversalCompactionMultiLevel &&
            option_config_ != kUniversalSubcompactions) {
          if (it != true_data.end() && it->first <= Key(range_end)) {
            // This range overlap with data already exist in DB
            ASSERT_NOK(s);
            failed_add_file++;
          } else {
            ASSERT_OK(s);
            success_add_file++;
          }
        } else {
          if ((it != true_data.end() && it->first <= Key(range_end)) ||
              need_flush || assigned_seqno > 0 || overlap_with_db) {
            // This range overlap with data already exist in DB
            ASSERT_NOK(s);
            failed_add_file++;
          } else {
            ASSERT_OK(s);
            success_add_file++;
          }
        }
      }

      if (s.ok()) {
        // Update true_data map to include the new inserted data
        for (int k = range_start; k <= range_end; k++) {
          true_data[Key(k)] = range_val;
        }
      }

      // Flush / Compact the DB
      if (i && i % 50 == 0) {
        ASSERT_OK(Flush());
      }
      if (i && i % 75 == 0) {
        ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      }
    }

    printf("Total: %" ROCKSDB_PRIszt
           " ranges\n"
           "AddFile()|Success: %d ranges\n"
           "AddFile()|RangeConflict: %d ranges\n"
           "Put(): %d ranges\n",
           key_ranges.size(), success_add_file, failed_add_file, memtable_add);

    // Verify the correctness of the data
    for (const auto& kv : true_data) {
      ASSERT_EQ(Get(kv.first), kv.second);
    }
    printf("keys/values verified\n");
    DestroyAndRecreateExternalSSTFilesDir();
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction));
}

TEST_P(ExternalSSTFileTest, PickedLevel) {
  env_->skip_fsync_ = true;
  Options options = CurrentOptions();
  options.disable_auto_compactions = false;
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = 4;
  DestroyAndReopen(options);

  std::map<std::string, std::string> true_data;

  // File 0 will go to last level (L3)
  ASSERT_OK(GenerateAndAddExternalFile(options, {1, 10}, -1, false, false, true,
                                       false, false, &true_data));
  EXPECT_EQ(FilesPerLevel(), "0,0,0,1");

  // File 1 will go to level L2 (since it overlap with file 0 in L3)
  ASSERT_OK(GenerateAndAddExternalFile(options, {2, 9}, -1, false, false, true,
                                       false, false, &true_data));
  EXPECT_EQ(FilesPerLevel(), "0,0,1,1");

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"ExternalSSTFileTest::PickedLevel:0", "BackgroundCallCompaction:0"},
      {"DBImpl::BackgroundCompaction:Start",
       "ExternalSSTFileTest::PickedLevel:1"},
      {"ExternalSSTFileTest::PickedLevel:2",
       "DBImpl::BackgroundCompaction:NonTrivial:AfterRun"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Flush 4 files containing the same keys
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(Put(Key(3), Key(3) + "put"));
    ASSERT_OK(Put(Key(8), Key(8) + "put"));
    true_data[Key(3)] = Key(3) + "put";
    true_data[Key(8)] = Key(8) + "put";
    ASSERT_OK(Flush());
  }

  // Wait for BackgroundCompaction() to be called
  TEST_SYNC_POINT("ExternalSSTFileTest::PickedLevel:0");
  TEST_SYNC_POINT("ExternalSSTFileTest::PickedLevel:1");

  EXPECT_EQ(FilesPerLevel(), "4,0,1,1");

  // This file overlaps with file 0 (L3), file 1 (L2) and the
  // output of compaction going to L1
  ASSERT_OK(GenerateAndAddExternalFile(options, {4, 7}, -1,
                                       true /* allow_global_seqno */, false,
                                       true, false, false, &true_data));
  EXPECT_EQ(FilesPerLevel(), "5,0,1,1");

  // This file does not overlap with any file or with the running compaction
  ASSERT_OK(GenerateAndAddExternalFile(options, {9000, 9001}, -1, false, false,
                                       false, false, false, &true_data));
  EXPECT_EQ(FilesPerLevel(), "5,0,1,2");

  // Hold compaction from finishing
  TEST_SYNC_POINT("ExternalSSTFileTest::PickedLevel:2");

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  EXPECT_EQ(FilesPerLevel(), "1,1,1,2");

  size_t kcnt = 0;
  VerifyDBFromMap(true_data, &kcnt, false);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(ExternalSSTFileTest, IngestNonExistingFile) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);

  Status s = db_->IngestExternalFile({"non_existing_file"},
                                     IngestExternalFileOptions());
  ASSERT_NOK(s);

  // Verify file deletion is not impacted (verify a bug fix)
  ASSERT_OK(Put(Key(1), Key(1)));
  ASSERT_OK(Put(Key(9), Key(9)));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(1), Key(1)));
  ASSERT_OK(Put(Key(9), Key(9)));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // After full compaction, there should be only 1 file.
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  int num_sst_files = 0;
  for (auto& f : files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kTableFile) {
      num_sst_files++;
    }
  }
  ASSERT_EQ(1, num_sst_files);
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
TEST_F(ExternalSSTFileTest, CompactDuringAddFileRandom) {
  env_->skip_fsync_ = true;
  Options options = CurrentOptions();
  options.disable_auto_compactions = false;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 2;
  DestroyAndReopen(options);

  std::function<void()> bg_compact = [&]() {
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  };

  int range_id = 0;
  std::vector<int> file_keys;
  std::function<void()> bg_addfile = [&]() {
    ASSERT_OK(GenerateAndAddExternalFile(options, file_keys, range_id,
                                         true /* allow_global_seqno */));
  };

  const int num_of_ranges = 1000;
  std::vector<port::Thread> threads;
  while (range_id < num_of_ranges) {
    int range_start = range_id * 10;
    int range_end = range_start + 10;

    file_keys.clear();
    for (int k = range_start + 1; k < range_end; k++) {
      file_keys.push_back(k);
    }
    ASSERT_OK(Put(Key(range_start), Key(range_start)));
    ASSERT_OK(Put(Key(range_end), Key(range_end)));
    ASSERT_OK(Flush());

    if (range_id % 10 == 0) {
      threads.emplace_back(bg_compact);
    }
    threads.emplace_back(bg_addfile);

    for (auto& t : threads) {
      t.join();
    }
    threads.clear();

    range_id++;
  }

  for (int rid = 0; rid < num_of_ranges; rid++) {
    int range_start = rid * 10;
    int range_end = range_start + 10;

    ASSERT_EQ(Get(Key(range_start)), Key(range_start)) << rid;
    ASSERT_EQ(Get(Key(range_end)), Key(range_end)) << rid;
    for (int k = range_start + 1; k < range_end; k++) {
      std::string v = Key(k) + std::to_string(rid);
      ASSERT_EQ(Get(Key(k)), v) << rid;
    }
  }
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_F(ExternalSSTFileTest, PickedLevelDynamic) {
  env_->skip_fsync_ = true;
  Options options = CurrentOptions();
  options.disable_auto_compactions = false;
  options.level0_file_num_compaction_trigger = 4;
  options.level_compaction_dynamic_level_bytes = true;
  options.num_levels = 4;
  DestroyAndReopen(options);
  std::map<std::string, std::string> true_data;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"ExternalSSTFileTest::PickedLevelDynamic:0",
       "BackgroundCallCompaction:0"},
      {"DBImpl::BackgroundCompaction:Start",
       "ExternalSSTFileTest::PickedLevelDynamic:1"},
      {"ExternalSSTFileTest::PickedLevelDynamic:2",
       "DBImpl::BackgroundCompaction:NonTrivial:AfterRun"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Flush 4 files containing the same keys
  for (int i = 0; i < 4; i++) {
    for (int k = 20; k <= 30; k++) {
      ASSERT_OK(Put(Key(k), Key(k) + "put"));
      true_data[Key(k)] = Key(k) + "put";
    }
    for (int k = 50; k <= 60; k++) {
      ASSERT_OK(Put(Key(k), Key(k) + "put"));
      true_data[Key(k)] = Key(k) + "put";
    }
    ASSERT_OK(Flush());
  }

  // Wait for BackgroundCompaction() to be called
  TEST_SYNC_POINT("ExternalSSTFileTest::PickedLevelDynamic:0");
  TEST_SYNC_POINT("ExternalSSTFileTest::PickedLevelDynamic:1");

  // This file overlaps with the output of the compaction (going to L3)
  // so the file will be added to L0 since L3 is the base level
  ASSERT_OK(GenerateAndAddExternalFile(options, {31, 32, 33, 34}, -1,
                                       true /* allow_global_seqno */, false,
                                       true, false, false, &true_data));
  EXPECT_EQ(FilesPerLevel(), "5");

  // This file does not overlap with the current running compactiong
  ASSERT_OK(GenerateAndAddExternalFile(options, {9000, 9001}, -1, false, false,
                                       true, false, false, &true_data));
  EXPECT_EQ(FilesPerLevel(), "5,0,0,1");

  // Hold compaction from finishing
  TEST_SYNC_POINT("ExternalSSTFileTest::PickedLevelDynamic:2");

  // Output of the compaction will go to L3
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  EXPECT_EQ(FilesPerLevel(), "1,0,0,2");

  Close();
  options.disable_auto_compactions = true;
  Reopen(options);

  ASSERT_OK(GenerateAndAddExternalFile(options, {1, 15, 19}, -1, false, false,
                                       true, false, false, &true_data));
  ASSERT_EQ(FilesPerLevel(), "1,0,0,3");

  ASSERT_OK(GenerateAndAddExternalFile(options, {1000, 1001, 1002}, -1, false,
                                       false, true, false, false, &true_data));
  ASSERT_EQ(FilesPerLevel(), "1,0,0,4");

  ASSERT_OK(GenerateAndAddExternalFile(options, {500, 600, 700}, -1, false,
                                       false, true, false, false, &true_data));
  ASSERT_EQ(FilesPerLevel(), "1,0,0,5");

  // File 5 overlaps with file 2 (L3 / base level)
  ASSERT_OK(GenerateAndAddExternalFile(options, {2, 10}, -1, false, false, true,
                                       false, false, &true_data));
  ASSERT_EQ(FilesPerLevel(), "2,0,0,5");

  // File 6 overlaps with file 2 (L3 / base level) and file 5 (L0)
  ASSERT_OK(GenerateAndAddExternalFile(options, {3, 9}, -1, false, false, true,
                                       false, false, &true_data));
  ASSERT_EQ(FilesPerLevel(), "3,0,0,5");

  // Verify data in files
  size_t kcnt = 0;
  VerifyDBFromMap(true_data, &kcnt, false);

  // Write range [5 => 10] to L0
  for (int i = 5; i <= 10; i++) {
    std::string k = Key(i);
    std::string v = k + "put";
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
  }
  ASSERT_OK(Flush());
  ASSERT_EQ(FilesPerLevel(), "4,0,0,5");

  // File 7 overlaps with file 4 (L3)
  ASSERT_OK(GenerateAndAddExternalFile(options, {650, 651, 652}, -1, false,
                                       false, true, false, false, &true_data));
  ASSERT_EQ(FilesPerLevel(), "5,0,0,5");

  VerifyDBFromMap(true_data, &kcnt, false);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(ExternalSSTFileTest, AddExternalSstFileWithCustomCompartor) {
  Options options = CurrentOptions();
  options.comparator = ReverseBytewiseComparator();
  DestroyAndReopen(options);

  SstFileWriter sst_file_writer(EnvOptions(), options);

  // Generate files with these key ranges
  // {14  -> 0}
  // {24 -> 10}
  // {34 -> 20}
  // {44 -> 30}
  // ..
  std::vector<std::string> generated_files;
  for (int i = 0; i < 10; i++) {
    std::string file_name = sst_files_dir_ + env_->GenerateUniqueId();
    ASSERT_OK(sst_file_writer.Open(file_name));

    int range_end = i * 10;
    int range_start = range_end + 15;
    for (int k = (range_start - 1); k >= range_end; k--) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k)));
    }
    ExternalSstFileInfo file_info;
    ASSERT_OK(sst_file_writer.Finish(&file_info));
    generated_files.push_back(file_name);
  }

  std::vector<std::string> in_files;

  // These 2nd and 3rd files overlap with each other
  in_files = {generated_files[0], generated_files[4], generated_files[5],
              generated_files[7]};
  ASSERT_NOK(DeprecatedAddFile(in_files));

  // These 2 files don't overlap with each other
  in_files = {generated_files[0], generated_files[2]};
  ASSERT_OK(DeprecatedAddFile(in_files));

  // These 2 files don't overlap with each other but overlap with keys in DB
  in_files = {generated_files[3], generated_files[7]};
  ASSERT_NOK(DeprecatedAddFile(in_files));

  // Files don't overlap and don't overlap with DB key range
  in_files = {generated_files[4], generated_files[6], generated_files[8]};
  ASSERT_OK(DeprecatedAddFile(in_files));

  for (int i = 0; i < 100; i++) {
    if (i % 20 <= 14) {
      ASSERT_EQ(Get(Key(i)), Key(i));
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
}

TEST_F(ExternalSSTFileTest, AddFileTrivialMoveBug) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.IncreaseParallelism(20);
  DestroyAndReopen(options);

  ASSERT_OK(GenerateAndAddExternalFile(options, {1, 4}, 1));  // L3
  ASSERT_OK(GenerateAndAddExternalFile(options, {2, 3}, 2));  // L2

  ASSERT_OK(GenerateAndAddExternalFile(options, {10, 14}, 3));  // L3
  ASSERT_OK(GenerateAndAddExternalFile(options, {12, 13}, 4));  // L2

  ASSERT_OK(GenerateAndAddExternalFile(options, {20, 24}, 5));  // L3
  ASSERT_OK(GenerateAndAddExternalFile(options, {22, 23}, 6));  // L2

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start", [&](void* /*arg*/) {
        // Fit in L3 but will overlap with the compaction output so will be
        // added to L2. Prior to the fix, a compaction will then trivially move
        // this file to L3 and break LSM consistency
        static std::atomic<bool> called = {false};
        if (!called) {
          called = true;
          ASSERT_OK(dbfull()->SetOptions({{"max_bytes_for_level_base", "1"}}));
          ASSERT_OK(GenerateAndAddExternalFile(options, {15, 16}, 7,
                                               true /* allow_global_seqno */));
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = false;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(ExternalSSTFileTest, CompactAddedFiles) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  DestroyAndReopen(options);

  ASSERT_OK(GenerateAndAddExternalFile(options, {1, 10}, 1));  // L3
  ASSERT_OK(GenerateAndAddExternalFile(options, {2, 9}, 2));   // L2
  ASSERT_OK(GenerateAndAddExternalFile(options, {3, 8}, 3));   // L1
  ASSERT_OK(GenerateAndAddExternalFile(options, {4, 7}, 4));   // L0

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
}

TEST_F(ExternalSSTFileTest, SstFileWriterNonSharedKeys) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  std::string file_path = sst_files_dir_ + "/not_shared";
  SstFileWriter sst_file_writer(EnvOptions(), options);

  std::string suffix(100, 'X');
  ASSERT_OK(sst_file_writer.Open(file_path));
  ASSERT_OK(sst_file_writer.Put("A" + suffix, "VAL"));
  ASSERT_OK(sst_file_writer.Put("BB" + suffix, "VAL"));
  ASSERT_OK(sst_file_writer.Put("CC" + suffix, "VAL"));
  ASSERT_OK(sst_file_writer.Put("CXD" + suffix, "VAL"));
  ASSERT_OK(sst_file_writer.Put("CZZZ" + suffix, "VAL"));
  ASSERT_OK(sst_file_writer.Put("ZAAAX" + suffix, "VAL"));

  ASSERT_OK(sst_file_writer.Finish());
  ASSERT_OK(DeprecatedAddFile({file_path}));
}

TEST_F(ExternalSSTFileTest, WithUnorderedWrite) {
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::WriteImpl:UnorderedWriteAfterWriteWAL",
        "ExternalSSTFileTest::WithUnorderedWrite:WaitWriteWAL"},
       {"DBImpl::WaitForPendingWrites:BeforeBlock",
        "DBImpl::WriteImpl:BeforeUnorderedWriteMemtable"}});
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFile:NeedFlush",
      [&](void* need_flush) { ASSERT_TRUE(*static_cast<bool*>(need_flush)); });

  Options options = CurrentOptions();
  options.unordered_write = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v1"));
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer([&]() { ASSERT_OK(Put("bar", "v2")); });

  TEST_SYNC_POINT("ExternalSSTFileTest::WithUnorderedWrite:WaitWriteWAL");
  ASSERT_OK(GenerateAndAddExternalFile(options, {{"bar", "v3"}}, -1,
                                       true /* allow_global_seqno */));
  ASSERT_EQ(Get("bar"), "v3");

  writer.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
TEST_P(ExternalSSTFileTest, IngestFileWithGlobalSeqnoRandomized) {
  env_->skip_fsync_ = true;
  Options options = CurrentOptions();
  options.IncreaseParallelism(20);
  options.level0_slowdown_writes_trigger = 256;
  options.level0_stop_writes_trigger = 256;

  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  for (int iter = 0; iter < 2; iter++) {
    bool write_to_memtable = (iter == 0);
    DestroyAndReopen(options);

    Random rnd(301);
    std::map<std::string, std::string> true_data;
    for (int i = 0; i < 500; i++) {
      std::vector<std::pair<std::string, std::string>> random_data;
      for (int j = 0; j < 100; j++) {
        std::string k = rnd.RandomString(rnd.Next() % 20);
        std::string v = rnd.RandomString(rnd.Next() % 50);
        random_data.emplace_back(k, v);
      }

      if (write_to_memtable && rnd.OneIn(4)) {
        // 25% of writes go through memtable
        for (auto& entry : random_data) {
          ASSERT_OK(Put(entry.first, entry.second));
          true_data[entry.first] = entry.second;
        }
      } else {
        ASSERT_OK(GenerateAndAddExternalFile(
            options, random_data, -1, true, write_global_seqno,
            verify_checksums_before_ingest, false, true, &true_data));
      }
    }
    size_t kcnt = 0;
    VerifyDBFromMap(true_data, &kcnt, false);
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    VerifyDBFromMap(true_data, &kcnt, false);
  }
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_P(ExternalSSTFileTest, IngestFileWithGlobalSeqnoAssignedLevel) {
  Options options = CurrentOptions();
  options.num_levels = 5;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  std::vector<std::pair<std::string, std::string>> file_data;
  std::map<std::string, std::string> true_data;

  // Insert 100 -> 200 into the memtable
  for (int i = 100; i <= 200; i++) {
    ASSERT_OK(Put(Key(i), "memtable"));
    true_data[Key(i)] = "memtable";
  }

  // Insert 0 -> 20 using AddFile
  file_data.clear();
  for (int i = 0; i <= 20; i++) {
    file_data.emplace_back(Key(i), "L4");
  }
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  ASSERT_OK(GenerateAndAddExternalFile(
      options, file_data, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, false, &true_data));

  // This file don't overlap with anything in the DB, will go to L4
  ASSERT_EQ("0,0,0,0,1", FilesPerLevel());

  // Insert 80 -> 130 using AddFile
  file_data.clear();
  for (int i = 80; i <= 130; i++) {
    file_data.emplace_back(Key(i), "L0");
  }
  ASSERT_OK(GenerateAndAddExternalFile(
      options, file_data, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, false, &true_data));

  // This file overlap with the memtable, so it will flush it and add
  // it self to L0
  ASSERT_EQ("2,0,0,0,1", FilesPerLevel());

  // Insert 30 -> 50 using AddFile
  file_data.clear();
  for (int i = 30; i <= 50; i++) {
    file_data.emplace_back(Key(i), "L4");
  }
  ASSERT_OK(GenerateAndAddExternalFile(
      options, file_data, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, false, &true_data));

  // This file don't overlap with anything in the DB and fit in L4 as well
  ASSERT_EQ("2,0,0,0,2", FilesPerLevel());

  // Insert 10 -> 40 using AddFile
  file_data.clear();
  for (int i = 10; i <= 40; i++) {
    file_data.emplace_back(Key(i), "L3");
  }
  ASSERT_OK(GenerateAndAddExternalFile(
      options, file_data, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, false, &true_data));

  // This file overlap with files in L4, we will ingest it in L3
  ASSERT_EQ("2,0,0,1,2", FilesPerLevel());

  size_t kcnt = 0;
  VerifyDBFromMap(true_data, &kcnt, false);
}

TEST_P(ExternalSSTFileTest, IngestFileWithGlobalSeqnoAssignedUniversal) {
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  Options options = CurrentOptions();
  options.num_levels = 5;
  options.compaction_style = kCompactionStyleUniversal;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  std::vector<std::pair<std::string, std::string>> file_data;
  std::map<std::string, std::string> true_data;

  // Write 200 -> 250 into the bottommost level
  for (int i = 200; i <= 250; i++) {
    ASSERT_OK(Put(Key(i), "bottommost"));
    true_data[Key(i)] = "bottommost";
  }
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,1", FilesPerLevel());

  // Take a snapshot to enforce global sequence number.
  const Snapshot* snap = db_->GetSnapshot();

  // Insert 100 -> 200 into the memtable
  for (int i = 100; i <= 200; i++) {
    ASSERT_OK(Put(Key(i), "memtable"));
    true_data[Key(i)] = "memtable";
  }

  // Insert 0 -> 20 using AddFile
  file_data.clear();
  for (int i = 0; i <= 20; i++) {
    file_data.emplace_back(Key(i), "L4");
  }

  ASSERT_OK(GenerateAndAddExternalFile(
      options, file_data, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, false, &true_data));

  // This file don't overlap with anything in the DB, will go to L4
  ASSERT_EQ("0,0,0,0,2", FilesPerLevel());

  // Insert 80 -> 130 using AddFile
  file_data.clear();
  for (int i = 80; i <= 130; i++) {
    file_data.emplace_back(Key(i), "L0");
  }
  ASSERT_OK(GenerateAndAddExternalFile(
      options, file_data, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, false, &true_data));

  // This file overlap with the memtable, so it will flush it and add
  // it self to L0
  ASSERT_EQ("2,0,0,0,2", FilesPerLevel());

  // Insert 30 -> 50 using AddFile
  file_data.clear();
  for (int i = 30; i <= 50; i++) {
    file_data.emplace_back(Key(i), "L4");
  }
  ASSERT_OK(GenerateAndAddExternalFile(
      options, file_data, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, false, &true_data));

  // This file don't overlap with anything in the DB and fit in L4 as well
  ASSERT_EQ("2,0,0,0,3", FilesPerLevel());

  // Insert 10 -> 40 using AddFile
  file_data.clear();
  for (int i = 10; i <= 40; i++) {
    file_data.emplace_back(Key(i), "L3");
  }
  ASSERT_OK(GenerateAndAddExternalFile(
      options, file_data, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, false, &true_data));

  // This file overlap with files in L4, we will ingest it into the closest
  // non-overlapping level, in this case, it's L3.
  ASSERT_EQ("2,0,0,1,3", FilesPerLevel());

  size_t kcnt = 0;
  VerifyDBFromMap(true_data, &kcnt, false);
  db_->ReleaseSnapshot(snap);
}

TEST_P(ExternalSSTFileTest, IngestFileWithGlobalSeqnoMemtableFlush) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  uint64_t entries_in_memtable;
  std::map<std::string, std::string> true_data;

  for (int k : {10, 20, 40, 80}) {
    ASSERT_OK(Put(Key(k), "memtable"));
    true_data[Key(k)] = "memtable";
  }
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumEntriesActiveMemTable,
                                  &entries_in_memtable));
  ASSERT_GE(entries_in_memtable, 1);

  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  // No need for flush
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {90, 100, 110}, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, false, &true_data));
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumEntriesActiveMemTable,
                                  &entries_in_memtable));
  ASSERT_GE(entries_in_memtable, 1);

  // This file will flush the memtable
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {19, 20, 21}, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, false, &true_data));
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumEntriesActiveMemTable,
                                  &entries_in_memtable));
  ASSERT_EQ(entries_in_memtable, 0);

  for (int k : {200, 201, 205, 206}) {
    ASSERT_OK(Put(Key(k), "memtable"));
    true_data[Key(k)] = "memtable";
  }
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumEntriesActiveMemTable,
                                  &entries_in_memtable));
  ASSERT_GE(entries_in_memtable, 1);

  // No need for flush, this file keys fit between the memtable keys
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {202, 203, 204}, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, false, &true_data));
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumEntriesActiveMemTable,
                                  &entries_in_memtable));
  ASSERT_GE(entries_in_memtable, 1);

  // This file will flush the memtable
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {206, 207}, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, false, &true_data));
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumEntriesActiveMemTable,
                                  &entries_in_memtable));
  ASSERT_EQ(entries_in_memtable, 0);

  size_t kcnt = 0;
  VerifyDBFromMap(true_data, &kcnt, false);
}

TEST_P(ExternalSSTFileTest, L0SortingIssue) {
  Options options = CurrentOptions();
  options.num_levels = 2;
  DestroyAndReopen(options);
  std::map<std::string, std::string> true_data;

  ASSERT_OK(Put(Key(1), "memtable"));
  ASSERT_OK(Put(Key(10), "memtable"));

  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  // No Flush needed, No global seqno needed, Ingest in L1
  ASSERT_OK(
      GenerateAndAddExternalFile(options, {7, 8}, -1, true, write_global_seqno,
                                 verify_checksums_before_ingest, false, false));
  // No Flush needed, but need a global seqno, Ingest in L0
  ASSERT_OK(
      GenerateAndAddExternalFile(options, {7, 8}, -1, true, write_global_seqno,
                                 verify_checksums_before_ingest, false, false));
  printf("%s\n", FilesPerLevel().c_str());

  // Overwrite what we added using external files
  ASSERT_OK(Put(Key(7), "memtable"));
  ASSERT_OK(Put(Key(8), "memtable"));

  // Read values from memtable
  ASSERT_EQ(Get(Key(7)), "memtable");
  ASSERT_EQ(Get(Key(8)), "memtable");

  // Flush and read from L0
  ASSERT_OK(Flush());
  printf("%s\n", FilesPerLevel().c_str());
  ASSERT_EQ(Get(Key(7)), "memtable");
  ASSERT_EQ(Get(Key(8)), "memtable");
}

TEST_F(ExternalSSTFileTest, CompactionDeadlock) {
  Options options = CurrentOptions();
  options.num_levels = 2;
  options.level0_file_num_compaction_trigger = 4;
  options.level0_slowdown_writes_trigger = 4;
  options.level0_stop_writes_trigger = 4;
  DestroyAndReopen(options);

  // atomic conter of currently running bg threads
  std::atomic<int> running_threads(0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::DelayWrite:Wait", "ExternalSSTFileTest::DeadLock:0"},
      {"ExternalSSTFileTest::DeadLock:1", "DBImpl::AddFile:Start"},
      {"DBImpl::AddFile:MutexLock", "ExternalSSTFileTest::DeadLock:2"},
      {"ExternalSSTFileTest::DeadLock:3", "BackgroundCallCompaction:0"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Start ingesting and extrnal file in the background
  ROCKSDB_NAMESPACE::port::Thread bg_ingest_file([&]() {
    running_threads += 1;
    ASSERT_OK(GenerateAndAddExternalFile(options, {5, 6}));
    running_threads -= 1;
  });

  ASSERT_OK(Put(Key(1), "memtable"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(2), "memtable"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(3), "memtable"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(4), "memtable"));
  ASSERT_OK(Flush());

  // This thread will try to insert into the memtable but since we have 4 L0
  // files this thread will be blocked and hold the writer thread
  ROCKSDB_NAMESPACE::port::Thread bg_block_put([&]() {
    running_threads += 1;
    ASSERT_OK(Put(Key(10), "memtable"));
    running_threads -= 1;
  });

  // Make sure DelayWrite is called first
  TEST_SYNC_POINT("ExternalSSTFileTest::DeadLock:0");

  // `DBImpl::AddFile:Start` will wait until we be here
  TEST_SYNC_POINT("ExternalSSTFileTest::DeadLock:1");

  // Wait for IngestExternalFile() to start and aquire mutex
  TEST_SYNC_POINT("ExternalSSTFileTest::DeadLock:2");

  // Now let compaction start
  TEST_SYNC_POINT("ExternalSSTFileTest::DeadLock:3");

  // Wait for max 5 seconds, if we did not finish all bg threads
  // then we hit the deadlock bug
  for (int i = 0; i < 10; i++) {
    if (running_threads.load() == 0) {
      break;
    }
    // Make sure we do a "real sleep", not a mock one.
    SystemClock::Default()->SleepForMicroseconds(500000);
  }

  ASSERT_EQ(running_threads.load(), 0);

  bg_ingest_file.join();
  bg_block_put.join();
}

TEST_F(ExternalSSTFileTest, DirtyExit) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  std::string file_path = sst_files_dir_ + "/dirty_exit";
  std::unique_ptr<SstFileWriter> sst_file_writer;

  // Destruct SstFileWriter without calling Finish()
  sst_file_writer.reset(new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(sst_file_writer->Open(file_path));
  sst_file_writer.reset();

  // Destruct SstFileWriter with a failing Finish
  sst_file_writer.reset(new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(sst_file_writer->Open(file_path));
  ASSERT_NOK(sst_file_writer->Finish());
}

TEST_F(ExternalSSTFileTest, FileWithCFInfo) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko", "toto"}, options);

  SstFileWriter sfw_default(EnvOptions(), options, handles_[0]);
  SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);
  SstFileWriter sfw_cf2(EnvOptions(), options, handles_[2]);
  SstFileWriter sfw_unknown(EnvOptions(), options);

  // default_cf.sst
  const std::string cf_default_sst = sst_files_dir_ + "/default_cf.sst";
  ASSERT_OK(sfw_default.Open(cf_default_sst));
  ASSERT_OK(sfw_default.Put("K1", "V1"));
  ASSERT_OK(sfw_default.Put("K2", "V2"));
  ASSERT_OK(sfw_default.Finish());

  // cf1.sst
  const std::string cf1_sst = sst_files_dir_ + "/cf1.sst";
  ASSERT_OK(sfw_cf1.Open(cf1_sst));
  ASSERT_OK(sfw_cf1.Put("K3", "V1"));
  ASSERT_OK(sfw_cf1.Put("K4", "V2"));
  ASSERT_OK(sfw_cf1.Finish());

  // cf_unknown.sst
  const std::string unknown_sst = sst_files_dir_ + "/cf_unknown.sst";
  ASSERT_OK(sfw_unknown.Open(unknown_sst));
  ASSERT_OK(sfw_unknown.Put("K5", "V1"));
  ASSERT_OK(sfw_unknown.Put("K6", "V2"));
  ASSERT_OK(sfw_unknown.Finish());

  IngestExternalFileOptions ifo;

  // SST CF don't match
  ASSERT_NOK(db_->IngestExternalFile(handles_[0], {cf1_sst}, ifo));
  // SST CF don't match
  ASSERT_NOK(db_->IngestExternalFile(handles_[2], {cf1_sst}, ifo));
  // SST CF match
  ASSERT_OK(db_->IngestExternalFile(handles_[1], {cf1_sst}, ifo));

  // SST CF don't match
  ASSERT_NOK(db_->IngestExternalFile(handles_[1], {cf_default_sst}, ifo));
  // SST CF don't match
  ASSERT_NOK(db_->IngestExternalFile(handles_[2], {cf_default_sst}, ifo));
  // SST CF match
  ASSERT_OK(db_->IngestExternalFile(handles_[0], {cf_default_sst}, ifo));

  // SST CF unknown
  ASSERT_OK(db_->IngestExternalFile(handles_[1], {unknown_sst}, ifo));
  // SST CF unknown
  ASSERT_OK(db_->IngestExternalFile(handles_[2], {unknown_sst}, ifo));
  // SST CF unknown
  ASSERT_OK(db_->IngestExternalFile(handles_[0], {unknown_sst}, ifo));

  // Cannot ingest a file into a dropped CF
  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  ASSERT_NOK(db_->IngestExternalFile(handles_[1], {unknown_sst}, ifo));

  // CF was not dropped, ok to Ingest
  ASSERT_OK(db_->IngestExternalFile(handles_[2], {unknown_sst}, ifo));
}

/*
 * Test and verify the functionality of ingestion_options.move_files and
 * ingestion_options.failed_move_fall_back_to_copy
 */
TEST_P(ExternSSTFileLinkFailFallbackTest, LinkFailFallBackExternalSst) {
  const bool fail_link = std::get<0>(GetParam());
  const bool failed_move_fall_back_to_copy = std::get<1>(GetParam());
  fs_->set_fail_link(fail_link);
  const EnvOptions env_options;
  DestroyAndReopen(options_);
  const int kNumKeys = 10000;
  IngestExternalFileOptions ifo;
  ifo.move_files = std::get<2>(GetParam());
  ifo.link_files = !ifo.move_files;
  ifo.failed_move_fall_back_to_copy = failed_move_fall_back_to_copy;

  std::string file_path = sst_files_dir_ + "file1.sst";
  // Create SstFileWriter for default column family
  SstFileWriter sst_file_writer(env_options, options_);
  ASSERT_OK(sst_file_writer.Open(file_path));
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(sst_file_writer.Put(Key(i), Key(i) + "_value"));
  }
  ASSERT_OK(sst_file_writer.Finish());
  uint64_t file_size = 0;
  ASSERT_OK(env_->GetFileSize(file_path, &file_size));

  bool copyfile = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::Prepare:CopyFile",
      [&](void* /* arg */) { copyfile = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  const Status s = db_->IngestExternalFile({file_path}, ifo);

  ColumnFamilyHandleImpl* cfh =
      static_cast<ColumnFamilyHandleImpl*>(dbfull()->DefaultColumnFamily());
  ColumnFamilyData* cfd = cfh->cfd();
  const InternalStats* internal_stats_ptr = cfd->internal_stats();
  const std::vector<InternalStats::CompactionStats>& comp_stats =
      internal_stats_ptr->TEST_GetCompactionStats();
  uint64_t bytes_copied = 0;
  uint64_t bytes_moved = 0;
  for (const auto& stats : comp_stats) {
    bytes_copied += stats.bytes_written;
    bytes_moved += stats.bytes_moved;
  }

  if (!fail_link) {
    // Link operation succeeds. External SST should be moved.
    ASSERT_OK(s);
    ASSERT_EQ(0, bytes_copied);
    ASSERT_EQ(file_size, bytes_moved);
    ASSERT_FALSE(copyfile);

    Status es = env_->FileExists(file_path);
    if (ifo.move_files) {
      ASSERT_TRUE(es.IsNotFound());
    } else {
      ASSERT_OK(es);
    }
  } else {
    // Link operation fails.
    ASSERT_EQ(0, bytes_moved);
    if (failed_move_fall_back_to_copy) {
      ASSERT_OK(s);
      // Copy file is true since a failed link falls back to copy file.
      ASSERT_TRUE(copyfile);
      ASSERT_EQ(file_size, bytes_copied);
    } else {
      ASSERT_TRUE(s.IsNotSupported());
      // Copy file is false since a failed link does not fall back to copy file.
      ASSERT_FALSE(copyfile);
      ASSERT_EQ(0, bytes_copied);
    }
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(ExternSSTFileLinkFailFallbackTest,
                        ExternSSTFileLinkFailFallbackTest,
                        testing::Combine(testing::Bool(), testing::Bool(),
                                         testing::Bool()));

class TestIngestExternalFileListener : public EventListener {
 public:
  void OnExternalFileIngested(DB* /*db*/,
                              const ExternalFileIngestionInfo& info) override {
    ingested_files.push_back(info);
  }

  std::vector<ExternalFileIngestionInfo> ingested_files;
};

TEST_P(ExternalSSTFileTest, IngestionListener) {
  Options options = CurrentOptions();
  TestIngestExternalFileListener* listener =
      new TestIngestExternalFileListener();
  options.listeners.emplace_back(listener);
  CreateAndReopenWithCF({"koko", "toto"}, options);

  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  // Ingest into default cf
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {1, 2}, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, true, nullptr, handles_[0]));
  ASSERT_EQ(listener->ingested_files.size(), 1);
  ASSERT_EQ(listener->ingested_files.back().cf_name, "default");
  ASSERT_EQ(listener->ingested_files.back().global_seqno, 0);
  ASSERT_EQ(listener->ingested_files.back().table_properties.column_family_id,
            0);
  ASSERT_EQ(listener->ingested_files.back().table_properties.column_family_name,
            "default");

  // Ingest into cf1
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {1, 2}, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, true, nullptr, handles_[1]));
  ASSERT_EQ(listener->ingested_files.size(), 2);
  ASSERT_EQ(listener->ingested_files.back().cf_name, "koko");
  ASSERT_EQ(listener->ingested_files.back().global_seqno, 0);
  ASSERT_EQ(listener->ingested_files.back().table_properties.column_family_id,
            1);
  ASSERT_EQ(listener->ingested_files.back().table_properties.column_family_name,
            "koko");

  // Ingest into cf2
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {1, 2}, -1, true, write_global_seqno,
      verify_checksums_before_ingest, false, true, nullptr, handles_[2]));
  ASSERT_EQ(listener->ingested_files.size(), 3);
  ASSERT_EQ(listener->ingested_files.back().cf_name, "toto");
  ASSERT_EQ(listener->ingested_files.back().global_seqno, 0);
  ASSERT_EQ(listener->ingested_files.back().table_properties.column_family_id,
            2);
  ASSERT_EQ(listener->ingested_files.back().table_properties.column_family_name,
            "toto");
}

TEST_F(ExternalSSTFileTest, SnapshotInconsistencyBug) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  const int kNumKeys = 10000;

  // Insert keys using normal path and take a snapshot
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), Key(i) + "_V1"));
  }
  const Snapshot* snap = db_->GetSnapshot();

  // Overwrite all keys using IngestExternalFile
  std::string sst_file_path = sst_files_dir_ + "file1.sst";
  SstFileWriter sst_file_writer(EnvOptions(), options);
  ASSERT_OK(sst_file_writer.Open(sst_file_path));
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(sst_file_writer.Put(Key(i), Key(i) + "_V2"));
  }
  ASSERT_OK(sst_file_writer.Finish());

  IngestExternalFileOptions ifo;
  ifo.move_files = true;
  ASSERT_OK(db_->IngestExternalFile({sst_file_path}, ifo));

  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_EQ(Get(Key(i), snap), Key(i) + "_V1");
    ASSERT_EQ(Get(Key(i)), Key(i) + "_V2");
  }

  db_->ReleaseSnapshot(snap);
}

TEST_P(ExternalSSTFileTest, IngestBehind) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 3;
  options.disable_auto_compactions = false;
  DestroyAndReopen(options);
  std::vector<std::pair<std::string, std::string>> file_data;
  std::map<std::string, std::string> true_data;

  // Insert 100 -> 200 into the memtable
  for (int i = 100; i <= 200; i++) {
    ASSERT_OK(Put(Key(i), "memtable"));
  }

  // Insert 100 -> 200 using IngestExternalFile
  file_data.clear();
  for (int i = 0; i <= 20; i++) {
    file_data.emplace_back(Key(i), "ingest_behind");
    true_data[Key(i)] = "ingest_behind";
  }

  bool allow_global_seqno = true;
  bool ingest_behind = true;
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());

  // Can't ingest behind since allow_ingest_behind isn't set to true
  ASSERT_NOK(GenerateAndAddExternalFile(
      options, file_data, -1, allow_global_seqno, write_global_seqno,
      verify_checksums_before_ingest, ingest_behind, false /*sort_data*/,
      &true_data));

  options.allow_ingest_behind = true;
  // check that we still can open the DB, as num_levels should be
  // sanitized to 3
  options.num_levels = 2;
  DestroyAndReopen(options);

  options.num_levels = 3;
  DestroyAndReopen(options);
  true_data.clear();
  // Insert 100 -> 200 into the memtable
  for (int i = 100; i <= 200; i++) {
    ASSERT_OK(Put(Key(i), "memtable"));
    true_data[Key(i)] = "memtable";
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Universal picker should go at second from the bottom level
  ASSERT_EQ("0,1", FilesPerLevel());
  ASSERT_OK(GenerateAndAddExternalFile(
      options, file_data, -1, allow_global_seqno, write_global_seqno,
      verify_checksums_before_ingest, true /*ingest_behind*/,
      false /*sort_data*/, &true_data));
  ASSERT_EQ("0,1,1", FilesPerLevel());
  // this time ingest should fail as the file doesn't fit to the bottom level
  ASSERT_NOK(GenerateAndAddExternalFile(
      options, file_data, -1, allow_global_seqno, write_global_seqno,
      verify_checksums_before_ingest, true /*ingest_behind*/,
      false /*sort_data*/, &true_data));
  ASSERT_EQ("0,1,1", FilesPerLevel());
  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &level_to_files);
  uint64_t ingested_file_number = level_to_files[2][0].fd.GetNumber();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Last level should not be compacted
  ASSERT_EQ("0,1,1", FilesPerLevel());
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &level_to_files);
  ASSERT_EQ(ingested_file_number, level_to_files[2][0].fd.GetNumber());
  size_t kcnt = 0;
  VerifyDBFromMap(true_data, &kcnt, false);

  // Auto-compaction should not include the last level.
  // Trigger compaction if size amplification exceeds 110%.
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options.level0_file_num_compaction_trigger = 4;
  ASSERT_OK(TryReopen(options));
  Random rnd(301);
  for (int i = 0; i < 4; ++i) {
    for (int j = 0; j < 10; j++) {
      true_data[Key(j)] = rnd.RandomString(1000);
      ASSERT_OK(Put(Key(j), true_data[Key(j)]));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &level_to_files);
  ASSERT_EQ(1, level_to_files[2].size());
  ASSERT_EQ(ingested_file_number, level_to_files[2][0].fd.GetNumber());

  // Turning off the option allows DB to compact ingested files.
  options.allow_ingest_behind = false;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &level_to_files);
  ASSERT_EQ(1, level_to_files[2].size());
  ASSERT_NE(ingested_file_number, level_to_files[2][0].fd.GetNumber());
  VerifyDBFromMap(true_data, &kcnt, false);
}

TEST_F(ExternalSSTFileTest, SkipBloomFilter) {
  Options options = CurrentOptions();

  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  table_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Create external SST file and include bloom filters
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);
  {
    std::string file_path = sst_files_dir_ + "sst_with_bloom.sst";
    SstFileWriter sst_file_writer(EnvOptions(), options);
    ASSERT_OK(sst_file_writer.Open(file_path));
    ASSERT_OK(sst_file_writer.Put("Key1", "Value1"));
    ASSERT_OK(sst_file_writer.Finish());

    ASSERT_OK(
        db_->IngestExternalFile({file_path}, IngestExternalFileOptions()));

    ASSERT_EQ(Get("Key1"), "Value1");
    ASSERT_GE(
        options.statistics->getTickerCount(Tickers::BLOCK_CACHE_FILTER_ADD), 1);
  }

  // Create external SST file but skip bloom filters
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);
  {
    std::string file_path = sst_files_dir_ + "sst_with_no_bloom.sst";
    SstFileWriter sst_file_writer(EnvOptions(), options, nullptr, true,
                                  Env::IOPriority::IO_TOTAL,
                                  true /* skip_filters */);
    ASSERT_OK(sst_file_writer.Open(file_path));
    ASSERT_OK(sst_file_writer.Put("Key1", "Value1"));
    ASSERT_OK(sst_file_writer.Finish());

    ASSERT_OK(
        db_->IngestExternalFile({file_path}, IngestExternalFileOptions()));

    ASSERT_EQ(Get("Key1"), "Value1");
    ASSERT_EQ(
        options.statistics->getTickerCount(Tickers::BLOCK_CACHE_FILTER_ADD), 0);
  }
}

TEST_F(ExternalSSTFileTest, IngestFileWrittenWithCompressionDictionary) {
  if (!ZSTD_Supported()) {
    return;
  }
  const int kNumEntries = 1 << 10;
  const int kNumBytesPerEntry = 1 << 10;
  Options options = CurrentOptions();
  options.compression = kZSTD;
  options.compression_opts.max_dict_bytes = 1 << 14;        // 16KB
  options.compression_opts.zstd_max_train_bytes = 1 << 18;  // 256KB
  DestroyAndReopen(options);

  std::atomic<int> num_compression_dicts(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WriteCompressionDictBlock:RawDict",
      [&](void* /* arg */) { ++num_compression_dicts; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  std::vector<std::pair<std::string, std::string>> random_data;
  for (int i = 0; i < kNumEntries; i++) {
    std::string val = rnd.RandomString(kNumBytesPerEntry);
    random_data.emplace_back(Key(i), std::move(val));
  }
  ASSERT_OK(GenerateAndAddExternalFile(options, std::move(random_data)));
  ASSERT_EQ(1, num_compression_dicts);
}

class ExternalSSTBlockChecksumTest
    : public ExternalSSTFileTestBase,
      public testing::WithParamInterface<uint32_t> {};

INSTANTIATE_TEST_CASE_P(FormatVersions, ExternalSSTBlockChecksumTest,
                        testing::ValuesIn(test::kFooterFormatVersionsToTest));

// Very slow, not worth the cost to run regularly
TEST_P(ExternalSSTBlockChecksumTest, DISABLED_HugeBlockChecksum) {
  BlockBasedTableOptions table_options;
  table_options.format_version = GetParam();
  for (auto t : GetSupportedChecksums()) {
    table_options.checksum = t;
    Options options = CurrentOptions();
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    SstFileWriter sst_file_writer(EnvOptions(), options);

    // 2^32 - 1, will lead to data block with more than 2^32 bytes
    size_t huge_size = std::numeric_limits<uint32_t>::max();

    std::string f = sst_files_dir_ + "f.sst";
    ASSERT_OK(sst_file_writer.Open(f));
    {
      Random64 r(123);
      std::string huge(huge_size, 0);
      for (size_t j = 0; j + 7 < huge_size; j += 8) {
        EncodeFixed64(&huge[j], r.Next());
      }
      ASSERT_OK(sst_file_writer.Put("Huge", huge));
    }

    ExternalSstFileInfo f_info;
    ASSERT_OK(sst_file_writer.Finish(&f_info));
    ASSERT_GT(f_info.file_size, uint64_t{huge_size} + 10);

    SstFileReader sst_file_reader(options);
    ASSERT_OK(sst_file_reader.Open(f));
    ASSERT_OK(sst_file_reader.VerifyChecksum());
  }
}

TEST_P(ExternalSSTFileTest, IngestFilesIntoMultipleColumnFamilies_Success) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options = CurrentOptions();
  options.env = fault_injection_env.get();
  CreateAndReopenWithCF({"pikachu", "eevee"}, options);

  // Exercise different situations in different column families: two are empty
  // (so no new sequence number is needed), but at least one overlaps with the
  // DB and needs to bump the sequence number.
  ASSERT_OK(db_->Put(WriteOptions(), "foo1", "oldvalue"));

  std::vector<ColumnFamilyHandle*> column_families;
  column_families.push_back(handles_[0]);
  column_families.push_back(handles_[1]);
  column_families.push_back(handles_[2]);
  std::vector<IngestExternalFileOptions> ifos(column_families.size());
  for (auto& ifo : ifos) {
    ifo.allow_global_seqno = true;  // Always allow global_seqno
    // May or may not write global_seqno
    ifo.write_global_seqno = std::get<0>(GetParam());
    // Whether to verify checksums before ingestion
    ifo.verify_checksums_before_ingest = std::get<1>(GetParam());
  }
  std::vector<std::vector<std::pair<std::string, std::string>>> data;
  data.push_back(
      {std::make_pair("foo1", "fv1"), std::make_pair("foo2", "fv2")});
  data.push_back(
      {std::make_pair("bar1", "bv1"), std::make_pair("bar2", "bv2")});
  data.push_back(
      {std::make_pair("bar3", "bv3"), std::make_pair("bar4", "bv4")});

  // Resize the true_data vector upon construction to avoid re-alloc
  std::vector<std::map<std::string, std::string>> true_data(
      column_families.size());
  ASSERT_OK(GenerateAndAddExternalFiles(options, column_families, ifos, data,
                                        -1, true, true_data));
  Close();
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu", "eevee"},
                           options);
  ASSERT_EQ(3, handles_.size());
  int cf = 0;
  for (const auto& verify_map : true_data) {
    for (const auto& elem : verify_map) {
      const std::string& key = elem.first;
      const std::string& value = elem.second;
      ASSERT_EQ(value, Get(cf, key));
    }
    ++cf;
  }
  Close();
  Destroy(options, true /* delete_cf_paths */);
}

TEST_P(ExternalSSTFileTest,
       IngestFilesIntoMultipleColumnFamilies_NoMixedStateWithSnapshot) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::IngestExternalFiles:InstallSVForFirstCF:0",
       "ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_MixedState:"
       "BeforeRead"},
      {"ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_MixedState:"
       "AfterRead",
       "DBImpl::IngestExternalFiles:InstallSVForFirstCF:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.env = fault_injection_env.get();
  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  const std::vector<std::map<std::string, std::string>> data_before_ingestion =
      {{{"foo1", "fv1_0"}, {"foo2", "fv2_0"}, {"foo3", "fv3_0"}},
       {{"bar1", "bv1_0"}, {"bar2", "bv2_0"}, {"bar3", "bv3_0"}},
       {{"bar4", "bv4_0"}, {"bar5", "bv5_0"}, {"bar6", "bv6_0"}}};
  for (size_t i = 0; i != handles_.size(); ++i) {
    int cf = static_cast<int>(i);
    const auto& orig_data = data_before_ingestion[i];
    for (const auto& kv : orig_data) {
      ASSERT_OK(Put(cf, kv.first, kv.second));
    }
    ASSERT_OK(Flush(cf));
  }

  std::vector<ColumnFamilyHandle*> column_families;
  column_families.push_back(handles_[0]);
  column_families.push_back(handles_[1]);
  column_families.push_back(handles_[2]);
  std::vector<IngestExternalFileOptions> ifos(column_families.size());
  for (auto& ifo : ifos) {
    ifo.allow_global_seqno = true;  // Always allow global_seqno
    // May or may not write global_seqno
    ifo.write_global_seqno = std::get<0>(GetParam());
    // Whether to verify checksums before ingestion
    ifo.verify_checksums_before_ingest = std::get<1>(GetParam());
  }
  std::vector<std::vector<std::pair<std::string, std::string>>> data;
  data.push_back(
      {std::make_pair("foo1", "fv1"), std::make_pair("foo2", "fv2")});
  data.push_back(
      {std::make_pair("bar1", "bv1"), std::make_pair("bar2", "bv2")});
  data.push_back(
      {std::make_pair("bar3", "bv3"), std::make_pair("bar4", "bv4")});
  // Resize the true_data vector upon construction to avoid re-alloc
  std::vector<std::map<std::string, std::string>> true_data(
      column_families.size());
  // Take snapshot before ingestion starts
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  read_opts.snapshot = dbfull()->GetSnapshot();
  std::vector<Iterator*> iters(handles_.size());

  // Range scan checks first kv of each CF before ingestion starts.
  for (size_t i = 0; i != handles_.size(); ++i) {
    iters[i] = dbfull()->NewIterator(read_opts, handles_[i]);
    iters[i]->SeekToFirst();
    ASSERT_TRUE(iters[i]->Valid());
    const std::string& key = iters[i]->key().ToString();
    const std::string& value = iters[i]->value().ToString();
    const std::map<std::string, std::string>& orig_data =
        data_before_ingestion[i];
    std::map<std::string, std::string>::const_iterator it = orig_data.find(key);
    ASSERT_NE(orig_data.end(), it);
    ASSERT_EQ(it->second, value);
    iters[i]->Next();
  }
  port::Thread ingest_thread([&]() {
    ASSERT_OK(GenerateAndAddExternalFiles(options, column_families, ifos, data,
                                          -1, true, true_data));
  });
  TEST_SYNC_POINT(
      "ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_MixedState:"
      "BeforeRead");
  // Should see only data before ingestion
  for (size_t i = 0; i != handles_.size(); ++i) {
    const auto& orig_data = data_before_ingestion[i];
    for (; iters[i]->Valid(); iters[i]->Next()) {
      const std::string& key = iters[i]->key().ToString();
      const std::string& value = iters[i]->value().ToString();
      std::map<std::string, std::string>::const_iterator it =
          orig_data.find(key);
      ASSERT_NE(orig_data.end(), it);
      ASSERT_EQ(it->second, value);
    }
  }
  TEST_SYNC_POINT(
      "ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_MixedState:"
      "AfterRead");
  ingest_thread.join();
  for (auto* iter : iters) {
    ASSERT_OK(iter->status());
    delete iter;
  }
  iters.clear();
  dbfull()->ReleaseSnapshot(read_opts.snapshot);

  Close();
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu", "eevee"},
                           options);
  // Should see consistent state after ingestion for all column families even
  // without snapshot.
  ASSERT_EQ(3, handles_.size());
  int cf = 0;
  for (const auto& verify_map : true_data) {
    for (const auto& elem : verify_map) {
      const std::string& key = elem.first;
      const std::string& value = elem.second;
      ASSERT_EQ(value, Get(cf, key));
    }
    ++cf;
  }
  Close();
  Destroy(options, true /* delete_cf_paths */);
}

TEST_P(ExternalSSTFileTest, IngestFilesIntoMultipleColumnFamilies_PrepareFail) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options = CurrentOptions();
  options.env = fault_injection_env.get();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::IngestExternalFiles:BeforeLastJobPrepare:0",
       "ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_PrepareFail:"
       "0"},
      {"ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies:PrepareFail:"
       "1",
       "DBImpl::IngestExternalFiles:BeforeLastJobPrepare:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  std::vector<ColumnFamilyHandle*> column_families;
  column_families.push_back(handles_[0]);
  column_families.push_back(handles_[1]);
  column_families.push_back(handles_[2]);
  std::vector<IngestExternalFileOptions> ifos(column_families.size());
  for (auto& ifo : ifos) {
    ifo.allow_global_seqno = true;  // Always allow global_seqno
    // May or may not write global_seqno
    ifo.write_global_seqno = std::get<0>(GetParam());
    // Whether to verify block checksums before ingest
    ifo.verify_checksums_before_ingest = std::get<1>(GetParam());
  }
  std::vector<std::vector<std::pair<std::string, std::string>>> data;
  data.push_back(
      {std::make_pair("foo1", "fv1"), std::make_pair("foo2", "fv2")});
  data.push_back(
      {std::make_pair("bar1", "bv1"), std::make_pair("bar2", "bv2")});
  data.push_back(
      {std::make_pair("bar3", "bv3"), std::make_pair("bar4", "bv4")});

  // Resize the true_data vector upon construction to avoid re-alloc
  std::vector<std::map<std::string, std::string>> true_data(
      column_families.size());
  port::Thread ingest_thread([&]() {
    ASSERT_NOK(GenerateAndAddExternalFiles(options, column_families, ifos, data,
                                           -1, true, true_data));
  });
  TEST_SYNC_POINT(
      "ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_PrepareFail:"
      "0");
  fault_injection_env->SetFilesystemActive(false);
  TEST_SYNC_POINT(
      "ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies:PrepareFail:"
      "1");
  ingest_thread.join();

  fault_injection_env->SetFilesystemActive(true);
  Close();
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu", "eevee"},
                           options);
  ASSERT_EQ(3, handles_.size());
  int cf = 0;
  for (const auto& verify_map : true_data) {
    for (const auto& elem : verify_map) {
      const std::string& key = elem.first;
      ASSERT_EQ("NOT_FOUND", Get(cf, key));
    }
    ++cf;
  }
  Close();
  Destroy(options, true /* delete_cf_paths */);
}

TEST_P(ExternalSSTFileTest, IngestFilesIntoMultipleColumnFamilies_CommitFail) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options = CurrentOptions();
  options.env = fault_injection_env.get();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::IngestExternalFiles:BeforeJobsRun:0",
       "ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_CommitFail:"
       "0"},
      {"ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_CommitFail:"
       "1",
       "DBImpl::IngestExternalFiles:BeforeJobsRun:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  std::vector<ColumnFamilyHandle*> column_families;
  column_families.push_back(handles_[0]);
  column_families.push_back(handles_[1]);
  column_families.push_back(handles_[2]);
  std::vector<IngestExternalFileOptions> ifos(column_families.size());
  for (auto& ifo : ifos) {
    ifo.allow_global_seqno = true;  // Always allow global_seqno
    // May or may not write global_seqno
    ifo.write_global_seqno = std::get<0>(GetParam());
    // Whether to verify block checksums before ingestion
    ifo.verify_checksums_before_ingest = std::get<1>(GetParam());
  }
  std::vector<std::vector<std::pair<std::string, std::string>>> data;
  data.push_back(
      {std::make_pair("foo1", "fv1"), std::make_pair("foo2", "fv2")});
  data.push_back(
      {std::make_pair("bar1", "bv1"), std::make_pair("bar2", "bv2")});
  data.push_back(
      {std::make_pair("bar3", "bv3"), std::make_pair("bar4", "bv4")});
  // Resize the true_data vector upon construction to avoid re-alloc
  std::vector<std::map<std::string, std::string>> true_data(
      column_families.size());
  port::Thread ingest_thread([&]() {
    ASSERT_NOK(GenerateAndAddExternalFiles(options, column_families, ifos, data,
                                           -1, true, true_data));
  });
  TEST_SYNC_POINT(
      "ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_CommitFail:"
      "0");
  fault_injection_env->SetFilesystemActive(false);
  TEST_SYNC_POINT(
      "ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_CommitFail:"
      "1");
  ingest_thread.join();

  fault_injection_env->SetFilesystemActive(true);
  Close();
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu", "eevee"},
                           options);
  ASSERT_EQ(3, handles_.size());
  int cf = 0;
  for (const auto& verify_map : true_data) {
    for (const auto& elem : verify_map) {
      const std::string& key = elem.first;
      ASSERT_EQ("NOT_FOUND", Get(cf, key));
    }
    ++cf;
  }
  Close();
  Destroy(options, true /* delete_cf_paths */);
}

TEST_P(ExternalSSTFileTest,
       IngestFilesIntoMultipleColumnFamilies_PartialManifestWriteFail) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options = CurrentOptions();
  options.env = fault_injection_env.get();

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);

  SyncPoint::GetInstance()->ClearTrace();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency({
      {"VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:0",
       "ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_"
       "PartialManifestWriteFail:0"},
      {"ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_"
       "PartialManifestWriteFail:1",
       "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<ColumnFamilyHandle*> column_families;
  column_families.push_back(handles_[0]);
  column_families.push_back(handles_[1]);
  column_families.push_back(handles_[2]);
  std::vector<IngestExternalFileOptions> ifos(column_families.size());
  for (auto& ifo : ifos) {
    ifo.allow_global_seqno = true;  // Always allow global_seqno
    // May or may not write global_seqno
    ifo.write_global_seqno = std::get<0>(GetParam());
    // Whether to verify block checksums before ingestion
    ifo.verify_checksums_before_ingest = std::get<1>(GetParam());
  }
  std::vector<std::vector<std::pair<std::string, std::string>>> data;
  data.push_back(
      {std::make_pair("foo1", "fv1"), std::make_pair("foo2", "fv2")});
  data.push_back(
      {std::make_pair("bar1", "bv1"), std::make_pair("bar2", "bv2")});
  data.push_back(
      {std::make_pair("bar3", "bv3"), std::make_pair("bar4", "bv4")});
  // Resize the true_data vector upon construction to avoid re-alloc
  std::vector<std::map<std::string, std::string>> true_data(
      column_families.size());
  port::Thread ingest_thread([&]() {
    ASSERT_NOK(GenerateAndAddExternalFiles(options, column_families, ifos, data,
                                           -1, true, true_data));
  });
  TEST_SYNC_POINT(
      "ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_"
      "PartialManifestWriteFail:0");
  fault_injection_env->SetFilesystemActive(false);
  TEST_SYNC_POINT(
      "ExternalSSTFileTest::IngestFilesIntoMultipleColumnFamilies_"
      "PartialManifestWriteFail:1");
  ingest_thread.join();

  ASSERT_OK(fault_injection_env->DropUnsyncedFileData());
  fault_injection_env->SetFilesystemActive(true);
  Close();
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu", "eevee"},
                           options);
  ASSERT_EQ(3, handles_.size());
  int cf = 0;
  for (const auto& verify_map : true_data) {
    for (const auto& elem : verify_map) {
      const std::string& key = elem.first;
      ASSERT_EQ("NOT_FOUND", Get(cf, key));
    }
    ++cf;
  }
  Close();
  Destroy(options, true /* delete_cf_paths */);
}

TEST_P(ExternalSSTFileTest, IngestFilesTriggerFlushingWithTwoWriteQueue) {
  Options options = CurrentOptions();
  // Use large buffer to avoid memtable flush
  options.write_buffer_size = 1024 * 1024;
  options.two_write_queues = true;
  DestroyAndReopen(options);

  ASSERT_OK(dbfull()->Put(WriteOptions(), "1000", "v1"));
  ASSERT_OK(dbfull()->Put(WriteOptions(), "1001", "v1"));
  ASSERT_OK(dbfull()->Put(WriteOptions(), "9999", "v1"));

  // Put one key which is overlap with keys in memtable.
  // It will trigger flushing memtable and require this thread is
  // currently at the front of the 2nd writer queue. We must make
  // sure that it won't enter the 2nd writer queue for the second time.
  std::vector<std::pair<std::string, std::string>> data;
  data.emplace_back("1001", "v2");
  ASSERT_OK(GenerateAndAddExternalFile(options, data, -1, true));
}

TEST_P(ExternalSSTFileTest, DeltaEncodingWhileGlobalSeqnoPresent) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  constexpr size_t kValueSize = 8;
  Random rnd(301);
  std::string value = rnd.RandomString(kValueSize);

  // Write some key to make global seqno larger than zero
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put("ab" + Key(i), value));
  }
  // Get a Snapshot to make RocksDB assign global seqno to ingested sst files.
  auto snap = dbfull()->GetSnapshot();

  std::string fname = sst_files_dir_ + "test_file";
  ROCKSDB_NAMESPACE::SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.Open(fname));
  std::string key1 = "ab";
  std::string key2 = "ab";

  // Make the prefix of key2 is same with key1 add zero seqno. The tail of every
  // key is composed as (seqno << 8 | value_type), and here `1` represents
  // ValueType::kTypeValue

  PutFixed64(&key2, PackSequenceAndType(0, kTypeValue));
  key2 += "cdefghijkl";

  ASSERT_OK(writer.Put(key1, value));
  ASSERT_OK(writer.Put(key2, value));

  ExternalSstFileInfo info;
  ASSERT_OK(writer.Finish(&info));

  ASSERT_OK(dbfull()->IngestExternalFile({info.file_path},
                                         IngestExternalFileOptions()));
  dbfull()->ReleaseSnapshot(snap);
  ASSERT_EQ(value, Get(key1));
  // You will get error here
  ASSERT_EQ(value, Get(key2));
}

TEST_P(ExternalSSTFileTest,
       DeltaEncodingWhileGlobalSeqnoPresentIteratorSwitch) {
  // Regression test for bug where global seqno corrupted the shared bytes
  // buffer when switching from reverse iteration to forward iteration.
  constexpr size_t kValueSize = 8;
  Options options = CurrentOptions();

  Random rnd(301);
  std::string value = rnd.RandomString(kValueSize);

  std::string key0 = "aa";
  std::string key1 = "ab";
  // Make the prefix of key2 is same with key1 add zero seqno. The tail of every
  // key is composed as (seqno << 8 | value_type), and here `1` represents
  // ValueType::kTypeValue
  std::string key2 = "ab";
  PutFixed64(&key2, PackSequenceAndType(0, kTypeValue));
  key2 += "cdefghijkl";
  std::string key3 = key2 + "_";

  // Write some key to make global seqno larger than zero
  ASSERT_OK(Put(key0, value));

  std::string fname = sst_files_dir_ + "test_file";
  ROCKSDB_NAMESPACE::SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.Open(fname));

  // key0 is a dummy to ensure the turnaround point (key1) comes from Prev
  // cache rather than block (restart keys are pinned in block).
  ASSERT_OK(writer.Put(key0, value));
  ASSERT_OK(writer.Put(key1, value));
  ASSERT_OK(writer.Put(key2, value));
  ASSERT_OK(writer.Put(key3, value));

  ExternalSstFileInfo info;
  ASSERT_OK(writer.Finish(&info));

  ASSERT_OK(dbfull()->IngestExternalFile({info.file_path},
                                         IngestExternalFileOptions()));
  ReadOptions read_opts;
  // Prevents Seek() when switching directions, which circumvents the bug.
  read_opts.total_order_seek = true;
  Iterator* iter = db_->NewIterator(read_opts);
  // Scan backwards to key2. File iterator will then be positioned at key1.
  iter->Seek(key3);
  ASSERT_EQ(key3, iter->key());
  iter->Prev();
  ASSERT_EQ(key2, iter->key());
  // Scan forwards and make sure key3 is present. Previously key3 would be
  // corrupted by the global seqno from key1.
  iter->Next();
  ASSERT_EQ(key3, iter->key());
  delete iter;
}

TEST_F(ExternalSSTFileTest, FIFOCompaction) {
  // FIFO always ingests SST files to L0 and assign latest sequence number.
  Options options = CurrentOptions();
  options.num_levels = 1;
  options.compaction_style = kCompactionStyleFIFO;
  options.max_open_files = -1;
  DestroyAndReopen(options);
  std::map<std::string, std::string> true_data;

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(Key(i), Key(i) + "_val"));
    true_data[Key(i)] = Key(i) + "_val";
  }
  ASSERT_OK(Flush());
  ASSERT_EQ("1", FilesPerLevel());
  std::vector<std::pair<std::string, std::string>> file_data;
  for (int i = 0; i <= 20; i++) {
    file_data.emplace_back(Key(i), Key(i) + "_ingest");
  }
  // Overlaps with memtable, will trigger flush
  ASSERT_OK(GenerateAndAddExternalFile(options, file_data, -1,
                                       /*allow_global_seqno=*/true, true, false,
                                       false, false, &true_data));
  ASSERT_EQ("2", FilesPerLevel());

  file_data.clear();
  for (int i = 100; i <= 120; i++) {
    file_data.emplace_back(Key(i), Key(i) + "_ingest");
  }
  // global sequence number is always assigned, so this will fail
  ASSERT_NOK(GenerateAndAddExternalFile(options, file_data, -1,
                                        /*allow_global_seqno=*/false, true,
                                        false, false, false, &true_data));
  ASSERT_OK(GenerateAndAddExternalFile(options, file_data, -1,
                                       /*allow_global_seqno=*/true, true, false,
                                       false, false, &true_data));

  // Compact to data to lower level to test multi-level FIFO later
  options.num_levels = 7;
  options.compaction_style = kCompactionStyleUniversal;
  ASSERT_OK(TryReopen(options));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  options.num_levels = 7;
  options.compaction_style = kCompactionStyleFIFO;
  ASSERT_OK(TryReopen(options));
  file_data.clear();
  for (int i = 200; i <= 220; i++) {
    file_data.emplace_back(Key(i), Key(i) + "_ingest");
  }
  // Files are ingested into L0 for multi-level FIFO
  ASSERT_OK(GenerateAndAddExternalFile(options, file_data, -1,
                                       /*allow_global_seqno=*/true, true, false,
                                       false, false, &true_data));

  ASSERT_EQ("1,0,0,0,0,0,1", FilesPerLevel());
  VerifyDBFromMap(true_data);
}

class ExternalSSTFileWithTimestampTest : public ExternalSSTFileTest {
 public:
  ExternalSSTFileWithTimestampTest() = default;

  static const std::string kValueNotFound;
  static const std::string kTsNotFound;

  std::string EncodeAsUint64(uint64_t v) {
    std::string dst;
    PutFixed64(&dst, v);
    return dst;
  }

  Status IngestExternalUDTFile(const std::vector<std::string>& files,
                               bool allow_global_seqno = true) {
    IngestExternalFileOptions opts;
    opts.snapshot_consistency = true;
    opts.allow_global_seqno = allow_global_seqno;
    return db_->IngestExternalFile(files, opts);
  }

  void VerifyValueAndTs(const std::string& key,
                        const std::string& read_timestamp,
                        const std::string& expected_value,
                        const std::string& expected_timestamp) {
    Slice read_ts = read_timestamp;
    ReadOptions read_options;
    read_options.timestamp = &read_ts;
    std::string value;
    std::string timestamp;
    Status s = db_->Get(read_options, key, &value, &timestamp);
    if (s.ok()) {
      ASSERT_EQ(value, expected_value);
      ASSERT_EQ(timestamp, expected_timestamp);
    } else if (s.IsNotFound()) {
      ASSERT_EQ(kValueNotFound, expected_value);
      ASSERT_EQ(kTsNotFound, expected_timestamp);
    } else {
      assert(false);
    }
  }
};

const std::string ExternalSSTFileWithTimestampTest::kValueNotFound =
    "NOT_FOUND";
const std::string ExternalSSTFileWithTimestampTest::kTsNotFound =
    "NOT_FOUND_TS";

TEST_F(ExternalSSTFileWithTimestampTest, Basic) {
  do {
    Options options = CurrentOptions();
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    options.persist_user_defined_timestamps = true;

    DestroyAndReopen(options);

    SstFileWriter sst_file_writer(EnvOptions(), options);

    // Current file size should be 0 after sst_file_writer init and before open
    // a file.
    ASSERT_EQ(sst_file_writer.FileSize(), 0);

    // file1.sst [0, 50)
    std::string file1 = sst_files_dir_ + "file1.sst";
    ASSERT_OK(sst_file_writer.Open(file1));
    for (int k = 0; k < 50; k++) {
      // write 3 versions of values for each key, write newer version first
      // they are treated as logically smaller by the comparator.
      for (int version = 3; version > 0; version--) {
        ASSERT_OK(
            sst_file_writer.Put(Key(k), EncodeAsUint64(k + version),
                                Key(k) + "_val" + std::to_string(version)));
      }
    }

    ExternalSstFileInfo file1_info;
    ASSERT_OK(sst_file_writer.Finish(&file1_info));
    // sst_file_writer already finished, cannot add this value
    ASSERT_NOK(sst_file_writer.Put(Key(100), EncodeAsUint64(1), "bad_val"));

    ASSERT_EQ(file1_info.file_path, file1);
    ASSERT_EQ(file1_info.num_entries, 150);
    ASSERT_EQ(file1_info.smallest_key, Key(0) + EncodeAsUint64(0 + 3));
    ASSERT_EQ(file1_info.largest_key, Key(49) + EncodeAsUint64(49 + 1));
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
    // Add file using file path
    ASSERT_OK(IngestExternalUDTFile({file1}));
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);

    for (int k = 0; k < 50; k++) {
      for (int version = 3; version > 0; version--) {
        VerifyValueAndTs(Key(k), EncodeAsUint64(k + version),
                         Key(k) + "_val" + std::to_string(version),
                         EncodeAsUint64(k + version));
      }
    }

    // file2.sst [50, 200)
    // Put [key=k, ts=k, value=k_val] for k in [50, 200)
    // RangeDelete[start_key=75, end_key=125, ts=100]
    std::string file2 = sst_files_dir_ + "file2.sst";
    int range_del_begin = 75, range_del_end = 125, range_del_ts = 100;
    ASSERT_OK(sst_file_writer.Open(file2));
    for (int k = 50; k < 200; k++) {
      ASSERT_OK(
          sst_file_writer.Put(Key(k), EncodeAsUint64(k), Key(k) + "_val"));
      if (k == range_del_ts) {
        ASSERT_OK(sst_file_writer.DeleteRange(
            Key(range_del_begin), Key(range_del_end), EncodeAsUint64(k)));
      }
    }

    ExternalSstFileInfo file2_info;
    ASSERT_OK(sst_file_writer.Finish(&file2_info));

    // Current file size should be non-zero after success write.
    ASSERT_GT(sst_file_writer.FileSize(), 0);

    ASSERT_EQ(file2_info.file_path, file2);
    ASSERT_EQ(file2_info.num_entries, 150);
    ASSERT_EQ(file2_info.smallest_key, Key(50) + EncodeAsUint64(50));
    ASSERT_EQ(file2_info.largest_key, Key(199) + EncodeAsUint64(199));
    ASSERT_EQ(file2_info.num_range_del_entries, 1);
    ASSERT_EQ(file2_info.smallest_range_del_key,
              Key(range_del_begin) + EncodeAsUint64(range_del_ts));
    ASSERT_EQ(file2_info.largest_range_del_key,
              Key(range_del_end) + EncodeAsUint64(range_del_ts));
    // Add file using file path
    ASSERT_OK(IngestExternalUDTFile({file2}));
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);

    for (int k = 50; k < 200; k++) {
      if (k < range_del_begin || k >= range_del_end) {
        VerifyValueAndTs(Key(k), EncodeAsUint64(k), Key(k) + "_val",
                         EncodeAsUint64(k));
      }
      //      else {
      //        // FIXME(yuzhangyu): when range tombstone and point data has the
      //        // same seq, on read path, make range tombstone overrides point
      //        // data if it has a newer user-defined timestamp. This is how
      //        // we determine point data's overriding relationship, so we
      //        //  should keep it consistent.
      //        VerifyValueAndTs(Key(k), EncodeAsUint64(k), Key(k) + "_val",
      //                         EncodeAsUint64(k));
      //        VerifyValueAndTs(Key(k), EncodeAsUint64(range_del_ts),
      //        kValueNotFound,
      //                         kTsNotFound);
      //      }
    }

    // file3.sst [100, 200), key range overlap with db
    std::string file3 = sst_files_dir_ + "file3.sst";
    ASSERT_OK(sst_file_writer.Open(file3));
    for (int k = 100; k < 200; k++) {
      ASSERT_OK(
          sst_file_writer.Put(Key(k), EncodeAsUint64(k + 1), Key(k) + "_val1"));
    }
    ExternalSstFileInfo file3_info;
    ASSERT_OK(sst_file_writer.Finish(&file3_info));
    ASSERT_EQ(file3_info.file_path, file3);
    ASSERT_EQ(file3_info.num_entries, 100);
    ASSERT_EQ(file3_info.smallest_key, Key(100) + EncodeAsUint64(101));
    ASSERT_EQ(file3_info.largest_key, Key(199) + EncodeAsUint64(200));

    // Allowing ingesting a file containing overlap key range with the db is
    // not safe without verifying the overlapped key has a higher timestamp
    // than what the db contains, so we do not allow this regardless of
    // whether global sequence number is allowed.
    ASSERT_NOK(IngestExternalUDTFile({file2}));
    ASSERT_NOK(IngestExternalUDTFile({file2}, /*allow_global_seqno*/ false));

    // Write [0, 50)
    // Write to DB newer versions to cover ingested data and move sequence
    // number forward.
    for (int k = 0; k < 50; k++) {
      ASSERT_OK(dbfull()->Put(WriteOptions(), Key(k), EncodeAsUint64(k + 4),
                              Key(k) + "_val" + std::to_string(4)));
    }

    // Read all 4 versions (3 from ingested, 1 from live writes).
    for (int k = 0; k < 50; k++) {
      for (int version = 4; version > 0; version--) {
        VerifyValueAndTs(Key(k), EncodeAsUint64(k + version),
                         Key(k) + "_val" + std::to_string(version),
                         EncodeAsUint64(k + version));
      }
    }
    SequenceNumber seq_num_before_ingestion = db_->GetLatestSequenceNumber();
    ASSERT_GT(seq_num_before_ingestion, 0U);

    // file4.sst [200, 250)
    std::string file4 = sst_files_dir_ + "file4.sst";
    ASSERT_OK(sst_file_writer.Open(file4));
    for (int k = 200; k < 250; k++) {
      ASSERT_OK(
          sst_file_writer.Put(Key(k), EncodeAsUint64(k), Key(k) + "_val"));
    }

    ExternalSstFileInfo file4_info;
    ASSERT_OK(sst_file_writer.Finish(&file4_info));

    // Current file size should be non-zero after success write.
    ASSERT_GT(sst_file_writer.FileSize(), 0);

    ASSERT_EQ(file4_info.file_path, file4);
    ASSERT_EQ(file4_info.num_entries, 50);
    ASSERT_EQ(file4_info.smallest_key, Key(200) + EncodeAsUint64(200));
    ASSERT_EQ(file4_info.largest_key, Key(249) + EncodeAsUint64(249));
    ASSERT_EQ(file4_info.num_range_del_entries, 0);
    ASSERT_EQ(file4_info.smallest_range_del_key, "");
    ASSERT_EQ(file4_info.largest_range_del_key, "");

    ASSERT_OK(IngestExternalUDTFile({file4}));

    for (int k = 200; k < 250; k++) {
      VerifyValueAndTs(Key(k), EncodeAsUint64(k), Key(k) + "_val",
                       EncodeAsUint64(k));
    }

    // In UDT mode, any external file that can be successfully ingested also
    // should not overlap with the db. As a result, they can always get the
    // seq 0 assigned.
    ASSERT_EQ(db_->GetLatestSequenceNumber(), seq_num_before_ingestion);

    // file5.sst (Key(200), ts = 199)
    // While DB has (Key(200), ts = 200) => user key without timestamp overlaps
    std::string file5 = sst_files_dir_ + "file5.sst";
    ASSERT_OK(sst_file_writer.Open(file5));
    ASSERT_OK(
        sst_file_writer.Put(Key(200), EncodeAsUint64(199), Key(200) + "_val"));

    ExternalSstFileInfo file5_info;
    ASSERT_OK(sst_file_writer.Finish(&file5_info));
    ASSERT_TRUE(IngestExternalUDTFile({file5}).IsInvalidArgument());

    // file6.sst (Key(200), ts = 201)
    // While DB has (Key(200), ts = 200) => user key without timestamp overlaps
    std::string file6 = sst_files_dir_ + "file6.sst";
    ASSERT_OK(sst_file_writer.Open(file6));
    ASSERT_OK(
        sst_file_writer.Put(Key(200), EncodeAsUint64(201), Key(0) + "_val"));

    ExternalSstFileInfo file6_info;
    ASSERT_OK(sst_file_writer.Finish(&file6_info));
    ASSERT_TRUE(IngestExternalUDTFile({file6}).IsInvalidArgument());

    // Check memtable overlap.
    ASSERT_OK(dbfull()->Put(WriteOptions(), Key(250), EncodeAsUint64(250),
                            Key(250) + "_val"));

    std::string file7 = sst_files_dir_ + "file7.sst";
    ASSERT_OK(sst_file_writer.Open(file7));
    ASSERT_OK(
        sst_file_writer.Put(Key(250), EncodeAsUint64(249), Key(250) + "_val2"));

    ExternalSstFileInfo file7_info;
    ASSERT_OK(sst_file_writer.Finish(&file7_info));
    ASSERT_TRUE(IngestExternalUDTFile({file7}).IsInvalidArgument());

    std::string file8 = sst_files_dir_ + "file8.sst";
    ASSERT_OK(sst_file_writer.Open(file8));
    ASSERT_OK(
        sst_file_writer.Put(Key(250), EncodeAsUint64(251), Key(250) + "_val3"));

    ExternalSstFileInfo file8_info;
    ASSERT_OK(sst_file_writer.Finish(&file8_info));
    ASSERT_TRUE(IngestExternalUDTFile({file8}).IsInvalidArgument());

    DestroyAndRecreateExternalSSTFilesDir();
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction |
                         kRangeDelSkipConfigs));
}

TEST_F(ExternalSSTFileWithTimestampTest, SanityCheck) {
  Options options = CurrentOptions();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.persist_user_defined_timestamps = true;
  DestroyAndReopen(options);

  SstFileWriter sst_file_writer(EnvOptions(), options);

  // file1.sst [0, 100)
  std::string file1 = sst_files_dir_ + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), EncodeAsUint64(k), Key(k) + "_val"));
  }

  ExternalSstFileInfo file1_info;
  ASSERT_OK(sst_file_writer.Finish(&file1_info));

  // file2.sst [50, 75)
  std::string file2 = sst_files_dir_ + "file2.sst";
  ASSERT_OK(sst_file_writer.Open(file2));
  for (int k = 50; k < 75; k++) {
    ASSERT_OK(
        sst_file_writer.Put(Key(k), EncodeAsUint64(k + 2), Key(k) + "_val"));
  }
  ExternalSstFileInfo file2_info;
  ASSERT_OK(sst_file_writer.Finish(&file2_info));

  // Cannot ingest when files' user key range overlaps. There is no
  // straightforward way to assign sequence number to the files so that they
  // meet the user-defined timestamps invariant: for the same user provided key,
  // the entry with a higher sequence number should not have a smaller
  // timestamp. In this case: file1 has (key=k, ts=k) for k in [50, 75),
  //               file2 has (key=k, ts=k+2) for k in [50, 75).
  // The invariant is only met if file2 is ingested after file1. In other cases
  // when user key ranges are interleaved in files, no order of ingestion can
  // guarantee this invariant. So we do not allow ingesting files with
  // overlapping key ranges.
  ASSERT_TRUE(IngestExternalUDTFile({file1, file2}).IsNotSupported());

  options.allow_ingest_behind = true;
  DestroyAndReopen(options);
  IngestExternalFileOptions opts;

  // TODO(yuzhangyu): support ingestion behind for user-defined timestamps?
  // Ingesting external files with user-defined timestamps requires searching
  // through the whole lsm tree to make sure there is no key range overlap with
  // the db. Ingestion behind currently is doing a simply placing it at the
  // bottom level step without a search, so we don't allow it either.
  opts.ingest_behind = true;
  ASSERT_TRUE(db_->IngestExternalFile({file1}, opts).IsNotSupported());

  DestroyAndRecreateExternalSSTFilesDir();
}

TEST_F(ExternalSSTFileWithTimestampTest, UDTSettingsCompatibilityCheck) {
  Options options = CurrentOptions();
  Options disable_udt_options = options;
  Options not_persist_udt_options = options;
  Options persist_udt_options = options;
  disable_udt_options.comparator = BytewiseComparator();
  not_persist_udt_options.comparator =
      test::BytewiseComparatorWithU64TsWrapper();
  not_persist_udt_options.persist_user_defined_timestamps = false;
  not_persist_udt_options.allow_concurrent_memtable_write = false;
  persist_udt_options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  persist_udt_options.persist_user_defined_timestamps = true;

  EnvOptions env_options = EnvOptions();

  SstFileWriter disable_udt_sst_writer(env_options, disable_udt_options);
  SstFileWriter not_persist_udt_sst_writer(env_options,
                                           not_persist_udt_options);
  SstFileWriter persist_udt_sst_writer(env_options, persist_udt_options);

  // File1: [0, 50), contains no timestamps
  // comparator name: leveldb.BytewiseComparator
  // user_defined_timestamps_persisted: true
  std::string disable_udt_sst_file = sst_files_dir_ + "file1.sst";
  ASSERT_OK(disable_udt_sst_writer.Open(disable_udt_sst_file));
  for (int k = 0; k < 50; k++) {
    ASSERT_NOK(
        disable_udt_sst_writer.Put(Key(k), EncodeAsUint64(1), Key(k) + "_val"));
    ASSERT_OK(disable_udt_sst_writer.Put(Key(k), Key(k) + "_val"));
  }
  ASSERT_OK(disable_udt_sst_writer.Finish());

  // File2: [50, 100), contains no timestamps
  // comparator name: leveldb.BytewiseComparator.u64ts
  // user_defined_timestamps_persisted: false
  std::string not_persist_udt_sst_file = sst_files_dir_ + "file2.sst";
  ASSERT_OK(not_persist_udt_sst_writer.Open(not_persist_udt_sst_file));
  for (int k = 50; k < 100; k++) {
    ASSERT_NOK(not_persist_udt_sst_writer.Put(Key(k), Key(k) + "_val"));
    ASSERT_NOK(not_persist_udt_sst_writer.Put(Key(k), EncodeAsUint64(k),
                                              Key(k) + "_val"));
    ASSERT_OK(not_persist_udt_sst_writer.Put(Key(k), EncodeAsUint64(0),
                                             Key(k) + "_val"));
  }
  ASSERT_OK(not_persist_udt_sst_writer.Finish());

  // File3: [100, 150), contains timestamp
  // comparator name: leveldb.BytewiseComparator.u64ts
  // user_defined_timestamps_persisted: true
  std::string persist_udt_sst_file = sst_files_dir_ + "file3.sst";
  ASSERT_OK(persist_udt_sst_writer.Open(persist_udt_sst_file));
  for (int k = 100; k < 150; k++) {
    ASSERT_NOK(persist_udt_sst_writer.Put(Key(k), Key(k) + "_val"));
    ASSERT_OK(
        persist_udt_sst_writer.Put(Key(k), EncodeAsUint64(k), Key(k) + "_val"));
  }
  ASSERT_OK(persist_udt_sst_writer.Finish());

  DestroyAndReopen(disable_udt_options);
  ASSERT_OK(
      IngestExternalUDTFile({disable_udt_sst_file, not_persist_udt_sst_file}));
  ASSERT_NOK(IngestExternalUDTFile({persist_udt_sst_file}));
  for (int k = 0; k < 100; k++) {
    ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
  }

  DestroyAndReopen(not_persist_udt_options);
  ASSERT_OK(
      IngestExternalUDTFile({disable_udt_sst_file, not_persist_udt_sst_file}));
  ASSERT_NOK(IngestExternalUDTFile({persist_udt_sst_file}));
  for (int k = 0; k < 100; k++) {
    VerifyValueAndTs(Key(k), EncodeAsUint64(0), Key(k) + "_val",
                     EncodeAsUint64(0));
  }

  DestroyAndReopen(persist_udt_options);
  ASSERT_NOK(
      IngestExternalUDTFile({disable_udt_sst_file, not_persist_udt_sst_file}));
  ASSERT_OK(IngestExternalUDTFile({persist_udt_sst_file}));
  for (int k = 100; k < 150; k++) {
    VerifyValueAndTs(Key(k), EncodeAsUint64(k), Key(k) + "_val",
                     EncodeAsUint64(k));
  }

  DestroyAndRecreateExternalSSTFilesDir();
}

TEST_F(ExternalSSTFileWithTimestampTest, TimestampsNotPersistedBasic) {
  do {
    Options options = CurrentOptions();
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    options.persist_user_defined_timestamps = false;
    options.allow_concurrent_memtable_write = false;

    DestroyAndReopen(options);

    SstFileWriter sst_file_writer(EnvOptions(), options);

    // file1.sst [0, 50)
    std::string file1 = sst_files_dir_ + "file1.sst";
    ASSERT_OK(sst_file_writer.Open(file1));
    for (int k = 0; k < 50; k++) {
      // Attempting to write 2 versions of values for each key, only the version
      // with timestamp 0 goes through.
      for (int version = 1; version >= 0; version--) {
        if (version == 1) {
          ASSERT_NOK(
              sst_file_writer.Put(Key(k), EncodeAsUint64(version),
                                  Key(k) + "_val" + std::to_string(version)));
        } else {
          ASSERT_OK(
              sst_file_writer.Put(Key(k), EncodeAsUint64(version),
                                  Key(k) + "_val" + std::to_string(version)));
        }
      }
    }

    ExternalSstFileInfo file1_info;
    ASSERT_OK(sst_file_writer.Finish(&file1_info));
    // sst_file_writer already finished, cannot add this value
    ASSERT_NOK(sst_file_writer.Put(Key(100), EncodeAsUint64(0), "bad_val"));

    ASSERT_EQ(file1_info.file_path, file1);
    ASSERT_EQ(file1_info.num_entries, 50);
    ASSERT_EQ(file1_info.smallest_key, Key(0));
    ASSERT_EQ(file1_info.largest_key, Key(49));
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
    // Add file using file path
    ASSERT_OK(IngestExternalUDTFile({file1}));
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);

    // Read ingested file: all data contain minimum timestamps.
    for (int k = 0; k < 50; k++) {
      VerifyValueAndTs(Key(k), EncodeAsUint64(0),
                       Key(k) + "_val" + std::to_string(0), EncodeAsUint64(0));
    }

    // file2.sst [50, 200)
    // Put [key=k, ts=0, value=k_val0] for k in [50, 200)
    // RangeDelete[start_key=75, end_key=125, ts=0]
    std::string file2 = sst_files_dir_ + "file2.sst";
    int range_del_begin = 75, range_del_end = 125;
    ASSERT_OK(sst_file_writer.Open(file2));
    for (int k = 50; k < 200; k++) {
      // All these timestamps will later be effectively 0
      ASSERT_OK(
          sst_file_writer.Put(Key(k), EncodeAsUint64(0), Key(k) + "_val0"));
    }
    ASSERT_OK(sst_file_writer.DeleteRange(
        Key(range_del_begin), Key(range_del_end), EncodeAsUint64(0)));

    ExternalSstFileInfo file2_info;
    ASSERT_OK(sst_file_writer.Finish(&file2_info));

    // Current file size should be non-zero after success write.
    ASSERT_GT(sst_file_writer.FileSize(), 0);

    ASSERT_EQ(file2_info.file_path, file2);
    ASSERT_EQ(file2_info.num_entries, 150);
    ASSERT_EQ(file2_info.smallest_key, Key(50));
    ASSERT_EQ(file2_info.largest_key, Key(199));
    ASSERT_EQ(file2_info.num_range_del_entries, 1);
    ASSERT_EQ(file2_info.smallest_range_del_key, Key(range_del_begin));
    ASSERT_EQ(file2_info.largest_range_del_key, Key(range_del_end));
    // Add file using file path
    ASSERT_OK(IngestExternalUDTFile({file2}));
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);

    // Range deletion covering point data in the same file is over-written.
    for (int k = 50; k < 200; k++) {
      VerifyValueAndTs(Key(k), EncodeAsUint64(0), Key(k) + "_val0",
                       EncodeAsUint64(0));
    }

    // file3.sst [100, 200), key range overlap with db
    std::string file3 = sst_files_dir_ + "file3.sst";
    ASSERT_OK(sst_file_writer.Open(file3));
    for (int k = 100; k < 200; k++) {
      ASSERT_OK(
          sst_file_writer.Put(Key(k), EncodeAsUint64(0), Key(k) + "_val0"));
    }
    ExternalSstFileInfo file3_info;
    ASSERT_OK(sst_file_writer.Finish(&file3_info));
    ASSERT_EQ(file3_info.file_path, file3);
    ASSERT_EQ(file3_info.num_entries, 100);
    ASSERT_EQ(file3_info.smallest_key, Key(100));
    ASSERT_EQ(file3_info.largest_key, Key(199));

    // In UDT mode, file with overlapping key range cannot be ingested.
    ASSERT_NOK(IngestExternalUDTFile({file3}));
    ASSERT_NOK(IngestExternalUDTFile({file3}, /*allow_global_seqno*/ false));

    // Write [0, 50)
    // Write to DB newer versions to cover ingested data and move sequence
    // number forward.
    for (int k = 0; k < 50; k++) {
      for (int version = 1; version < 3; version++) {
        ASSERT_OK(dbfull()->Put(WriteOptions(), Key(k), EncodeAsUint64(version),
                                Key(k) + "_val" + std::to_string(version)));
      }
    }

    // Read three versions (1 from ingested, 2 from live writes)
    for (int k = 0; k < 50; k++) {
      for (int version = 0; version < 3; version++) {
        VerifyValueAndTs(Key(k), EncodeAsUint64(version),
                         Key(k) + "_val" + std::to_string(version),
                         EncodeAsUint64(version));
      }
    }
    SequenceNumber seq_num_before_ingestion = db_->GetLatestSequenceNumber();
    ASSERT_GT(seq_num_before_ingestion, 0U);

    // file4.sst [200, 250)
    std::string file4 = sst_files_dir_ + "file4.sst";
    ASSERT_OK(sst_file_writer.Open(file4));
    for (int k = 200; k < 250; k++) {
      ASSERT_OK(
          sst_file_writer.Put(Key(k), EncodeAsUint64(0), Key(k) + "_val"));
    }

    ExternalSstFileInfo file4_info;
    ASSERT_OK(sst_file_writer.Finish(&file4_info));

    // Current file size should be non-zero after success write.
    ASSERT_GT(sst_file_writer.FileSize(), 0);

    ASSERT_EQ(file4_info.file_path, file4);
    ASSERT_EQ(file4_info.num_entries, 50);
    ASSERT_EQ(file4_info.smallest_key, Key(200));
    ASSERT_EQ(file4_info.largest_key, Key(249));
    ASSERT_EQ(file4_info.num_range_del_entries, 0);
    ASSERT_EQ(file4_info.smallest_range_del_key, "");
    ASSERT_EQ(file4_info.largest_range_del_key, "");

    ASSERT_OK(IngestExternalUDTFile({file4}));

    // Ingested files do not overlap with db, they can always have global seqno
    // 0 assigned.
    ASSERT_EQ(db_->GetLatestSequenceNumber(), seq_num_before_ingestion);

    DestroyAndRecreateExternalSSTFilesDir();
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction |
                         kRangeDelSkipConfigs));
}

INSTANTIATE_TEST_CASE_P(ExternalSSTFileTest, ExternalSSTFileTest,
                        testing::Combine(testing::Bool(), testing::Bool()));

class IngestDBGeneratedFileTest
    : public ExternalSSTFileTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  IngestDBGeneratedFileTest() {
    ingest_opts.allow_db_generated_files = true;
    ingest_opts.link_files = std::get<0>(GetParam());
    ingest_opts.verify_checksums_before_ingest = std::get<1>(GetParam());
    ingest_opts.snapshot_consistency = false;
  }

 protected:
  IngestExternalFileOptions ingest_opts;
};

INSTANTIATE_TEST_CASE_P(BasicMultiConfig, IngestDBGeneratedFileTest,
                        testing::Combine(testing::Bool(), testing::Bool()));

TEST_P(IngestDBGeneratedFileTest, FailureCase) {
  if (encrypted_env_ && ingest_opts.link_files) {
    // FIXME: should fail ingestion or support this combination.
    ROCKSDB_GTEST_SKIP(
        "Encrypted env and link_files do not work together, as we reopen the "
        "file after linking it which appends an extra encryption prefix.");
    return;
  }
  // Ingesting overlapping data should always fail.
  do {
    SCOPED_TRACE("option_config_ = " + std::to_string(option_config_));

    Options options = CurrentOptions();
    CreateAndReopenWithCF({"toto"}, options);
    // Fill CFs with overlapping keys. Will try to ingest CF1 into default CF.
    for (int k = 0; k < 50; ++k) {
      ASSERT_OK(Put(Key(k), "default_cf_" + Key(k)));
    }
    for (int k = 49; k < 100; ++k) {
      ASSERT_OK(Put(1, Key(k), "cf1_" + Key(k)));
    }
    ASSERT_OK(Flush(/*cf=*/1));
    {
      // Verify that largest key of the file has non-zero seqno.
      std::vector<std::vector<FileMetaData>> metadata;
      dbfull()->TEST_GetFilesMetaData(handles_[1], &metadata, nullptr);
      const FileMetaData& file = metadata[0][0];
      ValueType vtype;
      SequenceNumber seq;
      UnPackSequenceAndType(ExtractInternalKeyFooter(file.largest.Encode()),
                            &seq, &vtype);
      ASSERT_GE(seq, 0);
    }
    std::vector<LiveFileMetaData> live_meta;
    db_->GetLiveFilesMetaData(&live_meta);
    ASSERT_EQ(live_meta.size(), 1);
    std::vector<std::string> to_ingest_files;
    to_ingest_files.emplace_back(live_meta[0].directory + "/" +
                                 live_meta[0].relative_filename);
    // Ingesting a file whose boundary key has non-zero seqno.
    Status s = db_->IngestExternalFile(to_ingest_files, ingest_opts);
    // This error msg is from checking seqno of boundary keys.
    ASSERT_TRUE(
        s.ToString().find("External file has non zero sequence number") !=
        std::string::npos);
    ASSERT_NOK(s);

    {
      // Only non-boundary key with non-zero seqno.
      const Snapshot* snapshot = db_->GetSnapshot();
      ASSERT_OK(Put(1, Key(70), "cf1_" + Key(70)));
      ASSERT_OK(Flush(1));
      CompactRangeOptions cro;
      cro.bottommost_level_compaction =
          BottommostLevelCompaction::kForceOptimized;
      ASSERT_OK(db_->CompactRange(cro, handles_[1], nullptr, nullptr));

      // Verify that only the non-boundary key of the file has non-zero seqno.
      std::vector<std::vector<FileMetaData>> metadata;
      // File may be at different level for different options.
      dbfull()->TEST_GetFilesMetaData(handles_[1], &metadata, nullptr);
      bool found_file = false;
      for (const auto& level : metadata) {
        if (level.empty()) {
          continue;
        }
        ASSERT_FALSE(found_file);
        found_file = true;
        ASSERT_EQ(1, level.size());
        const FileMetaData& file = level[0];
        ValueType vtype;
        SequenceNumber seq;
        UnPackSequenceAndType(ExtractInternalKeyFooter(file.largest.Encode()),
                              &seq, &vtype);
        ASSERT_EQ(seq, 0);
        UnPackSequenceAndType(ExtractInternalKeyFooter(file.smallest.Encode()),
                              &seq, &vtype);
        ASSERT_EQ(seq, 0);
        ASSERT_GT(file.fd.largest_seqno, 0);
      }
      ASSERT_TRUE(found_file);
      live_meta.clear();
      db_->GetLiveFilesMetaData(&live_meta);
      ASSERT_EQ(live_meta.size(), 1);
      to_ingest_files[0] =
          live_meta[0].directory + "/" + live_meta[0].relative_filename;
      s = db_->IngestExternalFile(to_ingest_files, ingest_opts);
      ASSERT_NOK(s);
      // This error msg is from checking largest seqno in table property.
      ASSERT_TRUE(s.ToString().find("non zero largest sequence number") !=
                  std::string::npos);
      db_->ReleaseSnapshot(snapshot);
    }

    CompactRangeOptions cro;
    cro.bottommost_level_compaction =
        BottommostLevelCompaction::kForceOptimized;
    ASSERT_OK(db_->CompactRange(cro, handles_[1], nullptr, nullptr));
    live_meta.clear();
    db_->GetLiveFilesMetaData(&live_meta);
    ASSERT_EQ(live_meta.size(), 1);
    ASSERT_EQ(0, live_meta[0].largest_seqno);
    to_ingest_files[0] =
        live_meta[0].directory + "/" + live_meta[0].relative_filename;

    ingest_opts.allow_db_generated_files = false;
    // Ingesting a DB genrate file with allow_db_generated_files = false;
    s = db_->IngestExternalFile(to_ingest_files, ingest_opts);
    ASSERT_TRUE(s.ToString().find("External file version not found") !=
                std::string::npos);
    ASSERT_NOK(s);

    const std::string err =
        "An ingested file is assigned to a non-zero sequence number, which is "
        "incompatible with ingestion option allow_db_generated_files";
    ingest_opts.allow_db_generated_files = true;
    s = db_->IngestExternalFile(to_ingest_files, ingest_opts);
    ASSERT_TRUE(s.ToString().find(err) != std::string::npos);
    ASSERT_NOK(s);
    if (options.compaction_style != kCompactionStyleUniversal) {
      // FIXME: after fixing ingestion with universal compaction, currently
      //  will always ingest into L0.
      ingest_opts.fail_if_not_bottommost_level = true;
      s = db_->IngestExternalFile(to_ingest_files, ingest_opts);
      ASSERT_NOK(s);
      ASSERT_TRUE(s.ToString().find("Files cannot be ingested to Lmax") !=
                  std::string::npos);
      ingest_opts.fail_if_not_bottommost_level = false;
    }
    ingest_opts.write_global_seqno = true;
    s = db_->IngestExternalFile(to_ingest_files, ingest_opts);
    ASSERT_TRUE(s.ToString().find("write_global_seqno is deprecated and does "
                                  "not work with allow_db_generated_files") !=
                std::string::npos);
    ASSERT_NOK(s);
    ingest_opts.write_global_seqno = false;

    // Delete the overlapping key.
    ASSERT_OK(db_->Delete(WriteOptions(), handles_[1], Key(49)));
    ASSERT_OK(db_->CompactRange(cro, handles_[1], nullptr, nullptr));
    live_meta.clear();
    db_->GetLiveFilesMetaData(&live_meta);
    bool cf1_file_found = false;
    for (const auto& f : live_meta) {
      if (f.column_family_name == "toto") {
        ASSERT_FALSE(cf1_file_found);
        cf1_file_found = true;
        ASSERT_EQ(0, f.largest_seqno);
        to_ingest_files[0] = f.directory + "/" + f.relative_filename;
      }
    }
    ASSERT_TRUE(cf1_file_found);

    const Snapshot* snapshot = db_->GetSnapshot();
    ingest_opts.snapshot_consistency = true;
    s = db_->IngestExternalFile(to_ingest_files, ingest_opts);
    // snapshot_consistency with snapshot will assign a newest sequence number.
    ASSERT_TRUE(s.ToString().find(err) != std::string::npos);
    ASSERT_NOK(s);

    ingest_opts.snapshot_consistency = false;
    ASSERT_OK(db_->IngestExternalFile(to_ingest_files, ingest_opts));
    db_->ReleaseSnapshot(snapshot);

    // Verify default CF content.
    std::string val;
    for (int k = 0; k < 100; ++k) {
      ASSERT_OK(db_->Get(ReadOptions(), Key(k), &val));
      if (k < 50) {
        ASSERT_EQ(val, "default_cf_" + Key(k));
      } else {
        ASSERT_EQ(val, "cf1_" + Key(k));
      }
    }
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction));
}

class IngestDBGeneratedFileTest2
    : public ExternalSSTFileTestBase,
      public ::testing::WithParamInterface<
          std::tuple<bool, bool, bool, bool, bool>> {
 public:
  IngestDBGeneratedFileTest2() = default;
};

INSTANTIATE_TEST_CASE_P(VaryingOptions, IngestDBGeneratedFileTest2,
                        testing::Combine(testing::Bool(), testing::Bool(),
                                         testing::Bool(), testing::Bool(),
                                         testing::Bool()));

TEST_P(IngestDBGeneratedFileTest2, NotOverlapWithDB) {
  // Use a separate column family to sort some data, generate multiple SST
  // files. Then ingest these files into another column family or DB. The data
  // to be ingested does not overlap with existing data.
  IngestExternalFileOptions ingest_opts;
  ingest_opts.allow_db_generated_files = true;
  ingest_opts.snapshot_consistency = std::get<0>(GetParam());
  ingest_opts.allow_global_seqno = std::get<1>(GetParam());
  ingest_opts.allow_blocking_flush = std::get<2>(GetParam());
  ingest_opts.fail_if_not_bottommost_level = std::get<3>(GetParam());
  ingest_opts.link_files = std::get<4>(GetParam());

  do {
    SCOPED_TRACE("option_config_ = " + std::to_string(option_config_));
    Options options = CurrentOptions();
    // vector memtable for temp CF does not support concurrent write
    options.allow_concurrent_memtable_write = false;
    CreateAndReopenWithCF({"toto"}, options);

    // non-empty bottommost level
    WriteOptions wo;
    for (int k = 0; k < 50; ++k) {
      ASSERT_OK(db_->Put(wo, handles_[1], Key(k), "base_val_" + Key(k)));
    }
    ASSERT_OK(Flush());
    CompactRangeOptions cro;
    cro.bottommost_level_compaction =
        BottommostLevelCompaction::kForceOptimized;
    ASSERT_OK(db_->CompactRange(cro, handles_[1], nullptr, nullptr));
    // non-empty memtable
    for (int k = 50; k < 100; ++k) {
      ASSERT_OK(db_->Put(wo, handles_[1], Key(k), "base_val_" + Key(k)));
    }

    // load external data to sort, generate multiple files
    Options temp_cf_opts;
    ColumnFamilyHandle* temp_cfh;
    temp_cf_opts.target_file_size_base = 4 << 10;
    temp_cf_opts.memtable_factory.reset(new VectorRepFactory());
    temp_cf_opts.allow_concurrent_memtable_write = false;
    temp_cf_opts.compaction_style = kCompactionStyleUniversal;
    ASSERT_OK(db_->CreateColumnFamily(temp_cf_opts, "temp_cf", &temp_cfh));

    Random rnd(301);
    std::vector<std::string> expected_value;
    expected_value.resize(100);
    // Out of order insertion of keys from 100 to 199.
    for (int k = 99; k >= 0; --k) {
      expected_value[k] = rnd.RandomString(200);
      ASSERT_OK(db_->Put(wo, temp_cfh, Key(k + 100), expected_value[k]));
    }
    ASSERT_OK(db_->CompactRange(cro, temp_cfh, nullptr, nullptr));
    std::vector<std::string> sst_file_paths;
    ColumnFamilyMetaData cf_meta;
    db_->GetColumnFamilyMetaData(temp_cfh, &cf_meta);
    ASSERT_GT(cf_meta.file_count, 1);
    for (const auto& level_meta : cf_meta.levels) {
      if (level_meta.level + 1 < temp_cf_opts.num_levels) {
        ASSERT_EQ(0, level_meta.files.size());
      } else {
        ASSERT_GT(level_meta.files.size(), 1);
        for (const auto& meta : level_meta.files) {
          ASSERT_EQ(0, meta.largest_seqno);
          sst_file_paths.emplace_back(meta.directory + "/" +
                                      meta.relative_filename);
        }
      }
    }

    ASSERT_OK(
        db_->IngestExternalFile(handles_[1], sst_file_paths, ingest_opts));
    // Verify state of the CF1
    ReadOptions ro;
    std::string val;
    for (int k = 0; k < 100; ++k) {
      ASSERT_OK(db_->Get(ro, handles_[1], Key(k), &val));
      ASSERT_EQ(val, "base_val_" + Key(k));
      ASSERT_OK(db_->Get(ro, handles_[1], Key(100 + k), &val));
      ASSERT_EQ(val, expected_value[k]);
    }

    // Ingest into another DB.
    if (!encrypted_env_) {
      // Ingestion between encrypted env and non-encrypted env won't work.
      std::string db2_path = test::PerThreadDBPath("DB2");
      Options db2_options;
      db2_options.create_if_missing = true;
      DB* db2 = nullptr;
      ASSERT_OK(DB::Open(db2_options, db2_path, &db2));
      // Write some base data.
      expected_value.emplace_back(rnd.RandomString(100));
      ASSERT_OK(db2->Put(WriteOptions(), Key(200), expected_value.back()));
      ASSERT_OK(db2->CompactRange(cro, nullptr, nullptr));
      expected_value.emplace_back(rnd.RandomString(100));
      ASSERT_OK(db2->Put(WriteOptions(), Key(201), expected_value.back()));

      ASSERT_OK(db2->IngestExternalFile({sst_file_paths}, ingest_opts));
      {
        std::unique_ptr<Iterator> iter{db2->NewIterator(ReadOptions())};
        iter->SeekToFirst();
        // The DB should have keys 100-199 from ingested files, and keys 200 and
        // 201 from itself.
        for (int k = 100; k <= 201; ++k, iter->Next()) {
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ(iter->key(), Key(k));
          ASSERT_EQ(iter->value(), expected_value[k - 100]);
        }
        ASSERT_FALSE(iter->Valid());
        ASSERT_OK(iter->status());
      }

      // Dropping the original CF should not affect db2, reopening it should not
      // miss SST files.
      ASSERT_OK(db_->DropColumnFamily(temp_cfh));
      ASSERT_OK(db_->DestroyColumnFamilyHandle(temp_cfh));
      ASSERT_OK(db2->Close());
      delete db2;
      ASSERT_OK(DB::Open(db2_options, db2_path, &db2));
      ASSERT_OK(db2->Close());
      delete db2;
      ASSERT_OK(DestroyDB(db2_path, db2_options));
    } else {
      ASSERT_OK(db_->DropColumnFamily(temp_cfh));
      ASSERT_OK(db_->DestroyColumnFamilyHandle(temp_cfh));
    }
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction));
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
