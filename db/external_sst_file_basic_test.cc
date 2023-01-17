//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <functional>

#include "db/db_test_util.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_writer.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

#ifndef ROCKSDB_LITE
class ExternalSSTFileBasicTest
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  ExternalSSTFileBasicTest()
      : DBTestBase("external_sst_file_basic_test", /*env_do_fsync=*/true) {
    sst_files_dir_ = dbname_ + "_sst_files/";
    fault_injection_test_env_.reset(new FaultInjectionTestEnv(env_));
    DestroyAndRecreateExternalSSTFilesDir();

    // Check if the Env supports RandomRWFile
    std::string file_path = sst_files_dir_ + "test_random_rw_file";
    std::unique_ptr<WritableFile> wfile;
    assert(env_->NewWritableFile(file_path, &wfile, EnvOptions()).ok());
    wfile.reset();
    std::unique_ptr<RandomRWFile> rwfile;
    Status s = env_->NewRandomRWFile(file_path, &rwfile, EnvOptions());
    if (s.IsNotSupported()) {
      random_rwfile_supported_ = false;
    } else {
      EXPECT_OK(s);
      random_rwfile_supported_ = true;
    }
    rwfile.reset();
    EXPECT_OK(env_->DeleteFile(file_path));
  }

  void DestroyAndRecreateExternalSSTFilesDir() {
    ASSERT_OK(DestroyDir(env_, sst_files_dir_));
    ASSERT_OK(env_->CreateDir(sst_files_dir_));
  }

  Status DeprecatedAddFile(const std::vector<std::string>& files,
                           bool move_files = false,
                           bool skip_snapshot_check = false) {
    IngestExternalFileOptions opts;
    opts.move_files = move_files;
    opts.snapshot_consistency = !skip_snapshot_check;
    opts.allow_global_seqno = false;
    opts.allow_blocking_flush = false;
    return db_->IngestExternalFile(files, opts);
  }

  Status AddFileWithFileChecksum(
      const std::vector<std::string>& files,
      const std::vector<std::string>& files_checksums,
      const std::vector<std::string>& files_checksum_func_names,
      bool verify_file_checksum = true, bool move_files = false,
      bool skip_snapshot_check = false, bool write_global_seqno = true) {
    IngestExternalFileOptions opts;
    opts.move_files = move_files;
    opts.snapshot_consistency = !skip_snapshot_check;
    opts.allow_global_seqno = false;
    opts.allow_blocking_flush = false;
    opts.write_global_seqno = write_global_seqno;
    opts.verify_file_checksum = verify_file_checksum;

    IngestExternalFileArg arg;
    arg.column_family = db_->DefaultColumnFamily();
    arg.external_files = files;
    arg.options = opts;
    arg.files_checksums = files_checksums;
    arg.files_checksum_func_names = files_checksum_func_names;
    return db_->IngestExternalFiles({arg});
  }

  Status GenerateAndAddExternalFile(
      const Options options, std::vector<int> keys,
      const std::vector<ValueType>& value_types,
      std::vector<std::pair<int, int>> range_deletions, int file_id,
      bool write_global_seqno, bool verify_checksums_before_ingest,
      std::map<std::string, std::string>* true_data) {
    assert(value_types.size() == 1 || keys.size() == value_types.size());
    std::string file_path = sst_files_dir_ + std::to_string(file_id);
    SstFileWriter sst_file_writer(EnvOptions(), options);

    Status s = sst_file_writer.Open(file_path);
    if (!s.ok()) {
      return s;
    }
    for (size_t i = 0; i < range_deletions.size(); i++) {
      // Account for the effect of range deletions on true_data before
      // all point operators, even though sst_file_writer.DeleteRange
      // must be called before other sst_file_writer methods. This is
      // because point writes take precedence over range deletions
      // in the same ingested sst.
      std::string start_key = Key(range_deletions[i].first);
      std::string end_key = Key(range_deletions[i].second);
      s = sst_file_writer.DeleteRange(start_key, end_key);
      if (!s.ok()) {
        sst_file_writer.Finish();
        return s;
      }
      auto start_key_it = true_data->find(start_key);
      if (start_key_it == true_data->end()) {
        start_key_it = true_data->upper_bound(start_key);
      }
      auto end_key_it = true_data->find(end_key);
      if (end_key_it == true_data->end()) {
        end_key_it = true_data->upper_bound(end_key);
      }
      true_data->erase(start_key_it, end_key_it);
    }
    for (size_t i = 0; i < keys.size(); i++) {
      std::string key = Key(keys[i]);
      std::string value = Key(keys[i]) + std::to_string(file_id);
      ValueType value_type =
          (value_types.size() == 1 ? value_types[0] : value_types[i]);
      switch (value_type) {
        case ValueType::kTypeValue:
          s = sst_file_writer.Put(key, value);
          (*true_data)[key] = value;
          break;
        case ValueType::kTypeMerge:
          s = sst_file_writer.Merge(key, value);
          // we only use TestPutOperator in this test
          (*true_data)[key] = value;
          break;
        case ValueType::kTypeDeletion:
          s = sst_file_writer.Delete(key);
          true_data->erase(key);
          break;
        default:
          return Status::InvalidArgument("Value type is not supported");
      }
      if (!s.ok()) {
        sst_file_writer.Finish();
        return s;
      }
    }
    s = sst_file_writer.Finish();

    if (s.ok()) {
      IngestExternalFileOptions ifo;
      ifo.allow_global_seqno = true;
      ifo.write_global_seqno = write_global_seqno;
      ifo.verify_checksums_before_ingest = verify_checksums_before_ingest;
      s = db_->IngestExternalFile({file_path}, ifo);
    }
    return s;
  }

  Status GenerateAndAddExternalFile(
      const Options options, std::vector<int> keys,
      const std::vector<ValueType>& value_types, int file_id,
      bool write_global_seqno, bool verify_checksums_before_ingest,
      std::map<std::string, std::string>* true_data) {
    return GenerateAndAddExternalFile(
        options, keys, value_types, {}, file_id, write_global_seqno,
        verify_checksums_before_ingest, true_data);
  }

  Status GenerateAndAddExternalFile(
      const Options options, std::vector<int> keys, const ValueType value_type,
      int file_id, bool write_global_seqno, bool verify_checksums_before_ingest,
      std::map<std::string, std::string>* true_data) {
    return GenerateAndAddExternalFile(
        options, keys, std::vector<ValueType>(1, value_type), file_id,
        write_global_seqno, verify_checksums_before_ingest, true_data);
  }

  ~ExternalSSTFileBasicTest() override {
    DestroyDir(env_, sst_files_dir_).PermitUncheckedError();
  }

 protected:
  std::string sst_files_dir_;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_test_env_;
  bool random_rwfile_supported_;
};

TEST_F(ExternalSSTFileBasicTest, Basic) {
  Options options = CurrentOptions();

  SstFileWriter sst_file_writer(EnvOptions(), options);

  // Current file size should be 0 after sst_file_writer init and before open a
  // file.
  ASSERT_EQ(sst_file_writer.FileSize(), 0);

  // file1.sst (0 => 99)
  std::string file1 = sst_files_dir_ + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_OK(s) << s.ToString();

  // Current file size should be non-zero after success write.
  ASSERT_GT(sst_file_writer.FileSize(), 0);

  ASSERT_EQ(file1_info.file_path, file1);
  ASSERT_EQ(file1_info.num_entries, 100);
  ASSERT_EQ(file1_info.smallest_key, Key(0));
  ASSERT_EQ(file1_info.largest_key, Key(99));
  ASSERT_EQ(file1_info.num_range_del_entries, 0);
  ASSERT_EQ(file1_info.smallest_range_del_key, "");
  ASSERT_EQ(file1_info.largest_range_del_key, "");
  ASSERT_EQ(file1_info.file_checksum, kUnknownFileChecksum);
  ASSERT_EQ(file1_info.file_checksum_func_name, kUnknownFileChecksumFuncName);
  // sst_file_writer already finished, cannot add this value
  s = sst_file_writer.Put(Key(100), "bad_val");
  ASSERT_NOK(s) << s.ToString();
  s = sst_file_writer.DeleteRange(Key(100), Key(200));
  ASSERT_NOK(s) << s.ToString();

  DestroyAndReopen(options);
  // Add file using file path
  s = DeprecatedAddFile({file1});
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
  for (int k = 0; k < 100; k++) {
    ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
  }

  DestroyAndRecreateExternalSSTFilesDir();
}

class ChecksumVerifyHelper {
 private:
  Options options_;

 public:
  ChecksumVerifyHelper(Options& options) : options_(options) {}
  ~ChecksumVerifyHelper() {}

  Status GetSingleFileChecksumAndFuncName(
      const std::string& file_path, std::string* file_checksum,
      std::string* file_checksum_func_name) {
    Status s;
    EnvOptions soptions;
    std::unique_ptr<SequentialFile> file_reader;
    s = options_.env->NewSequentialFile(file_path, &file_reader, soptions);
    if (!s.ok()) {
      return s;
    }
    std::unique_ptr<char[]> scratch(new char[2048]);
    Slice result;
    FileChecksumGenFactory* file_checksum_gen_factory =
        options_.file_checksum_gen_factory.get();
    if (file_checksum_gen_factory == nullptr) {
      *file_checksum = kUnknownFileChecksum;
      *file_checksum_func_name = kUnknownFileChecksumFuncName;
      return Status::OK();
    } else {
      FileChecksumGenContext gen_context;
      std::unique_ptr<FileChecksumGenerator> file_checksum_gen =
          file_checksum_gen_factory->CreateFileChecksumGenerator(gen_context);
      *file_checksum_func_name = file_checksum_gen->Name();
      s = file_reader->Read(2048, &result, scratch.get());
      if (!s.ok()) {
        return s;
      }
      while (result.size() != 0) {
        file_checksum_gen->Update(scratch.get(), result.size());
        s = file_reader->Read(2048, &result, scratch.get());
        if (!s.ok()) {
          return s;
        }
      }
      file_checksum_gen->Finalize();
      *file_checksum = file_checksum_gen->GetChecksum();
    }
    return Status::OK();
  }
};

TEST_F(ExternalSSTFileBasicTest, BasicWithFileChecksumCrc32c) {
  Options options = CurrentOptions();
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  ChecksumVerifyHelper checksum_helper(options);

  SstFileWriter sst_file_writer(EnvOptions(), options);

  // Current file size should be 0 after sst_file_writer init and before open a
  // file.
  ASSERT_EQ(sst_file_writer.FileSize(), 0);

  // file1.sst (0 => 99)
  std::string file1 = sst_files_dir_ + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_OK(s) << s.ToString();
  std::string file_checksum, file_checksum_func_name;
  ASSERT_OK(checksum_helper.GetSingleFileChecksumAndFuncName(
      file1, &file_checksum, &file_checksum_func_name));

  // Current file size should be non-zero after success write.
  ASSERT_GT(sst_file_writer.FileSize(), 0);

  ASSERT_EQ(file1_info.file_path, file1);
  ASSERT_EQ(file1_info.num_entries, 100);
  ASSERT_EQ(file1_info.smallest_key, Key(0));
  ASSERT_EQ(file1_info.largest_key, Key(99));
  ASSERT_EQ(file1_info.num_range_del_entries, 0);
  ASSERT_EQ(file1_info.smallest_range_del_key, "");
  ASSERT_EQ(file1_info.largest_range_del_key, "");
  ASSERT_EQ(file1_info.file_checksum, file_checksum);
  ASSERT_EQ(file1_info.file_checksum_func_name, file_checksum_func_name);
  // sst_file_writer already finished, cannot add this value
  s = sst_file_writer.Put(Key(100), "bad_val");
  ASSERT_NOK(s) << s.ToString();
  s = sst_file_writer.DeleteRange(Key(100), Key(200));
  ASSERT_NOK(s) << s.ToString();

  DestroyAndReopen(options);
  // Add file using file path
  s = DeprecatedAddFile({file1});
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
  for (int k = 0; k < 100; k++) {
    ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
  }

  DestroyAndRecreateExternalSSTFilesDir();
}

TEST_F(ExternalSSTFileBasicTest, IngestFileWithFileChecksum) {
  Options old_options = CurrentOptions();
  Options options = CurrentOptions();
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  const ImmutableCFOptions ioptions(options);
  ChecksumVerifyHelper checksum_helper(options);

  SstFileWriter sst_file_writer(EnvOptions(), options);

  // file01.sst (1000 => 1099)
  std::string file1 = sst_files_dir_ + "file01.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 1000; k < 1100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(file1_info.file_path, file1);
  ASSERT_EQ(file1_info.num_entries, 100);
  ASSERT_EQ(file1_info.smallest_key, Key(1000));
  ASSERT_EQ(file1_info.largest_key, Key(1099));
  std::string file_checksum1, file_checksum_func_name1;
  ASSERT_OK(checksum_helper.GetSingleFileChecksumAndFuncName(
      file1, &file_checksum1, &file_checksum_func_name1));
  ASSERT_EQ(file1_info.file_checksum, file_checksum1);
  ASSERT_EQ(file1_info.file_checksum_func_name, file_checksum_func_name1);

  // file02.sst (1100 => 1299)
  std::string file2 = sst_files_dir_ + "file02.sst";
  ASSERT_OK(sst_file_writer.Open(file2));
  for (int k = 1100; k < 1300; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file2_info;
  s = sst_file_writer.Finish(&file2_info);
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(file2_info.file_path, file2);
  ASSERT_EQ(file2_info.num_entries, 200);
  ASSERT_EQ(file2_info.smallest_key, Key(1100));
  ASSERT_EQ(file2_info.largest_key, Key(1299));
  std::string file_checksum2, file_checksum_func_name2;
  ASSERT_OK(checksum_helper.GetSingleFileChecksumAndFuncName(
      file2, &file_checksum2, &file_checksum_func_name2));
  ASSERT_EQ(file2_info.file_checksum, file_checksum2);
  ASSERT_EQ(file2_info.file_checksum_func_name, file_checksum_func_name2);

  // file03.sst (1300 => 1499)
  std::string file3 = sst_files_dir_ + "file03.sst";
  ASSERT_OK(sst_file_writer.Open(file3));
  for (int k = 1300; k < 1500; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
  }
  ExternalSstFileInfo file3_info;
  s = sst_file_writer.Finish(&file3_info);
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(file3_info.file_path, file3);
  ASSERT_EQ(file3_info.num_entries, 200);
  ASSERT_EQ(file3_info.smallest_key, Key(1300));
  ASSERT_EQ(file3_info.largest_key, Key(1499));
  std::string file_checksum3, file_checksum_func_name3;
  ASSERT_OK(checksum_helper.GetSingleFileChecksumAndFuncName(
      file3, &file_checksum3, &file_checksum_func_name3));
  ASSERT_EQ(file3_info.file_checksum, file_checksum3);
  ASSERT_EQ(file3_info.file_checksum_func_name, file_checksum_func_name3);

  // file04.sst (1500 => 1799)
  std::string file4 = sst_files_dir_ + "file04.sst";
  ASSERT_OK(sst_file_writer.Open(file4));
  for (int k = 1500; k < 1800; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
  }
  ExternalSstFileInfo file4_info;
  s = sst_file_writer.Finish(&file4_info);
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(file4_info.file_path, file4);
  ASSERT_EQ(file4_info.num_entries, 300);
  ASSERT_EQ(file4_info.smallest_key, Key(1500));
  ASSERT_EQ(file4_info.largest_key, Key(1799));
  std::string file_checksum4, file_checksum_func_name4;
  ASSERT_OK(checksum_helper.GetSingleFileChecksumAndFuncName(
      file4, &file_checksum4, &file_checksum_func_name4));
  ASSERT_EQ(file4_info.file_checksum, file_checksum4);
  ASSERT_EQ(file4_info.file_checksum_func_name, file_checksum_func_name4);

  // file05.sst (1800 => 1899)
  std::string file5 = sst_files_dir_ + "file05.sst";
  ASSERT_OK(sst_file_writer.Open(file5));
  for (int k = 1800; k < 2000; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
  }
  ExternalSstFileInfo file5_info;
  s = sst_file_writer.Finish(&file5_info);
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(file5_info.file_path, file5);
  ASSERT_EQ(file5_info.num_entries, 200);
  ASSERT_EQ(file5_info.smallest_key, Key(1800));
  ASSERT_EQ(file5_info.largest_key, Key(1999));
  std::string file_checksum5, file_checksum_func_name5;
  ASSERT_OK(checksum_helper.GetSingleFileChecksumAndFuncName(
      file5, &file_checksum5, &file_checksum_func_name5));
  ASSERT_EQ(file5_info.file_checksum, file_checksum5);
  ASSERT_EQ(file5_info.file_checksum_func_name, file_checksum_func_name5);

  // file06.sst (2000 => 2199)
  std::string file6 = sst_files_dir_ + "file06.sst";
  ASSERT_OK(sst_file_writer.Open(file6));
  for (int k = 2000; k < 2200; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
  }
  ExternalSstFileInfo file6_info;
  s = sst_file_writer.Finish(&file6_info);
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(file6_info.file_path, file6);
  ASSERT_EQ(file6_info.num_entries, 200);
  ASSERT_EQ(file6_info.smallest_key, Key(2000));
  ASSERT_EQ(file6_info.largest_key, Key(2199));
  std::string file_checksum6, file_checksum_func_name6;
  ASSERT_OK(checksum_helper.GetSingleFileChecksumAndFuncName(
      file6, &file_checksum6, &file_checksum_func_name6));
  ASSERT_EQ(file6_info.file_checksum, file_checksum6);
  ASSERT_EQ(file6_info.file_checksum_func_name, file_checksum_func_name6);

  s = AddFileWithFileChecksum({file1}, {file_checksum1, "xyz"},
                              {file_checksum1}, true, false, false, false);
  // does not care the checksum input since db does not enable file checksum
  ASSERT_OK(s) << s.ToString();
  ASSERT_OK(env_->FileExists(file1));
  std::vector<LiveFileMetaData> live_files;
  dbfull()->GetLiveFilesMetaData(&live_files);
  std::set<std::string> set1;
  for (auto f : live_files) {
    set1.insert(f.name);
    ASSERT_EQ(f.file_checksum, kUnknownFileChecksum);
    ASSERT_EQ(f.file_checksum_func_name, kUnknownFileChecksumFuncName);
  }

  // check the temperature of the file being ingested
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(1, metadata.file_count);
  ASSERT_EQ(Temperature::kUnknown, metadata.levels[6].files[0].temperature);
  auto size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_GT(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kHot);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kCold);
  ASSERT_EQ(size, 0);

  // Reopen Db with checksum enabled
  Reopen(options);
  // Enable verify_file_checksum option
  // The checksum vector does not match, fail the ingestion
  s = AddFileWithFileChecksum({file2}, {file_checksum2, "xyz"},
                              {file_checksum_func_name2}, true, false, false,
                              false);
  ASSERT_NOK(s) << s.ToString();

  // Enable verify_file_checksum option
  // The checksum name does not match, fail the ingestion
  s = AddFileWithFileChecksum({file2}, {file_checksum2}, {"xyz"}, true, false,
                              false, false);
  ASSERT_NOK(s) << s.ToString();

  // Enable verify_file_checksum option
  // The checksum itself does not match, fail the ingestion
  s = AddFileWithFileChecksum({file2}, {"xyz"}, {file_checksum_func_name2},
                              true, false, false, false);
  ASSERT_NOK(s) << s.ToString();

  // Enable verify_file_checksum option
  // All matches, ingestion is successful
  s = AddFileWithFileChecksum({file2}, {file_checksum2},
                              {file_checksum_func_name2}, true, false, false,
                              false);
  ASSERT_OK(s) << s.ToString();
  std::vector<LiveFileMetaData> live_files1;
  dbfull()->GetLiveFilesMetaData(&live_files1);
  for (auto f : live_files1) {
    if (set1.find(f.name) == set1.end()) {
      ASSERT_EQ(f.file_checksum, file_checksum2);
      ASSERT_EQ(f.file_checksum_func_name, file_checksum_func_name2);
      set1.insert(f.name);
    }
  }
  ASSERT_OK(env_->FileExists(file2));

  // Enable verify_file_checksum option
  // No checksum information is provided, generate it when ingesting
  std::vector<std::string> checksum, checksum_func;
  s = AddFileWithFileChecksum({file3}, checksum, checksum_func, true, false,
                              false, false);
  ASSERT_OK(s) << s.ToString();
  std::vector<LiveFileMetaData> live_files2;
  dbfull()->GetLiveFilesMetaData(&live_files2);
  for (auto f : live_files2) {
    if (set1.find(f.name) == set1.end()) {
      ASSERT_EQ(f.file_checksum, file_checksum3);
      ASSERT_EQ(f.file_checksum_func_name, file_checksum_func_name3);
      set1.insert(f.name);
    }
  }
  ASSERT_OK(s) << s.ToString();
  ASSERT_OK(env_->FileExists(file3));

  // Does not enable verify_file_checksum options
  // The checksum name does not match, fail the ingestion
  s = AddFileWithFileChecksum({file4}, {file_checksum4}, {"xyz"}, false, false,
                              false, false);
  ASSERT_NOK(s) << s.ToString();

  // Does not enable verify_file_checksum options
  // Checksum function name matches, store the checksum being ingested.
  s = AddFileWithFileChecksum({file4}, {"asd"}, {file_checksum_func_name4},
                              false, false, false, false);
  ASSERT_OK(s) << s.ToString();
  std::vector<LiveFileMetaData> live_files3;
  dbfull()->GetLiveFilesMetaData(&live_files3);
  for (auto f : live_files3) {
    if (set1.find(f.name) == set1.end()) {
      ASSERT_FALSE(f.file_checksum == file_checksum4);
      ASSERT_EQ(f.file_checksum, "asd");
      ASSERT_EQ(f.file_checksum_func_name, file_checksum_func_name4);
      set1.insert(f.name);
    }
  }
  ASSERT_OK(s) << s.ToString();
  ASSERT_OK(env_->FileExists(file4));

  // enable verify_file_checksum options, DB enable checksum, and enable
  // write_global_seq. So the checksum stored is different from the one
  // ingested due to the sequence number changes.
  s = AddFileWithFileChecksum({file5}, {file_checksum5},
                              {file_checksum_func_name5}, true, false, false,
                              true);
  ASSERT_OK(s) << s.ToString();
  std::vector<LiveFileMetaData> live_files4;
  dbfull()->GetLiveFilesMetaData(&live_files4);
  for (auto f : live_files4) {
    if (set1.find(f.name) == set1.end()) {
      std::string cur_checksum5, cur_checksum_func_name5;
      ASSERT_OK(checksum_helper.GetSingleFileChecksumAndFuncName(
          dbname_ + f.name, &cur_checksum5, &cur_checksum_func_name5));
      ASSERT_EQ(f.file_checksum, cur_checksum5);
      ASSERT_EQ(f.file_checksum_func_name, file_checksum_func_name5);
      set1.insert(f.name);
    }
  }
  ASSERT_OK(s) << s.ToString();
  ASSERT_OK(env_->FileExists(file5));

  // Does not enable verify_file_checksum options and also the ingested file
  // checksum information is empty. DB will generate and store the checksum
  // in Manifest.
  std::vector<std::string> files_c6, files_name6;
  s = AddFileWithFileChecksum({file6}, files_c6, files_name6, false, false,
                              false, false);
  ASSERT_OK(s) << s.ToString();
  std::vector<LiveFileMetaData> live_files6;
  dbfull()->GetLiveFilesMetaData(&live_files6);
  for (auto f : live_files6) {
    if (set1.find(f.name) == set1.end()) {
      ASSERT_EQ(f.file_checksum, file_checksum6);
      ASSERT_EQ(f.file_checksum_func_name, file_checksum_func_name6);
      set1.insert(f.name);
    }
  }
  ASSERT_OK(s) << s.ToString();
  ASSERT_OK(env_->FileExists(file6));
  db_->GetColumnFamilyMetaData(&metadata);
  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_GT(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kHot);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kCold);
  ASSERT_EQ(size, 0);
}

TEST_F(ExternalSSTFileBasicTest, NoCopy) {
  Options options = CurrentOptions();
  const ImmutableCFOptions ioptions(options);

  SstFileWriter sst_file_writer(EnvOptions(), options);

  // file1.sst (0 => 99)
  std::string file1 = sst_files_dir_ + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_OK(s) << s.ToString();
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
  s = sst_file_writer.Finish(&file2_info);
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(file2_info.file_path, file2);
  ASSERT_EQ(file2_info.num_entries, 200);
  ASSERT_EQ(file2_info.smallest_key, Key(100));
  ASSERT_EQ(file2_info.largest_key, Key(299));

  // file3.sst (110 => 124) .. overlap with file2.sst
  std::string file3 = sst_files_dir_ + "file3.sst";
  ASSERT_OK(sst_file_writer.Open(file3));
  for (int k = 110; k < 125; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
  }
  ExternalSstFileInfo file3_info;
  s = sst_file_writer.Finish(&file3_info);
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(file3_info.file_path, file3);
  ASSERT_EQ(file3_info.num_entries, 15);
  ASSERT_EQ(file3_info.smallest_key, Key(110));
  ASSERT_EQ(file3_info.largest_key, Key(124));

  s = DeprecatedAddFile({file1}, true /* move file */);
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(Status::NotFound(), env_->FileExists(file1));

  s = DeprecatedAddFile({file2}, false /* copy file */);
  ASSERT_OK(s) << s.ToString();
  ASSERT_OK(env_->FileExists(file2));

  // This file has overlapping values with the existing data
  s = DeprecatedAddFile({file3}, true /* move file */);
  ASSERT_NOK(s) << s.ToString();
  ASSERT_OK(env_->FileExists(file3));

  for (int k = 0; k < 300; k++) {
    ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
  }
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithGlobalSeqnoPickedSeqno) {
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  do {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);
    std::map<std::string, std::string> true_data;

    int file_id = 1;

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 2, 3, 4, 5, 6}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {10, 11, 12, 13}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 4, 6}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {11, 15, 19}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {120, 130}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 130}, ValueType::kTypeValue, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3);

    // Write some keys through normal write path
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(i), "memtable"));
      true_data[Key(i)] = "memtable";
    }
    SequenceNumber last_seqno = dbfull()->GetLatestSequenceNumber();

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {60, 61, 62}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {40, 41, 42}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {20, 30, 40}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 2);

    const Snapshot* snapshot = db_->GetSnapshot();

    // We will need a seqno for the file regardless if the file overwrite
    // keys in the DB or not because we have a snapshot
    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1000, 1002}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {2000, 3002}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 20, 40, 100, 150}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    db_->ReleaseSnapshot(snapshot);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {5000, 5001}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // No snapshot anymore, no need to assign a seqno
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    size_t kcnt = 0;
    VerifyDBFromMap(true_data, &kcnt, false);
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithMultipleValueType) {
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  do {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.merge_operator.reset(new TestPutOperator());
    DestroyAndReopen(options);
    std::map<std::string, std::string> true_data;

    int file_id = 1;

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 2, 3, 4, 5, 6}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {10, 11, 12, 13}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 4, 6}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {11, 15, 19}, ValueType::kTypeDeletion, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {120, 130}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 130}, ValueType::kTypeDeletion, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {120}, {ValueType::kTypeValue}, {{120, 135}}, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {}, {}, {{110, 120}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // The range deletion ends on a key, but it doesn't actually delete
    // this key because the largest key in the range is exclusive. Still,
    // it counts as an overlap so a new seqno will be assigned.
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 5);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {}, {}, {{100, 109}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 5);

    // Write some keys through normal write path
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(i), "memtable"));
      true_data[Key(i)] = "memtable";
    }
    SequenceNumber last_seqno = dbfull()->GetLatestSequenceNumber();

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {60, 61, 62}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {40, 41, 42}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {20, 30, 40}, ValueType::kTypeDeletion, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 2);

    const Snapshot* snapshot = db_->GetSnapshot();

    // We will need a seqno for the file regardless if the file overwrite
    // keys in the DB or not because we have a snapshot
    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1000, 1002}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {2000, 3002}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 20, 40, 100, 150}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    db_->ReleaseSnapshot(snapshot);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {5000, 5001}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // No snapshot anymore, no need to assign a seqno
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    size_t kcnt = 0;
    VerifyDBFromMap(true_data, &kcnt, false);
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithMixedValueType) {
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  do {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.merge_operator.reset(new TestPutOperator());
    DestroyAndReopen(options);
    std::map<std::string, std::string> true_data;

    int file_id = 1;

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 2, 3, 4, 5, 6},
        {ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeValue,
         ValueType::kTypeMerge, ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {10, 11, 12, 13},
        {ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeValue,
         ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 4, 6},
        {ValueType::kTypeDeletion, ValueType::kTypeValue,
         ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {11, 15, 19},
        {ValueType::kTypeDeletion, ValueType::kTypeMerge,
         ValueType::kTypeValue},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {120, 130}, {ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 130}, {ValueType::kTypeMerge, ValueType::kTypeDeletion},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {150, 151, 152},
        {ValueType::kTypeValue, ValueType::kTypeMerge,
         ValueType::kTypeDeletion},
        {{150, 160}, {180, 190}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {150, 151, 152},
        {ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeValue},
        {{200, 250}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {300, 301, 302},
        {ValueType::kTypeValue, ValueType::kTypeMerge,
         ValueType::kTypeDeletion},
        {{1, 2}, {152, 154}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 5);

    // Write some keys through normal write path
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(i), "memtable"));
      true_data[Key(i)] = "memtable";
    }
    SequenceNumber last_seqno = dbfull()->GetLatestSequenceNumber();

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {60, 61, 62},
        {ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeValue},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {40, 41, 42},
        {ValueType::kTypeValue, ValueType::kTypeDeletion,
         ValueType::kTypeDeletion},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {20, 30, 40},
        {ValueType::kTypeDeletion, ValueType::kTypeDeletion,
         ValueType::kTypeDeletion},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 2);

    const Snapshot* snapshot = db_->GetSnapshot();

    // We will need a seqno for the file regardless if the file overwrite
    // keys in the DB or not because we have a snapshot
    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1000, 1002}, {ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {2000, 3002}, {ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 20, 40, 100, 150},
        {ValueType::kTypeDeletion, ValueType::kTypeDeletion,
         ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    db_->ReleaseSnapshot(snapshot);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {5000, 5001}, {ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // No snapshot anymore, no need to assign a seqno
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    size_t kcnt = 0;
    VerifyDBFromMap(true_data, &kcnt, false);
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_F(ExternalSSTFileBasicTest, FadviseTrigger) {
  Options options = CurrentOptions();
  const int kNumKeys = 10000;

  size_t total_fadvised_bytes = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileWriter::Rep::InvalidatePageCache", [&](void* arg) {
        size_t fadvise_size = *(reinterpret_cast<size_t*>(arg));
        total_fadvised_bytes += fadvise_size;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::unique_ptr<SstFileWriter> sst_file_writer;

  std::string sst_file_path = sst_files_dir_ + "file_fadvise_disable.sst";
  sst_file_writer.reset(
      new SstFileWriter(EnvOptions(), options, nullptr, false));
  ASSERT_OK(sst_file_writer->Open(sst_file_path));
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(sst_file_writer->Put(Key(i), Key(i)));
  }
  ASSERT_OK(sst_file_writer->Finish());
  // fadvise disabled
  ASSERT_EQ(total_fadvised_bytes, 0);

  sst_file_path = sst_files_dir_ + "file_fadvise_enable.sst";
  sst_file_writer.reset(
      new SstFileWriter(EnvOptions(), options, nullptr, true));
  ASSERT_OK(sst_file_writer->Open(sst_file_path));
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(sst_file_writer->Put(Key(i), Key(i)));
  }
  ASSERT_OK(sst_file_writer->Finish());
  // fadvise enabled
  ASSERT_EQ(total_fadvised_bytes, sst_file_writer->FileSize());
  ASSERT_GT(total_fadvised_bytes, 0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(ExternalSSTFileBasicTest, SyncFailure) {
  Options options;
  options.create_if_missing = true;
  options.env = fault_injection_test_env_.get();

  std::vector<std::pair<std::string, std::string>> test_cases = {
      {"ExternalSstFileIngestionJob::BeforeSyncIngestedFile",
       "ExternalSstFileIngestionJob::AfterSyncIngestedFile"},
      {"ExternalSstFileIngestionJob::BeforeSyncDir",
       "ExternalSstFileIngestionJob::AfterSyncDir"},
      {"ExternalSstFileIngestionJob::BeforeSyncGlobalSeqno",
       "ExternalSstFileIngestionJob::AfterSyncGlobalSeqno"}};

  for (size_t i = 0; i < test_cases.size(); i++) {
    bool no_sync = false;
    SyncPoint::GetInstance()->SetCallBack(test_cases[i].first, [&](void*) {
      fault_injection_test_env_->SetFilesystemActive(false);
    });
    SyncPoint::GetInstance()->SetCallBack(test_cases[i].second, [&](void*) {
      fault_injection_test_env_->SetFilesystemActive(true);
    });
    if (i == 0) {
      SyncPoint::GetInstance()->SetCallBack(
          "ExternalSstFileIngestionJob::Prepare:Reopen", [&](void* s) {
            Status* status = static_cast<Status*>(s);
            if (status->IsNotSupported()) {
              no_sync = true;
            }
          });
    }
    if (i == 2) {
      SyncPoint::GetInstance()->SetCallBack(
          "ExternalSstFileIngestionJob::NewRandomRWFile", [&](void* s) {
            Status* status = static_cast<Status*>(s);
            if (status->IsNotSupported()) {
              no_sync = true;
            }
          });
    }
    SyncPoint::GetInstance()->EnableProcessing();

    DestroyAndReopen(options);
    if (i == 2) {
      ASSERT_OK(Put("foo", "v1"));
    }

    Options sst_file_writer_options;
    sst_file_writer_options.env = fault_injection_test_env_.get();
    std::unique_ptr<SstFileWriter> sst_file_writer(
        new SstFileWriter(EnvOptions(), sst_file_writer_options));
    std::string file_name =
        sst_files_dir_ + "sync_failure_test_" + std::to_string(i) + ".sst";
    ASSERT_OK(sst_file_writer->Open(file_name));
    ASSERT_OK(sst_file_writer->Put("bar", "v2"));
    ASSERT_OK(sst_file_writer->Finish());

    IngestExternalFileOptions ingest_opt;
    if (i == 0) {
      ingest_opt.move_files = true;
    }
    const Snapshot* snapshot = db_->GetSnapshot();
    if (i == 2) {
      ingest_opt.write_global_seqno = true;
    }
    Status s = db_->IngestExternalFile({file_name}, ingest_opt);
    if (no_sync) {
      ASSERT_OK(s);
    } else {
      ASSERT_NOK(s);
    }
    db_->ReleaseSnapshot(snapshot);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    Destroy(options);
  }
}

TEST_F(ExternalSSTFileBasicTest, ReopenNotSupported) {
  Options options;
  options.create_if_missing = true;
  options.env = env_;

  SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::Prepare:Reopen", [&](void* arg) {
        Status* s = static_cast<Status*>(arg);
        *s = Status::NotSupported();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  DestroyAndReopen(options);

  Options sst_file_writer_options;
  sst_file_writer_options.env = env_;
  std::unique_ptr<SstFileWriter> sst_file_writer(
      new SstFileWriter(EnvOptions(), sst_file_writer_options));
  std::string file_name =
      sst_files_dir_ + "reopen_not_supported_test_" + ".sst";
  ASSERT_OK(sst_file_writer->Open(file_name));
  ASSERT_OK(sst_file_writer->Put("bar", "v2"));
  ASSERT_OK(sst_file_writer->Finish());

  IngestExternalFileOptions ingest_opt;
  ingest_opt.move_files = true;
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->IngestExternalFile({file_name}, ingest_opt));
  db_->ReleaseSnapshot(snapshot);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Destroy(options);
}

TEST_F(ExternalSSTFileBasicTest, VerifyChecksumReadahead) {
  Options options;
  options.create_if_missing = true;
  SpecialEnv senv(env_);
  options.env = &senv;
  DestroyAndReopen(options);

  Options sst_file_writer_options;
  sst_file_writer_options.env = env_;
  std::unique_ptr<SstFileWriter> sst_file_writer(
      new SstFileWriter(EnvOptions(), sst_file_writer_options));
  std::string file_name = sst_files_dir_ + "verify_checksum_readahead_test.sst";
  ASSERT_OK(sst_file_writer->Open(file_name));
  Random rnd(301);
  std::string value = rnd.RandomString(4000);
  for (int i = 0; i < 5000; i++) {
    ASSERT_OK(sst_file_writer->Put(DBTestBase::Key(i), value));
  }
  ASSERT_OK(sst_file_writer->Finish());

  // Ingest it once without verifying checksums to see the baseline
  // preads.
  IngestExternalFileOptions ingest_opt;
  ingest_opt.move_files = false;
  senv.count_random_reads_ = true;
  senv.random_read_bytes_counter_ = 0;
  ASSERT_OK(db_->IngestExternalFile({file_name}, ingest_opt));

  auto base_num_reads = senv.random_read_counter_.Read();
  // Make sure the counter is enabled.
  ASSERT_GT(base_num_reads, 0);

  // Ingest again and observe the reads made for for readahead.
  ingest_opt.move_files = false;
  ingest_opt.verify_checksums_before_ingest = true;
  ingest_opt.verify_checksums_readahead_size = size_t{2 * 1024 * 1024};

  senv.count_random_reads_ = true;
  senv.random_read_bytes_counter_ = 0;
  ASSERT_OK(db_->IngestExternalFile({file_name}, ingest_opt));

  // Make sure the counter is enabled.
  ASSERT_GT(senv.random_read_counter_.Read() - base_num_reads, 0);

  // The SST file is about 20MB. Readahead size is 2MB.
  // Give a conservative 15 reads for metadata blocks, the number
  // of random reads should be within 20 MB / 2MB + 15 = 25.
  ASSERT_LE(senv.random_read_counter_.Read() - base_num_reads, 40);

  Destroy(options);
}

TEST_F(ExternalSSTFileBasicTest, IngestRangeDeletionTombstoneWithGlobalSeqno) {
  for (int i = 5; i < 25; i++) {
    ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), Key(i),
                       Key(i) + "_val"));
  }

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);
  SstFileWriter sst_file_writer(EnvOptions(), options);

  // file.sst (delete 0 => 30)
  std::string file = sst_files_dir_ + "file.sst";
  ASSERT_OK(sst_file_writer.Open(file));
  ASSERT_OK(sst_file_writer.DeleteRange(Key(0), Key(30)));
  ExternalSstFileInfo file_info;
  ASSERT_OK(sst_file_writer.Finish(&file_info));
  ASSERT_EQ(file_info.file_path, file);
  ASSERT_EQ(file_info.num_entries, 0);
  ASSERT_EQ(file_info.smallest_key, "");
  ASSERT_EQ(file_info.largest_key, "");
  ASSERT_EQ(file_info.num_range_del_entries, 1);
  ASSERT_EQ(file_info.smallest_range_del_key, Key(0));
  ASSERT_EQ(file_info.largest_range_del_key, Key(30));

  IngestExternalFileOptions ifo;
  ifo.move_files = true;
  ifo.snapshot_consistency = true;
  ifo.allow_global_seqno = true;
  ifo.write_global_seqno = true;
  ifo.verify_checksums_before_ingest = false;
  ASSERT_OK(db_->IngestExternalFile({file}, ifo));

  for (int i = 5; i < 25; i++) {
    std::string res;
    ASSERT_TRUE(db_->Get(ReadOptions(), Key(i), &res).IsNotFound());
  }
}

TEST_P(ExternalSSTFileBasicTest, IngestionWithRangeDeletions) {
  int kNumLevels = 7;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = kNumLevels;
  Reopen(options);

  std::map<std::string, std::string> true_data;
  int file_id = 1;
  // prevent range deletions from being dropped due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();

  // range del [0, 50) in L6 file, [50, 100) in L0 file, [100, 150) in memtable
  for (int i = 0; i < 3; i++) {
    if (i != 0) {
      db_->Flush(FlushOptions());
      if (i == 1) {
        MoveFilesToLevel(kNumLevels - 1);
      }
    }
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(50 * i), Key(50 * (i + 1))));
  }
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 1));

  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  // overlaps with L0 file but not memtable, so flush is skipped and file is
  // ingested into L0
  SequenceNumber last_seqno = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {60, 90}, {ValueType::kTypeValue, ValueType::kTypeValue},
      {{65, 70}, {70, 85}}, file_id++, write_global_seqno,
      verify_checksums_before_ingest, &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), ++last_seqno);
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 1));

  // overlaps with L6 file but not memtable or L0 file, so flush is skipped and
  // file is ingested into L5
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {10, 40}, {ValueType::kTypeValue, ValueType::kTypeValue},
      file_id++, write_global_seqno, verify_checksums_before_ingest,
      &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), ++last_seqno);
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 1));

  // overlaps with L5 file but not memtable or L0 file, so flush is skipped and
  // file is ingested into L4
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {}, {}, {{5, 15}}, file_id++, write_global_seqno,
      verify_checksums_before_ingest, &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), ++last_seqno);
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 1));

  // ingested file overlaps with memtable, so flush is triggered before the file
  // is ingested such that the ingested data is considered newest. So L0 file
  // count increases by two.
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {100, 140}, {ValueType::kTypeValue, ValueType::kTypeValue},
      file_id++, write_global_seqno, verify_checksums_before_ingest,
      &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), ++last_seqno);
  ASSERT_EQ(4, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 1));

  // snapshot unneeded now that all range deletions are persisted
  db_->ReleaseSnapshot(snapshot);

  // overlaps with nothing, so places at bottom level and skips incrementing
  // seqnum.
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {151, 175}, {ValueType::kTypeValue, ValueType::kTypeValue},
      {{160, 200}}, file_id++, write_global_seqno,
      verify_checksums_before_ingest, &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno);
  ASSERT_EQ(4, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(2, NumTableFilesAtLevel(options.num_levels - 1));
}

TEST_F(ExternalSSTFileBasicTest, AdjacentRangeDeletionTombstones) {
  Options options = CurrentOptions();
  SstFileWriter sst_file_writer(EnvOptions(), options);

  // file8.sst (delete 300 => 400)
  std::string file8 = sst_files_dir_ + "file8.sst";
  ASSERT_OK(sst_file_writer.Open(file8));
  ASSERT_OK(sst_file_writer.DeleteRange(Key(300), Key(400)));
  ExternalSstFileInfo file8_info;
  Status s = sst_file_writer.Finish(&file8_info);
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(file8_info.file_path, file8);
  ASSERT_EQ(file8_info.num_entries, 0);
  ASSERT_EQ(file8_info.smallest_key, "");
  ASSERT_EQ(file8_info.largest_key, "");
  ASSERT_EQ(file8_info.num_range_del_entries, 1);
  ASSERT_EQ(file8_info.smallest_range_del_key, Key(300));
  ASSERT_EQ(file8_info.largest_range_del_key, Key(400));

  // file9.sst (delete 400 => 500)
  std::string file9 = sst_files_dir_ + "file9.sst";
  ASSERT_OK(sst_file_writer.Open(file9));
  ASSERT_OK(sst_file_writer.DeleteRange(Key(400), Key(500)));
  ExternalSstFileInfo file9_info;
  s = sst_file_writer.Finish(&file9_info);
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(file9_info.file_path, file9);
  ASSERT_EQ(file9_info.num_entries, 0);
  ASSERT_EQ(file9_info.smallest_key, "");
  ASSERT_EQ(file9_info.largest_key, "");
  ASSERT_EQ(file9_info.num_range_del_entries, 1);
  ASSERT_EQ(file9_info.smallest_range_del_key, Key(400));
  ASSERT_EQ(file9_info.largest_range_del_key, Key(500));

  // Range deletion tombstones are exclusive on their end key, so these SSTs
  // should not be considered as overlapping.
  s = DeprecatedAddFile({file8, file9});
  ASSERT_OK(s) << s.ToString();
  ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
  DestroyAndRecreateExternalSSTFilesDir();
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithBadBlockChecksum) {
  bool change_checksum_called = false;
  const auto& change_checksum = [&](void* arg) {
    if (!change_checksum_called) {
      char* buf = reinterpret_cast<char*>(arg);
      assert(nullptr != buf);
      buf[0] ^= 0x1;
      change_checksum_called = true;
    }
  };
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WriteMaybeCompressedBlock:TamperWithChecksum",
      change_checksum);
  SyncPoint::GetInstance()->EnableProcessing();
  int file_id = 0;
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  do {
    Options options = CurrentOptions();
    DestroyAndReopen(options);
    std::map<std::string, std::string> true_data;
    Status s = GenerateAndAddExternalFile(
        options, {1, 2, 3, 4, 5, 6}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data);
    if (verify_checksums_before_ingest) {
      ASSERT_NOK(s);
    } else {
      ASSERT_OK(s);
    }
    change_checksum_called = false;
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithFirstByteTampered) {
  if (!random_rwfile_supported_) {
    ROCKSDB_GTEST_SKIP("Test requires NewRandomRWFile support");
    return;
  }
  SyncPoint::GetInstance()->DisableProcessing();
  int file_id = 0;
  EnvOptions env_options;
  do {
    Options options = CurrentOptions();
    std::string file_path = sst_files_dir_ + std::to_string(file_id++);
    SstFileWriter sst_file_writer(env_options, options);
    Status s = sst_file_writer.Open(file_path);
    ASSERT_OK(s);
    for (int i = 0; i != 100; ++i) {
      std::string key = Key(i);
      std::string value = Key(i) + std::to_string(0);
      ASSERT_OK(sst_file_writer.Put(key, value));
    }
    ASSERT_OK(sst_file_writer.Finish());
    {
      // Get file size
      uint64_t file_size = 0;
      ASSERT_OK(env_->GetFileSize(file_path, &file_size));
      ASSERT_GT(file_size, 8);
      std::unique_ptr<RandomRWFile> rwfile;
      ASSERT_OK(env_->NewRandomRWFile(file_path, &rwfile, EnvOptions()));
      // Manually corrupt the file
      // We deterministically corrupt the first byte because we currently
      // cannot choose a random offset. The reason for this limitation is that
      // we do not checksum property block at present.
      const uint64_t offset = 0;
      char scratch[8] = {0};
      Slice buf;
      ASSERT_OK(rwfile->Read(offset, sizeof(scratch), &buf, scratch));
      scratch[0] ^= 0xff;  // flip one bit
      ASSERT_OK(rwfile->Write(offset, buf));
    }
    // Ingest file.
    IngestExternalFileOptions ifo;
    ifo.write_global_seqno = std::get<0>(GetParam());
    ifo.verify_checksums_before_ingest = std::get<1>(GetParam());
    s = db_->IngestExternalFile({file_path}, ifo);
    if (ifo.verify_checksums_before_ingest) {
      ASSERT_NOK(s);
    } else {
      ASSERT_OK(s);
    }
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_P(ExternalSSTFileBasicTest, IngestExternalFileWithCorruptedPropsBlock) {
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  if (!verify_checksums_before_ingest) {
    ROCKSDB_GTEST_BYPASS("Bypassing test when !verify_checksums_before_ingest");
    return;
  }
  if (!random_rwfile_supported_) {
    ROCKSDB_GTEST_SKIP("Test requires NewRandomRWFile support");
    return;
  }
  uint64_t props_block_offset = 0;
  size_t props_block_size = 0;
  const auto& get_props_block_offset = [&](void* arg) {
    props_block_offset = *reinterpret_cast<uint64_t*>(arg);
  };
  const auto& get_props_block_size = [&](void* arg) {
    props_block_size = *reinterpret_cast<uint64_t*>(arg);
  };
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockOffset",
      get_props_block_offset);
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockSize",
      get_props_block_size);
  SyncPoint::GetInstance()->EnableProcessing();
  int file_id = 0;
  Random64 rand(time(nullptr));
  do {
    std::string file_path = sst_files_dir_ + std::to_string(file_id++);
    Options options = CurrentOptions();
    SstFileWriter sst_file_writer(EnvOptions(), options);
    Status s = sst_file_writer.Open(file_path);
    ASSERT_OK(s);
    for (int i = 0; i != 100; ++i) {
      std::string key = Key(i);
      std::string value = Key(i) + std::to_string(0);
      ASSERT_OK(sst_file_writer.Put(key, value));
    }
    ASSERT_OK(sst_file_writer.Finish());

    {
      std::unique_ptr<RandomRWFile> rwfile;
      ASSERT_OK(env_->NewRandomRWFile(file_path, &rwfile, EnvOptions()));
      // Manually corrupt the file
      ASSERT_GT(props_block_size, 8);
      uint64_t offset =
          props_block_offset + rand.Next() % (props_block_size - 8);
      char scratch[8] = {0};
      Slice buf;
      ASSERT_OK(rwfile->Read(offset, sizeof(scratch), &buf, scratch));
      scratch[0] ^= 0xff;  // flip one bit
      ASSERT_OK(rwfile->Write(offset, buf));
    }

    // Ingest file.
    IngestExternalFileOptions ifo;
    ifo.write_global_seqno = std::get<0>(GetParam());
    ifo.verify_checksums_before_ingest = true;
    s = db_->IngestExternalFile({file_path}, ifo);
    ASSERT_NOK(s);
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_F(ExternalSSTFileBasicTest, OverlappingFiles) {
  Options options = CurrentOptions();

  std::vector<std::string> files;
  {
    SstFileWriter sst_file_writer(EnvOptions(), options);
    std::string file1 = sst_files_dir_ + "file1.sst";
    ASSERT_OK(sst_file_writer.Open(file1));
    ASSERT_OK(sst_file_writer.Put("a", "z"));
    ASSERT_OK(sst_file_writer.Put("i", "m"));
    ExternalSstFileInfo file1_info;
    ASSERT_OK(sst_file_writer.Finish(&file1_info));
    files.push_back(std::move(file1));
  }
  {
    SstFileWriter sst_file_writer(EnvOptions(), options);
    std::string file2 = sst_files_dir_ + "file2.sst";
    ASSERT_OK(sst_file_writer.Open(file2));
    ASSERT_OK(sst_file_writer.Put("i", "k"));
    ExternalSstFileInfo file2_info;
    ASSERT_OK(sst_file_writer.Finish(&file2_info));
    files.push_back(std::move(file2));
  }

  IngestExternalFileOptions ifo;
  ASSERT_OK(db_->IngestExternalFile(files, ifo));
  ASSERT_EQ(Get("a"), "z");
  ASSERT_EQ(Get("i"), "k");

  int total_keys = 0;
  Iterator* iter = db_->NewIterator(ReadOptions());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    total_keys++;
  }
  delete iter;
  ASSERT_EQ(total_keys, 2);

  ASSERT_EQ(2, NumTableFilesAtLevel(0));
}

TEST_F(ExternalSSTFileBasicTest, IngestFileAfterDBPut) {
  // Repro https://github.com/facebook/rocksdb/issues/6245.
  // Flush three files to L0. Ingest one more file to trigger L0->L1 compaction
  // via trivial move. The bug happened when L1 files were incorrectly sorted
  // resulting in an old value for "k" returned by `Get()`.
  Options options = CurrentOptions();

  ASSERT_OK(Put("k", "a"));
  Flush();
  ASSERT_OK(Put("k", "a"));
  Flush();
  ASSERT_OK(Put("k", "a"));
  Flush();
  SstFileWriter sst_file_writer(EnvOptions(), options);

  // Current file size should be 0 after sst_file_writer init and before open a
  // file.
  ASSERT_EQ(sst_file_writer.FileSize(), 0);

  std::string file1 = sst_files_dir_ + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  ASSERT_OK(sst_file_writer.Put("k", "b"));

  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_OK(s) << s.ToString();

  // Current file size should be non-zero after success write.
  ASSERT_GT(sst_file_writer.FileSize(), 0);

  IngestExternalFileOptions ifo;
  s = db_->IngestExternalFile({file1}, ifo);
  ASSERT_OK(s);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(Get("k"), "b");
}

TEST_F(ExternalSSTFileBasicTest, IngestWithTemperature) {
  Options options = CurrentOptions();
  const ImmutableCFOptions ioptions(options);
  options.bottommost_temperature = Temperature::kWarm;
  SstFileWriter sst_file_writer(EnvOptions(), options);
  options.level0_file_num_compaction_trigger = 2;
  Reopen(options);

  auto size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kHot);
  ASSERT_EQ(size, 0);

  // create file01.sst (1000 => 1099) and ingest it
  std::string file1 = sst_files_dir_ + "file01.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 1000; k < 1100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_OK(s);
  ASSERT_EQ(file1_info.file_path, file1);
  ASSERT_EQ(file1_info.num_entries, 100);
  ASSERT_EQ(file1_info.smallest_key, Key(1000));
  ASSERT_EQ(file1_info.largest_key, Key(1099));

  std::vector<std::string> files;
  std::vector<std::string> files_checksums;
  std::vector<std::string> files_checksum_func_names;
  Temperature file_temperature = Temperature::kWarm;

  files.push_back(file1);
  IngestExternalFileOptions in_opts;
  in_opts.move_files = false;
  in_opts.snapshot_consistency = true;
  in_opts.allow_global_seqno = false;
  in_opts.allow_blocking_flush = false;
  in_opts.write_global_seqno = true;
  in_opts.verify_file_checksum = false;
  IngestExternalFileArg arg;
  arg.column_family = db_->DefaultColumnFamily();
  arg.external_files = files;
  arg.options = in_opts;
  arg.files_checksums = files_checksums;
  arg.files_checksum_func_names = files_checksum_func_names;
  arg.file_temperature = file_temperature;
  s = db_->IngestExternalFiles({arg});
  ASSERT_OK(s);

  // check the temperature of the file being ingested
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(1, metadata.file_count);
  ASSERT_EQ(Temperature::kWarm, metadata.levels[6].files[0].temperature);
  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_GT(size, 1);

  // non-bottommost file still has unknown temperature
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("bar", "bar"));
  ASSERT_OK(Flush());
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(2, metadata.file_count);
  ASSERT_EQ(Temperature::kUnknown, metadata.levels[0].files[0].temperature);
  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_GT(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_GT(size, 0);

  // reopen and check the information is persisted
  Reopen(options);
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(2, metadata.file_count);
  ASSERT_EQ(Temperature::kUnknown, metadata.levels[0].files[0].temperature);
  ASSERT_EQ(Temperature::kWarm, metadata.levels[6].files[0].temperature);
  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_GT(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_GT(size, 0);

  // check other non-exist temperatures
  size = GetSstSizeHelper(Temperature::kHot);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kCold);
  ASSERT_EQ(size, 0);
  std::string prop;
  ASSERT_TRUE(dbfull()->GetProperty(
      DB::Properties::kLiveSstFilesSizeAtTemperature + std::to_string(22),
      &prop));
  ASSERT_EQ(std::atoi(prop.c_str()), 0);
}

TEST_F(ExternalSSTFileBasicTest, FailIfNotBottommostLevel) {
  Options options = GetDefaultOptions();

  std::string file_path = sst_files_dir_ + std::to_string(1);
  SstFileWriter sfw(EnvOptions(), options);

  ASSERT_OK(sfw.Open(file_path));
  ASSERT_OK(sfw.Put("b", "dontcare"));
  ASSERT_OK(sfw.Finish());

  // Test universal compaction + ingest with snapshot consistency
  options.create_if_missing = true;
  options.compaction_style = CompactionStyle::kCompactionStyleUniversal;
  DestroyAndReopen(options);
  {
    const Snapshot* snapshot = db_->GetSnapshot();
    ManagedSnapshot snapshot_guard(db_, snapshot);
    IngestExternalFileOptions ifo;
    ifo.fail_if_not_bottommost_level = true;
    ifo.snapshot_consistency = true;
    const Status s = db_->IngestExternalFile({file_path}, ifo);
    ASSERT_TRUE(s.IsTryAgain());
  }

  // Test level compaction
  options.compaction_style = CompactionStyle::kCompactionStyleLevel;
  options.num_levels = 2;
  DestroyAndReopen(options);
  ASSERT_OK(db_->Put(WriteOptions(), "a", "dontcare"));
  ASSERT_OK(db_->Put(WriteOptions(), "c", "dontcare"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_OK(db_->Put(WriteOptions(), "b", "dontcare"));
  ASSERT_OK(db_->Put(WriteOptions(), "d", "dontcare"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  {
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

    IngestExternalFileOptions ifo;
    ifo.fail_if_not_bottommost_level = true;
    const Status s = db_->IngestExternalFile({file_path}, ifo);
    ASSERT_TRUE(s.IsTryAgain());
  }
}

TEST_F(ExternalSSTFileBasicTest, VerifyChecksum) {
  const std::string kPutVal = "put_val";
  const std::string kIngestedVal = "ingested_val";

  ASSERT_OK(Put("k", kPutVal, WriteOptions()));
  ASSERT_OK(Flush());

  std::string external_file = sst_files_dir_ + "/file_to_ingest.sst";
  {
    SstFileWriter sst_file_writer{EnvOptions(), CurrentOptions()};

    ASSERT_OK(sst_file_writer.Open(external_file));
    ASSERT_OK(sst_file_writer.Put("k", kIngestedVal));
    ASSERT_OK(sst_file_writer.Finish());
  }

  ASSERT_OK(db_->IngestExternalFile(db_->DefaultColumnFamily(), {external_file},
                                    IngestExternalFileOptions()));

  ASSERT_OK(db_->VerifyChecksum());
}

TEST_F(ExternalSSTFileBasicTest, VerifySstUniqueId) {
  const std::string kPutVal = "put_val";
  const std::string kIngestedVal = "ingested_val";

  ASSERT_OK(Put("k", kPutVal, WriteOptions()));
  ASSERT_OK(Flush());

  std::string external_file = sst_files_dir_ + "/file_to_ingest.sst";
  {
    SstFileWriter sst_file_writer{EnvOptions(), CurrentOptions()};

    ASSERT_OK(sst_file_writer.Open(external_file));
    ASSERT_OK(sst_file_writer.Put("k", kIngestedVal));
    ASSERT_OK(sst_file_writer.Finish());
  }

  ASSERT_OK(db_->IngestExternalFile(db_->DefaultColumnFamily(), {external_file},
                                    IngestExternalFileOptions()));

  // Test ingest file without session_id and db_id (for example generated by an
  // older version of sst_writer)
  SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:Start", [&](void* props_vs) {
        auto props = static_cast<TableProperties*>(props_vs);
        // update table property session_id to a different one
        props->db_session_id = "";
        props->db_id = "";
      });
  std::atomic_int skipped = 0, passed = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::Open::SkippedVerifyUniqueId",
      [&](void* /*arg*/) { skipped++; });
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::Open::PassedVerifyUniqueId",
      [&](void* /*arg*/) { passed++; });
  SyncPoint::GetInstance()->EnableProcessing();

  auto options = CurrentOptions();
  ASSERT_TRUE(options.verify_sst_unique_id_in_manifest);
  Reopen(options);
  ASSERT_EQ(skipped, 0);
  ASSERT_EQ(passed, 2);  // one flushed + one ingested

  external_file = sst_files_dir_ + "/file_to_ingest2.sst";
  {
    SstFileWriter sst_file_writer{EnvOptions(), CurrentOptions()};

    ASSERT_OK(sst_file_writer.Open(external_file));
    ASSERT_OK(sst_file_writer.Put("k", kIngestedVal));
    ASSERT_OK(sst_file_writer.Finish());
  }

  ASSERT_OK(db_->IngestExternalFile(db_->DefaultColumnFamily(), {external_file},
                                    IngestExternalFileOptions()));

  // Two table file opens skipping verification:
  // * ExternalSstFileIngestionJob::GetIngestedFileInfo
  // * TableCache::GetTableReader
  ASSERT_EQ(skipped, 2);
  ASSERT_EQ(passed, 2);

  // Check same after re-open (except no GetIngestedFileInfo)
  skipped = 0;
  passed = 0;
  Reopen(options);
  ASSERT_EQ(skipped, 1);
  ASSERT_EQ(passed, 2);
}

TEST_F(ExternalSSTFileBasicTest, StableSnapshotWhileLoggingToManifest) {
  const std::string kPutVal = "put_val";
  const std::string kIngestedVal = "ingested_val";

  ASSERT_OK(Put("k", kPutVal, WriteOptions()));
  ASSERT_OK(Flush());

  std::string external_file = sst_files_dir_ + "/file_to_ingest.sst";
  {
    SstFileWriter sst_file_writer{EnvOptions(), CurrentOptions()};
    ASSERT_OK(sst_file_writer.Open(external_file));
    ASSERT_OK(sst_file_writer.Put("k", kIngestedVal));
    ASSERT_OK(sst_file_writer.Finish());
  }

  const Snapshot* snapshot = nullptr;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void* /* arg */) {
        // prevent background compaction job to call this callback
        ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
        snapshot = db_->GetSnapshot();
        ReadOptions read_opts;
        read_opts.snapshot = snapshot;
        std::string value;
        ASSERT_OK(db_->Get(read_opts, "k", &value));
        ASSERT_EQ(kPutVal, value);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->IngestExternalFile(db_->DefaultColumnFamily(), {external_file},
                                    IngestExternalFileOptions()));
  auto ingested_file_seqno = db_->GetLatestSequenceNumber();
  ASSERT_NE(nullptr, snapshot);
  // snapshot is taken before SST ingestion is done
  ASSERT_EQ(ingested_file_seqno, snapshot->GetSequenceNumber() + 1);

  ReadOptions read_opts;
  read_opts.snapshot = snapshot;
  std::string value;
  ASSERT_OK(db_->Get(read_opts, "k", &value));
  ASSERT_EQ(kPutVal, value);
  db_->ReleaseSnapshot(snapshot);

  // After reopen, sequence number should be up current such that
  // ingested value is read
  Reopen(CurrentOptions());
  ASSERT_OK(db_->Get(ReadOptions(), "k", &value));
  ASSERT_EQ(kIngestedVal, value);

  // New write should get higher seqno compared to ingested file
  ASSERT_OK(Put("k", kPutVal, WriteOptions()));
  ASSERT_EQ(db_->GetLatestSequenceNumber(), ingested_file_seqno + 1);
}

INSTANTIATE_TEST_CASE_P(ExternalSSTFileBasicTest, ExternalSSTFileBasicTest,
                        testing::Values(std::make_tuple(true, true),
                                        std::make_tuple(true, false),
                                        std::make_tuple(false, true),
                                        std::make_tuple(false, false)));

#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
