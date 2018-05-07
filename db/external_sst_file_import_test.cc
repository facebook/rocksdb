//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include <functional>
#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_writer.h"
#include "util/testutil.h"

namespace rocksdb {

class ExternalSSTFileTest : public DBTestBase {
 public:
  ExternalSSTFileTest() : DBTestBase("/external_sst_file_test") {
    sst_files_dir_ = dbname_ + "/sst_files/";
    DestroyAndRecreateExternalSSTFilesDir();
  }

  void DestroyAndRecreateExternalSSTFilesDir() {
    test::DestroyDir(env_, sst_files_dir_);
    env_->CreateDir(sst_files_dir_);
  }

  void LimitOptions(Options& options, int num_levels = -1) {
    if (options.num_levels < num_levels) {
      options.num_levels = num_levels;
    }
  }

  Status DeprecatedAddFile(
      const std::vector<LiveFileMetaData>* import_files_metadata,
      bool move_files = false) {
    ImportExternalFileOptions opts;
    opts.move_files = move_files;
    return db_->ImportExternalFile(*import_files_metadata, opts);
  }

  Status DeprecatedAddFile(
      ColumnFamilyHandle* column_family,
      const std::vector<LiveFileMetaData>* import_files_metadata,
      bool move_files = false) {
    ImportExternalFileOptions opts;
    opts.move_files = move_files;
    return db_->ImportExternalFile(column_family, *import_files_metadata,
                                   opts);
  }

  LiveFileMetaData LiveFileMetaDataInit(std::string name,
                                        std::string path,
                                        int level,
                                        SequenceNumber smallest_seqno,
                                        SequenceNumber largest_seqno) {
    LiveFileMetaData metadata;
    metadata.name = name;
    metadata.db_path = path;
    metadata.smallest_seqno = smallest_seqno;
    metadata.largest_seqno = largest_seqno;
    metadata.level = level;
    return metadata;
  }

  ~ExternalSSTFileTest() { test::DestroyDir(env_, sst_files_dir_); }

 protected:
  int last_file_id_ = 0;
  std::string sst_files_dir_;
};

TEST_F(ExternalSSTFileTest, Basic) {
  do {
    Options options = CurrentOptions();
    // Test cases below assume atleast 2 levels in the DB
    if (options.num_levels < 2) {
      options.num_levels = 2;
    }

    SstFileWriter sst_file_writer(EnvOptions(), options);

    // Current file size should be 0 after sst_file_writer init and before open
    // a file
    ASSERT_EQ(sst_file_writer.FileSize(), 0);

    // file1.sst (0 => 99)
    std::string file1_name = "file1.sst";
    std::string file1 = sst_files_dir_ + file1_name;
    ASSERT_OK(sst_file_writer.Open(file1));
    for (int k = 0; k < 100; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
    }
    ExternalSstFileInfo file1_info;
    Status s = sst_file_writer.Finish(&file1_info);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // Current file size should be non-zero after success write
    ASSERT_GT(sst_file_writer.FileSize(), 0);

    ASSERT_EQ(file1_info.file_path, file1);
    ASSERT_EQ(file1_info.num_entries, 100);
    ASSERT_EQ(file1_info.smallest_key, Key(0));
    ASSERT_EQ(file1_info.largest_key, Key(99));

    // file2.sst (100 => 199)
    std::string file2_name = "file2.sst";
    std::string file2 = sst_files_dir_ + file2_name;
    ASSERT_OK(sst_file_writer.Open(file2));
    for (int k = 100; k < 200; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
    }
    ExternalSstFileInfo file2_info;
    s = sst_file_writer.Finish(&file2_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(file2_info.file_path, file2);
    ASSERT_EQ(file2_info.num_entries, 100);
    ASSERT_EQ(file2_info.smallest_key, Key(100));
    ASSERT_EQ(file2_info.largest_key, Key(199));

    // file3.sst (195 => 299)
    // This file values overlap with file2 values
    std::string file3_name = "file3.sst";
    std::string file3 = sst_files_dir_ + file3_name;
    ASSERT_OK(sst_file_writer.Open(file3));
    for (int k = 195; k < 300; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
    }
    ExternalSstFileInfo file3_info;
    s = sst_file_writer.Finish(&file3_info);

    ASSERT_TRUE(s.ok()) << s.ToString();
    // Current file size should be non-zero after success finish
    ASSERT_GT(sst_file_writer.FileSize(), 0);
    ASSERT_EQ(file3_info.file_path, file3);
    ASSERT_EQ(file3_info.num_entries, 105);
    ASSERT_EQ(file3_info.smallest_key, Key(195));
    ASSERT_EQ(file3_info.largest_key, Key(299));

    // file4.sst (30 => 39)
    // This file values overlap with file1 values
    std::string file4_name = "file4.sst";
    std::string file4 = sst_files_dir_ + file4_name;
    ASSERT_OK(sst_file_writer.Open(file4));
    for (int k = 30; k < 40; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
    }
    ExternalSstFileInfo file4_info;
    s = sst_file_writer.Finish(&file4_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(file4_info.file_path, file4);
    ASSERT_EQ(file4_info.num_entries, 10);
    ASSERT_EQ(file4_info.smallest_key, Key(30));
    ASSERT_EQ(file4_info.largest_key, Key(39));

    // file5.sst (400 => 499)
    std::string file5_name = "file5.sst";
    std::string file5 = sst_files_dir_ + file5_name;
    ASSERT_OK(sst_file_writer.Open(file5));
    for (int k = 400; k < 500; k++) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
    }
    ExternalSstFileInfo file5_info;
    s = sst_file_writer.Finish(&file5_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(file5_info.file_path, file5);
    ASSERT_EQ(file5_info.num_entries, 100);
    ASSERT_EQ(file5_info.smallest_key, Key(400));
    ASSERT_EQ(file5_info.largest_key, Key(499));

    DestroyAndReopen(options);
    // Add file1 at L0
    {
      std::vector<LiveFileMetaData> import_files_metadata;

      import_files_metadata.push_back(
          LiveFileMetaDataInit(file1_name, sst_files_dir_, 0, 0, 0));

      s = DeprecatedAddFile(&import_files_metadata, false);
      ASSERT_TRUE(s.ok()) << s.ToString();
      for (int k = 0; k < 100; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
      }
    }

    DestroyAndReopen(options);
    // Add file1..file3 with file2-file3 overlap at L0 with file3 having lower
    // sequence number
    {
      std::vector<LiveFileMetaData> import_files_metadata;
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file1_name, sst_files_dir_, 0, 20, 29));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file2_name, sst_files_dir_, 0, 10, 19));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file3_name, sst_files_dir_, 0, 0, 0));

      s = DeprecatedAddFile(&import_files_metadata, false);
      ASSERT_TRUE(s.ok()) << s.ToString();
      for (int k = 0; k < 200; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
      }
      for (int k = 200; k < 300; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val_overlap");
      }
      ASSERT_EQ(db_->GetLatestSequenceNumber(), 29);
    }

    DestroyAndReopen(options);
    // Add file1..file3 with file2-file3 overlap at L0 with file3 having higher
    // sequence number
    {
      std::vector<LiveFileMetaData> import_files_metadata;
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file1_name, sst_files_dir_, 0, 30, 39));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file2_name, sst_files_dir_, 0, 10, 19));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file3_name, sst_files_dir_, 0, 20, 29));

      s = DeprecatedAddFile(&import_files_metadata, false);
      ASSERT_TRUE(s.ok()) << s.ToString();
      for (int k = 0; k < 195; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
      }
      for (int k = 195; k < 300; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val_overlap");
      }
      ASSERT_EQ(db_->GetLatestSequenceNumber(), 39);
    }

    LimitOptions(options, 3);
    DestroyAndReopen(options);
    // Add file1..file3 with file2-file3 overlap and file 3 at higher level
    {
      std::vector<LiveFileMetaData> import_files_metadata;
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file1_name, sst_files_dir_, 0, 30, 39));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file2_name, sst_files_dir_, 0, 10, 19));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file3_name, sst_files_dir_, 1, 20, 29));

      s = DeprecatedAddFile(&import_files_metadata, false);
      ASSERT_TRUE(s.ok()) << s.ToString();
      for (int k = 0; k < 200; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
      }
      for (int k = 200; k < 300; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val_overlap");
      }
      ASSERT_EQ(db_->GetLatestSequenceNumber(), 39);
    }

    LimitOptions(options, 3);
    DestroyAndReopen(options);
    // Add file1..file3 with file2-file3 overlap and file 2 at higher level
    {
      std::vector<LiveFileMetaData> import_files_metadata;
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file1_name, sst_files_dir_, 0, 30, 39));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file2_name, sst_files_dir_, 1, 40, 49));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file3_name, sst_files_dir_, 0, 20, 29));

      s = DeprecatedAddFile(&import_files_metadata, false);
      ASSERT_TRUE(s.ok()) << s.ToString();
      for (int k = 0; k < 195; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
      }
      for (int k = 195; k < 300; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val_overlap");
      }
      ASSERT_EQ(db_->GetLatestSequenceNumber(), 49);
    }

    LimitOptions(options, 3);
    DestroyAndReopen(options);
    // Add file2 and file3 with overlapping files in different levels
    // Add non-overlapping file1 in a subsequent call that should succeed
    // Add overlapping file4 in a subsequent call that should fail
    {
      std::vector<LiveFileMetaData> import_files_metadata_0;
      import_files_metadata_0.push_back(
          LiveFileMetaDataInit(file2_name, sst_files_dir_, 2, 0, 0));
      import_files_metadata_0.push_back(
          LiveFileMetaDataInit(file3_name, sst_files_dir_, 1, 0, 0));

      s = DeprecatedAddFile(&import_files_metadata_0, false);
      ASSERT_TRUE(s.ok()) << s.ToString();

      std::vector<LiveFileMetaData> import_files_metadata_1;
      import_files_metadata_1.push_back(
          LiveFileMetaDataInit(file1_name, sst_files_dir_, 0, 0, 99));

      s = DeprecatedAddFile(&import_files_metadata_1, false);
      ASSERT_TRUE(s.ok()) << s.ToString();

      std::vector<LiveFileMetaData> import_files_metadata_2;
      import_files_metadata_2.push_back(
          LiveFileMetaDataInit(file4_name, sst_files_dir_, 0, 100, 199));

      ASSERT_NOK(DeprecatedAddFile(&import_files_metadata_2, false));

      for (int k = 0; k < 195; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
      }
      for (int k = 195; k < 300; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val_overlap");
      }
      ASSERT_EQ(db_->GetLatestSequenceNumber(), 99);
    }

    LimitOptions(options, 5);
    DestroyAndReopen(options);
    // Add file1..5 at different levels
    {
      std::vector<LiveFileMetaData> import_files_metadata;
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file1_name, sst_files_dir_, 4, 0, 9));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file2_name, sst_files_dir_, 3, 10, 19));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file3_name, sst_files_dir_, 2, 20, 29));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file4_name, sst_files_dir_, 1, 30, 39));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file5_name, sst_files_dir_, 0, 40, 49));

      s = DeprecatedAddFile(&import_files_metadata, false);
      ASSERT_TRUE(s.ok()) << s.ToString();
      for (int k = 0; k < 30; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
      }
      for (int k = 30; k < 40; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val_overlap");
      }
      for (int k = 40; k < 195; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
      }
      for (int k = 195; k < 300; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val_overlap");
      }
      for (int k = 400; k < 499; k++) {
        ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
      }
    }

    LimitOptions(options, 5);
    DestroyAndReopen(options);
    // Add file1..5 at different levels with overlaps at certain level
    {
      std::vector<LiveFileMetaData> import_files_metadata;
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file1_name, sst_files_dir_, 4, 0, 0));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file2_name, sst_files_dir_, 3, 0, 0));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file3_name, sst_files_dir_, 3, 0, 0));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file4_name, sst_files_dir_, 1, 0, 0));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file5_name, sst_files_dir_, 0, 0, 0));

      ASSERT_NOK(DeprecatedAddFile(&import_files_metadata, false));
    }

    DestroyAndRecreateExternalSSTFilesDir();
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction));
}

TEST_F(ExternalSSTFileTest, FileWithCFInfo) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko", "toto"}, options);

  SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);
  SstFileWriter sfw_unknown(EnvOptions(), options);

  // cf1.sst
  const std::string cf1_sst_name = "cf1.sst";
  const std::string cf1_sst = sst_files_dir_ + cf1_sst_name;
  ASSERT_OK(sfw_cf1.Open(cf1_sst));
  ASSERT_OK(sfw_cf1.Put("K3", "V1"));
  ASSERT_OK(sfw_cf1.Put("K4", "V2"));
  ASSERT_OK(sfw_cf1.Finish());

  // cf_unknown.sst
  const std::string unknown_sst_name = "cf_unknown.sst";
  const std::string unknown_sst = sst_files_dir_ + unknown_sst_name;
  ASSERT_OK(sfw_unknown.Open(unknown_sst));
  ASSERT_OK(sfw_unknown.Put("K5", "V1"));
  ASSERT_OK(sfw_unknown.Put("K6", "V2"));
  ASSERT_OK(sfw_unknown.Finish());

  {
    // Import sst file corresponding to cf1 onto cf1 and cf2 and verify
    std::vector<LiveFileMetaData> import_files_metadata;
    import_files_metadata.push_back(
        LiveFileMetaDataInit(cf1_sst_name, sst_files_dir_, 0, 10, 19));

    ASSERT_OK(DeprecatedAddFile(handles_[2], &import_files_metadata, false));
    ASSERT_OK(DeprecatedAddFile(handles_[1], &import_files_metadata, false));
    ASSERT_EQ(Get(1, "K3"), "V1");
    ASSERT_EQ(Get(1, "K4"), "V2");
    ASSERT_EQ(Get(2, "K3"), "V1");
    ASSERT_EQ(Get(2, "K4"), "V2");
  }

  {
    // Import sst file corresponding to unknown cf onto default cf and cf2
    // and verify
    std::vector<LiveFileMetaData> import_files_metadata;
    import_files_metadata.push_back(
        LiveFileMetaDataInit(unknown_sst_name, sst_files_dir_, 0, 20, 29));

    ASSERT_OK(DeprecatedAddFile(handles_[0], &import_files_metadata, false));
    ASSERT_OK(DeprecatedAddFile(handles_[2], &import_files_metadata, false));
    ASSERT_EQ(Get(0, "K5"), "V1");
    ASSERT_EQ(Get(0, "K6"), "V2");
    ASSERT_EQ(Get(2, "K5"), "V1");
    ASSERT_EQ(Get(2, "K6"), "V2");
  }
}

TEST_F(ExternalSSTFileTest, IngestExportedSSTFromAnotherCF) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko", "toto"}, options);

  for (int i = 0; i < 100; ++i) {
    Put(1, Key(i), Key(i) + "_val");
  }
  ASSERT_OK(Flush(1));

  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));
  // Overwrite the value in the same set of keys
  for (int i = 0; i < 100; ++i) {
    Put(1, Key(i), Key(i) + "_overwrite");
  }

  // Flush to create L0 file
  ASSERT_OK(Flush(1));
  for (int i = 0; i < 100; ++i) {
    Put(1, Key(i), Key(i) + "_overwrite2");
  }

  // Flush again to create another L0 file. It should have higher sequencer
  ASSERT_OK(Flush(1));

  std::vector<LiveFileMetaData> metadata_vec;
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  const std::string export_name = test::TmpDir(env_) + "/export";
  ASSERT_OK(DestroyDB(export_name, options));
  env_->DeleteDir(export_name);
  ASSERT_OK(checkpoint->ExportColumnFamilyFiles(handles_[1], &metadata_vec,
                                                export_name));

  ASSERT_OK(DeprecatedAddFile(handles_[2], &metadata_vec, false));
  CreateColumnFamilies({"yoyo"}, options);

  ASSERT_OK(DeprecatedAddFile(handles_[3], &metadata_vec, false));

  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(Get(1, Key(i)), Get(2, Key(i)));
    ASSERT_EQ(Get(1, Key(i)), Get(3, Key(i)));
  }

  for (int k = 0; k < 25; k++) {
    ASSERT_OK(Delete(2, Key(k)));
  }
  for (int k = 25; k < 50; k++) {
    ASSERT_OK(Put(2, Key(k), Key(k) + "_overwrite3"));
  }

  for (int i = 0; i < 25; ++i) {
    ASSERT_EQ("NOT_FOUND", Get(2, Key(i)));
  }
  for (int i = 25; i < 50; ++i) {
    ASSERT_EQ(Key(i) + "_overwrite3", Get(2, Key(i)));
  }
  for (int i = 50; i < 100; ++i) {
    ASSERT_EQ(Key(i) + "_overwrite2", Get(2, Key(i)));
  }

  // Compact and check again
  ASSERT_OK(Flush(2));
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[2], nullptr, nullptr));

  for (int i = 0; i < 25; ++i) {
    ASSERT_EQ("NOT_FOUND", Get(2, Key(i)));
  }
  for (int i = 25; i < 50; ++i) {
    ASSERT_EQ(Key(i) + "_overwrite3", Get(2, Key(i)));
  }
  for (int i = 50; i < 100; ++i) {
    ASSERT_EQ(Key(i) + "_overwrite2", Get(2, Key(i)));
  }
}

TEST_F(ExternalSSTFileTest, IngestExportedSSTFromAnotherDB) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  for (int i = 0; i < 100; ++i) {
    Put(1, Key(i), Key(i) + "_val");
  }
  ASSERT_OK(Flush(1));
  // Compact to create a L1 file
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));

  // Overwrite the value in the same set of keys
  for (int i = 0; i < 50; ++i) {
    Put(1, Key(i), Key(i) + "_overwrite");
  }
  // Flush to create L0 file
  ASSERT_OK(Flush(1));

  for (int i = 0; i < 25; ++i) {
    Put(1, Key(i), Key(i) + "_overwrite2");
  }
  // Flush again to create another L0 file. It should have higher sequencer
  ASSERT_OK(Flush(1));

  std::vector<LiveFileMetaData> metadata_vec;
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  const std::string export_name = test::TmpDir(env_) + "/export";
  ASSERT_OK(DestroyDB(export_name, options));
  env_->DeleteDir(export_name);
  ASSERT_OK(checkpoint->ExportColumnFamilyFiles(handles_[1], &metadata_vec,
                                                export_name));

  // Create a new db and import the files.
  DB* db_copy;
  ASSERT_OK(DB::Open(options, dbname_ + "/db_copy", &db_copy));
  ColumnFamilyHandle* cfh;
  ASSERT_OK(
      db_copy->CreateColumnFamily(ColumnFamilyOptions(), "yoyo", &cfh));
  ASSERT_OK(db_copy->ImportExternalFile(cfh, metadata_vec,
                                        ImportExternalFileOptions()));

  for (int i = 0; i < 100; ++i) {
    std::string value;
    db_copy->Get(ReadOptions(), cfh, Key(i), &value);
    ASSERT_EQ(Get(1, Key(i)), value);
  }
  db_copy->DropColumnFamily(cfh);
  test::DestroyDir(env_, dbname_ + "/db_copy");
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
    std::vector<LiveFileMetaData> import_files_metadata;
    for (int i = 0; i < n; i++) {
      std::string fname = "file" + std::to_string(i) + ".sst";
      std::string fpath = sst_files_dir_ + fname;
      ExternalSstFileInfo files_info;
      ASSERT_OK(sst_file_writer.Open(fpath));
      for (int k = i * 100; k < (i + 1) * 100; k++) {
        ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
      }
      Status s = sst_file_writer.Finish(&files_info);
      ASSERT_TRUE(s.ok()) << s.ToString();
      ASSERT_EQ(files_info.file_path, fpath);
      ASSERT_EQ(files_info.num_entries, 100);
      ASSERT_EQ(files_info.smallest_key, Key(i * 100));
      ASSERT_EQ(files_info.largest_key, Key((i + 1) * 100 - 1));
      import_files_metadata.push_back(
          LiveFileMetaDataInit(fname, sst_files_dir_, 0, 0, 0));
    }
    import_files_metadata.push_back(LiveFileMetaDataInit(
        "file" + std::to_string(n) + ".sst", sst_files_dir_, 0, 0, 0));
    auto s = DeprecatedAddFile(&import_files_metadata, false);
    ASSERT_NOK(s) << s.ToString();
    for (int k = 0; k < n * 100; k++) {
      ASSERT_EQ("NOT_FOUND", Get(Key(k)));
    }
    import_files_metadata.pop_back();
    ASSERT_OK(DeprecatedAddFile(&import_files_metadata, false));
    for (int k = 0; k < n * 100; k++) {
      std::string value = Key(k) + "_val";
      ASSERT_EQ(Get(Key(k)), value);
    }
    DestroyAndRecreateExternalSSTFilesDir();
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction));
}

TEST_F(ExternalSSTFileTest, MultiThreaded) {
  // Bulk load 10 files every file contain 1000 keys
  int num_files = 10;
  int keys_per_file = 1000;

  // Generate file names
  std::vector<std::string> file_names;
  for (int i = 0; i < num_files; i++) {
    std::string file_name = "file_" + ToString(i) + ".sst";
    file_names.push_back(file_name);
  }

  do {
    Options options = CurrentOptions();

    LimitOptions(options, 2);
    std::atomic<int> thread_num(0);
    std::function<void()> write_file_func = [&]() {
      int file_idx = thread_num.fetch_add(1);
      int range_start = file_idx * keys_per_file;
      int range_end = range_start + keys_per_file;

      SstFileWriter sst_file_writer(EnvOptions(), options);

      ASSERT_OK(sst_file_writer.Open(sst_files_dir_ + file_names[file_idx]));

      for (int k = range_start; k < range_end; k++) {
        ASSERT_OK(sst_file_writer.Put(Key(k), Key(k)));
      }

      Status s = sst_file_writer.Finish();
      ASSERT_TRUE(s.ok()) << s.ToString();
    };
    // Write num_files files in parallel
    std::vector<port::Thread> sst_writer_threads;
    for (int i = 0; i < num_files; ++i) {
      sst_writer_threads.emplace_back(write_file_func);
    }

    for (auto& t : sst_writer_threads) {
      t.join();
    }


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

      std::vector<LiveFileMetaData> import_files_metadata;
      import_files_metadata.push_back(
          LiveFileMetaDataInit(file_names[file_idx], sst_files_dir_, 1, 0, 0));

      if (static_cast<size_t>(file_idx + 1) < file_names.size()) {
        import_files_metadata.push_back(LiveFileMetaDataInit(
            file_names[file_idx + 1], sst_files_dir_, 1, 0, 0));
      }

      Status s = DeprecatedAddFile(&import_files_metadata, move_file);
      if (s.ok()) {
        files_added += static_cast<int>(import_files_metadata.size());
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

    // Overwrite values of keys divisible by 100
    for (int k = 0; k < num_files * keys_per_file; k += 100) {
      std::string key = Key(k);
      Status s = Put(key, key + "_new");
      ASSERT_TRUE(s.ok());
    }

    for (int k = 0; k < num_files * keys_per_file; ++k) {
      std::string key = Key(k);
      std::string value = (k % 100 == 0) ? (key + "_new") : key;
      ASSERT_EQ(Get(key), value);
    }

    DestroyAndRecreateExternalSSTFilesDir();
  } while (ChangeOptions(kSkipPlainTable | kSkipFIFOCompaction));
}

TEST_F(ExternalSSTFileTest, CompactDuringAddFileRandom) {
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
    SstFileWriter sst_file_writer(EnvOptions(), options);
    ASSERT_EQ(sst_file_writer.FileSize(), 0);
    std::string file1_name = "file.sst";
    std::string file1 = sst_files_dir_ + file1_name;
    ASSERT_OK(sst_file_writer.Open(file1));
    for (auto k : file_keys) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + ToString(range_id)));
    }
    ExternalSstFileInfo file1_info;
    Status s = sst_file_writer.Finish(&file1_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::vector<LiveFileMetaData> import_files_metadata;
    import_files_metadata.push_back(
        LiveFileMetaDataInit(file1_name, sst_files_dir_, 0, 0, 0));
    ASSERT_OK(DeprecatedAddFile(&import_files_metadata, false));
  };

  std::vector<port::Thread> threads;
  while (range_id < 5000) {
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

  for (int rid = 0; rid < 5000; rid++) {
    int range_start = rid * 10;
    int range_end = range_start + 10;

    ASSERT_EQ(Get(Key(range_start)), Key(range_start)) << rid;
    ASSERT_EQ(Get(Key(range_end)), Key(range_end)) << rid;
    for (int k = range_start + 1; k < range_end; k++) {
      std::string v = Key(k) + ToString(rid);
      ASSERT_EQ(Get(Key(k)), v) << rid;
    }
  }
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
  std::vector<LiveFileMetaData> generated_files_metadata;
  for (int i = 0; i < 10; i++) {
    std::string file_name = env_->GenerateUniqueId();
    ASSERT_OK(sst_file_writer.Open(sst_files_dir_ + file_name));

    int range_end = i * 10;
    int range_start = range_end + 15;
    for (int k = (range_start - 1); k >= range_end; k--) {
      ASSERT_OK(sst_file_writer.Put(Key(k), Key(k)));
    }
    ExternalSstFileInfo file_info;
    ASSERT_OK(sst_file_writer.Finish(&file_info));
    generated_files_metadata.push_back(
        LiveFileMetaDataInit(file_name, sst_files_dir_, 1, 0, 0));
  }

  std::vector<LiveFileMetaData> import_files_metadata;

  // These 2nd and 3rd files overlap with each other
  import_files_metadata = {
      generated_files_metadata[0], generated_files_metadata[4],
      generated_files_metadata[5], generated_files_metadata[7]};
  ASSERT_NOK(DeprecatedAddFile(&import_files_metadata, false));

  // These 2 files dont overlap with each other
  import_files_metadata = {generated_files_metadata[0],
                           generated_files_metadata[2]};
  ASSERT_OK(DeprecatedAddFile(&import_files_metadata, false));

  // These 2 files dont overlap with each other but overlap with keys in DB
  import_files_metadata = {generated_files_metadata[3],
                           generated_files_metadata[7]};
  ASSERT_NOK(DeprecatedAddFile(&import_files_metadata, false));

  // Files dont overlap and dont overlap with DB key range
  import_files_metadata = {generated_files_metadata[4],
                           generated_files_metadata[6],
                           generated_files_metadata[8]};
  ASSERT_OK(DeprecatedAddFile(&import_files_metadata, false));

  for (int i = 0; i < 100; i++) {
    if (i % 20 <= 14) {
      ASSERT_EQ(Get(Key(i)), Key(i));
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
}

class TestImportExternalFileListener : public EventListener {
 public:
  void OnExternalFileImported(DB* /*db*/,
                              const ExternalFileImportedInfo& info) override {
    imported_files.push_back(info);
  }

  std::vector<ExternalFileImportedInfo> imported_files;
};

TEST_F(ExternalSSTFileTest, ImportedListener) {
  Options options = CurrentOptions();
  TestImportExternalFileListener* listener =
      new TestImportExternalFileListener();
  options.listeners.emplace_back(listener);
  CreateAndReopenWithCF({"koko", "toto"}, options);

  // file1.sst (0 => 99)
  SstFileWriter sst_file_writer(EnvOptions(), options);
  ASSERT_EQ(sst_file_writer.FileSize(), 0);
  std::string file1_name = "file1.sst";
  std::string file1 = sst_files_dir_ + file1_name;
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  std::vector<LiveFileMetaData> import_files_metadata;
  import_files_metadata.push_back(
      LiveFileMetaDataInit(file1_name, sst_files_dir_, 0, 0, 0));

  // Ingest into default cf
  ASSERT_OK(DeprecatedAddFile(&import_files_metadata, false));
  ASSERT_EQ(listener->imported_files.size(), 1);
  ASSERT_EQ(listener->imported_files.back().cf_name, "default");
  ASSERT_EQ(listener->imported_files.back().smallest_seqnum, 0);
  ASSERT_EQ(listener->imported_files.back().largest_seqnum, 0);
  ASSERT_EQ(listener->imported_files.back().level, 0);

  // Ingest into cf1
  ASSERT_OK(DeprecatedAddFile(handles_[1], &import_files_metadata, false));
  ASSERT_EQ(listener->imported_files.size(), 2);
  ASSERT_EQ(listener->imported_files.back().cf_name, "koko");
  ASSERT_EQ(listener->imported_files.back().smallest_seqnum, 0);
  ASSERT_EQ(listener->imported_files.back().largest_seqnum, 0);
  ASSERT_EQ(listener->imported_files.back().level, 0);

  // Ingest into cf2
  ASSERT_OK(DeprecatedAddFile(handles_[2], &import_files_metadata, false));
  ASSERT_EQ(listener->imported_files.size(), 3);
  ASSERT_EQ(listener->imported_files.back().cf_name, "toto");
  ASSERT_EQ(listener->imported_files.back().smallest_seqnum, 0);
  ASSERT_EQ(listener->imported_files.back().largest_seqnum, 0);
  ASSERT_EQ(listener->imported_files.back().level, 0);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr,
          "SKIPPED as External SST File Writer and Import are not supported "
          "in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
