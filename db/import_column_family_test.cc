#ifndef ROCKSDB_LITE

#include <functional>
#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_writer.h"
#include "util/testutil.h"

namespace rocksdb {

class ImportColumnFamilyTest : public DBTestBase {
 public:
  ImportColumnFamilyTest() : DBTestBase("/import_column_family_test") {
    sst_files_dir_ = dbname_ + "/sst_files/";
    DestroyAndRecreateExternalSSTFilesDir();
    export_files_dir_ = test::TmpDir(env_) + "/export";
  }

  void DestroyAndRecreateExternalSSTFilesDir() {
    test::DestroyDir(env_, sst_files_dir_);
    env_->CreateDir(sst_files_dir_);
    test::DestroyDir(env_, export_files_dir_);
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

  ~ImportColumnFamilyTest() {
    test::DestroyDir(env_, sst_files_dir_);
    test::DestroyDir(env_, export_files_dir_);
  }

 protected:
  std::string sst_files_dir_;
  std::string export_files_dir_;
};

TEST_F(ImportColumnFamilyTest, ImportSSTFileWriterFiles) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);
  SstFileWriter sfw_unknown(EnvOptions(), options);

  // cf1.sst
  const std::string cf1_sst_name = "cf1.sst";
  const std::string cf1_sst = sst_files_dir_ + cf1_sst_name;
  ASSERT_OK(sfw_cf1.Open(cf1_sst));
  ASSERT_OK(sfw_cf1.Put("K1", "V1"));
  ASSERT_OK(sfw_cf1.Put("K2", "V2"));
  ASSERT_OK(sfw_cf1.Finish());

  // cf_unknown.sst
  const std::string unknown_sst_name = "cf_unknown.sst";
  const std::string unknown_sst = sst_files_dir_ + unknown_sst_name;
  ASSERT_OK(sfw_unknown.Open(unknown_sst));
  ASSERT_OK(sfw_unknown.Put("K3", "V1"));
  ASSERT_OK(sfw_unknown.Put("K4", "V2"));
  ASSERT_OK(sfw_unknown.Finish());

  {
    // Import sst file corresponding to cf1 onto cf2 and verify.
    std::vector<LiveFileMetaData> metadata;
    metadata.push_back(
        LiveFileMetaDataInit(cf1_sst_name, sst_files_dir_, 0, 10, 19));

    ColumnFamilyHandle* cfh2;
    ASSERT_OK(db_->CreateColumnFamilyWithImport(
        options, "toto", ImportColumnFamilyOptions(), metadata, &cfh2));

    std::string value2;
    db_->Get(ReadOptions(), cfh2, "K1", &value2);
    ASSERT_EQ(value2, "V1");
    db_->Get(ReadOptions(), cfh2, "K2", &value2);
    ASSERT_EQ(value2, "V2");
    ASSERT_OK(db_->DropColumnFamily(cfh2));
  }

  {
    // Import sst file corresponding to unknown cf onto cf2 and verify.
    std::vector<LiveFileMetaData> metadata;
    metadata.push_back(
        LiveFileMetaDataInit(unknown_sst_name, sst_files_dir_, 0, 20, 29));

    ColumnFamilyHandle* cfh2;
    ASSERT_OK(db_->CreateColumnFamilyWithImport(
        options, "yoyo", ImportColumnFamilyOptions(), metadata, &cfh2));

    std::string value2;
    db_->Get(ReadOptions(), cfh2, "K3", &value2);
    ASSERT_EQ(value2, "V1");
    db_->Get(ReadOptions(), cfh2, "K4", &value2);
    ASSERT_EQ(value2, "V2");
    ASSERT_OK(db_->DropColumnFamily(cfh2));
  }
}

TEST_F(ImportColumnFamilyTest, ImportSSTFileWriterFilesWithOverlap) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);

  // file3.sst
  const std::string file3_sst_name = "file3.sst";
  const std::string file3_sst = sst_files_dir_ + file3_sst_name;
  ASSERT_OK(sfw_cf1.Open(file3_sst));
  for (int i = 0; i < 100; ++i) {
    sfw_cf1.Put(Key(i), Key(i) + "_val");
  }
  ASSERT_OK(sfw_cf1.Finish());

  // file2.sst
  const std::string file2_sst_name = "file2.sst";
  const std::string file2_sst = sst_files_dir_ + file2_sst_name;
  ASSERT_OK(sfw_cf1.Open(file2_sst));
  for (int i = 0; i < 100; i += 2) {
    sfw_cf1.Put(Key(i), Key(i) + "_overwrite1");
  }
  ASSERT_OK(sfw_cf1.Finish());

  // file1a.sst
  const std::string file1a_sst_name = "file1a.sst";
  const std::string file1a_sst = sst_files_dir_ + file1a_sst_name;
  ASSERT_OK(sfw_cf1.Open(file1a_sst));
  for (int i = 0; i < 52; i += 4) {
    sfw_cf1.Put(Key(i), Key(i) + "_overwrite2");
  }
  ASSERT_OK(sfw_cf1.Finish());

  // file1b.sst
  const std::string file1b_sst_name = "file1b.sst";
  const std::string file1b_sst = sst_files_dir_ + file1b_sst_name;
  ASSERT_OK(sfw_cf1.Open(file1b_sst));
  for (int i = 52; i < 100; i += 4) {
    sfw_cf1.Put(Key(i), Key(i) + "_overwrite2");
  }
  ASSERT_OK(sfw_cf1.Finish());

  // file0a.sst
  const std::string file0a_sst_name = "file0a.sst";
  const std::string file0a_sst = sst_files_dir_ + file0a_sst_name;
  ASSERT_OK(sfw_cf1.Open(file0a_sst));
  for (int i = 0; i < 100; i += 16) {
    sfw_cf1.Put(Key(i), Key(i) + "_overwrite3");
  }
  ASSERT_OK(sfw_cf1.Finish());

  // file0b.sst
  const std::string file0b_sst_name = "file0b.sst";
  const std::string file0b_sst = sst_files_dir_ + file0b_sst_name;
  ASSERT_OK(sfw_cf1.Open(file0b_sst));
  for (int i = 0; i < 100; i += 16) {
    sfw_cf1.Put(Key(i), Key(i) + "_overwrite3");
  }
  ASSERT_OK(sfw_cf1.Finish());

  // Import sst files and verify.
  std::vector<LiveFileMetaData> metadata;
  metadata.push_back(
      LiveFileMetaDataInit(file3_sst_name, sst_files_dir_, 3, 10, 19));
  metadata.push_back(
      LiveFileMetaDataInit(file2_sst_name, sst_files_dir_, 2, 20, 29));
  metadata.push_back(
      LiveFileMetaDataInit(file1a_sst_name, sst_files_dir_, 1, 30, 34));
  metadata.push_back(
      LiveFileMetaDataInit(file1b_sst_name, sst_files_dir_, 1, 35, 39));
  metadata.push_back(
      LiveFileMetaDataInit(file0a_sst_name, sst_files_dir_, 0, 40, 49));
  metadata.push_back(
      LiveFileMetaDataInit(file0b_sst_name, sst_files_dir_, 0, 50, 59));

  ColumnFamilyHandle* cfh2;
  ASSERT_OK(db_->CreateColumnFamilyWithImport(
      options, "toto", ImportColumnFamilyOptions(), metadata, &cfh2));

  for (int i = 0; i < 100; i++) {
    std::string value2;
    db_->Get(ReadOptions(), cfh2, Key(i), &value2);
    if (i % 16 == 0) {
      ASSERT_EQ(value2, Key(i) + "_overwrite3");
    } else if (i % 4 == 0) {
      ASSERT_EQ(value2, Key(i) + "_overwrite2");
    } else if (i % 2 == 0) {
      ASSERT_EQ(value2, Key(i) + "_overwrite1");
    } else {
      ASSERT_EQ(value2, Key(i) + "_val");
    }
  }

  for (int i = 0; i < 100; i += 5) {
    ASSERT_OK(db_->Put(WriteOptions(), cfh2, Key(i), Key(i) + "_overwrite4"));
  }

  // Flush and check again.
  ASSERT_OK(db_->Flush(FlushOptions(), cfh2));
  for (int i = 0; i < 100; i++) {
    std::string value2;
    db_->Get(ReadOptions(), cfh2, Key(i), &value2);
    if (i % 5 == 0) {
      ASSERT_EQ(value2, Key(i) + "_overwrite4");
    } else if (i % 16 == 0) {
      ASSERT_EQ(value2, Key(i) + "_overwrite3");
    } else if (i % 4 == 0) {
      ASSERT_EQ(value2, Key(i) + "_overwrite2");
    } else if (i % 2 == 0) {
      ASSERT_EQ(value2, Key(i) + "_overwrite1");
    } else {
      ASSERT_EQ(value2, Key(i) + "_val");
    }
  }

  // Compact and check again.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), cfh2, nullptr, nullptr));
  for (int i = 0; i < 100; i++) {
    std::string value2;
    db_->Get(ReadOptions(), cfh2, Key(i), &value2);
    if (i % 5 == 0) {
      ASSERT_EQ(value2, Key(i) + "_overwrite4");
    } else if (i % 16 == 0) {
      ASSERT_EQ(value2, Key(i) + "_overwrite3");
    } else if (i % 4 == 0) {
      ASSERT_EQ(value2, Key(i) + "_overwrite2");
    } else if (i % 2 == 0) {
      ASSERT_EQ(value2, Key(i) + "_overwrite1");
    } else {
      ASSERT_EQ(value2, Key(i) + "_val");
    }
  }

  ASSERT_OK(db_->DropColumnFamily(cfh2));
}

TEST_F(ImportColumnFamilyTest, ImportExportedSSTFromAnotherCF) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  for (int i = 0; i < 100; ++i) {
    Put(1, Key(i), Key(i) + "_val");
  }
  ASSERT_OK(Flush(1));

  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));

  // Overwrite the value in the same set of keys.
  for (int i = 0; i < 100; ++i) {
    Put(1, Key(i), Key(i) + "_overwrite");
  }

  // Flush to create L0 file.
  ASSERT_OK(Flush(1));
  for (int i = 0; i < 100; ++i) {
    Put(1, Key(i), Key(i) + "_overwrite2");
  }

  // Flush again to create another L0 file. It should have higher sequencer.
  ASSERT_OK(Flush(1));

  std::vector<LiveFileMetaData> metadata;
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->ExportColumnFamily(handles_[1], &metadata,
                                           export_files_dir_));

  ColumnFamilyHandle* cfh2;
  ImportColumnFamilyOptions import_options;
  import_options.move_files = false;
  ASSERT_OK(db_->CreateColumnFamilyWithImport(options, "toto", import_options,
                                              metadata, &cfh2));

  ColumnFamilyHandle* cfh3;
  import_options.move_files = true;
  ASSERT_OK(db_->CreateColumnFamilyWithImport(options, "yoyo", import_options,
                                              metadata, &cfh3));

  std::string value2, value3;

  for (int i = 0; i < 100; ++i) {
    db_->Get(ReadOptions(), cfh2, Key(i), &value2);
    db_->Get(ReadOptions(), cfh3, Key(i), &value3);
    ASSERT_EQ(Get(1, Key(i)), value2);
    ASSERT_EQ(Get(1, Key(i)), value3);
  }

  // Modify keys in cf2 and verify.
  for (int i = 0; i < 25; i++) {
    ASSERT_OK(db_->Delete(WriteOptions(), cfh2, Key(i)));
  }

  for (int i = 25; i < 50; i++) {
    ASSERT_OK(db_->Put(WriteOptions(), cfh2, Key(i), Key(i) + "_overwrite3"));
  }

  for (int i = 0; i < 25; ++i) {
    ASSERT_TRUE(db_->Get(ReadOptions(), cfh2, Key(i), &value2).IsNotFound());
  }
  for (int i = 25; i < 50; ++i) {
    db_->Get(ReadOptions(), cfh2, Key(i), &value2);
    ASSERT_EQ(Key(i) + "_overwrite3", value2);
  }
  for (int i = 50; i < 100; ++i) {
    db_->Get(ReadOptions(), cfh2, Key(i), &value2);
    ASSERT_EQ(Key(i) + "_overwrite2", value2);
  }

  // Compact and check again.
  ASSERT_OK(db_->Flush(FlushOptions(), cfh2));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), cfh2, nullptr, nullptr));

  for (int i = 0; i < 25; ++i) {
    ASSERT_TRUE(db_->Get(ReadOptions(), cfh2, Key(i), &value2).IsNotFound());
  }
  for (int i = 25; i < 50; ++i) {
    db_->Get(ReadOptions(), cfh2, Key(i), &value2);
    ASSERT_EQ(Key(i) + "_overwrite3", value2);
  }
  for (int i = 50; i < 100; ++i) {
    db_->Get(ReadOptions(), cfh2, Key(i), &value2);
    ASSERT_EQ(Key(i) + "_overwrite2", value2);
  }
  ASSERT_OK(db_->DropColumnFamily(cfh2));
  ASSERT_OK(db_->DropColumnFamily(cfh3));
}

TEST_F(ImportColumnFamilyTest, ImportExportedSSTFromAnotherDB) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  for (int i = 0; i < 100; ++i) {
    Put(1, Key(i), Key(i) + "_val");
  }
  ASSERT_OK(Flush(1));

  // Compact to create a L1 file.
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));

  // Overwrite the value in the same set of keys.
  for (int i = 0; i < 50; ++i) {
    Put(1, Key(i), Key(i) + "_overwrite");
  }

  // Flush to create L0 file.
  ASSERT_OK(Flush(1));

  for (int i = 0; i < 25; ++i) {
    Put(1, Key(i), Key(i) + "_overwrite2");
  }

  // Flush again to create another L0 file. It should have higher sequencer.
  ASSERT_OK(Flush(1));

  std::vector<LiveFileMetaData> metadata;
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->ExportColumnFamily(handles_[1], &metadata,
                                           export_files_dir_));

  // Create a new db and import the files.
  DB* db_copy;
  test::DestroyDir(env_, dbname_ + "/db_copy");
  ASSERT_OK(DB::Open(options, dbname_ + "/db_copy", &db_copy));
  ColumnFamilyHandle* cfh;
  ASSERT_OK(db_copy->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                  ImportColumnFamilyOptions(),
                                                  metadata, &cfh));

  for (int i = 0; i < 100; ++i) {
    std::string value;
    db_copy->Get(ReadOptions(), cfh, Key(i), &value);
    ASSERT_EQ(Get(1, Key(i)), value);
  }
  db_copy->DropColumnFamily(cfh);
  test::DestroyDir(env_, dbname_ + "/db_copy");
}

TEST_F(ImportColumnFamilyTest, ImportColumnFamilyNegativeTest) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  {
    // Create column family with existing cf name.
    std::vector<LiveFileMetaData> metadata;
    ColumnFamilyHandle* cfh;

    ASSERT_EQ(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "koko",
                                                ImportColumnFamilyOptions(),
                                                metadata, &cfh),
              Status::InvalidArgument("Column family already exists"));
  }

  {
    // Import with no files specified.
    std::vector<LiveFileMetaData> metadata;
    ColumnFamilyHandle* cfh;

    ASSERT_EQ(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                ImportColumnFamilyOptions(),
                                                metadata, &cfh),
              Status::InvalidArgument("The list of files is empty"));
  }

  {
    // Import with overlapping keys in sst files.
    std::vector<LiveFileMetaData> metadata;
    ColumnFamilyHandle* cfh;
    SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);
    const std::string file1_sst_name = "file1.sst";
    const std::string file1_sst = sst_files_dir_ + file1_sst_name;
    ASSERT_OK(sfw_cf1.Open(file1_sst));
    ASSERT_OK(sfw_cf1.Put("K1", "V1"));
    ASSERT_OK(sfw_cf1.Put("K2", "V2"));
    ASSERT_OK(sfw_cf1.Finish());
    const std::string file2_sst_name = "file2.sst";
    const std::string file2_sst = sst_files_dir_ + file2_sst_name;
    ASSERT_OK(sfw_cf1.Open(file2_sst));
    ASSERT_OK(sfw_cf1.Put("K2", "V2"));
    ASSERT_OK(sfw_cf1.Put("K3", "V3"));
    ASSERT_OK(sfw_cf1.Finish());

    metadata.push_back(
        LiveFileMetaDataInit(file1_sst_name, sst_files_dir_, 1, 10, 19));
    metadata.push_back(
        LiveFileMetaDataInit(file2_sst_name, sst_files_dir_, 1, 10, 19));

    ASSERT_EQ(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                ImportColumnFamilyOptions(),
                                                metadata, &cfh),
              Status::InvalidArgument("Files have overlapping ranges"));
  }
  {
    // Import with non existent sst file should fail with appropriate error.
    std::vector<LiveFileMetaData> metadata;
    ColumnFamilyHandle* cfh;
    SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);
    const std::string file1_sst_name = "file1.sst";
    const std::string file1_sst = sst_files_dir_ + file1_sst_name;
    ASSERT_OK(sfw_cf1.Open(file1_sst));
    ASSERT_OK(sfw_cf1.Put("K1", "V1"));
    ASSERT_OK(sfw_cf1.Put("K2", "V2"));
    ASSERT_OK(sfw_cf1.Finish());
    const std::string file3_sst_name = "file3.sst";

    metadata.push_back(
        LiveFileMetaDataInit(file1_sst_name, sst_files_dir_, 1, 10, 19));
    metadata.push_back(
        LiveFileMetaDataInit(file3_sst_name, sst_files_dir_, 1, 10, 19));

    ASSERT_EQ(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                ImportColumnFamilyOptions(),
                                                metadata, &cfh),
              Status::IOError("No such file or directory"));
  }
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
