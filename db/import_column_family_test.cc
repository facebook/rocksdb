#ifndef ROCKSDB_LITE

#include <functional>
#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_writer.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class ImportColumnFamilyTest : public DBTestBase {
 public:
  ImportColumnFamilyTest() : DBTestBase("/import_column_family_test") {
    sst_files_dir_ = dbname_ + "/sst_files/";
    DestroyAndRecreateExternalSSTFilesDir();
    export_files_dir_ = test::TmpDir(env_) + "/export";
    import_cfh_ = nullptr;
    import_cfh2_ = nullptr;
    metadata_ptr_ = nullptr;
  }

  ~ImportColumnFamilyTest() {
    if (import_cfh_) {
      db_->DropColumnFamily(import_cfh_);
      db_->DestroyColumnFamilyHandle(import_cfh_);
      import_cfh_ = nullptr;
    }
    if (import_cfh2_) {
      db_->DropColumnFamily(import_cfh2_);
      db_->DestroyColumnFamilyHandle(import_cfh2_);
      import_cfh2_ = nullptr;
    }
    if (metadata_ptr_) {
      delete metadata_ptr_;
      metadata_ptr_ = nullptr;
    }
    test::DestroyDir(env_, sst_files_dir_);
    test::DestroyDir(env_, export_files_dir_);
  }

  void DestroyAndRecreateExternalSSTFilesDir() {
    test::DestroyDir(env_, sst_files_dir_);
    env_->CreateDir(sst_files_dir_);
    test::DestroyDir(env_, export_files_dir_);
  }

  LiveFileMetaData LiveFileMetaDataInit(std::string name, std::string path,
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

 protected:
  std::string sst_files_dir_;
  std::string export_files_dir_;
  ColumnFamilyHandle* import_cfh_;
  ColumnFamilyHandle* import_cfh2_;
  ExportImportFilesMetaData* metadata_ptr_;
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
    // Import sst file corresponding to cf1 onto a new cf and verify
    ExportImportFilesMetaData metadata;
    metadata.files.push_back(
        LiveFileMetaDataInit(cf1_sst_name, sst_files_dir_, 0, 10, 19));
    metadata.db_comparator_name = options.comparator->Name();

    ASSERT_OK(db_->CreateColumnFamilyWithImport(
        options, "toto", ImportColumnFamilyOptions(), metadata, &import_cfh_));
    ASSERT_NE(import_cfh_, nullptr);

    std::string value;
    db_->Get(ReadOptions(), import_cfh_, "K1", &value);
    ASSERT_EQ(value, "V1");
    db_->Get(ReadOptions(), import_cfh_, "K2", &value);
    ASSERT_EQ(value, "V2");
    ASSERT_OK(db_->DropColumnFamily(import_cfh_));
    ASSERT_OK(db_->DestroyColumnFamilyHandle(import_cfh_));
    import_cfh_ = nullptr;
  }

  {
    // Import sst file corresponding to unknown cf onto a new cf and verify
    ExportImportFilesMetaData metadata;
    metadata.files.push_back(
        LiveFileMetaDataInit(unknown_sst_name, sst_files_dir_, 0, 20, 29));
    metadata.db_comparator_name = options.comparator->Name();

    ASSERT_OK(db_->CreateColumnFamilyWithImport(
        options, "yoyo", ImportColumnFamilyOptions(), metadata, &import_cfh_));
    ASSERT_NE(import_cfh_, nullptr);

    std::string value;
    db_->Get(ReadOptions(), import_cfh_, "K3", &value);
    ASSERT_EQ(value, "V1");
    db_->Get(ReadOptions(), import_cfh_, "K4", &value);
    ASSERT_EQ(value, "V2");
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
    sfw_cf1.Put(Key(i), Key(i) + "_overwrite4");
  }
  ASSERT_OK(sfw_cf1.Finish());

  // Import sst files and verify
  ExportImportFilesMetaData metadata;
  metadata.files.push_back(
      LiveFileMetaDataInit(file3_sst_name, sst_files_dir_, 3, 10, 19));
  metadata.files.push_back(
      LiveFileMetaDataInit(file2_sst_name, sst_files_dir_, 2, 20, 29));
  metadata.files.push_back(
      LiveFileMetaDataInit(file1a_sst_name, sst_files_dir_, 1, 30, 34));
  metadata.files.push_back(
      LiveFileMetaDataInit(file1b_sst_name, sst_files_dir_, 1, 35, 39));
  metadata.files.push_back(
      LiveFileMetaDataInit(file0a_sst_name, sst_files_dir_, 0, 40, 49));
  metadata.files.push_back(
      LiveFileMetaDataInit(file0b_sst_name, sst_files_dir_, 0, 50, 59));
  metadata.db_comparator_name = options.comparator->Name();

  ASSERT_OK(db_->CreateColumnFamilyWithImport(
      options, "toto", ImportColumnFamilyOptions(), metadata, &import_cfh_));
  ASSERT_NE(import_cfh_, nullptr);

  for (int i = 0; i < 100; i++) {
    std::string value;
    db_->Get(ReadOptions(), import_cfh_, Key(i), &value);
    if (i % 16 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite4");
    } else if (i % 4 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite2");
    } else if (i % 2 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite1");
    } else {
      ASSERT_EQ(value, Key(i) + "_val");
    }
  }

  for (int i = 0; i < 100; i += 5) {
    ASSERT_OK(
        db_->Put(WriteOptions(), import_cfh_, Key(i), Key(i) + "_overwrite5"));
  }

  // Flush and check again
  ASSERT_OK(db_->Flush(FlushOptions(), import_cfh_));
  for (int i = 0; i < 100; i++) {
    std::string value;
    db_->Get(ReadOptions(), import_cfh_, Key(i), &value);
    if (i % 5 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite5");
    } else if (i % 16 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite4");
    } else if (i % 4 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite2");
    } else if (i % 2 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite1");
    } else {
      ASSERT_EQ(value, Key(i) + "_val");
    }
  }

  // Compact and check again.
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), import_cfh_, nullptr, nullptr));
  for (int i = 0; i < 100; i++) {
    std::string value;
    db_->Get(ReadOptions(), import_cfh_, Key(i), &value);
    if (i % 5 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite5");
    } else if (i % 16 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite4");
    } else if (i % 4 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite2");
    } else if (i % 2 == 0) {
      ASSERT_EQ(value, Key(i) + "_overwrite1");
    } else {
      ASSERT_EQ(value, Key(i) + "_val");
    }
  }
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

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->ExportColumnFamily(handles_[1], export_files_dir_,
                                           &metadata_ptr_));
  ASSERT_NE(metadata_ptr_, nullptr);
  delete checkpoint;

  ImportColumnFamilyOptions import_options;
  import_options.move_files = false;
  ASSERT_OK(db_->CreateColumnFamilyWithImport(options, "toto", import_options,
                                              *metadata_ptr_, &import_cfh_));
  ASSERT_NE(import_cfh_, nullptr);

  import_options.move_files = true;
  ASSERT_OK(db_->CreateColumnFamilyWithImport(options, "yoyo", import_options,
                                              *metadata_ptr_, &import_cfh2_));
  ASSERT_NE(import_cfh2_, nullptr);
  delete metadata_ptr_;
  metadata_ptr_ = NULL;

  std::string value1, value2;

  for (int i = 0; i < 100; ++i) {
    db_->Get(ReadOptions(), import_cfh_, Key(i), &value1);
    ASSERT_EQ(Get(1, Key(i)), value1);
  }

  for (int i = 0; i < 100; ++i) {
    db_->Get(ReadOptions(), import_cfh2_, Key(i), &value2);
    ASSERT_EQ(Get(1, Key(i)), value2);
  }

  // Modify keys in cf1 and verify.
  for (int i = 0; i < 25; i++) {
    ASSERT_OK(db_->Delete(WriteOptions(), import_cfh_, Key(i)));
  }
  for (int i = 25; i < 50; i++) {
    ASSERT_OK(
        db_->Put(WriteOptions(), import_cfh_, Key(i), Key(i) + "_overwrite3"));
  }
  for (int i = 0; i < 25; ++i) {
    ASSERT_TRUE(
        db_->Get(ReadOptions(), import_cfh_, Key(i), &value1).IsNotFound());
  }
  for (int i = 25; i < 50; ++i) {
    db_->Get(ReadOptions(), import_cfh_, Key(i), &value1);
    ASSERT_EQ(Key(i) + "_overwrite3", value1);
  }
  for (int i = 50; i < 100; ++i) {
    db_->Get(ReadOptions(), import_cfh_, Key(i), &value1);
    ASSERT_EQ(Key(i) + "_overwrite2", value1);
  }

  for (int i = 0; i < 100; ++i) {
    db_->Get(ReadOptions(), import_cfh2_, Key(i), &value2);
    ASSERT_EQ(Get(1, Key(i)), value2);
  }

  // Compact and check again.
  ASSERT_OK(db_->Flush(FlushOptions(), import_cfh_));
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), import_cfh_, nullptr, nullptr));

  for (int i = 0; i < 25; ++i) {
    ASSERT_TRUE(
        db_->Get(ReadOptions(), import_cfh_, Key(i), &value1).IsNotFound());
  }
  for (int i = 25; i < 50; ++i) {
    db_->Get(ReadOptions(), import_cfh_, Key(i), &value1);
    ASSERT_EQ(Key(i) + "_overwrite3", value1);
  }
  for (int i = 50; i < 100; ++i) {
    db_->Get(ReadOptions(), import_cfh_, Key(i), &value1);
    ASSERT_EQ(Key(i) + "_overwrite2", value1);
  }

  for (int i = 0; i < 100; ++i) {
    db_->Get(ReadOptions(), import_cfh2_, Key(i), &value2);
    ASSERT_EQ(Get(1, Key(i)), value2);
  }
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

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->ExportColumnFamily(handles_[1], export_files_dir_,
                                           &metadata_ptr_));
  ASSERT_NE(metadata_ptr_, nullptr);
  delete checkpoint;

  // Create a new db and import the files.
  DB* db_copy;
  test::DestroyDir(env_, dbname_ + "/db_copy");
  ASSERT_OK(DB::Open(options, dbname_ + "/db_copy", &db_copy));
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db_copy->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                  ImportColumnFamilyOptions(),
                                                  *metadata_ptr_, &cfh));
  ASSERT_NE(cfh, nullptr);

  for (int i = 0; i < 100; ++i) {
    std::string value;
    db_copy->Get(ReadOptions(), cfh, Key(i), &value);
    ASSERT_EQ(Get(1, Key(i)), value);
  }
  db_copy->DropColumnFamily(cfh);
  db_copy->DestroyColumnFamilyHandle(cfh);
  delete db_copy;
  test::DestroyDir(env_, dbname_ + "/db_copy");
}

TEST_F(ImportColumnFamilyTest, LevelFilesOverlappingAtEndpoints) {
  // Imports a column family containing a level where two files overlap at their
  // endpoints. "Overlap" means the largest user key in one file is the same as
  // the smallest user key in the second file.
  const int kFileBytes = 128 << 10;  // 128KB
  const int kValueBytes = 1 << 10;   // 1KB
  const int kNumFiles = 4;

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = 2;
  CreateAndReopenWithCF({"koko"}, options);

  Random rnd(301);
  // Every key is snapshot protected to ensure older versions will not be
  // dropped during compaction.
  std::vector<const Snapshot*> snapshots;
  snapshots.reserve(kFileBytes / kValueBytes * kNumFiles);
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kFileBytes / kValueBytes; ++j) {
      auto value = RandomString(&rnd, kValueBytes);
      ASSERT_OK(Put(1, "key", value));
      snapshots.push_back(db_->GetSnapshot());
    }
    ASSERT_OK(Flush(1));
  }

  // Compact to create overlapping L1 files.
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));
  ASSERT_GT(NumTableFilesAtLevel(1, 1), 1);

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->ExportColumnFamily(handles_[1], export_files_dir_,
                                           &metadata_ptr_));
  ASSERT_NE(metadata_ptr_, nullptr);
  delete checkpoint;

  // Create a new db and import the files.
  DB* db_copy;
  test::DestroyDir(env_, dbname_ + "/db_copy");
  ASSERT_OK(DB::Open(options, dbname_ + "/db_copy", &db_copy));
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db_copy->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                  ImportColumnFamilyOptions(),
                                                  *metadata_ptr_, &cfh));
  ASSERT_NE(cfh, nullptr);

  {
    std::string value;
    ASSERT_OK(db_copy->Get(ReadOptions(), cfh, "key", &value));
  }
  db_copy->DropColumnFamily(cfh);
  db_copy->DestroyColumnFamilyHandle(cfh);
  delete db_copy;
  test::DestroyDir(env_, dbname_ + "/db_copy");
  for (const Snapshot* snapshot : snapshots) {
    db_->ReleaseSnapshot(snapshot);
  }
}

TEST_F(ImportColumnFamilyTest, ImportColumnFamilyNegativeTest) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"koko"}, options);

  {
    // Create column family with existing cf name.
    ExportImportFilesMetaData metadata;

    ASSERT_EQ(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "koko",
                                                ImportColumnFamilyOptions(),
                                                metadata, &import_cfh_),
              Status::InvalidArgument("Column family already exists"));
    ASSERT_EQ(import_cfh_, nullptr);
  }

  {
    // Import with no files specified.
    ExportImportFilesMetaData metadata;

    ASSERT_EQ(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                ImportColumnFamilyOptions(),
                                                metadata, &import_cfh_),
              Status::InvalidArgument("The list of files is empty"));
    ASSERT_EQ(import_cfh_, nullptr);
  }

  {
    // Import with overlapping keys in sst files.
    ExportImportFilesMetaData metadata;
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

    metadata.files.push_back(
        LiveFileMetaDataInit(file1_sst_name, sst_files_dir_, 1, 10, 19));
    metadata.files.push_back(
        LiveFileMetaDataInit(file2_sst_name, sst_files_dir_, 1, 10, 19));
    metadata.db_comparator_name = options.comparator->Name();

    ASSERT_EQ(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                ImportColumnFamilyOptions(),
                                                metadata, &import_cfh_),
              Status::InvalidArgument("Files have overlapping ranges"));
    ASSERT_EQ(import_cfh_, nullptr);
  }

  {
    // Import with a mismatching comparator, should fail with appropriate error.
    ExportImportFilesMetaData metadata;
    Options mismatch_options = CurrentOptions();
    mismatch_options.comparator = ReverseBytewiseComparator();
    SstFileWriter sfw_cf1(EnvOptions(), mismatch_options, handles_[1]);
    const std::string file1_sst_name = "file1.sst";
    const std::string file1_sst = sst_files_dir_ + file1_sst_name;
    ASSERT_OK(sfw_cf1.Open(file1_sst));
    ASSERT_OK(sfw_cf1.Put("K2", "V2"));
    ASSERT_OK(sfw_cf1.Put("K1", "V1"));
    ASSERT_OK(sfw_cf1.Finish());

    metadata.files.push_back(
        LiveFileMetaDataInit(file1_sst_name, sst_files_dir_, 1, 10, 19));
    metadata.db_comparator_name = mismatch_options.comparator->Name();

    ASSERT_EQ(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "coco",
                                                ImportColumnFamilyOptions(),
                                                metadata, &import_cfh_),
              Status::InvalidArgument("Comparator name mismatch"));
    ASSERT_EQ(import_cfh_, nullptr);
  }

  {
    // Import with non existent sst file should fail with appropriate error
    ExportImportFilesMetaData metadata;
    SstFileWriter sfw_cf1(EnvOptions(), options, handles_[1]);
    const std::string file1_sst_name = "file1.sst";
    const std::string file1_sst = sst_files_dir_ + file1_sst_name;
    ASSERT_OK(sfw_cf1.Open(file1_sst));
    ASSERT_OK(sfw_cf1.Put("K1", "V1"));
    ASSERT_OK(sfw_cf1.Put("K2", "V2"));
    ASSERT_OK(sfw_cf1.Finish());
    const std::string file3_sst_name = "file3.sst";

    metadata.files.push_back(
        LiveFileMetaDataInit(file1_sst_name, sst_files_dir_, 1, 10, 19));
    metadata.files.push_back(
        LiveFileMetaDataInit(file3_sst_name, sst_files_dir_, 1, 10, 19));
    metadata.db_comparator_name = options.comparator->Name();

    ASSERT_EQ(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                ImportColumnFamilyOptions(),
                                                metadata, &import_cfh_),
              Status::IOError("No such file or directory"));
    ASSERT_EQ(import_cfh_, nullptr);

    // Test successful import after a failure with the same CF name. Ensures
    // there is no side effect with CF when there is a failed import
    metadata.files.pop_back();
    metadata.db_comparator_name = options.comparator->Name();

    ASSERT_OK(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                                ImportColumnFamilyOptions(),
                                                metadata, &import_cfh_));
    ASSERT_NE(import_cfh_, nullptr);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr,
          "SKIPPED as External SST File Writer and Import are not supported "
          "in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
