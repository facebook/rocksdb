//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>

#include "db/version_edit.h"
#include "db/version_set.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class VersionBuilderTest : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  Options options_;
  ImmutableOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  VersionStorageInfo vstorage_;
  uint32_t file_num_;
  CompactionOptionsFIFO fifo_options_;
  std::vector<uint64_t> size_being_compacted_;

  VersionBuilderTest()
      : ucmp_(BytewiseComparator()),
        icmp_(ucmp_),
        ioptions_(options_),
        mutable_cf_options_(options_),
        vstorage_(&icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel,
                  nullptr, false),
        file_num_(1) {
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
    size_being_compacted_.resize(options_.num_levels);
  }

  ~VersionBuilderTest() override {
    for (int i = 0; i < vstorage_.num_levels(); i++) {
      for (auto* f : vstorage_.LevelFiles(i)) {
        if (--f->refs == 0) {
          delete f;
        }
      }
    }
  }

  InternalKey GetInternalKey(const char* ukey,
                             SequenceNumber smallest_seq = 100) {
    return InternalKey(ukey, smallest_seq, kTypeValue);
  }

  void Add(int level, uint64_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 0, uint32_t path_id = 0,
           SequenceNumber smallest_seq = 100, SequenceNumber largest_seq = 100,
           uint64_t num_entries = 0, uint64_t num_deletions = 0,
           bool sampled = false, SequenceNumber smallest_seqno = 0,
           SequenceNumber largest_seqno = 0,
           uint64_t oldest_blob_file_number = kInvalidBlobFileNumber) {
    assert(level < vstorage_.num_levels());
    FileMetaData* f = new FileMetaData(
        file_number, path_id, file_size, GetInternalKey(smallest, smallest_seq),
        GetInternalKey(largest, largest_seq), smallest_seqno, largest_seqno,
        /* marked_for_compact */ false, oldest_blob_file_number,
        kUnknownOldestAncesterTime, kUnknownFileCreationTime,
        kUnknownFileChecksum, kUnknownFileChecksumFuncName);
    f->compensated_file_size = file_size;
    f->num_entries = num_entries;
    f->num_deletions = num_deletions;
    vstorage_.AddFile(level, f);
    if (sampled) {
      f->init_stats_from_file = true;
      vstorage_.UpdateAccumulatedStats(f);
    }
  }

  void AddBlob(uint64_t blob_file_number, uint64_t total_blob_count,
               uint64_t total_blob_bytes, std::string checksum_method,
               std::string checksum_value,
               BlobFileMetaData::LinkedSsts linked_ssts,
               uint64_t garbage_blob_count, uint64_t garbage_blob_bytes) {
    auto shared_meta = SharedBlobFileMetaData::Create(
        blob_file_number, total_blob_count, total_blob_bytes,
        std::move(checksum_method), std::move(checksum_value));
    auto meta =
        BlobFileMetaData::Create(std::move(shared_meta), std::move(linked_ssts),
                                 garbage_blob_count, garbage_blob_bytes);

    vstorage_.AddBlobFile(std::move(meta));
  }

  void AddDummyFile(uint64_t table_file_number, uint64_t blob_file_number) {
    constexpr int level = 0;
    constexpr char smallest[] = "bar";
    constexpr char largest[] = "foo";
    constexpr uint64_t file_size = 100;
    constexpr uint32_t path_id = 0;
    constexpr SequenceNumber smallest_seq = 0;
    constexpr SequenceNumber largest_seq = 0;
    constexpr uint64_t num_entries = 0;
    constexpr uint64_t num_deletions = 0;
    constexpr bool sampled = false;

    Add(level, table_file_number, smallest, largest, file_size, path_id,
        smallest_seq, largest_seq, num_entries, num_deletions, sampled,
        smallest_seq, largest_seq, blob_file_number);
  }

  void AddDummyFileToEdit(VersionEdit* edit, uint64_t table_file_number,
                          uint64_t blob_file_number) {
    assert(edit);

    constexpr int level = 0;
    constexpr uint32_t path_id = 0;
    constexpr uint64_t file_size = 100;
    constexpr char smallest[] = "bar";
    constexpr char largest[] = "foo";
    constexpr SequenceNumber smallest_seqno = 100;
    constexpr SequenceNumber largest_seqno = 300;
    constexpr bool marked_for_compaction = false;

    edit->AddFile(level, table_file_number, path_id, file_size,
                  GetInternalKey(smallest), GetInternalKey(largest),
                  smallest_seqno, largest_seqno, marked_for_compaction,
                  blob_file_number, kUnknownOldestAncesterTime,
                  kUnknownFileCreationTime, kUnknownFileChecksum,
                  kUnknownFileChecksumFuncName);
  }

  static std::shared_ptr<BlobFileMetaData> GetBlobFileMetaData(
      const VersionStorageInfo::BlobFiles& blob_files,
      uint64_t blob_file_number) {
    const auto it = blob_files.find(blob_file_number);

    if (it == blob_files.end()) {
      return std::shared_ptr<BlobFileMetaData>();
    }

    const auto& meta = it->second;
    assert(meta);

    return meta;
  }

  void UpdateVersionStorageInfo() {
    vstorage_.UpdateFilesByCompactionPri(ioptions_.compaction_pri);
    vstorage_.UpdateNumNonEmptyLevels();
    vstorage_.GenerateFileIndexer();
    vstorage_.GenerateLevelFilesBrief();
    vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
    vstorage_.GenerateLevel0NonOverlapping();
    vstorage_.SetFinalized();
  }
};

void UnrefFilesInVersion(VersionStorageInfo* new_vstorage) {
  for (int i = 0; i < new_vstorage->num_levels(); i++) {
    for (auto* f : new_vstorage->LevelFiles(i)) {
      if (--f->refs == 0) {
        delete f;
      }
    }
  }
}

TEST_F(VersionBuilderTest, ApplyAndSaveTo) {
  Add(0, 1U, "150", "200", 100U);

  Add(1, 66U, "150", "200", 100U);
  Add(1, 88U, "201", "300", 100U);

  Add(2, 6U, "150", "179", 100U);
  Add(2, 7U, "180", "220", 100U);
  Add(2, 8U, "221", "300", 100U);

  Add(3, 26U, "150", "170", 100U);
  Add(3, 27U, "171", "179", 100U);
  Add(3, 28U, "191", "220", 100U);
  Add(3, 29U, "221", "300", 100U);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(2, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  version_edit.DeleteFile(3, 27U);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder version_builder(env_options, &ioptions_, table_cache,
                                 &vstorage_, version_set);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  ASSERT_OK(version_builder.Apply(&version_edit));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  ASSERT_EQ(400U, new_vstorage.NumLevelBytes(2));
  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(3));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyAndSaveToDynamic) {
  ioptions_.level_compaction_dynamic_level_bytes = true;

  Add(0, 1U, "150", "200", 100U, 0, 200U, 200U, 0, 0, false, 200U, 200U);
  Add(0, 88U, "201", "300", 100U, 0, 100U, 100U, 0, 0, false, 100U, 100U);

  Add(4, 6U, "150", "179", 100U);
  Add(4, 7U, "180", "220", 100U);
  Add(4, 8U, "221", "300", 100U);

  Add(5, 26U, "150", "170", 100U);
  Add(5, 27U, "171", "179", 100U);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(3, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  version_edit.DeleteFile(0, 1U);
  version_edit.DeleteFile(0, 88U);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder version_builder(env_options, &ioptions_, table_cache,
                                 &vstorage_, version_set);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  ASSERT_OK(version_builder.Apply(&version_edit));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  ASSERT_EQ(0U, new_vstorage.NumLevelBytes(0));
  ASSERT_EQ(100U, new_vstorage.NumLevelBytes(3));
  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(4));
  ASSERT_EQ(200U, new_vstorage.NumLevelBytes(5));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyAndSaveToDynamic2) {
  ioptions_.level_compaction_dynamic_level_bytes = true;

  Add(0, 1U, "150", "200", 100U, 0, 200U, 200U, 0, 0, false, 200U, 200U);
  Add(0, 88U, "201", "300", 100U, 0, 100U, 100U, 0, 0, false, 100U, 100U);

  Add(4, 6U, "150", "179", 100U);
  Add(4, 7U, "180", "220", 100U);
  Add(4, 8U, "221", "300", 100U);

  Add(5, 26U, "150", "170", 100U);
  Add(5, 27U, "171", "179", 100U);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(4, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  version_edit.DeleteFile(0, 1U);
  version_edit.DeleteFile(0, 88U);
  version_edit.DeleteFile(4, 6U);
  version_edit.DeleteFile(4, 7U);
  version_edit.DeleteFile(4, 8U);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder version_builder(env_options, &ioptions_, table_cache,
                                 &vstorage_, version_set);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  ASSERT_OK(version_builder.Apply(&version_edit));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  ASSERT_EQ(0U, new_vstorage.NumLevelBytes(0));
  ASSERT_EQ(100U, new_vstorage.NumLevelBytes(4));
  ASSERT_EQ(200U, new_vstorage.NumLevelBytes(5));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyMultipleAndSaveTo) {
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(2, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  version_edit.AddFile(2, 676, 0, 100U, GetInternalKey("401"),
                       GetInternalKey("450"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  version_edit.AddFile(2, 636, 0, 100U, GetInternalKey("601"),
                       GetInternalKey("650"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  version_edit.AddFile(2, 616, 0, 100U, GetInternalKey("501"),
                       GetInternalKey("550"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  version_edit.AddFile(2, 606, 0, 100U, GetInternalKey("701"),
                       GetInternalKey("750"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder version_builder(env_options, &ioptions_, table_cache,
                                 &vstorage_, version_set);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  ASSERT_OK(version_builder.Apply(&version_edit));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  ASSERT_EQ(500U, new_vstorage.NumLevelBytes(2));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyDeleteAndSaveTo) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder version_builder(env_options, &ioptions_, table_cache,
                                 &vstorage_, version_set);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);

  VersionEdit version_edit;
  version_edit.AddFile(2, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  version_edit.AddFile(2, 676, 0, 100U, GetInternalKey("401"),
                       GetInternalKey("450"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  version_edit.AddFile(2, 636, 0, 100U, GetInternalKey("601"),
                       GetInternalKey("650"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  version_edit.AddFile(2, 616, 0, 100U, GetInternalKey("501"),
                       GetInternalKey("550"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  version_edit.AddFile(2, 606, 0, 100U, GetInternalKey("701"),
                       GetInternalKey("750"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  ASSERT_OK(version_builder.Apply(&version_edit));

  VersionEdit version_edit2;
  version_edit.AddFile(2, 808, 0, 100U, GetInternalKey("901"),
                       GetInternalKey("950"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);
  version_edit2.DeleteFile(2, 616);
  version_edit2.DeleteFile(2, 636);
  version_edit.AddFile(2, 806, 0, 100U, GetInternalKey("801"),
                       GetInternalKey("850"), 200, 200, false,
                       kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                       kUnknownFileCreationTime, kUnknownFileChecksum,
                       kUnknownFileChecksumFuncName);

  ASSERT_OK(version_builder.Apply(&version_edit2));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(2));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyFileDeletionIncorrectLevel) {
  constexpr int level = 1;
  constexpr uint64_t file_number = 2345;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";

  Add(level, file_number, smallest, largest);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr int incorrect_level = 3;

  edit.DeleteFile(incorrect_level, file_number);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Cannot delete table file #2345 from level 3 since "
                          "it is on level 1"));
}

TEST_F(VersionBuilderTest, ApplyFileDeletionNotInLSMTree) {
  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr int level = 3;
  constexpr uint64_t file_number = 1234;

  edit.DeleteFile(level, file_number);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Cannot delete table file #1234 from level 3 since "
                          "it is not in the LSM tree"));
}

TEST_F(VersionBuilderTest, ApplyFileDeletionAndAddition) {
  constexpr int level = 1;
  constexpr uint64_t file_number = 2345;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";
  constexpr uint64_t file_size = 10000;
  constexpr uint32_t path_id = 0;
  constexpr SequenceNumber smallest_seq = 100;
  constexpr SequenceNumber largest_seq = 500;
  constexpr uint64_t num_entries = 0;
  constexpr uint64_t num_deletions = 0;
  constexpr bool sampled = false;
  constexpr SequenceNumber smallest_seqno = 1;
  constexpr SequenceNumber largest_seqno = 1000;

  Add(level, file_number, smallest, largest, file_size, path_id, smallest_seq,
      largest_seq, num_entries, num_deletions, sampled, smallest_seqno,
      largest_seqno);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit deletion;

  deletion.DeleteFile(level, file_number);

  ASSERT_OK(builder.Apply(&deletion));

  VersionEdit addition;

  constexpr bool marked_for_compaction = false;

  addition.AddFile(level, file_number, path_id, file_size,
                   GetInternalKey(smallest, smallest_seq),
                   GetInternalKey(largest, largest_seq), smallest_seqno,
                   largest_seqno, marked_for_compaction, kInvalidBlobFileNumber,
                   kUnknownOldestAncesterTime, kUnknownFileCreationTime,
                   kUnknownFileChecksum, kUnknownFileChecksumFuncName);

  ASSERT_OK(builder.Apply(&addition));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  ASSERT_OK(builder.SaveTo(&new_vstorage));
  ASSERT_EQ(new_vstorage.GetFileLocation(file_number).GetLevel(), level);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyFileAdditionAlreadyInBase) {
  constexpr int level = 1;
  constexpr uint64_t file_number = 2345;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";

  Add(level, file_number, smallest, largest);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr int new_level = 2;
  constexpr uint32_t path_id = 0;
  constexpr uint64_t file_size = 10000;
  constexpr SequenceNumber smallest_seqno = 100;
  constexpr SequenceNumber largest_seqno = 1000;
  constexpr bool marked_for_compaction = false;

  edit.AddFile(new_level, file_number, path_id, file_size,
               GetInternalKey(smallest), GetInternalKey(largest),
               smallest_seqno, largest_seqno, marked_for_compaction,
               kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
               kUnknownFileCreationTime, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Cannot add table file #2345 to level 2 since it is "
                          "already in the LSM tree on level 1"));
}

TEST_F(VersionBuilderTest, ApplyFileAdditionAlreadyApplied) {
  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr int level = 3;
  constexpr uint64_t file_number = 2345;
  constexpr uint32_t path_id = 0;
  constexpr uint64_t file_size = 10000;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";
  constexpr SequenceNumber smallest_seqno = 100;
  constexpr SequenceNumber largest_seqno = 1000;
  constexpr bool marked_for_compaction = false;

  edit.AddFile(level, file_number, path_id, file_size, GetInternalKey(smallest),
               GetInternalKey(largest), smallest_seqno, largest_seqno,
               marked_for_compaction, kInvalidBlobFileNumber,
               kUnknownOldestAncesterTime, kUnknownFileCreationTime,
               kUnknownFileChecksum, kUnknownFileChecksumFuncName);

  ASSERT_OK(builder.Apply(&edit));

  VersionEdit other_edit;

  constexpr int new_level = 2;

  other_edit.AddFile(new_level, file_number, path_id, file_size,
                     GetInternalKey(smallest), GetInternalKey(largest),
                     smallest_seqno, largest_seqno, marked_for_compaction,
                     kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                     kUnknownFileCreationTime, kUnknownFileChecksum,
                     kUnknownFileChecksumFuncName);

  const Status s = builder.Apply(&other_edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Cannot add table file #2345 to level 2 since it is "
                          "already in the LSM tree on level 3"));
}

TEST_F(VersionBuilderTest, ApplyFileAdditionAndDeletion) {
  constexpr int level = 1;
  constexpr uint64_t file_number = 2345;
  constexpr uint32_t path_id = 0;
  constexpr uint64_t file_size = 10000;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";
  constexpr SequenceNumber smallest_seqno = 100;
  constexpr SequenceNumber largest_seqno = 1000;
  constexpr bool marked_for_compaction = false;

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit addition;

  addition.AddFile(level, file_number, path_id, file_size,
                   GetInternalKey(smallest), GetInternalKey(largest),
                   smallest_seqno, largest_seqno, marked_for_compaction,
                   kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
                   kUnknownFileCreationTime, kUnknownFileChecksum,
                   kUnknownFileChecksumFuncName);

  ASSERT_OK(builder.Apply(&addition));

  VersionEdit deletion;

  deletion.DeleteFile(level, file_number);

  ASSERT_OK(builder.Apply(&deletion));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  ASSERT_OK(builder.SaveTo(&new_vstorage));
  ASSERT_FALSE(new_vstorage.GetFileLocation(file_number).IsValid());

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyBlobFileAddition) {
  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] = "bdb7f34a59dfa1592ce7f52e99f98c570c525cbd";

  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  // Add dummy table file to ensure the blob file is referenced.
  constexpr uint64_t table_file_number = 1;
  AddDummyFileToEdit(&edit, table_file_number, blob_file_number);

  ASSERT_OK(builder.Apply(&edit));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  const auto& new_blob_files = new_vstorage.GetBlobFiles();
  ASSERT_EQ(new_blob_files.size(), 1);

  const auto new_meta = GetBlobFileMetaData(new_blob_files, blob_file_number);

  ASSERT_NE(new_meta, nullptr);
  ASSERT_EQ(new_meta->GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(new_meta->GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(new_meta->GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(new_meta->GetChecksumMethod(), checksum_method);
  ASSERT_EQ(new_meta->GetChecksumValue(), checksum_value);
  ASSERT_EQ(new_meta->GetLinkedSsts(),
            BlobFileMetaData::LinkedSsts{table_file_number});
  ASSERT_EQ(new_meta->GetGarbageBlobCount(), 0);
  ASSERT_EQ(new_meta->GetGarbageBlobBytes(), 0);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyBlobFileAdditionAlreadyInBase) {
  // Attempt to add a blob file that is already present in the base version.

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] = "bdb7f34a59dfa1592ce7f52e99f98c570c525cbd";
  constexpr uint64_t garbage_blob_count = 123;
  constexpr uint64_t garbage_blob_bytes = 456789;

  AddBlob(blob_file_number, total_blob_count, total_blob_bytes, checksum_method,
          checksum_value, BlobFileMetaData::LinkedSsts(), garbage_blob_count,
          garbage_blob_bytes);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "Blob file #1234 already added"));
}

TEST_F(VersionBuilderTest, ApplyBlobFileAdditionAlreadyApplied) {
  // Attempt to add the same blob file twice using version edits.

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] = "bdb7f34a59dfa1592ce7f52e99f98c570c525cbd";

  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  ASSERT_OK(builder.Apply(&edit));

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "Blob file #1234 already added"));
}

TEST_F(VersionBuilderTest, ApplyBlobFileGarbageFileInBase) {
  // Increase the amount of garbage for a blob file present in the base version.

  constexpr uint64_t table_file_number = 1;
  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] = "bdb7f34a59dfa1592ce7f52e99f98c570c525cbd";
  constexpr uint64_t garbage_blob_count = 123;
  constexpr uint64_t garbage_blob_bytes = 456789;

  AddBlob(blob_file_number, total_blob_count, total_blob_bytes, checksum_method,
          checksum_value, BlobFileMetaData::LinkedSsts{table_file_number},
          garbage_blob_count, garbage_blob_bytes);

  const auto meta =
      GetBlobFileMetaData(vstorage_.GetBlobFiles(), blob_file_number);
  ASSERT_NE(meta, nullptr);

  // Add dummy table file to ensure the blob file is referenced.
  AddDummyFile(table_file_number, blob_file_number);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr uint64_t new_garbage_blob_count = 456;
  constexpr uint64_t new_garbage_blob_bytes = 111111;

  edit.AddBlobFileGarbage(blob_file_number, new_garbage_blob_count,
                          new_garbage_blob_bytes);

  ASSERT_OK(builder.Apply(&edit));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  const auto& new_blob_files = new_vstorage.GetBlobFiles();
  ASSERT_EQ(new_blob_files.size(), 1);

  const auto new_meta = GetBlobFileMetaData(new_blob_files, blob_file_number);

  ASSERT_NE(new_meta, nullptr);
  ASSERT_EQ(new_meta->GetSharedMeta(), meta->GetSharedMeta());
  ASSERT_EQ(new_meta->GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(new_meta->GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(new_meta->GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(new_meta->GetChecksumMethod(), checksum_method);
  ASSERT_EQ(new_meta->GetChecksumValue(), checksum_value);
  ASSERT_EQ(new_meta->GetLinkedSsts(),
            BlobFileMetaData::LinkedSsts{table_file_number});
  ASSERT_EQ(new_meta->GetGarbageBlobCount(),
            garbage_blob_count + new_garbage_blob_count);
  ASSERT_EQ(new_meta->GetGarbageBlobBytes(),
            garbage_blob_bytes + new_garbage_blob_bytes);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyBlobFileGarbageFileAdditionApplied) {
  // Increase the amount of garbage for a blob file added using a version edit.

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit addition;

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] = "bdb7f34a59dfa1592ce7f52e99f98c570c525cbd";

  addition.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                       checksum_method, checksum_value);

  // Add dummy table file to ensure the blob file is referenced.
  constexpr uint64_t table_file_number = 1;
  AddDummyFileToEdit(&addition, table_file_number, blob_file_number);

  ASSERT_OK(builder.Apply(&addition));

  constexpr uint64_t garbage_blob_count = 123;
  constexpr uint64_t garbage_blob_bytes = 456789;

  VersionEdit garbage;

  garbage.AddBlobFileGarbage(blob_file_number, garbage_blob_count,
                             garbage_blob_bytes);

  ASSERT_OK(builder.Apply(&garbage));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  const auto& new_blob_files = new_vstorage.GetBlobFiles();
  ASSERT_EQ(new_blob_files.size(), 1);

  const auto new_meta = GetBlobFileMetaData(new_blob_files, blob_file_number);

  ASSERT_NE(new_meta, nullptr);
  ASSERT_EQ(new_meta->GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(new_meta->GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(new_meta->GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(new_meta->GetChecksumMethod(), checksum_method);
  ASSERT_EQ(new_meta->GetChecksumValue(), checksum_value);
  ASSERT_EQ(new_meta->GetLinkedSsts(),
            BlobFileMetaData::LinkedSsts{table_file_number});
  ASSERT_EQ(new_meta->GetGarbageBlobCount(), garbage_blob_count);
  ASSERT_EQ(new_meta->GetGarbageBlobBytes(), garbage_blob_bytes);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyBlobFileGarbageFileNotFound) {
  // Attempt to increase the amount of garbage for a blob file that is
  // neither in the base version, nor was it added using a version edit.

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t garbage_blob_count = 5678;
  constexpr uint64_t garbage_blob_bytes = 999999;

  edit.AddBlobFileGarbage(blob_file_number, garbage_blob_count,
                          garbage_blob_bytes);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "Blob file #1234 not found"));
}

TEST_F(VersionBuilderTest, SaveBlobFilesTo) {
  // Add three blob files to base version.
  for (uint64_t i = 3; i >= 1; --i) {
    const uint64_t table_file_number = i;
    const uint64_t blob_file_number = i;
    const uint64_t total_blob_count = i * 1000;
    const uint64_t total_blob_bytes = i * 1000000;
    const uint64_t garbage_blob_count = i * 100;
    const uint64_t garbage_blob_bytes = i * 20000;

    AddBlob(blob_file_number, total_blob_count, total_blob_bytes,
            /* checksum_method */ std::string(),
            /* checksum_value */ std::string(),
            BlobFileMetaData::LinkedSsts{table_file_number}, garbage_blob_count,
            garbage_blob_bytes);

    // Add dummy table file to ensure the blob file is referenced.
    AddDummyFile(table_file_number, blob_file_number);
  }

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  // Add some garbage to the second and third blob files. The second blob file
  // remains valid since it does not consist entirely of garbage yet. The third
  // blob file is all garbage after the edit and will not be part of the new
  // version. The corresponding dummy table file is also removed for
  // consistency.
  edit.AddBlobFileGarbage(/* blob_file_number */ 2,
                          /* garbage_blob_count */ 200,
                          /* garbage_blob_bytes */ 100000);
  edit.AddBlobFileGarbage(/* blob_file_number */ 3,
                          /* garbage_blob_count */ 2700,
                          /* garbage_blob_bytes */ 2940000);
  edit.DeleteFile(/* level */ 0, /* file_number */ 3);

  // Add a fourth blob file.
  edit.AddBlobFile(/* blob_file_number */ 4, /* total_blob_count */ 4000,
                   /* total_blob_bytes */ 4000000,
                   /* checksum_method */ std::string(),
                   /* checksum_value */ std::string());

  ASSERT_OK(builder.Apply(&edit));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  const auto& new_blob_files = new_vstorage.GetBlobFiles();
  ASSERT_EQ(new_blob_files.size(), 3);

  const auto meta1 = GetBlobFileMetaData(new_blob_files, 1);

  ASSERT_NE(meta1, nullptr);
  ASSERT_EQ(meta1->GetBlobFileNumber(), 1);
  ASSERT_EQ(meta1->GetTotalBlobCount(), 1000);
  ASSERT_EQ(meta1->GetTotalBlobBytes(), 1000000);
  ASSERT_EQ(meta1->GetGarbageBlobCount(), 100);
  ASSERT_EQ(meta1->GetGarbageBlobBytes(), 20000);

  const auto meta2 = GetBlobFileMetaData(new_blob_files, 2);

  ASSERT_NE(meta2, nullptr);
  ASSERT_EQ(meta2->GetBlobFileNumber(), 2);
  ASSERT_EQ(meta2->GetTotalBlobCount(), 2000);
  ASSERT_EQ(meta2->GetTotalBlobBytes(), 2000000);
  ASSERT_EQ(meta2->GetGarbageBlobCount(), 400);
  ASSERT_EQ(meta2->GetGarbageBlobBytes(), 140000);

  const auto meta4 = GetBlobFileMetaData(new_blob_files, 4);

  ASSERT_NE(meta4, nullptr);
  ASSERT_EQ(meta4->GetBlobFileNumber(), 4);
  ASSERT_EQ(meta4->GetTotalBlobCount(), 4000);
  ASSERT_EQ(meta4->GetTotalBlobBytes(), 4000000);
  ASSERT_EQ(meta4->GetGarbageBlobCount(), 0);
  ASSERT_EQ(meta4->GetGarbageBlobBytes(), 0);

  // Delete the first table file, which makes the first blob file obsolete
  // since it's at the head and unreferenced.
  VersionBuilder second_builder(env_options, &ioptions_, table_cache,
                                &new_vstorage, version_set);

  VersionEdit second_edit;
  second_edit.DeleteFile(/* level */ 0, /* file_number */ 1);

  ASSERT_OK(second_builder.Apply(&second_edit));

  VersionStorageInfo newer_vstorage(&icmp_, ucmp_, options_.num_levels,
                                    kCompactionStyleLevel, &new_vstorage,
                                    force_consistency_checks);

  ASSERT_OK(second_builder.SaveTo(&newer_vstorage));

  const auto& newer_blob_files = newer_vstorage.GetBlobFiles();
  ASSERT_EQ(newer_blob_files.size(), 2);

  const auto newer_meta1 = GetBlobFileMetaData(newer_blob_files, 1);

  ASSERT_EQ(newer_meta1, nullptr);

  UnrefFilesInVersion(&newer_vstorage);
  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, SaveBlobFilesToConcurrentJobs) {
  // When multiple background jobs (flushes/compactions) are executing in
  // parallel, it is possible for the VersionEdit adding blob file K to be
  // applied *after* the VersionEdit adding blob file N (for N > K). This test
  // case makes sure this is handled correctly.

  // Add blob file #4 (referenced by table file #3) to base version.
  constexpr uint64_t base_table_file_number = 3;
  constexpr uint64_t base_blob_file_number = 4;
  constexpr uint64_t base_total_blob_count = 100;
  constexpr uint64_t base_total_blob_bytes = 1 << 20;

  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] = "\xfa\xce\xb0\x0c";
  constexpr uint64_t garbage_blob_count = 0;
  constexpr uint64_t garbage_blob_bytes = 0;

  AddDummyFile(base_table_file_number, base_blob_file_number);
  AddBlob(base_blob_file_number, base_total_blob_count, base_total_blob_bytes,
          checksum_method, checksum_value,
          BlobFileMetaData::LinkedSsts{base_table_file_number},
          garbage_blob_count, garbage_blob_bytes);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  // Add blob file #2 (referenced by table file #1).
  constexpr int level = 0;
  constexpr uint64_t table_file_number = 1;
  constexpr uint32_t path_id = 0;
  constexpr uint64_t file_size = 1 << 12;
  constexpr char smallest[] = "key1";
  constexpr char largest[] = "key987";
  constexpr SequenceNumber smallest_seqno = 0;
  constexpr SequenceNumber largest_seqno = 0;
  constexpr bool marked_for_compaction = false;

  constexpr uint64_t blob_file_number = 2;
  static_assert(blob_file_number < base_blob_file_number,
                "Added blob file should have a smaller file number");

  constexpr uint64_t total_blob_count = 234;
  constexpr uint64_t total_blob_bytes = 1 << 22;

  edit.AddFile(level, table_file_number, path_id, file_size,
               GetInternalKey(smallest), GetInternalKey(largest),
               smallest_seqno, largest_seqno, marked_for_compaction,
               blob_file_number, kUnknownOldestAncesterTime,
               kUnknownFileCreationTime, checksum_value, checksum_method);
  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  ASSERT_OK(builder.Apply(&edit));

  constexpr bool force_consistency_checks = true;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  const auto& new_blob_files = new_vstorage.GetBlobFiles();
  ASSERT_EQ(new_blob_files.size(), 2);

  const auto base_meta =
      GetBlobFileMetaData(new_blob_files, base_blob_file_number);

  ASSERT_NE(base_meta, nullptr);
  ASSERT_EQ(base_meta->GetBlobFileNumber(), base_blob_file_number);
  ASSERT_EQ(base_meta->GetTotalBlobCount(), base_total_blob_count);
  ASSERT_EQ(base_meta->GetTotalBlobBytes(), base_total_blob_bytes);
  ASSERT_EQ(base_meta->GetGarbageBlobCount(), garbage_blob_count);
  ASSERT_EQ(base_meta->GetGarbageBlobBytes(), garbage_blob_bytes);
  ASSERT_EQ(base_meta->GetChecksumMethod(), checksum_method);
  ASSERT_EQ(base_meta->GetChecksumValue(), checksum_value);

  const auto added_meta = GetBlobFileMetaData(new_blob_files, blob_file_number);

  ASSERT_NE(added_meta, nullptr);
  ASSERT_EQ(added_meta->GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(added_meta->GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(added_meta->GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(added_meta->GetGarbageBlobCount(), garbage_blob_count);
  ASSERT_EQ(added_meta->GetGarbageBlobBytes(), garbage_blob_bytes);
  ASSERT_EQ(added_meta->GetChecksumMethod(), checksum_method);
  ASSERT_EQ(added_meta->GetChecksumValue(), checksum_value);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, CheckConsistencyForBlobFiles) {
  // Initialize base version. The first table file points to a valid blob file
  // in this version; the second one does not refer to any blob files.

  Add(/* level */ 1, /* file_number */ 1, /* smallest */ "150",
      /* largest */ "200", /* file_size */ 100,
      /* path_id */ 0, /* smallest_seq */ 100, /* largest_seq */ 100,
      /* num_entries */ 0, /* num_deletions */ 0,
      /* sampled */ false, /* smallest_seqno */ 100, /* largest_seqno */ 100,
      /* oldest_blob_file_number */ 16);
  Add(/* level */ 1, /* file_number */ 23, /* smallest */ "201",
      /* largest */ "300", /* file_size */ 100,
      /* path_id */ 0, /* smallest_seq */ 200, /* largest_seq */ 200,
      /* num_entries */ 0, /* num_deletions */ 0,
      /* sampled */ false, /* smallest_seqno */ 200, /* largest_seqno */ 200,
      kInvalidBlobFileNumber);

  AddBlob(/* blob_file_number */ 16, /* total_blob_count */ 1000,
          /* total_blob_bytes */ 1000000,
          /* checksum_method */ std::string(),
          /* checksum_value */ std::string(), BlobFileMetaData::LinkedSsts{1},
          /* garbage_blob_count */ 500, /* garbage_blob_bytes */ 300000);

  UpdateVersionStorageInfo();

  // Add a new table file that points to the existing blob file, and add a
  // new table file--blob file pair.
  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  edit.AddFile(/* level */ 1, /* file_number */ 606, /* path_id */ 0,
               /* file_size */ 100, /* smallest */ GetInternalKey("701"),
               /* largest */ GetInternalKey("750"), /* smallest_seqno */ 200,
               /* largest_seqno */ 200, /* marked_for_compaction */ false,
               /* oldest_blob_file_number */ 16, kUnknownOldestAncesterTime,
               kUnknownFileCreationTime, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName);

  edit.AddFile(/* level */ 1, /* file_number */ 700, /* path_id */ 0,
               /* file_size */ 100, /* smallest */ GetInternalKey("801"),
               /* largest */ GetInternalKey("850"), /* smallest_seqno */ 200,
               /* largest_seqno */ 200, /* marked_for_compaction */ false,
               /* oldest_blob_file_number */ 1000, kUnknownOldestAncesterTime,
               kUnknownFileCreationTime, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName);
  edit.AddBlobFile(/* blob_file_number */ 1000, /* total_blob_count */ 2000,
                   /* total_blob_bytes */ 200000,
                   /* checksum_method */ std::string(),
                   /* checksum_value */ std::string());

  ASSERT_OK(builder.Apply(&edit));

  // Save to a new version in order to trigger consistency checks.
  constexpr bool force_consistency_checks = true;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, CheckConsistencyForBlobFilesInconsistentLinks) {
  // Initialize base version. Links between the table file and the blob file
  // are inconsistent.

  Add(/* level */ 1, /* file_number */ 1, /* smallest */ "150",
      /* largest */ "200", /* file_size */ 100,
      /* path_id */ 0, /* smallest_seq */ 100, /* largest_seq */ 100,
      /* num_entries */ 0, /* num_deletions */ 0,
      /* sampled */ false, /* smallest_seqno */ 100, /* largest_seqno */ 100,
      /* oldest_blob_file_number */ 256);

  AddBlob(/* blob_file_number */ 16, /* total_blob_count */ 1000,
          /* total_blob_bytes */ 1000000,
          /* checksum_method */ std::string(),
          /* checksum_value */ std::string(), BlobFileMetaData::LinkedSsts{1},
          /* garbage_blob_count */ 500, /* garbage_blob_bytes */ 300000);

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  // Save to a new version in order to trigger consistency checks.
  constexpr bool force_consistency_checks = true;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  const Status s = builder.SaveTo(&new_vstorage);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(
      s.getState(),
      "Links are inconsistent between table files and blob file #16"));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, CheckConsistencyForBlobFilesAllGarbage) {
  // Initialize base version. The table file points to a blob file that is
  // all garbage.

  Add(/* level */ 1, /* file_number */ 1, /* smallest */ "150",
      /* largest */ "200", /* file_size */ 100,
      /* path_id */ 0, /* smallest_seq */ 100, /* largest_seq */ 100,
      /* num_entries */ 0, /* num_deletions */ 0,
      /* sampled */ false, /* smallest_seqno */ 100, /* largest_seqno */ 100,
      /* oldest_blob_file_number */ 16);

  AddBlob(/* blob_file_number */ 16, /* total_blob_count */ 1000,
          /* total_blob_bytes */ 1000000,
          /* checksum_method */ std::string(),
          /* checksum_value */ std::string(), BlobFileMetaData::LinkedSsts{1},
          /* garbage_blob_count */ 1000, /* garbage_blob_bytes */ 1000000);

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  // Save to a new version in order to trigger consistency checks.
  constexpr bool force_consistency_checks = true;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  const Status s = builder.SaveTo(&new_vstorage);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(
      std::strstr(s.getState(), "Blob file #16 consists entirely of garbage"));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, CheckConsistencyForBlobFilesAllGarbageLinkedSsts) {
  // Initialize base version, with a table file pointing to a blob file
  // that has no garbage at this point.

  Add(/* level */ 1, /* file_number */ 1, /* smallest */ "150",
      /* largest */ "200", /* file_size */ 100,
      /* path_id */ 0, /* smallest_seq */ 100, /* largest_seq */ 100,
      /* num_entries */ 0, /* num_deletions */ 0,
      /* sampled */ false, /* smallest_seqno */ 100, /* largest_seqno */ 100,
      /* oldest_blob_file_number */ 16);

  AddBlob(/* blob_file_number */ 16, /* total_blob_count */ 1000,
          /* total_blob_bytes */ 1000000,
          /* checksum_method */ std::string(),
          /* checksum_value */ std::string(), BlobFileMetaData::LinkedSsts{1},
          /* garbage_blob_count */ 0, /* garbage_blob_bytes */ 0);

  UpdateVersionStorageInfo();

  // Mark the entire blob file garbage but do not remove the linked SST.
  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  edit.AddBlobFileGarbage(/* blob_file_number */ 16,
                          /* garbage_blob_count */ 1000,
                          /* garbage_blob_bytes */ 1000000);

  ASSERT_OK(builder.Apply(&edit));

  // Save to a new version in order to trigger consistency checks.
  constexpr bool force_consistency_checks = true;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  const Status s = builder.SaveTo(&new_vstorage);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(
      std::strstr(s.getState(), "Blob file #16 consists entirely of garbage"));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, MaintainLinkedSstsForBlobFiles) {
  // Initialize base version. Table files 1..10 are linked to blob files 1..5,
  // while table files 11..20 are not linked to any blob files.

  for (uint64_t i = 1; i <= 10; ++i) {
    std::ostringstream oss;
    oss << std::setw(2) << std::setfill('0') << i;

    const std::string key = oss.str();

    Add(/* level */ 1, /* file_number */ i, /* smallest */ key.c_str(),
        /* largest */ key.c_str(), /* file_size */ 100,
        /* path_id */ 0, /* smallest_seq */ i * 100, /* largest_seq */ i * 100,
        /* num_entries */ 0, /* num_deletions */ 0,
        /* sampled */ false, /* smallest_seqno */ i * 100,
        /* largest_seqno */ i * 100,
        /* oldest_blob_file_number */ ((i - 1) % 5) + 1);
  }

  for (uint64_t i = 1; i <= 5; ++i) {
    AddBlob(/* blob_file_number */ i, /* total_blob_count */ 2000,
            /* total_blob_bytes */ 2000000,
            /* checksum_method */ std::string(),
            /* checksum_value */ std::string(),
            BlobFileMetaData::LinkedSsts{i, i + 5},
            /* garbage_blob_count */ 1000, /* garbage_blob_bytes */ 1000000);
  }

  for (uint64_t i = 11; i <= 20; ++i) {
    std::ostringstream oss;
    oss << std::setw(2) << std::setfill('0') << i;

    const std::string key = oss.str();

    Add(/* level */ 1, /* file_number */ i, /* smallest */ key.c_str(),
        /* largest */ key.c_str(), /* file_size */ 100,
        /* path_id */ 0, /* smallest_seq */ i * 100, /* largest_seq */ i * 100,
        /* num_entries */ 0, /* num_deletions */ 0,
        /* sampled */ false, /* smallest_seqno */ i * 100,
        /* largest_seqno */ i * 100, kInvalidBlobFileNumber);
  }

  UpdateVersionStorageInfo();

  {
    const auto& blob_files = vstorage_.GetBlobFiles();
    ASSERT_EQ(blob_files.size(), 5);

    const std::vector<BlobFileMetaData::LinkedSsts> expected_linked_ssts{
        {1, 6}, {2, 7}, {3, 8}, {4, 9}, {5, 10}};

    for (size_t i = 0; i < 5; ++i) {
      const auto meta =
          GetBlobFileMetaData(blob_files, /* blob_file_number */ i + 1);
      ASSERT_NE(meta, nullptr);
      ASSERT_EQ(meta->GetLinkedSsts(), expected_linked_ssts[i]);
    }
  }

  VersionEdit edit;

  // Add an SST that references a blob file.
  edit.AddFile(
      /* level */ 1, /* file_number */ 21, /* path_id */ 0,
      /* file_size */ 100, /* smallest */ GetInternalKey("21", 2100),
      /* largest */ GetInternalKey("21", 2100), /* smallest_seqno */ 2100,
      /* largest_seqno */ 2100, /* marked_for_compaction */ false,
      /* oldest_blob_file_number */ 1, kUnknownOldestAncesterTime,
      kUnknownFileCreationTime, kUnknownFileChecksum,
      kUnknownFileChecksumFuncName);

  // Add an SST that does not reference any blob files.
  edit.AddFile(
      /* level */ 1, /* file_number */ 22, /* path_id */ 0,
      /* file_size */ 100, /* smallest */ GetInternalKey("22", 2200),
      /* largest */ GetInternalKey("22", 2200), /* smallest_seqno */ 2200,
      /* largest_seqno */ 2200, /* marked_for_compaction */ false,
      kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
      kUnknownFileCreationTime, kUnknownFileChecksum,
      kUnknownFileChecksumFuncName);

  // Delete a file that references a blob file.
  edit.DeleteFile(/* level */ 1, /* file_number */ 6);

  // Delete a file that does not reference any blob files.
  edit.DeleteFile(/* level */ 1, /* file_number */ 16);

  // Trivially move a file that references a blob file. Note that we save
  // the original BlobFileMetaData object so we can check that no new object
  // gets created.
  auto meta3 =
      GetBlobFileMetaData(vstorage_.GetBlobFiles(), /* blob_file_number */ 3);

  edit.DeleteFile(/* level */ 1, /* file_number */ 3);
  edit.AddFile(/* level */ 2, /* file_number */ 3, /* path_id */ 0,
               /* file_size */ 100, /* smallest */ GetInternalKey("03", 300),
               /* largest */ GetInternalKey("03", 300),
               /* smallest_seqno */ 300,
               /* largest_seqno */ 300, /* marked_for_compaction */ false,
               /* oldest_blob_file_number */ 3, kUnknownOldestAncesterTime,
               kUnknownFileCreationTime, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName);

  // Trivially move a file that does not reference any blob files.
  edit.DeleteFile(/* level */ 1, /* file_number */ 13);
  edit.AddFile(/* level */ 2, /* file_number */ 13, /* path_id */ 0,
               /* file_size */ 100, /* smallest */ GetInternalKey("13", 1300),
               /* largest */ GetInternalKey("13", 1300),
               /* smallest_seqno */ 1300,
               /* largest_seqno */ 1300, /* marked_for_compaction */ false,
               kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
               kUnknownFileCreationTime, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName);

  // Add one more SST file that references a blob file, then promptly
  // delete it in a second version edit before the new version gets saved.
  // This file should not show up as linked to the blob file in the new version.
  edit.AddFile(/* level */ 1, /* file_number */ 23, /* path_id */ 0,
               /* file_size */ 100, /* smallest */ GetInternalKey("23", 2300),
               /* largest */ GetInternalKey("23", 2300),
               /* smallest_seqno */ 2300,
               /* largest_seqno */ 2300, /* marked_for_compaction */ false,
               /* oldest_blob_file_number */ 5, kUnknownOldestAncesterTime,
               kUnknownFileCreationTime, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName);

  VersionEdit edit2;

  edit2.DeleteFile(/* level */ 1, /* file_number */ 23);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  ASSERT_OK(builder.Apply(&edit));
  ASSERT_OK(builder.Apply(&edit2));

  constexpr bool force_consistency_checks = true;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  {
    const auto& blob_files = new_vstorage.GetBlobFiles();
    ASSERT_EQ(blob_files.size(), 5);

    const std::vector<BlobFileMetaData::LinkedSsts> expected_linked_ssts{
        {1, 21}, {2, 7}, {3, 8}, {4, 9}, {5, 10}};

    for (size_t i = 0; i < 5; ++i) {
      const auto meta =
          GetBlobFileMetaData(blob_files, /* blob_file_number */ i + 1);
      ASSERT_NE(meta, nullptr);
      ASSERT_EQ(meta->GetLinkedSsts(), expected_linked_ssts[i]);
    }

    // Make sure that no new BlobFileMetaData got created for the blob file
    // affected by the trivial move.
    ASSERT_EQ(GetBlobFileMetaData(blob_files, /* blob_file_number */ 3), meta3);
  }

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, CheckConsistencyForFileDeletedTwice) {
  Add(0, 1U, "150", "200", 100U);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.DeleteFile(0, 1U);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder version_builder(env_options, &ioptions_, table_cache,
                                 &vstorage_, version_set);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr,
                                  true /* force_consistency_checks */);
  ASSERT_OK(version_builder.Apply(&version_edit));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  VersionBuilder version_builder2(env_options, &ioptions_, table_cache,
                                 &new_vstorage, version_set);
  VersionStorageInfo new_vstorage2(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr,
                                  true /* force_consistency_checks */);
  ASSERT_NOK(version_builder2.Apply(&version_edit));

  UnrefFilesInVersion(&new_vstorage);
  UnrefFilesInVersion(&new_vstorage2);
}

TEST_F(VersionBuilderTest, EstimatedActiveKeys) {
  const uint32_t kTotalSamples = 20;
  const uint32_t kNumLevels = 5;
  const uint32_t kFilesPerLevel = 8;
  const uint32_t kNumFiles = kNumLevels * kFilesPerLevel;
  const uint32_t kEntriesPerFile = 1000;
  const uint32_t kDeletionsPerFile = 100;
  for (uint32_t i = 0; i < kNumFiles; ++i) {
    Add(static_cast<int>(i / kFilesPerLevel), i + 1,
        ToString((i + 100) * 1000).c_str(),
        ToString((i + 100) * 1000 + 999).c_str(),
        100U,  0, 100, 100,
        kEntriesPerFile, kDeletionsPerFile,
        (i < kTotalSamples));
  }
  // minus 2X for the number of deletion entries because:
  // 1x for deletion entry does not count as a data entry.
  // 1x for each deletion entry will actually remove one data entry.
  ASSERT_EQ(vstorage_.GetEstimatedActiveKeys(),
            (kEntriesPerFile - 2 * kDeletionsPerFile) * kNumFiles);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
