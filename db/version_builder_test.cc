//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstring>
#include <memory>
#include <string>
#include "db/version_edit.h"
#include "db/version_set.h"
#include "logging/logging.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class VersionBuilderTest : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  Options options_;
  ImmutableCFOptions ioptions_;
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

  void Add(int level, uint32_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 0, uint32_t path_id = 0,
           SequenceNumber smallest_seq = 100, SequenceNumber largest_seq = 100,
           uint64_t num_entries = 0, uint64_t num_deletions = 0,
           bool sampled = false, SequenceNumber smallest_seqno = 0,
           SequenceNumber largest_seqno = 0) {
    assert(level < vstorage_.num_levels());
    FileMetaData* f = new FileMetaData(
        file_number, path_id, file_size, GetInternalKey(smallest, smallest_seq),
        GetInternalKey(largest, largest_seq), smallest_seqno, largest_seqno,
        /* marked_for_compact */ false, kInvalidBlobFileNumber,
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
               std::string checksum_value, uint64_t garbage_blob_count,
               uint64_t garbage_blob_bytes) {
    auto shared_meta = std::make_shared<SharedBlobFileMetaData>(
        blob_file_number, total_blob_count, total_blob_bytes,
        std::move(checksum_method), std::move(checksum_value));
    auto meta = std::make_shared<BlobFileMetaData>(
        std::move(shared_meta), garbage_blob_count, garbage_blob_bytes);

    vstorage_.AddBlobFile(std::move(meta));
  }

  static std::shared_ptr<BlobFileMetaData> GetBlobFileMetaData(
      const VersionStorageInfo::BlobFiles& blob_files,
      uint64_t blob_file_number) {
    const auto it = blob_files.find(blob_file_number);

    if (it == blob_files.end()) {
      return std::shared_ptr<BlobFileMetaData>();
    }

    return it->second;
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

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage);

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

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage);

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

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage);

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

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage);

  ASSERT_EQ(500U, new_vstorage.NumLevelBytes(2));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyDeleteAndSaveTo) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
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
  version_builder.Apply(&version_edit);

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
  version_builder.Apply(&version_edit2);

  version_builder.SaveTo(&new_vstorage);

  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(2));

  UnrefFilesInVersion(&new_vstorage);
}

// Consistency check:
//  * oldest blob file reference points to valid (first file on level, not first
//  file, also check kInvalidBlobFileNumber)
//  * garbage count
// ApplyBlobFileGarbage:
//  * success (found in base vstorage/changed)
//  * not found
//  * multiple accumulate
// SaveBlobFilesTo:
//  * merging logic: only in base, only in changed, updated in changed, one list
//  shorter cases
//  * all garbage file does not get saved to new version

TEST_F(VersionBuilderTest, ApplyBlobFileAddition) {
  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  VersionBuilder builder(env_options, table_cache, &vstorage_);

  VersionEdit edit;

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] = "bdb7f34a59dfa1592ce7f52e99f98c570c525cbd";

  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  ASSERT_OK(builder.Apply(&edit));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  const auto& blob_files = new_vstorage.GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1);

  const auto meta = GetBlobFileMetaData(blob_files, blob_file_number);

  ASSERT_NE(meta, nullptr);
  ASSERT_EQ(meta->GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(meta->GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(meta->GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(meta->GetChecksumMethod(), checksum_method);
  ASSERT_EQ(meta->GetChecksumValue(), checksum_value);
  ASSERT_EQ(meta->GetGarbageBlobCount(), 0);
  ASSERT_EQ(meta->GetGarbageBlobBytes(), 0);
}

TEST_F(VersionBuilderTest, ApplyBlobFileAdditionAlreadyInBase) {
  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] = "bdb7f34a59dfa1592ce7f52e99f98c570c525cbd";
  constexpr uint64_t garbage_blob_count = 123;
  constexpr uint64_t garbage_blob_bytes = 456789;

  AddBlob(blob_file_number, total_blob_count, total_blob_bytes, checksum_method,
          checksum_value, garbage_blob_count, garbage_blob_bytes);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  VersionBuilder builder(env_options, table_cache, &vstorage_);

  VersionEdit edit;

  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "Blob file #1234 already added"));
}

TEST_F(VersionBuilderTest, ApplyBlobFileAdditionAlreadyApplied) {
  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  VersionBuilder builder(env_options, table_cache, &vstorage_);

  VersionEdit edit;

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] = "bdb7f34a59dfa1592ce7f52e99f98c570c525cbd";

  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  // Attempt to add the same blob file twice.
  ASSERT_OK(builder.Apply(&edit));

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "Blob file #1234 already added"));
}

TEST_F(VersionBuilderTest, ApplyBlobFileGarbageAlreadyInBase) {
  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] = "bdb7f34a59dfa1592ce7f52e99f98c570c525cbd";
  constexpr uint64_t garbage_blob_count = 123;
  constexpr uint64_t garbage_blob_bytes = 456789;

  AddBlob(blob_file_number, total_blob_count, total_blob_bytes, checksum_method,
          checksum_value, garbage_blob_count, garbage_blob_bytes);

  const auto meta =
      GetBlobFileMetaData(vstorage_.GetBlobFiles(), blob_file_number);
  ASSERT_NE(meta, nullptr);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  VersionBuilder builder(env_options, table_cache, &vstorage_);

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
  ASSERT_EQ(new_meta->GetGarbageBlobCount(),
            garbage_blob_count + new_garbage_blob_count);
  ASSERT_EQ(new_meta->GetGarbageBlobBytes(),
            garbage_blob_bytes + new_garbage_blob_bytes);
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
