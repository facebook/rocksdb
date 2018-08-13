//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <string>
#include "db/version_edit.h"
#include "db/version_set.h"
#include "util/logging.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

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

  ~VersionBuilderTest() {
    for (int i = 0; i <= vstorage_.num_levels(); i++) {
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
           SequenceNumber largest_seqno = 0, uint8_t sst_variety = 0,
           const std::vector<uint64_t>& sst_depend = {}) {
    assert(level <= vstorage_.num_levels());
    FileMetaData* f = new FileMetaData;
    f->fd = FileDescriptor(file_number, path_id, file_size);
    f->smallest = GetInternalKey(smallest, smallest_seq);
    f->largest = GetInternalKey(largest, largest_seq);
    f->fd.smallest_seqno = smallest_seqno;
    f->fd.largest_seqno = largest_seqno;
    f->sst_variety = sst_variety;
    f->sst_depend = sst_depend;
    f->compensated_file_size = file_size;
    f->refs = 0;
    f->num_entries = num_entries;
    f->num_deletions = num_deletions;
    vstorage_.AddFile(level, f);
    if (sampled) {
      f->init_stats_from_file = true;
      vstorage_.UpdateAccumulatedStats(f);
    }
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
  for (int i = 0; i <= new_vstorage->num_levels(); i++) {
    for (auto* f : new_vstorage->LevelFiles(i)) {
      if (--f->refs == 0) {
        delete f;
      }
    }
  }
}

bool VerifyDependFiles(VersionStorageInfo* new_vstorage,
                       const std::vector<uint64_t>& depend_files) {
  auto& vstorage_depend_files = new_vstorage->depend_files();
  if (vstorage_depend_files.size() != depend_files.size()) {
    return false;
  }
  for (auto sst_id : depend_files) {
    if (vstorage_depend_files.count(sst_id) == 0) {
      return false;
    }
  }
  return true;
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
                       GetInternalKey("350"), 200, 200, false, 1, {27U});
  version_edit.DeleteFile(3, 27U);

  EnvOptions env_options;

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage);

  ASSERT_EQ(400U, new_vstorage.NumLevelBytes(2));
  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(3));
  ASSERT_TRUE(VerifyDependFiles(&new_vstorage, {27U}));

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
                       GetInternalKey("350"), 200, 200, false, 1, {1U});
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
  ASSERT_TRUE(VerifyDependFiles(&new_vstorage, {1U}));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyAndSaveToDynamic2) {
  ioptions_.level_compaction_dynamic_level_bytes = true;

  Add(0, 1U, "150", "200", 100U, 0, 200U, 200U, 0, 0, false, 200U, 200U);
  Add(0, 88U, "201", "300", 100U, 0, 100U, 100U, 0, 0, false, 100U, 100U,
      1, {4U, 5U});

  Add(4, 6U, "150", "179", 100U);
  Add(4, 7U, "180", "220", 100U);
  Add(4, 8U, "221", "300", 100U);

  Add(5, 26U, "150", "170", 100U);
  Add(5, 27U, "171", "179", 100U);

  Add(vstorage_.num_levels(), 4U, "90", "119", 100U);
  Add(vstorage_.num_levels(), 5U, "120", "149", 100U);

  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(4, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false, 1, {1U, 4U});
  version_edit.AddFile(4, 5, 0, 100U, GetInternalKey("120"),
                       GetInternalKey("149"), 200, 200, false, 0, {});
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
  ASSERT_EQ(200U, new_vstorage.NumLevelBytes(4));
  ASSERT_EQ(200U, new_vstorage.NumLevelBytes(5));
  ASSERT_TRUE(VerifyDependFiles(&new_vstorage, {1U, 4U}));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyAndSaveToDynamic3) {
  ioptions_.level_compaction_dynamic_level_bytes = true;

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(1, 11U, 0, 100U, GetInternalKey("100"),
                       GetInternalKey("119"), 2, 2, false, 1,
                       {4U, 5U});
  version_edit.AddFile(1, 12U, 0, 100U, GetInternalKey("120"),
                       GetInternalKey("129"), 2, 2, false, 0, {});
  version_edit.AddFile(1, 13U, 0, 100U, GetInternalKey("130"),
                       GetInternalKey("139"), 2, 2, false, 0, {});
  version_edit.AddFile(1, 14U, 0, 100U, GetInternalKey("140"),
                       GetInternalKey("149"), 2, 2, false, 0, {});
  version_edit.AddFile(1, 15U, 0, 100U, GetInternalKey("150"),
                       GetInternalKey("149"), 2, 2, false, 0, {});

  
  version_edit.AddFile(vstorage_.num_levels(), 2U, 0, 100U,
                       GetInternalKey("100"), GetInternalKey("109"), 2, 2,
                       false, 0, {});
  version_edit.AddFile(vstorage_.num_levels(), 4U, 0, 50U,
                       GetInternalKey("100"), GetInternalKey("114"), 2, 2,
                       false, 1, {2U});
  version_edit.AddFile(vstorage_.num_levels(), 5U, 0, 50U,
                       GetInternalKey("115"), GetInternalKey("119"), 2, 2,
                       false, 0, {});
  version_builder.Apply(&version_edit);

  VersionEdit version_edit2;
  version_edit2.AddFile(2, 21U, 0, 100U, GetInternalKey("110"),
                        GetInternalKey("159"), 2, 2, false, 1,
                        {11U, 12U, 13U, 14U, 15U});
  Add(vstorage_.num_levels(), 31U, "115", "119", 50U);
  version_edit2.DeleteFile(1, 11U);
  version_edit2.DeleteFile(1, 12U);
  version_edit2.DeleteFile(1, 13U);
  version_edit2.DeleteFile(1, 14U);
  version_edit2.DeleteFile(1, 15U);
  version_builder.Apply(&version_edit2);

  VersionEdit version_edit3;
  version_edit3.AddFile(2, 22U, 0, 100U, GetInternalKey("100"),
                        GetInternalKey("159"), 2, 2, false, 1,
                        {4U, 5U, 12U, 13U, 14U, 15U});
  version_edit3.DeleteFile(2, 21U);
  version_builder.Apply(&version_edit3);

  VersionEdit version_edit4;
  version_edit4.AddFile(2, 23U, 0, 100U, GetInternalKey("140"),
                        GetInternalKey("159"), 2, 2, false, 1,
                        {4U, 12U, 13U, 14U, 15U});
  version_edit4.AddFile(2, 5U, 0, 50U, GetInternalKey("115"),
                        GetInternalKey("119"), 2, 2, false, 0, {});
  version_edit4.DeleteFile(2, 22U);
  version_builder.Apply(&version_edit4);

  VersionEdit version_edit5;
  version_edit5.AddFile(2, 24U, 0, 100U, GetInternalKey("140"),
                        GetInternalKey("159"), 2, 2, false, 1,
                        {14U, 15U});
  version_edit5.DeleteFile(2, 23U);
  version_builder.Apply(&version_edit5);


  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  version_builder.SaveTo(&new_vstorage);

  ASSERT_EQ(0U, new_vstorage.NumLevelBytes(1));
  ASSERT_EQ(150U, new_vstorage.NumLevelBytes(2));
  ASSERT_TRUE(VerifyDependFiles(&new_vstorage, {14U, 15U}));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyMultipleAndSaveTo) {
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(2, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false, 0, {});
  version_edit.AddFile(2, 676, 0, 100U, GetInternalKey("401"),
                       GetInternalKey("450"), 200, 200, false, 0, {});
  version_edit.AddFile(2, 636, 0, 100U, GetInternalKey("601"),
                       GetInternalKey("650"), 200, 200, false, 0, {});
  version_edit.AddFile(2, 616, 0, 100U, GetInternalKey("501"),
                       GetInternalKey("550"), 200, 200, false, 0, {});
  version_edit.AddFile(2, 606, 0, 100U, GetInternalKey("701"),
                       GetInternalKey("750"), 200, 200, false, 0, {});

  EnvOptions env_options;

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage);

  ASSERT_EQ(500U, new_vstorage.NumLevelBytes(2));
  ASSERT_TRUE(VerifyDependFiles(&new_vstorage, {}));

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
                       GetInternalKey("350"), 200, 200, false, 0, {});
  version_edit.AddFile(2, 676, 0, 100U, GetInternalKey("401"),
                       GetInternalKey("450"), 200, 200, false, 0, {});
  version_edit.AddFile(2, 636, 0, 100U, GetInternalKey("601"),
                       GetInternalKey("650"), 200, 200, false, 0, {});
  version_edit.AddFile(2, 616, 0, 100U, GetInternalKey("501"),
                       GetInternalKey("550"), 200, 200, false, 0, {});
  version_edit.AddFile(2, 606, 0, 100U, GetInternalKey("701"),
                       GetInternalKey("750"), 200, 200, false, 0, {});
  version_builder.Apply(&version_edit);

  VersionEdit version_edit2;
  version_edit.AddFile(2, 808, 0, 100U, GetInternalKey("901"),
                       GetInternalKey("950"), 200, 200, false, 0, {});
  version_edit2.DeleteFile(2, 616);
  version_edit2.DeleteFile(2, 636);
  version_edit.AddFile(2, 806, 0, 100U, GetInternalKey("801"),
                       GetInternalKey("850"), 200, 200, false, 0, {});
  version_builder.Apply(&version_edit2);

  version_builder.SaveTo(&new_vstorage);

  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(2));
  ASSERT_TRUE(VerifyDependFiles(&new_vstorage, {}));

  UnrefFilesInVersion(&new_vstorage);
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

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
