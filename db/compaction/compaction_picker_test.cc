//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <limits>
#include <string>
#include <utility>

#include "db/compaction/compaction.h"
#include "db/compaction/compaction_picker_fifo.h"
#include "db/compaction/compaction_picker_level.h"
#include "db/compaction/compaction_picker_universal.h"
#include "db/compaction/file_pri.h"
#include "rocksdb/advanced_options.h"
#include "table/unique_id_impl.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class CountingLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* /*format*/, va_list /*ap*/) override { log_count++; }
  size_t log_count;
};

class CompactionPickerTestBase : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  Options options_;
  ImmutableOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  MutableDBOptions mutable_db_options_;
  LevelCompactionPicker level_compaction_picker;
  std::string cf_name_;
  CountingLogger logger_;
  LogBuffer log_buffer_;
  uint32_t file_num_;
  CompactionOptionsFIFO fifo_options_;
  std::unique_ptr<VersionStorageInfo> vstorage_;
  std::vector<std::unique_ptr<FileMetaData>> files_;
  // does not own FileMetaData
  std::unordered_map<uint32_t, std::pair<FileMetaData*, int>> file_map_;
  // input files to compaction process.
  std::vector<CompactionInputFiles> input_files_;
  int compaction_level_start_;

  explicit CompactionPickerTestBase(const Comparator* _ucmp)
      : ucmp_(_ucmp),
        icmp_(ucmp_),
        options_(CreateOptions(ucmp_)),
        ioptions_(options_),
        mutable_cf_options_(options_),
        mutable_db_options_(),
        level_compaction_picker(ioptions_, &icmp_),
        cf_name_("dummy"),
        log_buffer_(InfoLogLevel::INFO_LEVEL, &logger_),
        file_num_(1),
        vstorage_(nullptr) {
    mutable_cf_options_.ttl = 0;
    mutable_cf_options_.periodic_compaction_seconds = 0;
    // ioptions_.compaction_pri = kMinOverlappingRatio has its own set of
    // tests to cover.
    ioptions_.compaction_pri = kByCompensatedSize;
    fifo_options_.max_table_files_size = 1;
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
    ioptions_.cf_paths.emplace_back("dummy",
                                    std::numeric_limits<uint64_t>::max());
  }

  ~CompactionPickerTestBase() override {}

  void NewVersionStorage(int num_levels, CompactionStyle style) {
    DeleteVersionStorage();
    options_.num_levels = num_levels;
    vstorage_.reset(new VersionStorageInfo(
        &icmp_, ucmp_, options_.num_levels, style, nullptr, false,
        EpochNumberRequirement::kMustPresent));
    vstorage_->PrepareForVersionAppend(ioptions_, mutable_cf_options_);
  }

  // Create a new VersionStorageInfo object so we can add mode files and then
  // merge it with the existing VersionStorageInfo
  void AddVersionStorage() {
    temp_vstorage_.reset(new VersionStorageInfo(
        &icmp_, ucmp_, options_.num_levels, ioptions_.compaction_style,
        vstorage_.get(), false, EpochNumberRequirement::kMustPresent));
  }

  void DeleteVersionStorage() {
    vstorage_.reset();
    temp_vstorage_.reset();
    files_.clear();
    file_map_.clear();
    input_files_.clear();
  }

  // REQUIRES: smallest and largest are c-style strings ending with '\0'
  void Add(int level, uint32_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 1, uint32_t path_id = 0,
           SequenceNumber smallest_seq = 100, SequenceNumber largest_seq = 100,
           size_t compensated_file_size = 0, bool marked_for_compact = false,
           Temperature temperature = Temperature::kUnknown,
           uint64_t oldest_ancestor_time = kUnknownOldestAncesterTime,
           Slice ts_of_smallest = Slice(), Slice ts_of_largest = Slice(),
           uint64_t epoch_number = kUnknownEpochNumber) {
    assert(ts_of_smallest.size() == ucmp_->timestamp_size());
    assert(ts_of_largest.size() == ucmp_->timestamp_size());

    VersionStorageInfo* vstorage;
    if (temp_vstorage_) {
      vstorage = temp_vstorage_.get();
    } else {
      vstorage = vstorage_.get();
    }
    assert(level < vstorage->num_levels());
    char* smallest_key_buf = nullptr;
    char* largest_key_buf = nullptr;

    if (!ts_of_smallest.empty()) {
      smallest_key_buf = new char[strlen(smallest) + ucmp_->timestamp_size()];
      memcpy(smallest_key_buf, smallest, strlen(smallest));
      memcpy(smallest_key_buf + strlen(smallest), ts_of_smallest.data(),
             ucmp_->timestamp_size());
      largest_key_buf = new char[strlen(largest) + ucmp_->timestamp_size()];
      memcpy(largest_key_buf, largest, strlen(largest));
      memcpy(largest_key_buf + strlen(largest), ts_of_largest.data(),
             ucmp_->timestamp_size());
    }

    InternalKey smallest_ikey = InternalKey(
        smallest_key_buf ? Slice(smallest_key_buf,
                                 ucmp_->timestamp_size() + strlen(smallest))
                         : smallest,
        smallest_seq, kTypeValue);
    InternalKey largest_ikey = InternalKey(
        largest_key_buf
            ? Slice(largest_key_buf, ucmp_->timestamp_size() + strlen(largest))
            : largest,
        largest_seq, kTypeValue);

    FileMetaData* f = new FileMetaData(
        file_number, path_id, file_size, smallest_ikey, largest_ikey,
        smallest_seq, largest_seq, marked_for_compact, temperature,
        kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
        kUnknownFileCreationTime, epoch_number, kUnknownFileChecksum,
        kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0);
    f->compensated_file_size =
        (compensated_file_size != 0) ? compensated_file_size : file_size;
    f->oldest_ancester_time = oldest_ancestor_time;
    vstorage->AddFile(level, f);
    files_.emplace_back(f);
    file_map_.insert({file_number, {f, level}});

    delete[] smallest_key_buf;
    delete[] largest_key_buf;
  }

  void SetCompactionInputFilesLevels(int level_count, int start_level) {
    input_files_.resize(level_count);
    for (int i = 0; i < level_count; ++i) {
      input_files_[i].level = start_level + i;
    }
    compaction_level_start_ = start_level;
  }

  void AddToCompactionFiles(uint32_t file_number) {
    auto iter = file_map_.find(file_number);
    assert(iter != file_map_.end());
    int level = iter->second.second;
    assert(level < vstorage_->num_levels());
    input_files_[level - compaction_level_start_].files.emplace_back(
        iter->second.first);
  }

  void UpdateVersionStorageInfo() {
    if (temp_vstorage_) {
      VersionBuilder builder(FileOptions(), &ioptions_, nullptr,
                             vstorage_.get(), nullptr);
      ASSERT_OK(builder.SaveTo(temp_vstorage_.get()));
      vstorage_ = std::move(temp_vstorage_);
    }
    vstorage_->PrepareForVersionAppend(ioptions_, mutable_cf_options_);
    vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
    vstorage_->SetFinalized();
  }

 private:
  Options CreateOptions(const Comparator* ucmp) const {
    Options opts;
    opts.comparator = ucmp;
    return opts;
  }

  std::unique_ptr<VersionStorageInfo> temp_vstorage_;
};

class CompactionPickerTest : public CompactionPickerTestBase {
 public:
  explicit CompactionPickerTest()
      : CompactionPickerTestBase(BytewiseComparator()) {}

  ~CompactionPickerTest() override {}
};

class CompactionPickerU64TsTest : public CompactionPickerTestBase {
 public:
  explicit CompactionPickerU64TsTest()
      : CompactionPickerTestBase(test::BytewiseComparatorWithU64TsWrapper()) {}

  ~CompactionPickerU64TsTest() override {}
};

TEST_F(CompactionPickerTest, Empty) {
  NewVersionStorage(6, kCompactionStyleLevel);
  UpdateVersionStorageInfo();
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, Single) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  Add(0, 1U, "p", "q");
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, Level0Trigger) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, Level1Trigger) {
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(1, 66U, "150", "200", 1000000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(66U, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, Level1Trigger2) {
  mutable_cf_options_.target_file_size_base = 10000000000;
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(1, 66U, "150", "200", 1000000001U);
  Add(1, 88U, "201", "300", 1000000000U);
  Add(2, 6U, "150", "179", 1000000000U);
  Add(2, 7U, "180", "220", 1000000000U);
  Add(2, 8U, "221", "300", 1000000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(66U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(uint64_t{1073741824}, compaction->OutputFilePreallocationSize());
}

TEST_F(CompactionPickerTest, LevelMaxScore) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.target_file_size_base = 10000000;
  mutable_cf_options_.max_bytes_for_level_base = 10 * 1024 * 1024;
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);
  Add(0, 1U, "150", "200", 1000000U);
  // Level 1 score 1.2
  Add(1, 66U, "150", "200", 6000000U);
  Add(1, 88U, "201", "300", 6000000U);
  // Level 2 score 1.8. File 7 is the largest. Should be picked
  Add(2, 6U, "150", "179", 60000000U);
  Add(2, 7U, "180", "220", 60000001U);
  Add(2, 8U, "221", "300", 60000000U);
  // Level 3 score slightly larger than 1
  Add(3, 26U, "150", "170", 260000000U);
  Add(3, 27U, "171", "179", 260000000U);
  Add(3, 28U, "191", "220", 260000000U);
  Add(3, 29U, "221", "300", 260000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(7U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(mutable_cf_options_.target_file_size_base +
                mutable_cf_options_.target_file_size_base / 10,
            compaction->OutputFilePreallocationSize());
}

TEST_F(CompactionPickerTest, NeedsCompactionLevel) {
  const int kLevels = 6;
  const int kFileCount = 20;

  for (int level = 0; level < kLevels - 1; ++level) {
    NewVersionStorage(kLevels, kCompactionStyleLevel);
    uint64_t file_size = vstorage_->MaxBytesForLevel(level) * 2 / kFileCount;
    for (int file_count = 1; file_count <= kFileCount; ++file_count) {
      // start a brand new version in each test.
      NewVersionStorage(kLevels, kCompactionStyleLevel);
      for (int i = 0; i < file_count; ++i) {
        Add(level, i, std::to_string((i + 100) * 1000).c_str(),
            std::to_string((i + 100) * 1000 + 999).c_str(), file_size, 0,
            i * 100, i * 100 + 99);
      }
      UpdateVersionStorageInfo();
      ASSERT_EQ(vstorage_->CompactionScoreLevel(0), level);
      ASSERT_EQ(level_compaction_picker.NeedsCompaction(vstorage_.get()),
                vstorage_->CompactionScore(0) >= 1);
      // release the version storage
      DeleteVersionStorage();
    }
  }
}

TEST_F(CompactionPickerTest, Level0TriggerDynamic) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(1, static_cast<int>(compaction->num_input_levels()));
  ASSERT_EQ(num_levels - 1, compaction->output_level());
}

TEST_F(CompactionPickerTest, Level0TriggerDynamic2) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");
  Add(num_levels - 1, 3U, "200", "250", 300U);

  UpdateVersionStorageInfo();
  ASSERT_EQ(vstorage_->base_level(), num_levels - 2);

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(1, static_cast<int>(compaction->num_input_levels()));
  ASSERT_EQ(num_levels - 2, compaction->output_level());
}

TEST_F(CompactionPickerTest, Level0TriggerDynamic3) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");
  Add(num_levels - 1, 3U, "200", "250", 300U);
  Add(num_levels - 1, 4U, "300", "350", 3000U);

  UpdateVersionStorageInfo();
  ASSERT_EQ(vstorage_->base_level(), num_levels - 3);

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(1, static_cast<int>(compaction->num_input_levels()));
  ASSERT_EQ(num_levels - 3, compaction->output_level());
}

TEST_F(CompactionPickerTest, Level0TriggerDynamic4) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;

  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");
  Add(num_levels - 1, 3U, "200", "250", 300U);
  Add(num_levels - 1, 4U, "300", "350", 3000U);
  Add(num_levels - 3, 5U, "150", "180", 3U);
  Add(num_levels - 3, 6U, "181", "300", 3U);
  Add(num_levels - 3, 7U, "400", "450", 3U);

  UpdateVersionStorageInfo();
  ASSERT_EQ(vstorage_->base_level(), num_levels - 3);

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(num_levels - 3, compaction->level(1));
  ASSERT_EQ(5U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(2, static_cast<int>(compaction->num_input_levels()));
  ASSERT_EQ(num_levels - 3, compaction->output_level());
}

TEST_F(CompactionPickerTest, LevelTriggerDynamic4) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(num_levels - 1, 2U, "200", "250", 300U);
  Add(num_levels - 1, 3U, "300", "350", 3000U);
  Add(num_levels - 1, 4U, "400", "450", 3U);
  Add(num_levels - 2, 5U, "150", "180", 300U);
  Add(num_levels - 2, 6U, "181", "350", 500U);
  Add(num_levels - 2, 7U, "400", "450", 200U);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(5U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(0, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(num_levels - 1, compaction->output_level());
}

// Universal and FIFO Compactions are not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
TEST_F(CompactionPickerTest, NeedsCompactionUniversal) {
  NewVersionStorage(1, kCompactionStyleUniversal);
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_EQ(universal_compaction_picker.NeedsCompaction(vstorage_.get()),
            false);

  // verify the trigger given different number of L0 files.
  for (int i = 1;
       i <= mutable_cf_options_.level0_file_num_compaction_trigger * 2; ++i) {
    NewVersionStorage(1, kCompactionStyleUniversal);
    Add(0, i, std::to_string((i + 100) * 1000).c_str(),
        std::to_string((i + 100) * 1000 + 999).c_str(), 1000000, 0, i * 100,
        i * 100 + 99);
    UpdateVersionStorageInfo();
    ASSERT_EQ(level_compaction_picker.NeedsCompaction(vstorage_.get()),
              vstorage_->CompactionScore(0) >= 1);
  }
}

TEST_F(CompactionPickerTest, CompactionUniversalIngestBehindReservedLevel) {
  const uint64_t kFileSize = 100000;
  NewVersionStorage(1, kCompactionStyleUniversal);
  ioptions_.allow_ingest_behind = true;
  ioptions_.num_levels = 3;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_EQ(universal_compaction_picker.NeedsCompaction(vstorage_.get()),
            false);

  NewVersionStorage(3, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(0, 2U, "201", "250", kFileSize, 0, 401, 450);
  Add(0, 4U, "260", "300", kFileSize, 0, 260, 300);
  Add(1, 5U, "100", "151", kFileSize, 0, 200, 251);
  Add(1, 3U, "301", "350", kFileSize, 0, 101, 150);
  Add(2, 6U, "120", "200", kFileSize, 0, 20, 100);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  // output level should be the one above the bottom-most
  ASSERT_EQ(1, compaction->output_level());
}
// Tests if the files can be trivially moved in multi level
// universal compaction when allow_trivial_move option is set
// In this test as the input files overlaps, they cannot
// be trivially moved.

TEST_F(CompactionPickerTest, CannotTrivialMoveUniversal) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.compaction_options_universal.allow_trivial_move = true;
  NewVersionStorage(1, kCompactionStyleUniversal);
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_EQ(universal_compaction_picker.NeedsCompaction(vstorage_.get()),
            false);

  NewVersionStorage(3, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(0, 2U, "201", "250", kFileSize, 0, 401, 450);
  Add(0, 4U, "260", "300", kFileSize, 0, 260, 300);
  Add(1, 5U, "100", "151", kFileSize, 0, 200, 251);
  Add(1, 3U, "301", "350", kFileSize, 0, 101, 150);
  Add(2, 6U, "120", "200", kFileSize, 0, 20, 100);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  ASSERT_TRUE(!compaction->is_trivial_move());
}
// Tests if the files can be trivially moved in multi level
// universal compaction when allow_trivial_move option is set
// In this test as the input files doesn't overlaps, they should
// be trivially moved.
TEST_F(CompactionPickerTest, AllowsTrivialMoveUniversal) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.compaction_options_universal.allow_trivial_move = true;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(3, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(0, 2U, "201", "250", kFileSize, 0, 401, 450);
  Add(0, 4U, "260", "300", kFileSize, 0, 260, 300);
  Add(1, 5U, "010", "080", kFileSize, 0, 200, 251);
  Add(2, 3U, "301", "350", kFileSize, 0, 101, 150);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  ASSERT_TRUE(compaction->is_trivial_move());
}

TEST_F(CompactionPickerTest, UniversalPeriodicCompaction1) {
  // The case where universal periodic compaction can be picked
  // with some newer files being compacted.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.periodic_compaction_seconds = 1000;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(0, 2U, "201", "250", kFileSize, 0, 401, 450);
  Add(0, 4U, "260", "300", kFileSize, 0, 260, 300);
  Add(3, 5U, "010", "080", kFileSize, 0, 200, 251);
  Add(4, 3U, "301", "350", kFileSize, 0, 101, 150);
  Add(4, 6U, "501", "750", kFileSize, 0, 101, 150);

  file_map_[2].first->being_compacted = true;
  UpdateVersionStorageInfo();
  vstorage_->TEST_AddFileMarkedForPeriodicCompaction(4, file_map_[3].first);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
}

TEST_F(CompactionPickerTest, UniversalPeriodicCompaction2) {
  // The case where universal periodic compaction does not
  // pick up only level to compact if it doesn't cover
  // any file marked as periodic compaction.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.periodic_compaction_seconds = 1000;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(3, 5U, "010", "080", kFileSize, 0, 200, 251);
  Add(4, 3U, "301", "350", kFileSize, 0, 101, 150);
  Add(4, 6U, "501", "750", kFileSize, 0, 101, 150);

  file_map_[5].first->being_compacted = true;
  UpdateVersionStorageInfo();
  vstorage_->TEST_AddFileMarkedForPeriodicCompaction(0, file_map_[1].first);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  ASSERT_FALSE(compaction);
}

TEST_F(CompactionPickerTest, UniversalPeriodicCompaction3) {
  // The case where universal periodic compaction does not
  // pick up only the last sorted run which is an L0 file if it isn't
  // marked as periodic compaction.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.periodic_compaction_seconds = 1000;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(0, 5U, "010", "080", kFileSize, 0, 200, 251);
  Add(0, 6U, "501", "750", kFileSize, 0, 101, 150);

  file_map_[5].first->being_compacted = true;
  UpdateVersionStorageInfo();
  vstorage_->TEST_AddFileMarkedForPeriodicCompaction(0, file_map_[1].first);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  ASSERT_FALSE(compaction);
}

TEST_F(CompactionPickerTest, UniversalPeriodicCompaction4) {
  // The case where universal periodic compaction couldn't form
  // a compaction that includes any file marked for periodic compaction.
  // Right now we form the compaction anyway if it is more than one
  // sorted run. Just put the case here to validate that it doesn't
  // crash.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.periodic_compaction_seconds = 1000;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(2, 2U, "010", "080", kFileSize, 0, 200, 251);
  Add(3, 5U, "010", "080", kFileSize, 0, 200, 251);
  Add(4, 3U, "301", "350", kFileSize, 0, 101, 150);
  Add(4, 6U, "501", "750", kFileSize, 0, 101, 150);

  file_map_[2].first->being_compacted = true;
  UpdateVersionStorageInfo();
  vstorage_->TEST_AddFileMarkedForPeriodicCompaction(0, file_map_[2].first);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_TRUE(!compaction ||
              compaction->start_level() != compaction->output_level());
}

TEST_F(CompactionPickerTest, UniversalPeriodicCompaction5) {
  // Test single L0 file periodic compaction triggering.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.periodic_compaction_seconds = 1000;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 6U, "150", "200", kFileSize, 0, 500, 550);
  UpdateVersionStorageInfo();
  vstorage_->TEST_AddFileMarkedForPeriodicCompaction(0, file_map_[6].first);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(6U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(4, compaction->output_level());
}

TEST_F(CompactionPickerTest, UniversalPeriodicCompaction6) {
  // Test single sorted run non-L0 periodic compaction
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.periodic_compaction_seconds = 1000;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(4, 5U, "150", "200", kFileSize, 0, 500, 550);
  Add(4, 6U, "350", "400", kFileSize, 0, 500, 550);
  UpdateVersionStorageInfo();
  vstorage_->TEST_AddFileMarkedForPeriodicCompaction(4, file_map_[6].first);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->start_level());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(5U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(4, compaction->output_level());
}

TEST_F(CompactionPickerTest, UniversalIncrementalSpace1) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.max_compaction_bytes = 555555;
  mutable_cf_options_.compaction_options_universal.incremental = true;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 30;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(2, 2U, "010", "080", kFileSize, 0, 200, 251);
  Add(3, 5U, "310", "380", kFileSize, 0, 200, 251);
  Add(3, 6U, "410", "880", kFileSize, 0, 200, 251);
  Add(3, 7U, "910", "980", 1, 0, 200, 251);
  Add(4, 10U, "201", "250", kFileSize, 0, 101, 150);
  Add(4, 11U, "301", "350", kFileSize, 0, 101, 150);
  Add(4, 12U, "401", "450", kFileSize, 0, 101, 150);
  Add(4, 13U, "501", "750", kFileSize, 0, 101, 150);
  Add(4, 14U, "801", "850", kFileSize, 0, 101, 150);
  Add(4, 15U, "901", "950", kFileSize, 0, 101, 150);
  //  Add(4, 15U, "960", "970", kFileSize, 0, 101, 150);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->output_level());
  ASSERT_EQ(3, compaction->start_level());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(5U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(0, 1)->fd.GetNumber());
  // ASSERT_EQ(4U, compaction->num_input_files(1));
  ASSERT_EQ(11U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(12U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(13U, compaction->input(1, 2)->fd.GetNumber());
  ASSERT_EQ(14U, compaction->input(1, 3)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, UniversalIncrementalSpace2) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.max_compaction_bytes = 400000;
  mutable_cf_options_.compaction_options_universal.incremental = true;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 30;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(1, 2U, "010", "080", kFileSize, 0, 200, 251);
  Add(2, 5U, "310", "380", kFileSize, 0, 200, 251);
  Add(2, 6U, "410", "880", kFileSize, 0, 200, 251);
  Add(2, 7U, "910", "980", kFileSize, 0, 200, 251);
  Add(4, 10U, "201", "250", kFileSize, 0, 101, 150);
  Add(4, 11U, "301", "350", kFileSize, 0, 101, 150);
  Add(4, 12U, "401", "450", kFileSize, 0, 101, 150);
  Add(4, 13U, "501", "750", kFileSize, 0, 101, 150);
  Add(4, 14U, "801", "850", kFileSize, 0, 101, 150);
  Add(4, 15U, "901", "950", kFileSize, 0, 101, 150);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->output_level());
  ASSERT_EQ(2, compaction->start_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(7U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(15U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, UniversalIncrementalSpace3) {
  // Test bottom level files falling between gaps between two upper level
  // files
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.max_compaction_bytes = 300000;
  mutable_cf_options_.compaction_options_universal.incremental = true;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 30;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(2, 2U, "010", "080", kFileSize, 0, 200, 251);
  Add(3, 5U, "000", "180", kFileSize, 0, 200, 251);
  Add(3, 6U, "181", "190", kFileSize, 0, 200, 251);
  Add(3, 7U, "710", "810", kFileSize, 0, 200, 251);
  Add(3, 8U, "820", "830", kFileSize, 0, 200, 251);
  Add(3, 9U, "900", "991", kFileSize, 0, 200, 251);
  Add(4, 10U, "201", "250", kFileSize, 0, 101, 150);
  Add(4, 11U, "301", "350", kFileSize, 0, 101, 150);
  Add(4, 12U, "401", "450", kFileSize, 0, 101, 150);
  Add(4, 13U, "501", "750", kFileSize, 0, 101, 150);
  Add(4, 14U, "801", "850", kFileSize, 0, 101, 150);
  Add(4, 15U, "901", "950", kFileSize, 0, 101, 150);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->output_level());
  ASSERT_EQ(2, compaction->start_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(5U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(0, compaction->num_input_files(2));
}

TEST_F(CompactionPickerTest, UniversalIncrementalSpace4) {
  // Test compaction candidates always cover many files.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.max_compaction_bytes = 3200000;
  mutable_cf_options_.compaction_options_universal.incremental = true;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 30;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(2, 2U, "010", "080", kFileSize, 0, 200, 251);

  // Generate files like following:
  // L3: (1101, 1180) (1201, 1280) ... (7901, 7908)
  // L4: (1130, 1150) (1160, 1210) (1230, 1250) (1260 1310) ... (7960, 8010)
  for (int i = 11; i < 79; i++) {
    Add(3, 100 + i * 3, std::to_string(i * 100).c_str(),
        std::to_string(i * 100 + 80).c_str(), kFileSize, 0, 200, 251);
    // Add a tie breaker
    if (i == 66) {
      Add(3, 10000U, "6690", "6699", kFileSize, 0, 200, 251);
    }

    Add(4, 100 + i * 3 + 1, std::to_string(i * 100 + 30).c_str(),
        std::to_string(i * 100 + 50).c_str(), kFileSize, 0, 200, 251);
    Add(4, 100 + i * 3 + 2, std::to_string(i * 100 + 60).c_str(),
        std::to_string(i * 100 + 110).c_str(), kFileSize, 0, 200, 251);
  }
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->output_level());
  ASSERT_EQ(3, compaction->start_level());
  ASSERT_EQ(6U, compaction->num_input_files(0));
  ASSERT_EQ(100 + 62U * 3, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(10000U, compaction->input(0, 5)->fd.GetNumber());
  ASSERT_EQ(11, compaction->num_input_files(1));
}

TEST_F(CompactionPickerTest, UniversalIncrementalSpace5) {
  // Test compaction candidates always cover many files with some single
  // files larger than size threshold.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.max_compaction_bytes = 3200000;
  mutable_cf_options_.compaction_options_universal.incremental = true;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 30;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(2, 2U, "010", "080", kFileSize, 0, 200, 251);

  // Generate files like following:
  // L3: (1101, 1180) (1201, 1280) ... (7901, 7908)
  // L4: (1130, 1150) (1160, 1210) (1230, 1250) (1260 1310) ... (7960, 8010)
  for (int i = 11; i < 70; i++) {
    Add(3, 100 + i * 3, std::to_string(i * 100).c_str(),
        std::to_string(i * 100 + 80).c_str(),
        i % 10 == 9 ? kFileSize * 100 : kFileSize, 0, 200, 251);

    Add(4, 100 + i * 3 + 1, std::to_string(i * 100 + 30).c_str(),
        std::to_string(i * 100 + 50).c_str(), kFileSize, 0, 200, 251);
    Add(4, 100 + i * 3 + 2, std::to_string(i * 100 + 60).c_str(),
        std::to_string(i * 100 + 110).c_str(), kFileSize, 0, 200, 251);
  }
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->output_level());
  ASSERT_EQ(3, compaction->start_level());
  ASSERT_EQ(6U, compaction->num_input_files(0));
  ASSERT_EQ(100 + 14 * 3, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(100 + 19 * 3, compaction->input(0, 5)->fd.GetNumber());
  ASSERT_EQ(13, compaction->num_input_files(1));
}

TEST_F(CompactionPickerTest, NeedsCompactionFIFO) {
  NewVersionStorage(1, kCompactionStyleFIFO);
  const int kFileCount =
      mutable_cf_options_.level0_file_num_compaction_trigger * 3;
  const uint64_t kFileSize = 100000;
  const uint64_t kMaxSize = kFileSize * kFileCount / 2;

  fifo_options_.max_table_files_size = kMaxSize;
  mutable_cf_options_.compaction_options_fifo = fifo_options_;
  FIFOCompactionPicker fifo_compaction_picker(ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), false);

  // verify whether compaction is needed based on the current
  // size of L0 files.
  for (int i = 1; i <= kFileCount; ++i) {
    NewVersionStorage(1, kCompactionStyleFIFO);
    Add(0, i, std::to_string((i + 100) * 1000).c_str(),
        std::to_string((i + 100) * 1000 + 999).c_str(), kFileSize, 0, i * 100,
        i * 100 + 99);
    UpdateVersionStorageInfo();
    ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()),
              vstorage_->CompactionScore(0) >= 1);
  }
}

TEST_F(CompactionPickerTest, FIFOToWarm1) {
  NewVersionStorage(1, kCompactionStyleFIFO);
  const uint64_t kFileSize = 100000;
  const uint64_t kMaxSize = kFileSize * 100000;
  uint64_t kWarmThreshold = 2000;

  fifo_options_.max_table_files_size = kMaxSize;
  fifo_options_.age_for_warm = kWarmThreshold;
  mutable_cf_options_.compaction_options_fifo = fifo_options_;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_compaction_bytes = kFileSize * 100;
  FIFOCompactionPicker fifo_compaction_picker(ioptions_, &icmp_);

  int64_t current_time = 0;
  ASSERT_OK(Env::Default()->GetCurrentTime(&current_time));
  uint64_t threshold_time =
      static_cast<uint64_t>(current_time) - kWarmThreshold;
  Add(0, 6U, "240", "290", 2 * kFileSize, 0, 2900, 3000, 0, true,
      Temperature::kUnknown, static_cast<uint64_t>(current_time) - 100);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 2700, 2800, 0, true,
      Temperature::kUnknown, threshold_time + 100);
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 2500, 2600, 0, true,
      Temperature::kUnknown, threshold_time - 2000);
  Add(0, 3U, "200", "300", 4 * kFileSize, 0, 2300, 2400, 0, true,
      Temperature::kUnknown, threshold_time - 3000);
  UpdateVersionStorageInfo();

  ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), true);
  std::unique_ptr<Compaction> compaction(fifo_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(3U, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, FIFOToWarm2) {
  NewVersionStorage(1, kCompactionStyleFIFO);
  const uint64_t kFileSize = 100000;
  const uint64_t kMaxSize = kFileSize * 100000;
  uint64_t kWarmThreshold = 2000;

  fifo_options_.max_table_files_size = kMaxSize;
  fifo_options_.age_for_warm = kWarmThreshold;
  mutable_cf_options_.compaction_options_fifo = fifo_options_;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_compaction_bytes = kFileSize * 100;
  FIFOCompactionPicker fifo_compaction_picker(ioptions_, &icmp_);

  int64_t current_time = 0;
  ASSERT_OK(Env::Default()->GetCurrentTime(&current_time));
  uint64_t threshold_time =
      static_cast<uint64_t>(current_time) - kWarmThreshold;
  Add(0, 6U, "240", "290", 2 * kFileSize, 0, 2900, 3000, 0, true,
      Temperature::kUnknown, static_cast<uint64_t>(current_time) - 100);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 2700, 2800, 0, true,
      Temperature::kUnknown, threshold_time + 100);
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 2500, 2600, 0, true,
      Temperature::kUnknown, threshold_time - 2000);
  Add(0, 3U, "200", "300", 4 * kFileSize, 0, 2300, 2400, 0, true,
      Temperature::kUnknown, threshold_time - 3000);
  Add(0, 2U, "200", "300", 4 * kFileSize, 0, 2100, 2200, 0, true,
      Temperature::kUnknown, threshold_time - 4000);
  UpdateVersionStorageInfo();

  ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), true);
  std::unique_ptr<Compaction> compaction(fifo_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, FIFOToWarmMaxSize) {
  NewVersionStorage(1, kCompactionStyleFIFO);
  const uint64_t kFileSize = 100000;
  const uint64_t kMaxSize = kFileSize * 100000;
  uint64_t kWarmThreshold = 2000;

  fifo_options_.max_table_files_size = kMaxSize;
  fifo_options_.age_for_warm = kWarmThreshold;
  mutable_cf_options_.compaction_options_fifo = fifo_options_;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_compaction_bytes = kFileSize * 9;
  FIFOCompactionPicker fifo_compaction_picker(ioptions_, &icmp_);

  int64_t current_time = 0;
  ASSERT_OK(Env::Default()->GetCurrentTime(&current_time));
  uint64_t threshold_time =
      static_cast<uint64_t>(current_time) - kWarmThreshold;
  Add(0, 6U, "240", "290", 2 * kFileSize, 0, 2900, 3000, 0, true,
      Temperature::kUnknown, static_cast<uint64_t>(current_time) - 100);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 2700, 2800, 0, true,
      Temperature::kUnknown, threshold_time + 100);
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 2500, 2600, 0, true,
      Temperature::kUnknown, threshold_time - 2000);
  Add(0, 3U, "200", "300", 4 * kFileSize, 0, 2300, 2400, 0, true,
      Temperature::kUnknown, threshold_time - 3000);
  Add(0, 2U, "200", "300", 4 * kFileSize, 0, 2100, 2200, 0, true,
      Temperature::kUnknown, threshold_time - 4000);
  Add(0, 1U, "200", "300", 4 * kFileSize, 0, 2000, 2100, 0, true,
      Temperature::kUnknown, threshold_time - 5000);
  UpdateVersionStorageInfo();

  ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), true);
  std::unique_ptr<Compaction> compaction(fifo_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, FIFOToWarmWithExistingWarm) {
  NewVersionStorage(1, kCompactionStyleFIFO);
  const uint64_t kFileSize = 100000;
  const uint64_t kMaxSize = kFileSize * 100000;
  uint64_t kWarmThreshold = 2000;

  fifo_options_.max_table_files_size = kMaxSize;
  fifo_options_.age_for_warm = kWarmThreshold;
  mutable_cf_options_.compaction_options_fifo = fifo_options_;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_compaction_bytes = kFileSize * 100;
  FIFOCompactionPicker fifo_compaction_picker(ioptions_, &icmp_);

  int64_t current_time = 0;
  ASSERT_OK(Env::Default()->GetCurrentTime(&current_time));
  uint64_t threshold_time =
      static_cast<uint64_t>(current_time) - kWarmThreshold;
  Add(0, 6U, "240", "290", 2 * kFileSize, 0, 2900, 3000, 0, true,
      Temperature::kUnknown, static_cast<uint64_t>(current_time) - 100);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 2700, 2800, 0, true,
      Temperature::kUnknown, threshold_time + 100);
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 2500, 2600, 0, true,
      Temperature::kUnknown, threshold_time - 2000);
  Add(0, 3U, "200", "300", 4 * kFileSize, 0, 2300, 2400, 0, true,
      Temperature::kUnknown, threshold_time - 3000);
  Add(0, 2U, "200", "300", 4 * kFileSize, 0, 2100, 2200, 0, true,
      Temperature::kUnknown, threshold_time - 4000);
  Add(0, 1U, "200", "300", 4 * kFileSize, 0, 2000, 2100, 0, true,
      Temperature::kWarm, threshold_time - 5000);
  UpdateVersionStorageInfo();

  ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), true);
  std::unique_ptr<Compaction> compaction(fifo_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, FIFOToWarmWithOngoing) {
  NewVersionStorage(1, kCompactionStyleFIFO);
  const uint64_t kFileSize = 100000;
  const uint64_t kMaxSize = kFileSize * 100000;
  uint64_t kWarmThreshold = 2000;

  fifo_options_.max_table_files_size = kMaxSize;
  fifo_options_.age_for_warm = kWarmThreshold;
  mutable_cf_options_.compaction_options_fifo = fifo_options_;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_compaction_bytes = kFileSize * 100;
  FIFOCompactionPicker fifo_compaction_picker(ioptions_, &icmp_);

  int64_t current_time = 0;
  ASSERT_OK(Env::Default()->GetCurrentTime(&current_time));
  uint64_t threshold_time =
      static_cast<uint64_t>(current_time) - kWarmThreshold;
  Add(0, 6U, "240", "290", 2 * kFileSize, 0, 2900, 3000, 0, true,
      Temperature::kUnknown, static_cast<uint64_t>(current_time) - 100);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 2700, 2800, 0, true,
      Temperature::kUnknown, threshold_time + 100);
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 2500, 2600, 0, true,
      Temperature::kUnknown, threshold_time - 2000);
  Add(0, 3U, "200", "300", 4 * kFileSize, 0, 2300, 2400, 0, true,
      Temperature::kUnknown, threshold_time - 3000);
  Add(0, 2U, "200", "300", 4 * kFileSize, 0, 2100, 2200, 0, true,
      Temperature::kUnknown, threshold_time - 4000);
  Add(0, 1U, "200", "300", 4 * kFileSize, 0, 2000, 2100, 0, true,
      Temperature::kWarm, threshold_time - 5000);
  file_map_[2].first->being_compacted = true;
  UpdateVersionStorageInfo();

  ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), true);
  std::unique_ptr<Compaction> compaction(fifo_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  // Stop if a file is being compacted
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, FIFOToWarmWithHotBetweenWarms) {
  NewVersionStorage(1, kCompactionStyleFIFO);
  const uint64_t kFileSize = 100000;
  const uint64_t kMaxSize = kFileSize * 100000;
  uint64_t kWarmThreshold = 2000;

  fifo_options_.max_table_files_size = kMaxSize;
  fifo_options_.age_for_warm = kWarmThreshold;
  mutable_cf_options_.compaction_options_fifo = fifo_options_;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_compaction_bytes = kFileSize * 100;
  FIFOCompactionPicker fifo_compaction_picker(ioptions_, &icmp_);

  int64_t current_time = 0;
  ASSERT_OK(Env::Default()->GetCurrentTime(&current_time));
  uint64_t threshold_time =
      static_cast<uint64_t>(current_time) - kWarmThreshold;
  Add(0, 6U, "240", "290", 2 * kFileSize, 0, 2900, 3000, 0, true,
      Temperature::kUnknown, static_cast<uint64_t>(current_time) - 100);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 2700, 2800, 0, true,
      Temperature::kUnknown, threshold_time + 100);
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 2500, 2600, 0, true,
      Temperature::kUnknown, threshold_time - 2000);
  Add(0, 3U, "200", "300", 4 * kFileSize, 0, 2300, 2400, 0, true,
      Temperature::kWarm, threshold_time - 3000);
  Add(0, 2U, "200", "300", 4 * kFileSize, 0, 2100, 2200, 0, true,
      Temperature::kUnknown, threshold_time - 4000);
  Add(0, 1U, "200", "300", 4 * kFileSize, 0, 2000, 2100, 0, true,
      Temperature::kWarm, threshold_time - 5000);
  UpdateVersionStorageInfo();

  ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), true);
  std::unique_ptr<Compaction> compaction(fifo_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  // Stop if a file is being compacted
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
}

#endif  // ROCKSDB_LITE

TEST_F(CompactionPickerTest, CompactionPriMinOverlapping1) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.target_file_size_base = 100000000000;
  mutable_cf_options_.target_file_size_multiplier = 10;
  mutable_cf_options_.max_bytes_for_level_base = 10 * 1024 * 1024;
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);

  Add(2, 6U, "150", "179", 50000000U);
  Add(2, 7U, "180", "220", 50000000U);
  Add(2, 8U, "321", "400", 50000000U);  // File not overlapping
  Add(2, 9U, "721", "800", 50000000U);

  Add(3, 26U, "150", "170", 260000000U);
  Add(3, 27U, "171", "179", 260000000U);
  Add(3, 28U, "191", "220", 260000000U);
  Add(3, 29U, "221", "300", 260000000U);
  Add(3, 30U, "750", "900", 260000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Pick file 8 because it overlaps with 0 files on level 3.
  ASSERT_EQ(8U, compaction->input(0, 0)->fd.GetNumber());
  // Compaction input size * 1.1
  ASSERT_GE(uint64_t{55000000}, compaction->OutputFilePreallocationSize());
}

TEST_F(CompactionPickerTest, CompactionPriMinOverlapping2) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.target_file_size_base = 10000000;
  mutable_cf_options_.target_file_size_multiplier = 10;
  mutable_cf_options_.max_bytes_for_level_base = 10 * 1024 * 1024;

  Add(2, 6U, "150", "175",
      60000000U);  // Overlaps with file 26, 27, total size 521M
  Add(2, 7U, "176", "200", 60000000U);  // Overlaps with file 27, 28, total size
                                        // 520M, the smallest overlapping
  Add(2, 8U, "201", "300",
      60000000U);  // Overlaps with file 28, 29, total size 521M

  Add(3, 25U, "100", "110", 261000000U);
  Add(3, 26U, "150", "170", 261000000U);
  Add(3, 27U, "171", "179", 260000000U);
  Add(3, 28U, "191", "220", 260000000U);
  Add(3, 29U, "221", "300", 261000000U);
  Add(3, 30U, "321", "400", 261000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Picking file 7 because overlapping ratio is the biggest.
  ASSERT_EQ(7U, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, CompactionPriMinOverlapping3) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.max_bytes_for_level_base = 10000000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;

  // file 7 and 8 over lap with the same file, but file 8 is smaller so
  // it will be picked.
  Add(2, 6U, "150", "167", 60000000U);  // Overlaps with file 26, 27
  Add(2, 7U, "168", "169", 60000000U);  // Overlaps with file 27
  Add(2, 8U, "201", "300", 61000000U);  // Overlaps with file 28, but the file
                                        // itself is larger. Should be picked.

  Add(3, 26U, "160", "165", 260000000U);
  Add(3, 27U, "166", "170", 260000000U);
  Add(3, 28U, "180", "400", 260000000U);
  Add(3, 29U, "401", "500", 260000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Picking file 8 because overlapping ratio is the biggest.
  ASSERT_EQ(8U, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, CompactionPriMinOverlapping4) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.max_bytes_for_level_base = 10000000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  mutable_cf_options_.ignore_max_compaction_bytes_for_input = false;

  // file 7 and 8 over lap with the same file, but file 8 is smaller so
  // it will be picked.
  // Overlaps with file 26, 27. And the file is compensated so will be
  // picked up.
  Add(2, 6U, "150", "167", 60000000U, 0, 100, 100, 180000000U);
  Add(2, 7U, "168", "169", 60000000U);  // Overlaps with file 27
  Add(2, 8U, "201", "300", 61000000U);  // Overlaps with file 28

  Add(3, 26U, "160", "165", 60000000U);
  // Boosted file size in output level is not considered.
  Add(3, 27U, "166", "170", 60000000U, 0, 100, 100, 260000000U);
  Add(3, 28U, "180", "400", 60000000U);
  Add(3, 29U, "401", "500", 60000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Picking file 8 because overlapping ratio is the biggest.
  ASSERT_EQ(6U, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, CompactionPriRoundRobin) {
  std::vector<InternalKey> test_cursors = {InternalKey("249", 100, kTypeValue),
                                           InternalKey("600", 100, kTypeValue),
                                           InternalKey()};
  std::vector<uint32_t> selected_files = {8U, 6U, 6U};

  ioptions_.compaction_pri = kRoundRobin;
  mutable_cf_options_.max_bytes_for_level_base = 12000000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  for (size_t i = 0; i < test_cursors.size(); i++) {
    // start a brand new version in each test.
    NewVersionStorage(6, kCompactionStyleLevel);
    vstorage_->ResizeCompactCursors(6);
    // Set the cursor
    vstorage_->AddCursorForOneLevel(2, test_cursors[i]);
    Add(2, 6U, "150", "199", 50000000U);  // Overlap with 26U, 27U
    Add(2, 7U, "200", "249", 50000000U);  // File not overlapping
    Add(2, 8U, "300", "600", 50000000U);  // Overlap with 28U, 29U

    Add(3, 26U, "130", "165", 60000000U);
    Add(3, 27U, "166", "170", 60000000U);
    Add(3, 28U, "270", "340", 60000000U);
    Add(3, 29U, "401", "500", 60000000U);
    UpdateVersionStorageInfo();
    LevelCompactionPicker local_level_compaction_picker =
        LevelCompactionPicker(ioptions_, &icmp_);
    std::unique_ptr<Compaction> compaction(
        local_level_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
            &log_buffer_));
    ASSERT_TRUE(compaction.get() != nullptr);
    // Since the max bytes for level 2 is 120M, picking one file to compact
    // makes the post-compaction level size less than 120M, there is exactly one
    // file picked for round-robin compaction
    ASSERT_EQ(1U, compaction->num_input_files(0));
    ASSERT_EQ(selected_files[i], compaction->input(0, 0)->fd.GetNumber());
    // release the version storage
    DeleteVersionStorage();
  }
}

TEST_F(CompactionPickerTest, CompactionPriMultipleFilesRoundRobin1) {
  ioptions_.compaction_pri = kRoundRobin;
  mutable_cf_options_.max_compaction_bytes = 100000000u;
  mutable_cf_options_.max_bytes_for_level_base = 120;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  // start a brand new version in each test.
  NewVersionStorage(6, kCompactionStyleLevel);
  vstorage_->ResizeCompactCursors(6);
  // Set the cursor (file picking should start with 7U)
  vstorage_->AddCursorForOneLevel(2, InternalKey("199", 100, kTypeValue));
  Add(2, 6U, "150", "199", 500U);
  Add(2, 7U, "200", "249", 500U);
  Add(2, 8U, "300", "600", 500U);
  Add(2, 9U, "700", "800", 500U);
  Add(2, 10U, "850", "950", 500U);

  Add(3, 26U, "130", "165", 600U);
  Add(3, 27U, "166", "170", 600U);
  Add(3, 28U, "270", "340", 600U);
  Add(3, 29U, "401", "500", 600U);
  Add(3, 30U, "601", "800", 600U);
  Add(3, 31U, "830", "890", 600U);
  UpdateVersionStorageInfo();
  LevelCompactionPicker local_level_compaction_picker =
      LevelCompactionPicker(ioptions_, &icmp_);
  std::unique_ptr<Compaction> compaction(
      local_level_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);

  // The maximum compaction bytes is very large in this case so we can igore its
  // constraint in this test case. The maximum bytes for level 2 is 1200
  // bytes, and thus at least 3 files should be picked so that the bytes in
  // level 2 is less than the maximum
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_EQ(7U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(8U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(9U, compaction->input(0, 2)->fd.GetNumber());
  // release the version storage
  DeleteVersionStorage();
}

TEST_F(CompactionPickerTest, CompactionPriMultipleFilesRoundRobin2) {
  ioptions_.compaction_pri = kRoundRobin;
  mutable_cf_options_.max_compaction_bytes = 2500u;
  mutable_cf_options_.max_bytes_for_level_base = 120;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  // start a brand new version in each test.
  NewVersionStorage(6, kCompactionStyleLevel);
  vstorage_->ResizeCompactCursors(6);
  // Set the cursor (file picking should start with 6U)
  vstorage_->AddCursorForOneLevel(2, InternalKey("1000", 100, kTypeValue));
  Add(2, 6U, "150", "199", 500U);  // Overlap with 26U, 27U
  Add(2, 7U, "200", "249", 500U);  // Overlap with 27U
  Add(2, 8U, "300", "600", 500U);  // Overlap with 28U, 29U
  Add(2, 9U, "700", "800", 500U);
  Add(2, 10U, "850", "950", 500U);

  Add(3, 26U, "130", "165", 600U);
  Add(3, 27U, "166", "230", 600U);
  Add(3, 28U, "270", "340", 600U);
  Add(3, 29U, "401", "500", 600U);
  Add(3, 30U, "601", "800", 600U);
  Add(3, 31U, "830", "890", 600U);
  UpdateVersionStorageInfo();
  LevelCompactionPicker local_level_compaction_picker =
      LevelCompactionPicker(ioptions_, &icmp_);
  std::unique_ptr<Compaction> compaction(
      local_level_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);

  // The maximum compaction bytes is only 2500 bytes now. Even though we are
  // required to choose 3 files so that the post-compaction level size is less
  // than 1200 bytes. We cannot pick 3 files to compact since the maximum
  // compaction size is 2500. After picking files 6U and 7U, the number of
  // compaction bytes has reached 2200, and thus no more space to add another
  // input file with 50M bytes.
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(6U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(0, 1)->fd.GetNumber());
  // release the version storage
  DeleteVersionStorage();
}

TEST_F(CompactionPickerTest, CompactionPriMultipleFilesRoundRobin3) {
  ioptions_.compaction_pri = kRoundRobin;
  mutable_cf_options_.max_compaction_bytes = 1000000u;
  mutable_cf_options_.max_bytes_for_level_base = 120;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  // start a brand new version in each test.
  NewVersionStorage(6, kCompactionStyleLevel);
  vstorage_->ResizeCompactCursors(6);
  // Set the cursor (file picking should start with 9U)
  vstorage_->AddCursorForOneLevel(2, InternalKey("700", 100, kTypeValue));
  Add(2, 6U, "150", "199", 500U);
  Add(2, 7U, "200", "249", 500U);
  Add(2, 8U, "300", "600", 500U);
  Add(2, 9U, "700", "800", 500U);
  Add(2, 10U, "850", "950", 500U);

  Add(3, 26U, "130", "165", 600U);
  Add(3, 27U, "166", "170", 600U);
  Add(3, 28U, "270", "340", 600U);
  Add(3, 29U, "401", "500", 600U);
  Add(3, 30U, "601", "800", 600U);
  Add(3, 31U, "830", "890", 600U);
  UpdateVersionStorageInfo();
  LevelCompactionPicker local_level_compaction_picker =
      LevelCompactionPicker(ioptions_, &icmp_);
  std::unique_ptr<Compaction> compaction(
      local_level_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);

  // Cannot pick more files since we reach the last file in level 2
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(9U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(10U, compaction->input(0, 1)->fd.GetNumber());
  // release the version storage
  DeleteVersionStorage();
}

TEST_F(CompactionPickerTest, CompactionPriMinOverlappingManyFiles) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.max_bytes_for_level_base = 15000000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;

  // file 7 and 8 over lap with the same file, but file 8 is smaller so
  // it will be picked.
  Add(2, 13U, "010", "011",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 14U, "020", "021",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 15U, "030", "031",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 16U, "040", "041",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 17U, "050", "051",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 18U, "060", "061",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 19U, "070", "071",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 20U, "080", "081",
      6100U);  // Overlaps with a large file. Not picked

  Add(2, 6U, "150", "167", 60000000U);  // Overlaps with file 26, 27
  Add(2, 7U, "168", "169", 60000000U);  // Overlaps with file 27
  Add(2, 8U, "201", "300", 61000000U);  // Overlaps with file 28, but the file
                                        // itself is larger. Should be picked.
  Add(2, 9U, "610", "611",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 10U, "620", "621",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 11U, "630", "631",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 12U, "640", "641",
      6100U);  // Overlaps with a large file. Not picked

  Add(3, 31U, "001", "100", 260000000U);
  Add(3, 26U, "160", "165", 260000000U);
  Add(3, 27U, "166", "170", 260000000U);
  Add(3, 28U, "180", "400", 260000000U);
  Add(3, 29U, "401", "500", 260000000U);
  Add(3, 30U, "601", "700", 260000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Picking file 8 because overlapping ratio is the biggest.
  ASSERT_EQ(8U, compaction->input(0, 0)->fd.GetNumber());
}

// This test exhibits the bug where we don't properly reset parent_index in
// PickCompaction()
TEST_F(CompactionPickerTest, ParentIndexResetBug) {
  int num_levels = ioptions_.num_levels;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");       // <- marked for compaction
  Add(1, 3U, "400", "500", 600);  // <- this one needs compacting
  Add(2, 4U, "150", "200");
  Add(2, 5U, "201", "210");
  Add(2, 6U, "300", "310");
  Add(2, 7U, "400", "500");  // <- being compacted

  vstorage_->LevelFiles(2)[3]->being_compacted = true;
  vstorage_->LevelFiles(0)[0]->marked_for_compaction = true;

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
}

// This test checks ExpandWhileOverlapping() by having overlapping user keys
// ranges (with different sequence numbers) in the input files.
TEST_F(CompactionPickerTest, OverlappingUserKeys) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kByCompensatedSize;

  Add(1, 1U, "100", "150", 1U);
  // Overlapping user keys
  Add(1, 2U, "200", "400", 1U);
  Add(1, 3U, "400", "500", 1000000000U, 0, 0);
  Add(2, 4U, "600", "700", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys2) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // Overlapping user keys on same level and output level
  Add(1, 1U, "200", "400", 1000000000U);
  Add(1, 2U, "400", "500", 1U, 0, 0);
  Add(2, 3U, "000", "100", 1U);
  Add(2, 4U, "100", "600", 1U, 0, 0);
  Add(2, 5U, "600", "700", 1U, 0, 0);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(3U, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(5U, compaction->input(1, 2)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys3) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // Chain of overlapping user key ranges (forces ExpandWhileOverlapping() to
  // expand multiple times)
  Add(1, 1U, "100", "150", 1U);
  Add(1, 2U, "150", "200", 1U, 0, 0);
  Add(1, 3U, "200", "250", 1000000000U, 0, 0);
  Add(1, 4U, "250", "300", 1U, 0, 0);
  Add(1, 5U, "300", "350", 1U, 0, 0);
  // Output level overlaps with the beginning and the end of the chain
  Add(2, 6U, "050", "100", 1U);
  Add(2, 7U, "350", "400", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(5U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(0, 3)->fd.GetNumber());
  ASSERT_EQ(5U, compaction->input(0, 4)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys4) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_bytes_for_level_base = 1000000;

  Add(1, 1U, "100", "150", 1U);
  Add(1, 2U, "150", "199", 1U, 0, 0);
  Add(1, 3U, "200", "250", 1100000U, 0, 0);
  Add(1, 4U, "251", "300", 1U, 0, 0);
  Add(1, 5U, "300", "350", 1U, 0, 0);

  Add(2, 6U, "100", "115", 1U);
  Add(2, 7U, "125", "325", 1U);
  Add(2, 8U, "350", "400", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(3U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys5) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // Overlapping user keys on same level and output level
  Add(1, 1U, "200", "400", 1000000000U);
  Add(1, 2U, "400", "500", 1U, 0, 0);
  Add(2, 3U, "000", "100", 1U);
  Add(2, 4U, "100", "600", 1U, 0, 0);
  Add(2, 5U, "600", "700", 1U, 0, 0);

  vstorage_->LevelFiles(2)[2]->being_compacted = true;

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, OverlappingUserKeys6) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // Overlapping user keys on same level and output level
  Add(1, 1U, "200", "400", 1U, 0, 0);
  Add(1, 2U, "401", "500", 1U, 0, 0);
  Add(2, 3U, "000", "100", 1U);
  Add(2, 4U, "100", "300", 1U, 0, 0);
  Add(2, 5U, "305", "450", 1U, 0, 0);
  Add(2, 6U, "460", "600", 1U, 0, 0);
  Add(2, 7U, "600", "700", 1U, 0, 0);

  vstorage_->LevelFiles(1)[0]->marked_for_compaction = true;
  vstorage_->LevelFiles(1)[1]->marked_for_compaction = true;

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(3U, compaction->num_input_files(1));
}

TEST_F(CompactionPickerTest, OverlappingUserKeys7) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;
  // Overlapping user keys on same level and output level
  Add(1, 1U, "200", "400", 1U, 0, 0);
  Add(1, 2U, "401", "500", 1000000000U, 0, 0);
  Add(2, 3U, "100", "250", 1U);
  Add(2, 4U, "300", "600", 1U, 0, 0);
  Add(2, 5U, "600", "800", 1U, 0, 0);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_GE(1U, compaction->num_input_files(0));
  ASSERT_GE(2U, compaction->num_input_files(1));
  // File 5 has to be included in the compaction
  ASSERT_EQ(5U, compaction->inputs(1)->back()->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys8) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;
  // grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up
  // Expand input level as much as possible
  // no overlapping case
  Add(1, 1U, "101", "150", 1U);
  Add(1, 2U, "151", "200", 1U);
  Add(1, 3U, "201", "300", 1000000000U);
  Add(1, 4U, "301", "400", 1U);
  Add(1, 5U, "401", "500", 1U);
  Add(2, 6U, "150", "200", 1U);
  Add(2, 7U, "200", "450", 1U, 0, 0);
  Add(2, 8U, "500", "600", 1U);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys9) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;
  // grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up
  // Expand input level as much as possible
  // overlapping case
  Add(1, 1U, "121", "150", 1U);
  Add(1, 2U, "151", "200", 1U);
  Add(1, 3U, "201", "300", 1000000000U);
  Add(1, 4U, "301", "400", 1U);
  Add(1, 5U, "401", "500", 1U);
  Add(2, 6U, "100", "120", 1U);
  Add(2, 7U, "150", "200", 1U);
  Add(2, 8U, "200", "450", 1U, 0, 0);
  Add(2, 9U, "501", "600", 1U);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(5U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(0, 3)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(8U, compaction->input(1, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys10) {
  // Locked file encountered when pulling in extra input-level files with same
  // user keys. Verify we pick the next-best file from the same input level.
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;

  // file_number 2U is largest and thus first choice. But it overlaps with
  // file_number 1U which is being compacted. So instead we pick the next-
  // biggest file, 3U, which is eligible for compaction.
  Add(1 /* level */, 1U /* file_number */, "100" /* smallest */,
      "150" /* largest */, 1U /* file_size */);
  file_map_[1U].first->being_compacted = true;
  Add(1 /* level */, 2U /* file_number */, "150" /* smallest */,
      "200" /* largest */, 1000000000U /* file_size */, 0 /* smallest_seq */,
      0 /* largest_seq */);
  Add(1 /* level */, 3U /* file_number */, "201" /* smallest */,
      "250" /* largest */, 900000000U /* file_size */);
  Add(2 /* level */, 4U /* file_number */, "100" /* smallest */,
      "150" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 5U /* file_number */, "151" /* smallest */,
      "200" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 6U /* file_number */, "201" /* smallest */,
      "250" /* largest */, 1U /* file_size */);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(3U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys11) {
  // Locked file encountered when pulling in extra output-level files with same
  // user keys. Expected to skip that compaction and pick the next-best choice.
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;

  // score(L1) = 3.7
  // score(L2) = 1.85
  // There is no eligible file in L1 to compact since both candidates pull in
  // file_number 5U, which overlaps with a file pending compaction (6U). The
  // first eligible compaction is from L2->L3.
  Add(1 /* level */, 2U /* file_number */, "151" /* smallest */,
      "200" /* largest */, 1000000000U /* file_size */);
  Add(1 /* level */, 3U /* file_number */, "201" /* smallest */,
      "250" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 4U /* file_number */, "100" /* smallest */,
      "149" /* largest */, 5000000000U /* file_size */);
  Add(2 /* level */, 5U /* file_number */, "150" /* smallest */,
      "201" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 6U /* file_number */, "201" /* smallest */,
      "249" /* largest */, 1U /* file_size */, 0 /* smallest_seq */,
      0 /* largest_seq */);
  file_map_[6U].first->being_compacted = true;
  Add(3 /* level */, 7U /* file_number */, "100" /* smallest */,
      "149" /* largest */, 1U /* file_size */);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(4U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, FileTtlBooster) {
  // Set TTL to 2048
  // TTL boosting for all levels starts at 1024,
  // Whole TTL range is 2048 * 31 / 32 - 1024 = 1984 - 1024 = 960.
  // From second last level (L5), range starts at
  // 1024 + 480, 1024 + 240, 1024 + 120 (which is L3).
  // Boosting step 124 / 16 = 7.75 -> 7
  //
  const uint64_t kCurrentTime = 1000000;
  FileMetaData meta;

  {
    FileTtlBooster booster(kCurrentTime, 2048, 7, 3);

    // Not triggering if the file is younger than ttl/2
    meta.oldest_ancester_time = kCurrentTime - 1023;
    ASSERT_EQ(1, booster.GetBoostScore(&meta));
    meta.oldest_ancester_time = kCurrentTime - 1024;
    ASSERT_EQ(1, booster.GetBoostScore(&meta));
    meta.oldest_ancester_time = kCurrentTime + 10;
    ASSERT_EQ(1, booster.GetBoostScore(&meta));

    // Within one boosting step
    meta.oldest_ancester_time = kCurrentTime - (1024 + 120 + 6);
    ASSERT_EQ(1, booster.GetBoostScore(&meta));

    // One boosting step
    meta.oldest_ancester_time = kCurrentTime - (1024 + 120 + 7);
    ASSERT_EQ(2, booster.GetBoostScore(&meta));
    meta.oldest_ancester_time = kCurrentTime - (1024 + 120 + 8);
    ASSERT_EQ(2, booster.GetBoostScore(&meta));

    // Multiple boosting steps
    meta.oldest_ancester_time = kCurrentTime - (1024 + 120 + 30);
    ASSERT_EQ(5, booster.GetBoostScore(&meta));

    // Very high boosting steps
    meta.oldest_ancester_time = kCurrentTime - (1024 + 120 + 700);
    ASSERT_EQ(101, booster.GetBoostScore(&meta));
  }
  {
    // Test second last level
    FileTtlBooster booster(kCurrentTime, 2048, 7, 5);
    meta.oldest_ancester_time = kCurrentTime - (1024 + 480);
    ASSERT_EQ(1, booster.GetBoostScore(&meta));
    meta.oldest_ancester_time = kCurrentTime - (1024 + 480 + 60);
    ASSERT_EQ(3, booster.GetBoostScore(&meta));
  }
  {
    // Test last level
    FileTtlBooster booster(kCurrentTime, 2048, 7, 6);
    meta.oldest_ancester_time = kCurrentTime - (1024 + 480);
    ASSERT_EQ(1, booster.GetBoostScore(&meta));
    meta.oldest_ancester_time = kCurrentTime - (1024 + 480 + 60);
    ASSERT_EQ(1, booster.GetBoostScore(&meta));
    meta.oldest_ancester_time = kCurrentTime - 3000;
    ASSERT_EQ(1, booster.GetBoostScore(&meta));
  }
}

TEST_F(CompactionPickerTest, NotScheduleL1IfL0WithHigherPri1) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 900000000U;

  // 6 L0 files, score 3.
  Add(0, 1U, "000", "400", 1U);
  Add(0, 2U, "001", "400", 1U, 0, 0);
  Add(0, 3U, "001", "400", 1000000000U, 0, 0);
  Add(0, 31U, "001", "400", 1000000000U, 0, 0);
  Add(0, 32U, "001", "400", 1000000000U, 0, 0);
  Add(0, 33U, "001", "400", 1000000000U, 0, 0);

  // L1 total size 2GB, score 2.2. If one file being compacted, score 1.1.
  Add(1, 4U, "050", "300", 1000000000U, 0, 0);
  file_map_[4u].first->being_compacted = true;
  Add(1, 5U, "301", "350", 1000000000U, 0, 0);

  // Output level overlaps with the beginning and the end of the chain
  Add(2, 6U, "050", "100", 1U);
  Add(2, 7U, "300", "400", 1U);

  // No compaction should be scheduled, if L0 has higher priority than L1
  // but L0->L1 compaction is blocked by a file in L1 being compacted.
  UpdateVersionStorageInfo();
  ASSERT_EQ(0, vstorage_->CompactionScoreLevel(0));
  ASSERT_EQ(1, vstorage_->CompactionScoreLevel(1));
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, NotScheduleL1IfL0WithHigherPri2) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 900000000U;

  // 6 L0 files, score 3.
  Add(0, 1U, "000", "400", 1U);
  Add(0, 2U, "001", "400", 1U, 0, 0);
  Add(0, 3U, "001", "400", 1000000000U, 0, 0);
  Add(0, 31U, "001", "400", 1000000000U, 0, 0);
  Add(0, 32U, "001", "400", 1000000000U, 0, 0);
  Add(0, 33U, "001", "400", 1000000000U, 0, 0);

  // L1 total size 2GB, score 2.2. If one file being compacted, score 1.1.
  Add(1, 4U, "050", "300", 1000000000U, 0, 0);
  Add(1, 5U, "301", "350", 1000000000U, 0, 0);

  // Output level overlaps with the beginning and the end of the chain
  Add(2, 6U, "050", "100", 1U);
  Add(2, 7U, "300", "400", 1U);

  // If no file in L1 being compacted, L0->L1 compaction will be scheduled.
  UpdateVersionStorageInfo();  // being_compacted flag is cleared here.
  ASSERT_EQ(0, vstorage_->CompactionScoreLevel(0));
  ASSERT_EQ(1, vstorage_->CompactionScoreLevel(1));
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
}

TEST_F(CompactionPickerTest, NotScheduleL1IfL0WithHigherPri3) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 900000000U;

  // 6 L0 files, score 3.
  Add(0, 1U, "000", "400", 1U);
  Add(0, 2U, "001", "400", 1U, 0, 0);
  Add(0, 3U, "001", "400", 1000000000U, 0, 0);
  Add(0, 31U, "001", "400", 1000000000U, 0, 0);
  Add(0, 32U, "001", "400", 1000000000U, 0, 0);
  Add(0, 33U, "001", "400", 1000000000U, 0, 0);

  // L1 score more than 6.
  Add(1, 4U, "050", "300", 1000000000U, 0, 0);
  file_map_[4u].first->being_compacted = true;
  Add(1, 5U, "301", "350", 1000000000U, 0, 0);
  Add(1, 51U, "351", "400", 6000000000U, 0, 0);

  // Output level overlaps with the beginning and the end of the chain
  Add(2, 6U, "050", "100", 1U);
  Add(2, 7U, "300", "400", 1U);

  // If score in L1 is larger than L0, L1 compaction goes through despite
  // there is pending L0 compaction.
  UpdateVersionStorageInfo();
  ASSERT_EQ(1, vstorage_->CompactionScoreLevel(0));
  ASSERT_EQ(0, vstorage_->CompactionScoreLevel(1));
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
}

TEST_F(CompactionPickerTest, EstimateCompactionBytesNeeded1) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200", 200);
  Add(0, 2U, "150", "200", 200);
  Add(0, 3U, "150", "200", 200);
  // Level 1 is over target by 200
  Add(1, 4U, "400", "500", 600);
  Add(1, 5U, "600", "700", 600);
  // Level 2 is less than target 10000 even added size of level 1
  // Size ratio of L2/L1 is 9600 / 1200 = 8
  Add(2, 6U, "150", "200", 2500);
  Add(2, 7U, "201", "210", 2000);
  Add(2, 8U, "300", "310", 2600);
  Add(2, 9U, "400", "500", 2500);
  // Level 3 exceeds target 100,000 of 1000
  Add(3, 10U, "400", "500", 101000);
  // Level 4 exceeds target 1,000,000 by 900 after adding size from level 3
  // Size ratio L4/L3 is 9.9
  // After merge from L3, L4 size is 1000900
  Add(4, 11U, "400", "500", 999900);
  Add(5, 12U, "400", "500", 8007200);

  UpdateVersionStorageInfo();

  ASSERT_EQ(200u * 9u + 10900u + 900u * 9,
            vstorage_->estimated_compaction_needed_bytes());
}

TEST_F(CompactionPickerTest, EstimateCompactionBytesNeeded2) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200", 200);
  Add(0, 2U, "150", "200", 200);
  Add(0, 4U, "150", "200", 200);
  Add(0, 5U, "150", "200", 200);
  Add(0, 6U, "150", "200", 200);
  // Level 1 size will be 1400 after merging with L0
  Add(1, 7U, "400", "500", 200);
  Add(1, 8U, "600", "700", 200);
  // Level 2 is less than target 10000 even added size of level 1
  Add(2, 9U, "150", "200", 9100);
  // Level 3 over the target, but since level 4 is empty, we assume it will be
  // a trivial move.
  Add(3, 10U, "400", "500", 101000);

  UpdateVersionStorageInfo();

  // estimated L1->L2 merge: 400 * (9100.0 / 1400.0 + 1.0)
  ASSERT_EQ(1400u + 3000u, vstorage_->estimated_compaction_needed_bytes());
}

TEST_F(CompactionPickerTest, EstimateCompactionBytesNeeded3) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200", 2000);
  Add(0, 2U, "150", "200", 2000);
  Add(0, 4U, "150", "200", 2000);
  Add(0, 5U, "150", "200", 2000);
  Add(0, 6U, "150", "200", 1000);
  // Level 1 size will be 10000 after merging with L0
  Add(1, 7U, "400", "500", 500);
  Add(1, 8U, "600", "700", 500);

  Add(2, 9U, "150", "200", 10000);

  UpdateVersionStorageInfo();

  ASSERT_EQ(10000u + 18000u, vstorage_->estimated_compaction_needed_bytes());
}

TEST_F(CompactionPickerTest, EstimateCompactionBytesNeededDynamicLevel) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);

  // Set Last level size 50000
  // num_levels - 1 target 5000
  // num_levels - 2 is base level with target 1000 (rounded up to
  // max_bytes_for_level_base).
  Add(num_levels - 1, 10U, "400", "500", 50000);

  Add(0, 1U, "150", "200", 200);
  Add(0, 2U, "150", "200", 200);
  Add(0, 4U, "150", "200", 200);
  Add(0, 5U, "150", "200", 200);
  Add(0, 6U, "150", "200", 200);
  // num_levels - 3 is over target by 100 + 1000
  Add(num_levels - 3, 7U, "400", "500", 550);
  Add(num_levels - 3, 8U, "600", "700", 550);
  // num_levels - 2 is over target by 1100 + 200
  Add(num_levels - 2, 9U, "150", "200", 5200);

  UpdateVersionStorageInfo();

  // Merging to the second last level: (5200 / 2100 + 1) * 1100
  // Merging to the last level: (50000 / 6300 + 1) * 1300
  ASSERT_EQ(2100u + 3823u + 11617u,
            vstorage_->estimated_compaction_needed_bytes());
}

TEST_F(CompactionPickerTest, IsBottommostLevelTest) {
  // case 1: Higher levels are empty
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  bool result =
      Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_TRUE(result);

  // case 2: Higher levels have no overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  Add(3, 7U, "k", "p");
  Add(3, 8U, "t", "w");
  Add(4, 9U, "a", "b");
  Add(5, 10U, "c", "cc");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_TRUE(result);

  // case 3.1: Higher levels (level 3) have overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  Add(3, 7U, "e", "g");
  Add(3, 8U, "h", "k");
  Add(4, 9U, "a", "b");
  Add(5, 10U, "c", "cc");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  // case 3.2: Higher levels (level 5) have overlap
  DeleteVersionStorage();
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  Add(3, 7U, "j", "k");
  Add(3, 8U, "l", "m");
  Add(4, 9U, "a", "b");
  Add(5, 10U, "c", "cc");
  Add(5, 11U, "h", "k");
  Add(5, 12U, "y", "yy");
  Add(5, 13U, "z", "zz");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  // case 3.3: Higher levels (level 5) have overlap, but it's only overlapping
  // one key ("d")
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  Add(3, 7U, "j", "k");
  Add(3, 8U, "l", "m");
  Add(4, 9U, "a", "b");
  Add(5, 10U, "c", "cc");
  Add(5, 11U, "ccc", "d");
  Add(5, 12U, "y", "yy");
  Add(5, 13U, "z", "zz");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  // Level 0 files overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "s", "t");
  Add(0, 2U, "a", "m");
  Add(0, 3U, "b", "z");
  Add(0, 4U, "e", "f");
  Add(5, 10U, "y", "z");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(1, 0);
  AddToCompactionFiles(1U);
  AddToCompactionFiles(2U);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(4U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  // Level 0 files don't overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "s", "t");
  Add(0, 2U, "a", "m");
  Add(0, 3U, "b", "k");
  Add(0, 4U, "e", "f");
  Add(5, 10U, "y", "z");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(1, 0);
  AddToCompactionFiles(1U);
  AddToCompactionFiles(2U);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(4U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_TRUE(result);

  // Level 1 files overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "s", "t");
  Add(0, 2U, "a", "m");
  Add(0, 3U, "b", "k");
  Add(0, 4U, "e", "f");
  Add(1, 5U, "a", "m");
  Add(1, 6U, "n", "o");
  Add(1, 7U, "w", "y");
  Add(5, 10U, "y", "z");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 0);
  AddToCompactionFiles(1U);
  AddToCompactionFiles(2U);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(4U);
  AddToCompactionFiles(5U);
  AddToCompactionFiles(6U);
  AddToCompactionFiles(7U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  DeleteVersionStorage();
}

TEST_F(CompactionPickerTest, MaxCompactionBytesHit) {
  mutable_cf_options_.max_bytes_for_level_base = 1000000u;
  mutable_cf_options_.max_compaction_bytes = 800000u;
  mutable_cf_options_.ignore_max_compaction_bytes_for_input = false;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick file 2 and 5.
  // It can expand because adding file 1 and 3, the compaction size will
  // exceed mutable_cf_options_.max_bytes_for_level_base.
  Add(1, 1U, "100", "150", 300000U);
  Add(1, 2U, "151", "200", 300001U, 0, 0);
  Add(1, 3U, "201", "250", 300000U, 0, 0);
  Add(1, 4U, "251", "300", 300000U, 0, 0);
  Add(2, 5U, "100", "256", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(5U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, MaxCompactionBytesNotHit) {
  mutable_cf_options_.max_bytes_for_level_base = 800000u;
  mutable_cf_options_.max_compaction_bytes = 1000000u;
  mutable_cf_options_.ignore_max_compaction_bytes_for_input = false;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick file 2 and 5.
  // and it expands to file 1 and 3 too.
  Add(1, 1U, "100", "150", 300000U);
  Add(1, 2U, "151", "200", 300001U, 0, 0);
  Add(1, 3U, "201", "250", 300000U, 0, 0);
  Add(1, 4U, "251", "300", 300000U, 0, 0);
  Add(2, 5U, "000", "251", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(5U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, IsTrivialMoveOn) {
  mutable_cf_options_.max_bytes_for_level_base = 10000u;
  mutable_cf_options_.max_compaction_bytes = 10001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick file 2
  Add(1, 1U, "100", "150", 3000U);
  Add(1, 2U, "151", "200", 3001U);
  Add(1, 3U, "201", "250", 3000U);
  Add(1, 4U, "251", "300", 3000U);

  Add(3, 5U, "120", "130", 7000U);
  Add(3, 6U, "170", "180", 7000U);
  Add(3, 7U, "220", "230", 7000U);
  Add(3, 8U, "270", "280", 7000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, L0TrivialMove1) {
  mutable_cf_options_.max_bytes_for_level_base = 10000000u;
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;
  mutable_cf_options_.max_compaction_bytes = 10000000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(0, 1U, "100", "150", 3000U, 0, 710, 800);
  Add(0, 2U, "151", "200", 3001U, 0, 610, 700);
  Add(0, 3U, "301", "350", 3000U, 0, 510, 600);
  Add(0, 4U, "451", "400", 3000U, 0, 410, 500);

  Add(1, 5U, "120", "130", 7000U);
  Add(1, 6U, "170", "180", 7000U);
  Add(1, 7U, "220", "230", 7000U);
  Add(1, 8U, "270", "280", 7000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(2, compaction->num_input_files(0));
  ASSERT_EQ(3, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(4, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_TRUE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, L0TrivialMoveOneFile) {
  mutable_cf_options_.max_bytes_for_level_base = 10000000u;
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;
  mutable_cf_options_.max_compaction_bytes = 10000000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(0, 1U, "100", "150", 3000U, 0, 710, 800);
  Add(0, 2U, "551", "600", 3001U, 0, 610, 700);
  Add(0, 3U, "101", "150", 3000U, 0, 510, 600);
  Add(0, 4U, "451", "400", 3000U, 0, 410, 500);

  Add(1, 5U, "120", "130", 7000U);
  Add(1, 6U, "170", "180", 7000U);
  Add(1, 7U, "220", "230", 7000U);
  Add(1, 8U, "270", "280", 7000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(1, compaction->num_input_files(0));
  ASSERT_EQ(4, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_TRUE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, L0TrivialMoveWholeL0) {
  mutable_cf_options_.max_bytes_for_level_base = 10000000u;
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;
  mutable_cf_options_.max_compaction_bytes = 10000000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(0, 1U, "300", "350", 3000U, 0, 710, 800);
  Add(0, 2U, "651", "600", 3001U, 0, 610, 700);
  Add(0, 3U, "501", "550", 3000U, 0, 510, 600);
  Add(0, 4U, "451", "400", 3000U, 0, 410, 500);

  Add(1, 5U, "120", "130", 7000U);
  Add(1, 6U, "970", "980", 7000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(4, compaction->num_input_files(0));
  ASSERT_EQ(1, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(4, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(2, compaction->input(0, 3)->fd.GetNumber());
  ASSERT_TRUE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, IsTrivialMoveOffSstPartitioned) {
  mutable_cf_options_.max_bytes_for_level_base = 10000u;
  mutable_cf_options_.max_compaction_bytes = 10001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.sst_partitioner_factory = NewSstPartitionerFixedPrefixFactory(1);
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick file 2
  Add(1, 1U, "100", "150", 3000U);
  Add(1, 2U, "151", "200", 3001U);
  Add(1, 3U, "201", "250", 3000U);
  Add(1, 4U, "251", "300", 3000U);

  Add(3, 5U, "120", "130", 7000U);
  Add(3, 6U, "170", "180", 7000U);
  Add(3, 7U, "220", "230", 7000U);
  Add(3, 8U, "270", "280", 7000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  // No trivial move, because partitioning is applied
  ASSERT_TRUE(!compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, IsTrivialMoveOff) {
  mutable_cf_options_.max_bytes_for_level_base = 1000000u;
  mutable_cf_options_.max_compaction_bytes = 10000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick all files from level 1
  Add(1, 1U, "100", "150", 300000U, 0, 0);
  Add(1, 2U, "150", "200", 300000U, 0, 0);
  Add(1, 3U, "200", "250", 300000U, 0, 0);
  Add(1, 4U, "250", "300", 300000U, 0, 0);

  Add(3, 5U, "120", "130", 6000U);
  Add(3, 6U, "140", "150", 6000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_FALSE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, TrivialMoveMultipleFiles1) {
  mutable_cf_options_.max_bytes_for_level_base = 1000u;
  mutable_cf_options_.max_compaction_bytes = 10000001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(2, 1U, "100", "150", 3000U);
  Add(2, 2U, "151", "200", 3001U);
  Add(2, 3U, "301", "350", 3000U);
  Add(2, 4U, "451", "400", 3000U);
  Add(2, 5U, "551", "500", 3000U);
  Add(2, 6U, "651", "600", 3000U);
  Add(2, 7U, "751", "700", 3000U);
  Add(2, 8U, "851", "900", 3000U);

  Add(3, 15U, "120", "130", 700U);
  Add(3, 16U, "170", "180", 700U);
  Add(3, 17U, "220", "230", 700U);
  Add(3, 18U, "870", "880", 700U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(4, compaction->num_input_files(0));
  ASSERT_EQ(3, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(4, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(5, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(6, compaction->input(0, 3)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, TrivialMoveMultipleFiles2) {
  mutable_cf_options_.max_bytes_for_level_base = 1000u;
  mutable_cf_options_.max_compaction_bytes = 10000001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(2, 1U, "100", "150", 3000U);
  Add(2, 2U, "151", "160", 3001U);
  Add(2, 3U, "161", "179", 3000U);
  Add(2, 4U, "220", "400", 3000U);
  Add(2, 5U, "551", "500", 3000U);
  Add(2, 6U, "651", "600", 3000U);
  Add(2, 7U, "751", "700", 3000U);
  Add(2, 8U, "851", "900", 3000U);

  Add(3, 15U, "120", "130", 700U);
  Add(3, 17U, "220", "230", 700U);
  Add(3, 18U, "870", "880", 700U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(2, compaction->num_input_files(0));
  ASSERT_EQ(2, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, TrivialMoveMultipleFiles3) {
  mutable_cf_options_.max_bytes_for_level_base = 1000u;
  mutable_cf_options_.max_compaction_bytes = 10000001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  NewVersionStorage(6, kCompactionStyleLevel);

  // Even if consecutive files can be trivial moved, we don't pick them
  // since in case trivial move can't be issued for a reason, we cannot
  // fall back to normal compactions.
  Add(2, 1U, "100", "150", 3000U);
  Add(2, 2U, "151", "160", 3001U);
  Add(2, 5U, "551", "500", 3000U);
  Add(2, 6U, "651", "600", 3000U);
  Add(2, 7U, "751", "700", 3000U);
  Add(2, 8U, "851", "900", 3000U);

  Add(3, 15U, "120", "130", 700U);
  Add(3, 17U, "220", "230", 700U);
  Add(3, 18U, "870", "880", 700U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(1, compaction->num_input_files(0));
  ASSERT_EQ(2, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, TrivialMoveMultipleFiles4) {
  mutable_cf_options_.max_bytes_for_level_base = 1000u;
  mutable_cf_options_.max_compaction_bytes = 10000001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(2, 1U, "100", "150", 4000U);
  Add(2, 2U, "151", "160", 4001U);
  Add(2, 3U, "161", "179", 4000U);

  Add(3, 15U, "120", "130", 700U);
  Add(3, 17U, "220", "230", 700U);
  Add(3, 18U, "870", "880", 700U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(2, compaction->num_input_files(0));
  ASSERT_EQ(2, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, TrivialMoveMultipleFiles5) {
  mutable_cf_options_.max_bytes_for_level_base = 1000u;
  mutable_cf_options_.max_compaction_bytes = 10000001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  NewVersionStorage(6, kCompactionStyleLevel);

  // File 4 and 5 aren't clean cut, so only 2 and 3 are picked.
  Add(2, 1U, "100", "150", 4000U);
  Add(2, 2U, "151", "160", 4001U);
  Add(2, 3U, "161", "179", 4000U);
  Add(2, 4U, "180", "185", 4000U);
  Add(2, 5U, "185", "190", 4000U);

  Add(3, 15U, "120", "130", 700U);
  Add(3, 17U, "220", "230", 700U);
  Add(3, 18U, "870", "880", 700U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(2, compaction->num_input_files(0));
  ASSERT_EQ(2, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, TrivialMoveMultipleFiles6) {
  mutable_cf_options_.max_bytes_for_level_base = 1000u;
  mutable_cf_options_.max_compaction_bytes = 10000001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(2, 1U, "100", "150", 3000U);
  Add(2, 2U, "151", "200", 3001U);
  Add(2, 3U, "301", "350", 3000U);
  Add(2, 4U, "451", "400", 3000U);
  Add(2, 5U, "551", "500", 3000U);
  file_map_[5U].first->being_compacted = true;
  Add(2, 6U, "651", "600", 3000U);
  Add(2, 7U, "751", "700", 3000U);
  Add(2, 8U, "851", "900", 3000U);

  Add(3, 15U, "120", "130", 700U);
  Add(3, 16U, "170", "180", 700U);
  Add(3, 17U, "220", "230", 700U);
  Add(3, 18U, "870", "880", 700U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
  ASSERT_EQ(1, compaction->num_input_levels());
  // Since the next file is being compacted. Stopping at 3 and 4.
  ASSERT_EQ(2, compaction->num_input_files(0));
  ASSERT_EQ(3, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(4, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, CacheNextCompactionIndex) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;

  Add(1 /* level */, 1U /* file_number */, "100" /* smallest */,
      "149" /* largest */, 1000000000U /* file_size */);
  file_map_[1U].first->being_compacted = true;
  Add(1 /* level */, 2U /* file_number */, "150" /* smallest */,
      "199" /* largest */, 900000000U /* file_size */);
  Add(1 /* level */, 3U /* file_number */, "200" /* smallest */,
      "249" /* largest */, 800000000U /* file_size */);
  Add(1 /* level */, 4U /* file_number */, "250" /* smallest */,
      "299" /* largest */, 700000000U /* file_size */);
  Add(2 /* level */, 5U /* file_number */, "150" /* smallest */,
      "199" /* largest */, 100U /* file_size */);
  Add(2 /* level */, 6U /* file_number */, "200" /* smallest */,
      "240" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 7U /* file_number */, "260" /* smallest */,
      "270" /* largest */, 1U /* file_size */);
  file_map_[5U].first->being_compacted = true;

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(3U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2, vstorage_->NextCompactionIndex(1 /* level */));

  compaction.reset(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(4U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3, vstorage_->NextCompactionIndex(1 /* level */));

  compaction.reset(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() == nullptr);
  ASSERT_EQ(4, vstorage_->NextCompactionIndex(1 /* level */));
}

TEST_F(CompactionPickerTest, IntraL0MaxCompactionBytesNotHit) {
  // Intra L0 compaction triggers only if there are at least
  // level0_file_num_compaction_trigger + 2 L0 files.
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_compaction_bytes = 1000000u;
  NewVersionStorage(6, kCompactionStyleLevel);

  // All 5 L0 files will be picked for intra L0 compaction. The one L1 file
  // spans entire L0 key range and is marked as being compacted to avoid
  // L0->L1 compaction.
  Add(0, 1U, "100", "150", 200000U, 0, 100, 101);
  Add(0, 2U, "151", "200", 200000U, 0, 102, 103);
  Add(0, 3U, "201", "250", 200000U, 0, 104, 105);
  Add(0, 4U, "251", "300", 200000U, 0, 106, 107);
  Add(0, 5U, "301", "350", 200000U, 0, 108, 109);
  Add(1, 6U, "100", "350", 200000U, 0, 110, 111);
  vstorage_->LevelFiles(1)[0]->being_compacted = true;
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(5U, compaction->num_input_files(0));
  ASSERT_EQ(CompactionReason::kLevelL0FilesNum,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
}

TEST_F(CompactionPickerTest, IntraL0MaxCompactionBytesHit) {
  // Intra L0 compaction triggers only if there are at least
  // level0_file_num_compaction_trigger + 2 L0 files.
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_compaction_bytes = 999999u;
  NewVersionStorage(6, kCompactionStyleLevel);

  // 4 out of 5 L0 files will be picked for intra L0 compaction due to
  // max_compaction_bytes limit (the minimum number of files for triggering
  // intra L0 compaction is 4). The one L1 file spans entire L0 key range and
  // is marked as being compacted to avoid L0->L1 compaction.
  Add(0, 1U, "100", "150", 200000U, 0, 100, 101);
  Add(0, 2U, "151", "200", 200000U, 0, 102, 103);
  Add(0, 3U, "201", "250", 200000U, 0, 104, 105);
  Add(0, 4U, "251", "300", 200000U, 0, 106, 107);
  Add(0, 5U, "301", "350", 200000U, 0, 108, 109);
  Add(1, 6U, "100", "350", 200000U, 0, 109, 110);
  vstorage_->LevelFiles(1)[0]->being_compacted = true;
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
      &log_buffer_));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(4U, compaction->num_input_files(0));
  ASSERT_EQ(CompactionReason::kLevelL0FilesNum,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
}

#ifndef ROCKSDB_LITE
TEST_F(CompactionPickerTest, UniversalMarkedCompactionFullOverlap) {
  const uint64_t kFileSize = 100000;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  // This test covers the case where a "regular" universal compaction is
  // scheduled first, followed by a delete triggered compaction. The latter
  // should fail
  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550, /*compensated_file_size*/ 0,
      /*marked_for_compact*/ false, /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 3);
  Add(0, 2U, "201", "250", 2 * kFileSize, 0, 401, 450,
      /*compensated_file_size*/ 0, /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 2);
  Add(0, 4U, "260", "300", 4 * kFileSize, 0, 260, 300,
      /*compensated_file_size*/ 0, /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 1);
  Add(3, 5U, "010", "080", 8 * kFileSize, 0, 200, 251);
  Add(4, 3U, "301", "350", 8 * kFileSize, 0, 101, 150);
  Add(4, 6U, "501", "750", 8 * kFileSize, 0, 101, 150);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  ASSERT_TRUE(compaction);
  // Validate that its a compaction to reduce sorted runs
  ASSERT_EQ(CompactionReason::kUniversalSortedRunNum,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(2U, compaction->num_input_files(0));

  AddVersionStorage();
  // Simulate a flush and mark the file for compaction
  Add(0, 7U, "150", "200", kFileSize, 0, 551, 600, 0, true,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 4);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction2(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_FALSE(compaction2);
}

TEST_F(CompactionPickerTest, UniversalMarkedCompactionFullOverlap2) {
  const uint64_t kFileSize = 100000;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  // This test covers the case where a delete triggered compaction is
  // scheduled first, followed by a "regular" compaction. The latter
  // should fail
  NewVersionStorage(5, kCompactionStyleUniversal);

  // Mark file number 4 for compaction
  Add(0, 4U, "260", "300", 4 * kFileSize, 0, 260, 300, 0, true,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 1);
  Add(3, 5U, "240", "290", 8 * kFileSize, 0, 201, 250);
  Add(4, 3U, "301", "350", 8 * kFileSize, 0, 101, 150);
  Add(4, 6U, "501", "750", 8 * kFileSize, 0, 101, 150);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  ASSERT_TRUE(compaction);
  // Validate that its a delete triggered compaction
  ASSERT_EQ(CompactionReason::kFilesMarkedForCompaction,
            compaction->compaction_reason());
  ASSERT_EQ(3, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));

  AddVersionStorage();
  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550, /*compensated_file_size*/ 0,
      /*marked_for_compact*/ false, /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 3);
  Add(0, 2U, "201", "250", 2 * kFileSize, 0, 401, 450,
      /*compensated_file_size*/ 0, /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 2);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction2(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_FALSE(compaction2);
}

TEST_F(CompactionPickerTest, UniversalMarkedCompactionStartOutputOverlap) {
  // The case where universal periodic compaction can be picked
  // with some newer files being compacted.
  const uint64_t kFileSize = 100000;

  ioptions_.compaction_style = kCompactionStyleUniversal;

  bool input_level_overlap = false;
  bool output_level_overlap = false;
  // Let's mark 2 files in 2 different levels for compaction. The
  // compaction picker will randomly pick one, so use the sync point to
  // ensure a deterministic order. Loop until both cases are covered
  size_t random_index = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionPicker::PickFilesMarkedForCompaction", [&](void* arg) {
        size_t* index = static_cast<size_t*>(arg);
        *index = random_index;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  while (!input_level_overlap || !output_level_overlap) {
    // Ensure that the L0 file gets picked first
    random_index = !input_level_overlap ? 0 : 1;
    UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
    NewVersionStorage(5, kCompactionStyleUniversal);

    Add(0, 1U, "260", "300", 4 * kFileSize, 0, 260, 300, 0, true);
    Add(3, 2U, "010", "020", 2 * kFileSize, 0, 201, 248);
    Add(3, 3U, "250", "270", 2 * kFileSize, 0, 202, 249);
    Add(3, 4U, "290", "310", 2 * kFileSize, 0, 203, 250);
    Add(3, 5U, "310", "320", 2 * kFileSize, 0, 204, 251, 0, true);
    Add(4, 6U, "301", "350", 8 * kFileSize, 0, 101, 150);
    Add(4, 7U, "501", "750", 8 * kFileSize, 0, 101, 150);
    UpdateVersionStorageInfo();

    std::unique_ptr<Compaction> compaction(
        universal_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
            &log_buffer_));

    ASSERT_TRUE(compaction);
    // Validate that its a delete triggered compaction
    ASSERT_EQ(CompactionReason::kFilesMarkedForCompaction,
              compaction->compaction_reason());
    ASSERT_TRUE(compaction->start_level() == 0 ||
                compaction->start_level() == 3);
    if (compaction->start_level() == 0) {
      // The L0 file was picked. The next compaction will detect an
      // overlap on its input level
      input_level_overlap = true;
      ASSERT_EQ(3, compaction->output_level());
      ASSERT_EQ(1U, compaction->num_input_files(0));
      ASSERT_EQ(3U, compaction->num_input_files(1));
    } else {
      // The level 3 file was picked. The next compaction will pick
      // the L0 file and will detect overlap when adding output
      // level inputs
      output_level_overlap = true;
      ASSERT_EQ(4, compaction->output_level());
      ASSERT_EQ(2U, compaction->num_input_files(0));
      ASSERT_EQ(1U, compaction->num_input_files(1));
    }

    vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
    // After recomputing the compaction score, only one marked file will remain
    random_index = 0;
    std::unique_ptr<Compaction> compaction2(
        universal_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
            &log_buffer_));
    ASSERT_FALSE(compaction2);
    DeleteVersionStorage();
  }
}

TEST_F(CompactionPickerTest, UniversalMarkedL0NoOverlap) {
  const uint64_t kFileSize = 100000;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  // This test covers the case where a delete triggered compaction is
  // scheduled and should result in a full compaction
  NewVersionStorage(1, kCompactionStyleUniversal);

  // Mark file number 4 for compaction
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 260, 300, 0, true);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 201, 250);
  Add(0, 3U, "301", "350", 4 * kFileSize, 0, 101, 150);
  Add(0, 6U, "501", "750", 8 * kFileSize, 0, 50, 100);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  ASSERT_TRUE(compaction);
  // Validate that its a delete triggered compaction
  ASSERT_EQ(CompactionReason::kFilesMarkedForCompaction,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(4U, compaction->num_input_files(0));
  ASSERT_TRUE(file_map_[4].first->being_compacted);
  ASSERT_TRUE(file_map_[5].first->being_compacted);
  ASSERT_TRUE(file_map_[3].first->being_compacted);
  ASSERT_TRUE(file_map_[6].first->being_compacted);
}

TEST_F(CompactionPickerTest, UniversalMarkedL0WithOverlap) {
  const uint64_t kFileSize = 100000;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  // This test covers the case where a file is being compacted, and a
  // delete triggered compaction is then scheduled. The latter should stop
  // at the first file being compacted
  NewVersionStorage(1, kCompactionStyleUniversal);

  // Mark file number 4 for compaction
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 260, 300, 0, true);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 201, 250);
  Add(0, 3U, "301", "350", 4 * kFileSize, 0, 101, 150);
  Add(0, 6U, "501", "750", 8 * kFileSize, 0, 50, 100);
  UpdateVersionStorageInfo();
  file_map_[3].first->being_compacted = true;

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  ASSERT_TRUE(compaction);
  // Validate that its a delete triggered compaction
  ASSERT_EQ(CompactionReason::kFilesMarkedForCompaction,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_TRUE(file_map_[4].first->being_compacted);
  ASSERT_TRUE(file_map_[5].first->being_compacted);
}

TEST_F(CompactionPickerTest, UniversalMarkedL0Overlap2) {
  const uint64_t kFileSize = 100000;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  // This test covers the case where a delete triggered compaction is
  // scheduled first, followed by a "regular" compaction. The latter
  // should fail
  NewVersionStorage(1, kCompactionStyleUniversal);

  // Mark file number 5 for compaction
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 260, 300,
      /*compensated_file_size*/ 0, /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 4);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 201, 250, 0, true,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 3);
  Add(0, 3U, "301", "350", 4 * kFileSize, 0, 101, 150,
      /*compensated_file_size*/ 0, /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 2);
  Add(0, 6U, "501", "750", 8 * kFileSize, 0, 50, 100,
      /*compensated_file_size*/ 0, /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 1);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  ASSERT_TRUE(compaction);
  // Validate that its a delete triggered compaction
  ASSERT_EQ(CompactionReason::kFilesMarkedForCompaction,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_TRUE(file_map_[5].first->being_compacted);
  ASSERT_TRUE(file_map_[3].first->being_compacted);
  ASSERT_TRUE(file_map_[6].first->being_compacted);

  AddVersionStorage();
  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550, /*compensated_file_size*/ 0,
      /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 6);
  Add(0, 2U, "201", "250", kFileSize, 0, 401, 450, /*compensated_file_size*/ 0,
      /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncesterTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 5);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction2(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  ASSERT_TRUE(compaction2);
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_TRUE(file_map_[1].first->being_compacted);
  ASSERT_TRUE(file_map_[2].first->being_compacted);
  ASSERT_TRUE(file_map_[4].first->being_compacted);
}

TEST_F(CompactionPickerTest, UniversalMarkedManualCompaction) {
  const uint64_t kFileSize = 100000;
  const int kNumLevels = 7;

  // This test makes sure the `files_marked_for_compaction_` is updated after
  // creating manual compaction.
  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(kNumLevels, kCompactionStyleUniversal);

  // Add 3 files marked for compaction
  Add(0, 3U, "301", "350", 4 * kFileSize, 0, 101, 150, 0, true);
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 260, 300, 0, true);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 201, 250, 0, true);
  UpdateVersionStorageInfo();

  // All 3 files are marked for compaction
  ASSERT_EQ(3U, vstorage_->FilesMarkedForCompaction().size());

  bool manual_conflict = false;
  InternalKey* manual_end = nullptr;
  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.CompactRange(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          ColumnFamilyData::kCompactAllLevels, 6, CompactRangeOptions(),
          nullptr, nullptr, &manual_end, &manual_conflict,
          std::numeric_limits<uint64_t>::max(), ""));

  ASSERT_TRUE(compaction);

  ASSERT_EQ(CompactionReason::kManualCompaction,
            compaction->compaction_reason());
  ASSERT_EQ(kNumLevels - 1, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_TRUE(file_map_[3].first->being_compacted);
  ASSERT_TRUE(file_map_[4].first->being_compacted);
  ASSERT_TRUE(file_map_[5].first->being_compacted);

  // After creating the manual compaction, all files should be cleared from
  // `FilesMarkedForCompaction`. So they won't be picked by others.
  ASSERT_EQ(0U, vstorage_->FilesMarkedForCompaction().size());
}

TEST_F(CompactionPickerTest, UniversalSizeAmpTierCompactionNonLastLevel) {
  // This test make sure size amplification compaction could still be triggered
  // if the last sorted run is not the last level.
  const uint64_t kFileSize = 100000;
  const int kNumLevels = 7;
  const int kLastLevel = kNumLevels - 1;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  ioptions_.preclude_last_level_data_seconds = 1000;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 200;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(kNumLevels, kCompactionStyleUniversal);
  Add(0, 100U, "100", "300", 1 * kFileSize);
  Add(0, 101U, "200", "400", 1 * kFileSize);
  Add(4, 90U, "100", "600", 4 * kFileSize);
  Add(5, 80U, "200", "300", 2 * kFileSize);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  // Make sure it's a size amp compaction and includes all files
  ASSERT_EQ(compaction->compaction_reason(),
            CompactionReason::kUniversalSizeAmplification);
  ASSERT_EQ(compaction->output_level(), kLastLevel);
  ASSERT_EQ(compaction->input_levels(0)->num_files, 2);
  ASSERT_EQ(compaction->input_levels(4)->num_files, 1);
  ASSERT_EQ(compaction->input_levels(5)->num_files, 1);
}

TEST_F(CompactionPickerTest, UniversalSizeRatioTierCompactionLastLevel) {
  // This test makes sure the size amp calculation skips the last level (L6), so
  // size amp compaction is not triggered, instead a size ratio compaction is
  // triggered.
  const uint64_t kFileSize = 100000;
  const int kNumLevels = 7;
  const int kLastLevel = kNumLevels - 1;
  const int kPenultimateLevel = kLastLevel - 1;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  ioptions_.preclude_last_level_data_seconds = 1000;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 200;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(kNumLevels, kCompactionStyleUniversal);
  Add(0, 100U, "100", "300", 1 * kFileSize);
  Add(0, 101U, "200", "400", 1 * kFileSize);
  Add(5, 90U, "100", "600", 4 * kFileSize);
  Add(6, 80U, "200", "300", 2 * kFileSize);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  // Internally, size amp compaction is evaluated before size ratio compaction.
  // Here to make sure it's size ratio compaction instead of size amp
  ASSERT_EQ(compaction->compaction_reason(),
            CompactionReason::kUniversalSizeRatio);
  ASSERT_EQ(compaction->output_level(), kPenultimateLevel - 1);
  ASSERT_EQ(compaction->input_levels(0)->num_files, 2);
  ASSERT_EQ(compaction->input_levels(5)->num_files, 0);
  ASSERT_EQ(compaction->input_levels(6)->num_files, 0);
}

TEST_F(CompactionPickerTest, UniversalSizeAmpTierCompactionNotSuport) {
  // Tiered compaction only support level_num > 2 (otherwise the penultimate
  // level is going to be level 0, which may make thing more complicated), so
  // when there's only 2 level, still treating level 1 as the last level for
  // size amp compaction
  const uint64_t kFileSize = 100000;
  const int kNumLevels = 2;
  const int kLastLevel = kNumLevels - 1;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  ioptions_.preclude_last_level_data_seconds = 1000;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 200;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(kNumLevels, kCompactionStyleUniversal);
  Add(0, 100U, "100", "300", 1 * kFileSize);
  Add(0, 101U, "200", "400", 1 * kFileSize);
  Add(0, 90U, "100", "600", 4 * kFileSize);
  Add(1, 80U, "200", "300", 2 * kFileSize);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  // size amp compaction is still triggered even preclude_last_level is set
  ASSERT_EQ(compaction->compaction_reason(),
            CompactionReason::kUniversalSizeAmplification);
  ASSERT_EQ(compaction->output_level(), kLastLevel);
  ASSERT_EQ(compaction->input_levels(0)->num_files, 3);
  ASSERT_EQ(compaction->input_levels(1)->num_files, 1);
}

TEST_F(CompactionPickerTest, UniversalSizeAmpTierCompactionLastLevel) {
  // This test makes sure the size amp compaction for tiered storage could still
  // be triggered, but only for non-last-level files
  const uint64_t kFileSize = 100000;
  const int kNumLevels = 7;
  const int kLastLevel = kNumLevels - 1;
  const int kPenultimateLevel = kLastLevel - 1;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  ioptions_.preclude_last_level_data_seconds = 1000;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 200;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(kNumLevels, kCompactionStyleUniversal);
  Add(0, 100U, "100", "300", 3 * kFileSize);
  Add(0, 101U, "200", "400", 2 * kFileSize);
  Add(5, 90U, "100", "600", 2 * kFileSize);
  Add(6, 80U, "200", "300", 2 * kFileSize);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));

  // It's a Size Amp compaction, but doesn't include the last level file and
  // output to the penultimate level.
  ASSERT_EQ(compaction->compaction_reason(),
            CompactionReason::kUniversalSizeAmplification);
  ASSERT_EQ(compaction->output_level(), kPenultimateLevel);
  ASSERT_EQ(compaction->input_levels(0)->num_files, 2);
  ASSERT_EQ(compaction->input_levels(5)->num_files, 1);
  ASSERT_EQ(compaction->input_levels(6)->num_files, 0);
}

TEST_F(CompactionPickerU64TsTest, Overlap) {
  int num_levels = ioptions_.num_levels;
  NewVersionStorage(num_levels, kCompactionStyleLevel);

  constexpr int level = 0;
  constexpr uint64_t file_number = 20ULL;
  constexpr char smallest[4] = "500";
  constexpr char largest[4] = "600";
  constexpr uint64_t ts_of_smallest = 12345ULL;
  constexpr uint64_t ts_of_largest = 56789ULL;

  {
    std::string ts1;
    PutFixed64(&ts1, ts_of_smallest);
    std::string ts2;
    PutFixed64(&ts2, ts_of_largest);
    Add(level, file_number, smallest, largest,
        /*file_size=*/1U, /*path_id=*/0,
        /*smallest_seq=*/100, /*largest_seq=*/100, /*compensated_file_size=*/0,
        /*marked_for_compact=*/false, /*temperature=*/Temperature::kUnknown,
        /*oldest_ancestor_time=*/kUnknownOldestAncesterTime, ts1, ts2);
    UpdateVersionStorageInfo();
  }

  std::unordered_set<uint64_t> input{file_number};

  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(level_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input, vstorage_.get(), CompactionOptions()));
  std::unique_ptr<Compaction> comp1(level_compaction_picker.CompactFiles(
      CompactionOptions(), input_files, level, vstorage_.get(),
      mutable_cf_options_, mutable_db_options_, /*output_path_id=*/0));

  {
    // [600, ts=50000] to [600, ts=50000] is the range to check.
    // ucmp->Compare(smallest_user_key, c->GetLargestUserKey()) > 0, but
    // ucmp->CompareWithoutTimestamp(smallest_user_key,
    //                               c->GetLargestUserKey()) == 0.
    // Should still be considered overlapping.
    std::string user_key_with_ts1(largest);
    PutFixed64(&user_key_with_ts1, ts_of_largest - 1);
    std::string user_key_with_ts2(largest);
    PutFixed64(&user_key_with_ts2, ts_of_largest - 1);
    ASSERT_TRUE(level_compaction_picker.RangeOverlapWithCompaction(
        user_key_with_ts1, user_key_with_ts2, level));
  }
  {
    // [500, ts=60000] to [500, ts=60000] is the range to check.
    // ucmp->Compare(largest_user_key, c->GetSmallestUserKey()) < 0, but
    // ucmp->CompareWithoutTimestamp(largest_user_key,
    //                               c->GetSmallestUserKey()) == 0.
    // Should still be considered overlapping.
    std::string user_key_with_ts1(smallest);
    PutFixed64(&user_key_with_ts1, ts_of_smallest + 1);
    std::string user_key_with_ts2(smallest);
    PutFixed64(&user_key_with_ts2, ts_of_smallest + 1);
    ASSERT_TRUE(level_compaction_picker.RangeOverlapWithCompaction(
        user_key_with_ts1, user_key_with_ts2, level));
  }
}

TEST_F(CompactionPickerU64TsTest, CannotTrivialMoveUniversal) {
  constexpr uint64_t kFileSize = 100000;

  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.compaction_options_universal.allow_trivial_move = true;
  NewVersionStorage(1, kCompactionStyleUniversal);
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_FALSE(universal_compaction_picker.NeedsCompaction(vstorage_.get()));

  std::string ts1;
  PutFixed64(&ts1, 9000);
  std::string ts2;
  PutFixed64(&ts2, 8000);
  std::string ts3;
  PutFixed64(&ts3, 7000);
  std::string ts4;
  PutFixed64(&ts4, 6000);

  NewVersionStorage(3, kCompactionStyleUniversal);
  // A compaction should be triggered and pick file 2
  Add(1, 1U, "150", "150", kFileSize, /*path_id=*/0, /*smallest_seq=*/100,
      /*largest_seq=*/100, /*compensated_file_size=*/kFileSize,
      /*marked_for_compact=*/false, Temperature::kUnknown,
      kUnknownOldestAncesterTime, ts1, ts2);
  Add(2, 2U, "150", "150", kFileSize, /*path_id=*/0, /*smallest_seq=*/100,
      /*largest_seq=*/100, /*compensated_file_size=*/kFileSize,
      /*marked_for_compact=*/false, Temperature::kUnknown,
      kUnknownOldestAncesterTime, ts3, ts4);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          &log_buffer_));
  assert(compaction);
  ASSERT_TRUE(!compaction->is_trivial_move());
}

class PerKeyPlacementCompactionPickerTest
    : public CompactionPickerTest,
      public testing::WithParamInterface<bool> {
 public:
  PerKeyPlacementCompactionPickerTest() : CompactionPickerTest() {}

  void SetUp() override { enable_per_key_placement_ = GetParam(); }

 protected:
  bool enable_per_key_placement_ = false;
};

TEST_P(PerKeyPlacementCompactionPickerTest, OverlapWithNormalCompaction) {
  SyncPoint::GetInstance()->SetCallBack(
      "Compaction::SupportsPerKeyPlacement:Enabled", [&](void* arg) {
        auto supports_per_key_placement = static_cast<bool*>(arg);
        *supports_per_key_placement = enable_per_key_placement_;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  int num_levels = ioptions_.num_levels;
  NewVersionStorage(num_levels, kCompactionStyleLevel);

  Add(0, 21U, "100", "150", 60000000U);
  Add(0, 22U, "300", "350", 60000000U);
  Add(5, 40U, "200", "250", 60000000U);
  Add(6, 50U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(40);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(level_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(level_compaction_picker.CompactFiles(
      comp_options, input_files, 5, vstorage_.get(), mutable_cf_options_,
      mutable_db_options_, 0));

  input_set.clear();
  input_files.clear();
  input_set.insert(21);
  input_set.insert(22);
  input_set.insert(50);
  ASSERT_OK(level_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            level_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 6,
                Compaction::EvaluatePenultimateLevel(vstorage_.get(), ioptions_,
                                                     0, 6)));
}

TEST_P(PerKeyPlacementCompactionPickerTest, NormalCompactionOverlap) {
  SyncPoint::GetInstance()->SetCallBack(
      "Compaction::SupportsPerKeyPlacement:Enabled", [&](void* arg) {
        auto supports_per_key_placement = static_cast<bool*>(arg);
        *supports_per_key_placement = enable_per_key_placement_;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  int num_levels = ioptions_.num_levels;
  NewVersionStorage(num_levels, kCompactionStyleLevel);

  Add(0, 21U, "100", "150", 60000000U);
  Add(0, 22U, "300", "350", 60000000U);
  Add(4, 40U, "200", "220", 60000000U);
  Add(4, 41U, "230", "250", 60000000U);
  Add(6, 50U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(21);
  input_set.insert(22);
  input_set.insert(50);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(level_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(level_compaction_picker.CompactFiles(
      comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
      mutable_db_options_, 0));

  input_set.clear();
  input_files.clear();
  input_set.insert(40);
  input_set.insert(41);
  ASSERT_OK(level_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            level_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 5, Compaction::kInvalidLevel));
}

TEST_P(PerKeyPlacementCompactionPickerTest,
       OverlapWithNormalCompactionUniveral) {
  SyncPoint::GetInstance()->SetCallBack(
      "Compaction::SupportsPerKeyPlacement:Enabled", [&](void* arg) {
        auto supports_per_key_placement = static_cast<bool*>(arg);
        *supports_per_key_placement = enable_per_key_placement_;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  int num_levels = ioptions_.num_levels;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  Add(0, 21U, "100", "150", 60000000U);
  Add(0, 22U, "300", "350", 60000000U);
  Add(5, 40U, "200", "250", 60000000U);
  Add(6, 50U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(40);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(universal_compaction_picker.CompactFiles(
      comp_options, input_files, 5, vstorage_.get(), mutable_cf_options_,
      mutable_db_options_, 0));

  input_set.clear();
  input_files.clear();
  input_set.insert(21);
  input_set.insert(22);
  input_set.insert(50);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            universal_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 6,
                Compaction::EvaluatePenultimateLevel(vstorage_.get(), ioptions_,
                                                     0, 6)));
}

TEST_P(PerKeyPlacementCompactionPickerTest, NormalCompactionOverlapUniversal) {
  SyncPoint::GetInstance()->SetCallBack(
      "Compaction::SupportsPerKeyPlacement:Enabled", [&](void* arg) {
        auto supports_per_key_placement = static_cast<bool*>(arg);
        *supports_per_key_placement = enable_per_key_placement_;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  int num_levels = ioptions_.num_levels;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  Add(0, 21U, "100", "150", 60000000U);
  Add(0, 22U, "300", "350", 60000000U);
  Add(4, 40U, "200", "220", 60000000U);
  Add(4, 41U, "230", "250", 60000000U);
  Add(6, 50U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(21);
  input_set.insert(22);
  input_set.insert(50);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(universal_compaction_picker.CompactFiles(
      comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
      mutable_db_options_, 0));

  input_set.clear();
  input_files.clear();
  input_set.insert(40);
  input_set.insert(41);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            universal_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 5, Compaction::kInvalidLevel));
}

TEST_P(PerKeyPlacementCompactionPickerTest, PenultimateOverlapUniversal) {
  // This test is make sure the Tiered compaction would lock whole range of
  // both output level and penultimate level
  if (enable_per_key_placement_) {
    ioptions_.preclude_last_level_data_seconds = 10000;
  }

  int num_levels = ioptions_.num_levels;
  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  // L4:   [200, 220]  [230, 250]       [360, 380]
  // L5:
  // L6: [101,                    351]
  Add(4, 40U, "200", "220", 60000000U);
  Add(4, 41U, "230", "250", 60000000U);
  Add(4, 42U, "360", "380", 60000000U);
  Add(6, 60U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  // the existing compaction is the 1st L4 file + L6 file
  // then compaction of the 2nd L4 file to L5 (penultimate level) is overlapped
  // when the tiered compaction feature is on.
  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(40);
  input_set.insert(60);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(universal_compaction_picker.CompactFiles(
      comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
      mutable_db_options_, 0));

  input_set.clear();
  input_files.clear();
  input_set.insert(41);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            universal_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 5, Compaction::kInvalidLevel));

  // compacting the 3rd L4 file is always safe:
  input_set.clear();
  input_files.clear();
  input_set.insert(42);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_FALSE(universal_compaction_picker.FilesRangeOverlapWithCompaction(
      input_files, 5, Compaction::kInvalidLevel));
}

TEST_P(PerKeyPlacementCompactionPickerTest, LastLevelOnlyOverlapUniversal) {
  if (enable_per_key_placement_) {
    ioptions_.preclude_last_level_data_seconds = 10000;
  }

  int num_levels = ioptions_.num_levels;
  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  // L4:   [200, 220]  [230, 250]       [360, 380]
  // L5:
  // L6: [101,                    351]
  Add(4, 40U, "200", "220", 60000000U);
  Add(4, 41U, "230", "250", 60000000U);
  Add(4, 42U, "360", "380", 60000000U);
  Add(6, 60U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(60);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(universal_compaction_picker.CompactFiles(
      comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
      mutable_db_options_, 0));

  // cannot compact file 41 if the preclude_last_level feature is on, otherwise
  // compact file 41 is okay.
  input_set.clear();
  input_files.clear();
  input_set.insert(41);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            universal_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 5, Compaction::kInvalidLevel));

  // compacting the 3rd L4 file is always safe:
  input_set.clear();
  input_files.clear();
  input_set.insert(42);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_FALSE(universal_compaction_picker.FilesRangeOverlapWithCompaction(
      input_files, 5, Compaction::kInvalidLevel));
}

TEST_P(PerKeyPlacementCompactionPickerTest,
       LastLevelOnlyFailPenultimateUniversal) {
  // This is to test last_level only compaction still unable to do the
  // penultimate level compaction if there's already a file in the penultimate
  // level.
  // This should rarely happen in universal compaction, as the non-empty L5
  // should be included in the compaction.
  if (enable_per_key_placement_) {
    ioptions_.preclude_last_level_data_seconds = 10000;
  }

  int num_levels = ioptions_.num_levels;
  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  // L4:   [200, 220]
  // L5:              [230, 250]
  // L6: [101,                    351]
  Add(4, 40U, "200", "220", 60000000U);
  Add(5, 50U, "230", "250", 60000000U);
  Add(6, 60U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(60);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(universal_compaction_picker.CompactFiles(
      comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
      mutable_db_options_, 0));

  ASSERT_TRUE(comp1);
  ASSERT_EQ(comp1->GetPenultimateLevel(), Compaction::kInvalidLevel);

  // As comp1 cannot be output to the penultimate level, compacting file 40 to
  // L5 is always safe.
  input_set.clear();
  input_files.clear();
  input_set.insert(40);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_FALSE(universal_compaction_picker.FilesRangeOverlapWithCompaction(
      input_files, 5, Compaction::kInvalidLevel));

  std::unique_ptr<Compaction> comp2(universal_compaction_picker.CompactFiles(
      comp_options, input_files, 5, vstorage_.get(), mutable_cf_options_,
      mutable_db_options_, 0));
  ASSERT_TRUE(comp2);
  ASSERT_EQ(Compaction::kInvalidLevel, comp2->GetPenultimateLevel());
}

TEST_P(PerKeyPlacementCompactionPickerTest,
       LastLevelOnlyConflictWithOngoingUniversal) {
  // This is to test last_level only compaction still unable to do the
  // penultimate level compaction if there's already an ongoing compaction to
  // the penultimate level
  if (enable_per_key_placement_) {
    ioptions_.preclude_last_level_data_seconds = 10000;
  }

  int num_levels = ioptions_.num_levels;
  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  // L4:   [200, 220]  [230, 250]       [360, 380]
  // L5:
  // L6: [101,                    351]
  Add(4, 40U, "200", "220", 60000000U);
  Add(4, 41U, "230", "250", 60000000U);
  Add(4, 42U, "360", "380", 60000000U);
  Add(6, 60U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  // create an ongoing compaction to L5 (penultimate level)
  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(40);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(universal_compaction_picker.CompactFiles(
      comp_options, input_files, 5, vstorage_.get(), mutable_cf_options_,
      mutable_db_options_, 0));

  ASSERT_TRUE(comp1);
  ASSERT_EQ(comp1->GetPenultimateLevel(), Compaction::kInvalidLevel);

  input_set.clear();
  input_files.clear();
  input_set.insert(60);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            universal_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 6,
                Compaction::EvaluatePenultimateLevel(vstorage_.get(), ioptions_,
                                                     6, 6)));

  if (!enable_per_key_placement_) {
    std::unique_ptr<Compaction> comp2(universal_compaction_picker.CompactFiles(
        comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
        mutable_db_options_, 0));
    ASSERT_TRUE(comp2);
    ASSERT_EQ(Compaction::kInvalidLevel, comp2->GetPenultimateLevel());
  }
}

TEST_P(PerKeyPlacementCompactionPickerTest,
       LastLevelOnlyNoConflictWithOngoingUniversal) {
  // This is similar to `LastLevelOnlyConflictWithOngoingUniversal`, the only
  // change is the ongoing compaction to L5 has no overlap with the last level
  // compaction, so it's safe to move data from the last level to the
  // penultimate level.
  if (enable_per_key_placement_) {
    ioptions_.preclude_last_level_data_seconds = 10000;
  }

  int num_levels = ioptions_.num_levels;
  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  // L4:   [200, 220]  [230, 250]       [360, 380]
  // L5:
  // L6: [101,                    351]
  Add(4, 40U, "200", "220", 60000000U);
  Add(4, 41U, "230", "250", 60000000U);
  Add(4, 42U, "360", "380", 60000000U);
  Add(6, 60U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  // create an ongoing compaction to L5 (penultimate level)
  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(42);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(universal_compaction_picker.CompactFiles(
      comp_options, input_files, 5, vstorage_.get(), mutable_cf_options_,
      mutable_db_options_, 0));

  ASSERT_TRUE(comp1);
  ASSERT_EQ(comp1->GetPenultimateLevel(), Compaction::kInvalidLevel);

  input_set.clear();
  input_files.clear();
  input_set.insert(60);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  // always safe to move data up
  ASSERT_FALSE(universal_compaction_picker.FilesRangeOverlapWithCompaction(
      input_files, 6,
      Compaction::EvaluatePenultimateLevel(vstorage_.get(), ioptions_, 6, 6)));

  // 2 compactions can be run in parallel
  std::unique_ptr<Compaction> comp2(universal_compaction_picker.CompactFiles(
      comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
      mutable_db_options_, 0));
  ASSERT_TRUE(comp2);
  if (enable_per_key_placement_) {
    ASSERT_NE(Compaction::kInvalidLevel, comp2->GetPenultimateLevel());
  } else {
    ASSERT_EQ(Compaction::kInvalidLevel, comp2->GetPenultimateLevel());
  }
}

INSTANTIATE_TEST_CASE_P(PerKeyPlacementCompactionPickerTest,
                        PerKeyPlacementCompactionPickerTest, ::testing::Bool());

#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
