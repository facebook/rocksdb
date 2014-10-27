//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/compaction_picker.h"
#include <string>
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class CountingLogger : public Logger {
 public:
  virtual void Logv(const char* format, va_list ap) override { log_count++; }
  size_t log_count;
};

class CompactionPickerTest {
 public:
  const Comparator* ucmp;
  InternalKeyComparator icmp;
  Options options;
  ImmutableCFOptions ioptions;
  MutableCFOptions mutable_cf_options;
  LevelCompactionPicker level_compaction_picker;
  std::string cf_name;
  CountingLogger logger;
  LogBuffer log_buffer;
  VersionStorageInfo vstorage;
  uint32_t file_num;
  CompactionOptionsFIFO fifo_options;
  std::vector<uint64_t> size_being_compacted;

  CompactionPickerTest()
      : ucmp(BytewiseComparator()),
        icmp(ucmp),
        ioptions(options),
        mutable_cf_options(options, ioptions),
        level_compaction_picker(ioptions, &icmp),
        cf_name("dummy"),
        log_buffer(InfoLogLevel::INFO_LEVEL, &logger),
        vstorage(&icmp, ucmp, options.num_levels, kCompactionStyleLevel,
                 nullptr),
        file_num(1) {
    fifo_options.max_table_files_size = 1;
    mutable_cf_options.RefreshDerivedOptions(ioptions);
    size_being_compacted.resize(options.num_levels);
  }

  ~CompactionPickerTest() {
    auto* files = vstorage.GetFiles();
    for (int i = 0; i < vstorage.NumberLevels(); i++) {
      for (auto* f : files[i]) {
        delete f;
      }
    }
  }

  void Add(int level, uint32_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 0, uint32_t path_id = 0,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    assert(level < vstorage.NumberLevels());
    auto& files = vstorage.GetFiles()[level];
    FileMetaData* f = new FileMetaData;
    f->fd = FileDescriptor(file_number, path_id, file_size);
    f->smallest = InternalKey(smallest, smallest_seq, kTypeValue);
    f->largest = InternalKey(largest, largest_seq, kTypeValue);
    f->compensated_file_size = file_size;
    files.push_back(f);
  }

  void UpdateVersionStorageInfo() {
    vstorage.ComputeCompactionScore(mutable_cf_options, fifo_options,
                                    size_being_compacted);
    vstorage.UpdateFilesBySize();
    vstorage.UpdateNumNonEmptyLevels();
    vstorage.GenerateFileIndexer();
    vstorage.GenerateLevelFilesBrief();
    vstorage.SetFinalized();
  }
};

TEST(CompactionPickerTest, Empty) {
  UpdateVersionStorageInfo();
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name, mutable_cf_options, &vstorage, &log_buffer));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST(CompactionPickerTest, Single) {
  mutable_cf_options.level0_file_num_compaction_trigger = 2;
  Add(0, 1U, "p", "q");
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name, mutable_cf_options, &vstorage, &log_buffer));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST(CompactionPickerTest, Level0Trigger) {
  mutable_cf_options.level0_file_num_compaction_trigger = 2;
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name, mutable_cf_options, &vstorage, &log_buffer));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
}

TEST(CompactionPickerTest, Level1Trigger) {
  Add(1, 66U, "150", "200", 1000000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name, mutable_cf_options, &vstorage, &log_buffer));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1, compaction->num_input_files(0));
  ASSERT_EQ(66U, compaction->input(0, 0)->fd.GetNumber());
}

TEST(CompactionPickerTest, Level1Trigger2) {
  Add(1, 66U, "150", "200", 1000000000U);
  Add(1, 88U, "201", "300", 1000000000U);
  Add(2, 6U, "150", "180", 1000000000U);
  Add(2, 7U, "180", "220", 1000000000U);
  Add(2, 8U, "220", "300", 1000000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name, mutable_cf_options, &vstorage, &log_buffer));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1, compaction->num_input_files(0));
  ASSERT_EQ(2, compaction->num_input_files(1));
  ASSERT_EQ(66U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 1)->fd.GetNumber());
}

}  // namespace rocksdb

int main(int argc, char** argv) { return rocksdb::test::RunAllTests(); }
