//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include "db/db_impl/db_impl.h"
#include "db/log_writer.h"
#include "rocksdb/convenience.h"
#include "rocksdb/file_system.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/mock_table.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class GenerateLevelFilesBriefTest : public testing::Test {
 public:
  std::vector<FileMetaData*> files_;
  LevelFilesBrief file_level_;
  Arena arena_;

  GenerateLevelFilesBriefTest() { }

  ~GenerateLevelFilesBriefTest() override {
    for (size_t i = 0; i < files_.size(); i++) {
      delete files_[i];
    }
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    FileMetaData* f = new FileMetaData(
        files_.size() + 1, 0, 0,
        InternalKey(smallest, smallest_seq, kTypeValue),
        InternalKey(largest, largest_seq, kTypeValue), smallest_seq,
        largest_seq, /* marked_for_compact */ false, kInvalidBlobFileNumber,
        kUnknownOldestAncesterTime, kUnknownFileCreationTime,
        kUnknownFileChecksum, kUnknownFileChecksumFuncName);
    files_.push_back(f);
  }

  int Compare() {
    int diff = 0;
    for (size_t i = 0; i < files_.size(); i++) {
      if (file_level_.files[i].fd.GetNumber() != files_[i]->fd.GetNumber()) {
        diff++;
      }
    }
    return diff;
  }
};

TEST_F(GenerateLevelFilesBriefTest, Empty) {
  DoGenerateLevelFilesBrief(&file_level_, files_, &arena_);
  ASSERT_EQ(0u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

TEST_F(GenerateLevelFilesBriefTest, Single) {
  Add("p", "q");
  DoGenerateLevelFilesBrief(&file_level_, files_, &arena_);
  ASSERT_EQ(1u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

TEST_F(GenerateLevelFilesBriefTest, Multiple) {
  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  DoGenerateLevelFilesBrief(&file_level_, files_, &arena_);
  ASSERT_EQ(4u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

class CountingLogger : public Logger {
 public:
  CountingLogger() : log_count(0) {}
  using Logger::Logv;
  void Logv(const char* /*format*/, va_list /*ap*/) override { log_count++; }
  int log_count;
};

Options GetOptionsWithNumLevels(int num_levels,
                                std::shared_ptr<CountingLogger> logger) {
  Options opt;
  opt.num_levels = num_levels;
  opt.info_log = logger;
  return opt;
}

class VersionStorageInfoTestBase : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  std::shared_ptr<CountingLogger> logger_;
  Options options_;
  ImmutableOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  VersionStorageInfo vstorage_;

  InternalKey GetInternalKey(const char* ukey,
                             SequenceNumber smallest_seq = 100) {
    return InternalKey(ukey, smallest_seq, kTypeValue);
  }

  explicit VersionStorageInfoTestBase(const Comparator* ucmp)
      : ucmp_(ucmp),
        icmp_(ucmp_),
        logger_(new CountingLogger()),
        options_(GetOptionsWithNumLevels(6, logger_)),
        ioptions_(options_),
        mutable_cf_options_(options_),
        vstorage_(&icmp_, ucmp_, 6, kCompactionStyleLevel,
                  /*src_vstorage=*/nullptr,
                  /*_force_consistency_checks=*/false) {}

  ~VersionStorageInfoTestBase() override {
    for (int i = 0; i < vstorage_.num_levels(); ++i) {
      for (auto* f : vstorage_.LevelFiles(i)) {
        if (--f->refs == 0) {
          delete f;
        }
      }
    }
  }

  void Add(int level, uint32_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 0) {
    assert(level < vstorage_.num_levels());
    FileMetaData* f = new FileMetaData(
        file_number, 0, file_size, GetInternalKey(smallest, 0),
        GetInternalKey(largest, 0), /* smallest_seq */ 0, /* largest_seq */ 0,
        /* marked_for_compact */ false, kInvalidBlobFileNumber,
        kUnknownOldestAncesterTime, kUnknownFileCreationTime,
        kUnknownFileChecksum, kUnknownFileChecksumFuncName);
    f->compensated_file_size = file_size;
    vstorage_.AddFile(level, f);
  }

  void Add(int level, uint32_t file_number, const InternalKey& smallest,
           const InternalKey& largest, uint64_t file_size = 0) {
    assert(level < vstorage_.num_levels());
    FileMetaData* f = new FileMetaData(
        file_number, 0, file_size, smallest, largest, /* smallest_seq */ 0,
        /* largest_seq */ 0, /* marked_for_compact */ false,
        kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
        kUnknownFileCreationTime, kUnknownFileChecksum,
        kUnknownFileChecksumFuncName);
    f->compensated_file_size = file_size;
    vstorage_.AddFile(level, f);
  }

  std::string GetOverlappingFiles(int level, const InternalKey& begin,
                                  const InternalKey& end) {
    std::vector<FileMetaData*> inputs;
    vstorage_.GetOverlappingInputs(level, &begin, &end, &inputs);

    std::string result;
    for (size_t i = 0; i < inputs.size(); ++i) {
      if (i > 0) {
        result += ",";
      }
      AppendNumberTo(&result, inputs[i]->fd.GetNumber());
    }
    return result;
  }
};

class VersionStorageInfoTest : public VersionStorageInfoTestBase {
 public:
  VersionStorageInfoTest() : VersionStorageInfoTestBase(BytewiseComparator()) {}

  ~VersionStorageInfoTest() override {}
};

TEST_F(VersionStorageInfoTest, MaxBytesForLevelStatic) {
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.max_bytes_for_level_base = 10;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  Add(4, 100U, "1", "2");
  Add(5, 101U, "1", "2");

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(1), 10U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 50U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 250U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1250U);

  ASSERT_EQ(0, logger_->log_count);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamic) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  Add(5, 1U, "1", "2", 500U);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(vstorage_.base_level(), 5);

  Add(5, 2U, "3", "4", 550U);
  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1000U);
  ASSERT_EQ(vstorage_.base_level(), 4);

  Add(4, 3U, "3", "4", 550U);
  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1000U);
  ASSERT_EQ(vstorage_.base_level(), 4);

  Add(3, 4U, "3", "4", 250U);
  Add(3, 5U, "5", "7", 300U);
  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(1, logger_->log_count);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1005U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 1000U);
  ASSERT_EQ(vstorage_.base_level(), 3);

  Add(1, 6U, "3", "4", 5U);
  Add(1, 7U, "8", "9", 5U);
  logger_->log_count = 0;
  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(1, logger_->log_count);
  ASSERT_GT(vstorage_.MaxBytesForLevel(4), 1005U);
  ASSERT_GT(vstorage_.MaxBytesForLevel(3), 1005U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 1005U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(1), 1000U);
  ASSERT_EQ(vstorage_.base_level(), 1);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicLotsOfData) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 100;
  mutable_cf_options_.max_bytes_for_level_multiplier = 2;
  Add(0, 1U, "1", "2", 50U);
  Add(1, 2U, "1", "2", 50U);
  Add(2, 3U, "1", "2", 500U);
  Add(3, 4U, "1", "2", 500U);
  Add(4, 5U, "1", "2", 1700U);
  Add(5, 6U, "1", "2", 500U);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 800U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 400U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 200U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(1), 100U);
  ASSERT_EQ(vstorage_.base_level(), 1);
  ASSERT_EQ(0, logger_->log_count);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicLargeLevel) {
  uint64_t kOneGB = 1000U * 1000U * 1000U;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 10U * kOneGB;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  Add(0, 1U, "1", "2", 50U);
  Add(3, 4U, "1", "2", 32U * kOneGB);
  Add(4, 5U, "1", "2", 500U * kOneGB);
  Add(5, 6U, "1", "2", 3000U * kOneGB);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(5), 3000U * kOneGB);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 300U * kOneGB);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 30U * kOneGB);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 10U * kOneGB);
  ASSERT_EQ(vstorage_.base_level(), 2);
  ASSERT_EQ(0, logger_->log_count);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicWithLargeL0_1) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 40000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;

  Add(0, 1U, "1", "2", 10000U);
  Add(0, 2U, "1", "2", 10000U);
  Add(0, 3U, "1", "2", 10000U);

  Add(5, 4U, "1", "2", 1286250U);
  Add(4, 5U, "1", "2", 200000U);
  Add(3, 6U, "1", "2", 40000U);
  Add(2, 7U, "1", "2", 8000U);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(2, vstorage_.base_level());
  // level multiplier should be 3.5
  ASSERT_EQ(vstorage_.level_multiplier(), 5.0);
  // Level size should be around 30,000, 105,000, 367,500
  ASSERT_EQ(40000U, vstorage_.MaxBytesForLevel(2));
  ASSERT_EQ(51450U, vstorage_.MaxBytesForLevel(3));
  ASSERT_EQ(257250U, vstorage_.MaxBytesForLevel(4));
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicWithLargeL0_2) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 10000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;

  Add(0, 11U, "1", "2", 10000U);
  Add(0, 12U, "1", "2", 10000U);
  Add(0, 13U, "1", "2", 10000U);

  Add(5, 4U, "1", "2", 1286250U);
  Add(4, 5U, "1", "2", 200000U);
  Add(3, 6U, "1", "2", 40000U);
  Add(2, 7U, "1", "2", 8000U);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(2, vstorage_.base_level());
  // level multiplier should be 3.5
  ASSERT_LT(vstorage_.level_multiplier(), 3.6);
  ASSERT_GT(vstorage_.level_multiplier(), 3.4);
  // Level size should be around 30,000, 105,000, 367,500
  ASSERT_EQ(30000U, vstorage_.MaxBytesForLevel(2));
  ASSERT_LT(vstorage_.MaxBytesForLevel(3), 110000U);
  ASSERT_GT(vstorage_.MaxBytesForLevel(3), 100000U);
  ASSERT_LT(vstorage_.MaxBytesForLevel(4), 370000U);
  ASSERT_GT(vstorage_.MaxBytesForLevel(4), 360000U);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicWithLargeL0_3) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 10000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;

  Add(0, 11U, "1", "2", 5000U);
  Add(0, 12U, "1", "2", 5000U);
  Add(0, 13U, "1", "2", 5000U);
  Add(0, 14U, "1", "2", 5000U);
  Add(0, 15U, "1", "2", 5000U);
  Add(0, 16U, "1", "2", 5000U);

  Add(5, 4U, "1", "2", 1286250U);
  Add(4, 5U, "1", "2", 200000U);
  Add(3, 6U, "1", "2", 40000U);
  Add(2, 7U, "1", "2", 8000U);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(2, vstorage_.base_level());
  // level multiplier should be 3.5
  ASSERT_LT(vstorage_.level_multiplier(), 3.6);
  ASSERT_GT(vstorage_.level_multiplier(), 3.4);
  // Level size should be around 30,000, 105,000, 367,500
  ASSERT_EQ(30000U, vstorage_.MaxBytesForLevel(2));
  ASSERT_LT(vstorage_.MaxBytesForLevel(3), 110000U);
  ASSERT_GT(vstorage_.MaxBytesForLevel(3), 100000U);
  ASSERT_LT(vstorage_.MaxBytesForLevel(4), 370000U);
  ASSERT_GT(vstorage_.MaxBytesForLevel(4), 360000U);
}

TEST_F(VersionStorageInfoTest, EstimateLiveDataSize) {
  // Test whether the overlaps are detected as expected
  Add(1, 1U, "4", "7", 1U);  // Perfect overlap with last level
  Add(2, 2U, "3", "5", 1U);  // Partial overlap with last level
  Add(2, 3U, "6", "8", 1U);  // Partial overlap with last level
  Add(3, 4U, "1", "9", 1U);  // Contains range of last level
  Add(4, 5U, "4", "5", 1U);  // Inside range of last level
  Add(4, 6U, "6", "7", 1U);  // Inside range of last level
  Add(5, 7U, "4", "7", 10U);
  ASSERT_EQ(10U, vstorage_.EstimateLiveDataSize());
}

TEST_F(VersionStorageInfoTest, EstimateLiveDataSize2) {
  Add(0, 1U, "9", "9", 1U);  // Level 0 is not ordered
  Add(0, 2U, "5", "6", 1U);  // Ignored because of [5,6] in l1
  Add(1, 3U, "1", "2", 1U);  // Ignored because of [2,3] in l2
  Add(1, 4U, "3", "4", 1U);  // Ignored because of [2,3] in l2
  Add(1, 5U, "5", "6", 1U);
  Add(2, 6U, "2", "3", 1U);
  Add(3, 7U, "7", "8", 1U);
  ASSERT_EQ(4U, vstorage_.EstimateLiveDataSize());
}

TEST_F(VersionStorageInfoTest, GetOverlappingInputs) {
  // Two files that overlap at the range deletion tombstone sentinel.
  Add(1, 1U, {"a", 0, kTypeValue}, {"b", kMaxSequenceNumber, kTypeRangeDeletion}, 1);
  Add(1, 2U, {"b", 0, kTypeValue}, {"c", 0, kTypeValue}, 1);
  // Two files that overlap at the same user key.
  Add(1, 3U, {"d", 0, kTypeValue}, {"e", kMaxSequenceNumber, kTypeValue}, 1);
  Add(1, 4U, {"e", 0, kTypeValue}, {"f", 0, kTypeValue}, 1);
  // Two files that do not overlap.
  Add(1, 5U, {"g", 0, kTypeValue}, {"h", 0, kTypeValue}, 1);
  Add(1, 6U, {"i", 0, kTypeValue}, {"j", 0, kTypeValue}, 1);
  vstorage_.UpdateNumNonEmptyLevels();
  vstorage_.GenerateLevelFilesBrief();

  ASSERT_EQ("1,2", GetOverlappingFiles(
      1, {"a", 0, kTypeValue}, {"b", 0, kTypeValue}));
  ASSERT_EQ("1", GetOverlappingFiles(
      1, {"a", 0, kTypeValue}, {"b", kMaxSequenceNumber, kTypeRangeDeletion}));
  ASSERT_EQ("2", GetOverlappingFiles(
      1, {"b", kMaxSequenceNumber, kTypeValue}, {"c", 0, kTypeValue}));
  ASSERT_EQ("3,4", GetOverlappingFiles(
      1, {"d", 0, kTypeValue}, {"e", 0, kTypeValue}));
  ASSERT_EQ("3", GetOverlappingFiles(
      1, {"d", 0, kTypeValue}, {"e", kMaxSequenceNumber, kTypeRangeDeletion}));
  ASSERT_EQ("3,4", GetOverlappingFiles(
      1, {"e", kMaxSequenceNumber, kTypeValue}, {"f", 0, kTypeValue}));
  ASSERT_EQ("3,4", GetOverlappingFiles(
      1, {"e", 0, kTypeValue}, {"f", 0, kTypeValue}));
  ASSERT_EQ("5", GetOverlappingFiles(
      1, {"g", 0, kTypeValue}, {"h", 0, kTypeValue}));
  ASSERT_EQ("6", GetOverlappingFiles(
      1, {"i", 0, kTypeValue}, {"j", 0, kTypeValue}));
}

TEST_F(VersionStorageInfoTest, FileLocationAndMetaDataByNumber) {
  Add(0, 11U, "1", "2", 5000U);
  Add(0, 12U, "1", "2", 5000U);

  Add(2, 7U, "1", "2", 8000U);

  ASSERT_EQ(vstorage_.GetFileLocation(11U),
            VersionStorageInfo::FileLocation(0, 0));
  ASSERT_NE(vstorage_.GetFileMetaDataByNumber(11U), nullptr);

  ASSERT_EQ(vstorage_.GetFileLocation(12U),
            VersionStorageInfo::FileLocation(0, 1));
  ASSERT_NE(vstorage_.GetFileMetaDataByNumber(12U), nullptr);

  ASSERT_EQ(vstorage_.GetFileLocation(7U),
            VersionStorageInfo::FileLocation(2, 0));
  ASSERT_NE(vstorage_.GetFileMetaDataByNumber(7U), nullptr);

  ASSERT_FALSE(vstorage_.GetFileLocation(999U).IsValid());
  ASSERT_EQ(vstorage_.GetFileMetaDataByNumber(999U), nullptr);
}

class VersionStorageInfoTimestampTest : public VersionStorageInfoTestBase {
 public:
  VersionStorageInfoTimestampTest()
      : VersionStorageInfoTestBase(test::ComparatorWithU64Ts()) {}
  ~VersionStorageInfoTimestampTest() override {}
  std::string Timestamp(uint64_t ts) const {
    std::string ret;
    PutFixed64(&ret, ts);
    return ret;
  }
  std::string PackUserKeyAndTimestamp(const Slice& ukey, uint64_t ts) const {
    std::string ret;
    ret.assign(ukey.data(), ukey.size());
    PutFixed64(&ret, ts);
    return ret;
  }
};

TEST_F(VersionStorageInfoTimestampTest, GetOverlappingInputs) {
  Add(/*level=*/1, /*file_number=*/1, /*smallest=*/
      {PackUserKeyAndTimestamp("a", /*ts=*/9), /*s=*/0, kTypeValue},
      /*largest=*/
      {PackUserKeyAndTimestamp("a", /*ts=*/8), /*s=*/0, kTypeValue},
      /*file_size=*/100);
  Add(/*level=*/1, /*file_number=*/2, /*smallest=*/
      {PackUserKeyAndTimestamp("a", /*ts=*/5), /*s=*/0, kTypeValue},
      /*largest=*/
      {PackUserKeyAndTimestamp("b", /*ts=*/10), /*s=*/0, kTypeValue},
      /*file_size=*/100);
  Add(/*level=*/1, /*file_number=*/3, /*smallest=*/
      {PackUserKeyAndTimestamp("c", /*ts=*/12), /*s=*/0, kTypeValue},
      /*largest=*/
      {PackUserKeyAndTimestamp("d", /*ts=*/1), /*s=*/0, kTypeValue},
      /*file_size=*/100);
  vstorage_.UpdateNumNonEmptyLevels();
  vstorage_.GenerateLevelFilesBrief();
  ASSERT_EQ(
      "1,2",
      GetOverlappingFiles(
          /*level=*/1,
          {PackUserKeyAndTimestamp("a", /*ts=*/12), /*s=*/0, kTypeValue},
          {PackUserKeyAndTimestamp("a", /*ts=*/11), /*s=*/0, kTypeValue}));
  ASSERT_EQ("3",
            GetOverlappingFiles(
                /*level=*/1,
                {PackUserKeyAndTimestamp("c", /*ts=*/15), /*s=*/0, kTypeValue},
                {PackUserKeyAndTimestamp("c", /*ts=*/2), /*s=*/0, kTypeValue}));
}

class FindLevelFileTest : public testing::Test {
 public:
  LevelFilesBrief file_level_;
  bool disjoint_sorted_files_;
  Arena arena_;

  FindLevelFileTest() : disjoint_sorted_files_(true) { }

  ~FindLevelFileTest() override {}

  void LevelFileInit(size_t num = 0) {
    char* mem = arena_.AllocateAligned(num * sizeof(FdWithKeyRange));
    file_level_.files = new (mem)FdWithKeyRange[num];
    file_level_.num_files = 0;
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    InternalKey smallest_key = InternalKey(smallest, smallest_seq, kTypeValue);
    InternalKey largest_key = InternalKey(largest, largest_seq, kTypeValue);

    Slice smallest_slice = smallest_key.Encode();
    Slice largest_slice = largest_key.Encode();

    char* mem = arena_.AllocateAligned(
        smallest_slice.size() + largest_slice.size());
    memcpy(mem, smallest_slice.data(), smallest_slice.size());
    memcpy(mem + smallest_slice.size(), largest_slice.data(),
        largest_slice.size());

    // add to file_level_
    size_t num = file_level_.num_files;
    auto& file = file_level_.files[num];
    file.fd = FileDescriptor(num + 1, 0, 0);
    file.smallest_key = Slice(mem, smallest_slice.size());
    file.largest_key = Slice(mem + smallest_slice.size(),
        largest_slice.size());
    file_level_.num_files++;
  }

  int Find(const char* key) {
    InternalKey target(key, 100, kTypeValue);
    InternalKeyComparator cmp(BytewiseComparator());
    return FindFile(cmp, file_level_, target.Encode());
  }

  bool Overlaps(const char* smallest, const char* largest) {
    InternalKeyComparator cmp(BytewiseComparator());
    Slice s(smallest != nullptr ? smallest : "");
    Slice l(largest != nullptr ? largest : "");
    return SomeFileOverlapsRange(cmp, disjoint_sorted_files_, file_level_,
                                 (smallest != nullptr ? &s : nullptr),
                                 (largest != nullptr ? &l : nullptr));
  }
};

TEST_F(FindLevelFileTest, LevelEmpty) {
  LevelFileInit(0);

  ASSERT_EQ(0, Find("foo"));
  ASSERT_TRUE(! Overlaps("a", "z"));
  ASSERT_TRUE(! Overlaps(nullptr, "z"));
  ASSERT_TRUE(! Overlaps("a", nullptr));
  ASSERT_TRUE(! Overlaps(nullptr, nullptr));
}

TEST_F(FindLevelFileTest, LevelSingle) {
  LevelFileInit(1);

  Add("p", "q");
  ASSERT_EQ(0, Find("a"));
  ASSERT_EQ(0, Find("p"));
  ASSERT_EQ(0, Find("p1"));
  ASSERT_EQ(0, Find("q"));
  ASSERT_EQ(1, Find("q1"));
  ASSERT_EQ(1, Find("z"));

  ASSERT_TRUE(! Overlaps("a", "b"));
  ASSERT_TRUE(! Overlaps("z1", "z2"));
  ASSERT_TRUE(Overlaps("a", "p"));
  ASSERT_TRUE(Overlaps("a", "q"));
  ASSERT_TRUE(Overlaps("a", "z"));
  ASSERT_TRUE(Overlaps("p", "p1"));
  ASSERT_TRUE(Overlaps("p", "q"));
  ASSERT_TRUE(Overlaps("p", "z"));
  ASSERT_TRUE(Overlaps("p1", "p2"));
  ASSERT_TRUE(Overlaps("p1", "z"));
  ASSERT_TRUE(Overlaps("q", "q"));
  ASSERT_TRUE(Overlaps("q", "q1"));

  ASSERT_TRUE(! Overlaps(nullptr, "j"));
  ASSERT_TRUE(! Overlaps("r", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "p"));
  ASSERT_TRUE(Overlaps(nullptr, "p1"));
  ASSERT_TRUE(Overlaps("q", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
}

TEST_F(FindLevelFileTest, LevelMultiple) {
  LevelFileInit(4);

  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_EQ(0, Find("100"));
  ASSERT_EQ(0, Find("150"));
  ASSERT_EQ(0, Find("151"));
  ASSERT_EQ(0, Find("199"));
  ASSERT_EQ(0, Find("200"));
  ASSERT_EQ(1, Find("201"));
  ASSERT_EQ(1, Find("249"));
  ASSERT_EQ(1, Find("250"));
  ASSERT_EQ(2, Find("251"));
  ASSERT_EQ(2, Find("299"));
  ASSERT_EQ(2, Find("300"));
  ASSERT_EQ(2, Find("349"));
  ASSERT_EQ(2, Find("350"));
  ASSERT_EQ(3, Find("351"));
  ASSERT_EQ(3, Find("400"));
  ASSERT_EQ(3, Find("450"));
  ASSERT_EQ(4, Find("451"));

  ASSERT_TRUE(! Overlaps("100", "149"));
  ASSERT_TRUE(! Overlaps("251", "299"));
  ASSERT_TRUE(! Overlaps("451", "500"));
  ASSERT_TRUE(! Overlaps("351", "399"));

  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
}

TEST_F(FindLevelFileTest, LevelMultipleNullBoundaries) {
  LevelFileInit(4);

  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_TRUE(! Overlaps(nullptr, "149"));
  ASSERT_TRUE(! Overlaps("451", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "150"));
  ASSERT_TRUE(Overlaps(nullptr, "199"));
  ASSERT_TRUE(Overlaps(nullptr, "200"));
  ASSERT_TRUE(Overlaps(nullptr, "201"));
  ASSERT_TRUE(Overlaps(nullptr, "400"));
  ASSERT_TRUE(Overlaps(nullptr, "800"));
  ASSERT_TRUE(Overlaps("100", nullptr));
  ASSERT_TRUE(Overlaps("200", nullptr));
  ASSERT_TRUE(Overlaps("449", nullptr));
  ASSERT_TRUE(Overlaps("450", nullptr));
}

TEST_F(FindLevelFileTest, LevelOverlapSequenceChecks) {
  LevelFileInit(1);

  Add("200", "200", 5000, 3000);
  ASSERT_TRUE(! Overlaps("199", "199"));
  ASSERT_TRUE(! Overlaps("201", "300"));
  ASSERT_TRUE(Overlaps("200", "200"));
  ASSERT_TRUE(Overlaps("190", "200"));
  ASSERT_TRUE(Overlaps("200", "210"));
}

TEST_F(FindLevelFileTest, LevelOverlappingFiles) {
  LevelFileInit(2);

  Add("150", "600");
  Add("400", "500");
  disjoint_sorted_files_ = false;
  ASSERT_TRUE(! Overlaps("100", "149"));
  ASSERT_TRUE(! Overlaps("601", "700"));
  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
  ASSERT_TRUE(Overlaps("450", "700"));
  ASSERT_TRUE(Overlaps("600", "700"));
}

class VersionSetTestBase {
 public:
  const static std::string kColumnFamilyName1;
  const static std::string kColumnFamilyName2;
  const static std::string kColumnFamilyName3;
  int num_initial_edits_;

  explicit VersionSetTestBase(const std::string& name)
      : env_(nullptr),
        dbname_(test::PerThreadDBPath(name)),
        options_(),
        db_options_(options_),
        cf_options_(options_),
        immutable_options_(db_options_, cf_options_),
        mutable_cf_options_(cf_options_),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(db_options_.db_write_buffer_size),
        shutting_down_(false),
        mock_table_factory_(std::make_shared<mock::MockTableFactory>()) {
    EXPECT_OK(test::CreateEnvFromSystem(ConfigOptions(), &env_, &env_guard_));
    if (env_ == Env::Default() && getenv("MEM_ENV")) {
      env_guard_.reset(NewMemEnv(Env::Default()));
      env_ = env_guard_.get();
    }
    EXPECT_NE(nullptr, env_);

    fs_ = env_->GetFileSystem();
    EXPECT_OK(fs_->CreateDirIfMissing(dbname_, IOOptions(), nullptr));

    options_.env = env_;
    db_options_.env = env_;
    db_options_.fs = fs_;
    immutable_options_.env = env_;
    immutable_options_.fs = fs_;
    immutable_options_.clock = env_->GetSystemClock().get();

    versions_.reset(
        new VersionSet(dbname_, &db_options_, env_options_, table_cache_.get(),
                       &write_buffer_manager_, &write_controller_,
                       /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                       /*db_session_id*/ ""));
    reactive_versions_ = std::make_shared<ReactiveVersionSet>(
        dbname_, &db_options_, env_options_, table_cache_.get(),
        &write_buffer_manager_, &write_controller_, nullptr);
    db_options_.db_paths.emplace_back(dbname_,
                                      std::numeric_limits<uint64_t>::max());
  }

  virtual ~VersionSetTestBase() {
    if (getenv("KEEP_DB")) {
      fprintf(stdout, "DB is still at %s\n", dbname_.c_str());
    } else {
      Options options;
      options.env = env_;
      EXPECT_OK(DestroyDB(dbname_, options));
    }
  }

 protected:
  virtual void PrepareManifest(
      std::vector<ColumnFamilyDescriptor>* column_families,
      SequenceNumber* last_seqno, std::unique_ptr<log::Writer>* log_writer) {
    assert(column_families != nullptr);
    assert(last_seqno != nullptr);
    assert(log_writer != nullptr);
    VersionEdit new_db;
    if (db_options_.write_dbid_to_manifest) {
      DBOptions tmp_db_options;
      tmp_db_options.env = env_;
      std::unique_ptr<DBImpl> impl(new DBImpl(tmp_db_options, dbname_));
      std::string db_id;
      impl->GetDbIdentityFromIdentityFile(&db_id);
      new_db.SetDBId(db_id);
    }
    new_db.SetLogNumber(0);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(0);

    const std::vector<std::string> cf_names = {
        kDefaultColumnFamilyName, kColumnFamilyName1, kColumnFamilyName2,
        kColumnFamilyName3};
    const int kInitialNumOfCfs = static_cast<int>(cf_names.size());
    autovector<VersionEdit> new_cfs;
    uint64_t last_seq = 1;
    uint32_t cf_id = 1;
    for (int i = 1; i != kInitialNumOfCfs; ++i) {
      VersionEdit new_cf;
      new_cf.AddColumnFamily(cf_names[i]);
      new_cf.SetColumnFamily(cf_id++);
      new_cf.SetLogNumber(0);
      new_cf.SetNextFile(2);
      new_cf.SetLastSequence(last_seq++);
      new_cfs.emplace_back(new_cf);
    }
    *last_seqno = last_seq;
    num_initial_edits_ = static_cast<int>(new_cfs.size() + 1);
    std::unique_ptr<WritableFileWriter> file_writer;
    const std::string manifest = DescriptorFileName(dbname_, 1);
    const auto& fs = env_->GetFileSystem();
    Status s = WritableFileWriter::Create(
        fs, manifest, fs->OptimizeForManifestWrite(env_options_), &file_writer,
        nullptr);
    ASSERT_OK(s);
    {
      log_writer->reset(new log::Writer(std::move(file_writer), 0, false));
      std::string record;
      new_db.EncodeTo(&record);
      s = (*log_writer)->AddRecord(record);
      for (const auto& e : new_cfs) {
        record.clear();
        e.EncodeTo(&record);
        s = (*log_writer)->AddRecord(record);
        ASSERT_OK(s);
      }
    }
    ASSERT_OK(s);

    cf_options_.table_factory = mock_table_factory_;
    for (const auto& cf_name : cf_names) {
      column_families->emplace_back(cf_name, cf_options_);
    }
  }

  // Create DB with 3 column families.
  void NewDB() {
    SequenceNumber last_seqno;
    std::unique_ptr<log::Writer> log_writer;
    SetIdentityFile(env_, dbname_);
    PrepareManifest(&column_families_, &last_seqno, &log_writer);
    log_writer.reset();
    // Make "CURRENT" file point to the new manifest file.
    Status s = SetCurrentFile(fs_.get(), dbname_, 1, nullptr);
    ASSERT_OK(s);

    EXPECT_OK(versions_->Recover(column_families_, false));
    EXPECT_EQ(column_families_.size(),
              versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  }

  void ReopenDB() {
    versions_.reset(
        new VersionSet(dbname_, &db_options_, env_options_, table_cache_.get(),
                       &write_buffer_manager_, &write_controller_,
                       /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                       /*db_session_id*/ ""));
    EXPECT_OK(versions_->Recover(column_families_, false));
  }

  void VerifyManifest(std::string* manifest_path) const {
    assert(manifest_path != nullptr);
    uint64_t manifest_file_number = 0;
    Status s = versions_->GetCurrentManifestPath(
        dbname_, fs_.get(), manifest_path, &manifest_file_number);
    ASSERT_OK(s);
    ASSERT_EQ(1, manifest_file_number);
  }

  Status LogAndApplyToDefaultCF(VersionEdit& edit) {
    mutex_.Lock();
    Status s =
        versions_->LogAndApply(versions_->GetColumnFamilySet()->GetDefault(),
                               mutable_cf_options_, &edit, &mutex_);
    mutex_.Unlock();
    return s;
  }

  Status LogAndApplyToDefaultCF(
      const autovector<std::unique_ptr<VersionEdit>>& edits) {
    autovector<VersionEdit*> vedits;
    for (auto& e : edits) {
      vedits.push_back(e.get());
    }
    mutex_.Lock();
    Status s =
        versions_->LogAndApply(versions_->GetColumnFamilySet()->GetDefault(),
                               mutable_cf_options_, vedits, &mutex_);
    mutex_.Unlock();
    return s;
  }

  void CreateNewManifest() {
    constexpr FSDirectory* db_directory = nullptr;
    constexpr bool new_descriptor_log = true;
    mutex_.Lock();
    VersionEdit dummy;
    ASSERT_OK(versions_->LogAndApply(
        versions_->GetColumnFamilySet()->GetDefault(), mutable_cf_options_,
        &dummy, &mutex_, db_directory, new_descriptor_log));
    mutex_.Unlock();
  }

  ColumnFamilyData* CreateColumnFamily(const std::string& cf_name,
                                       const ColumnFamilyOptions& cf_options) {
    VersionEdit new_cf;
    new_cf.AddColumnFamily(cf_name);
    uint32_t new_id = versions_->GetColumnFamilySet()->GetNextColumnFamilyID();
    new_cf.SetColumnFamily(new_id);
    new_cf.SetLogNumber(0);
    new_cf.SetComparatorName(cf_options.comparator->Name());
    Status s;
    mutex_.Lock();
    s = versions_->LogAndApply(/*column_family_data=*/nullptr,
                               MutableCFOptions(cf_options), &new_cf, &mutex_,
                               /*db_directory=*/nullptr,
                               /*new_descriptor_log=*/false, &cf_options);
    mutex_.Unlock();
    EXPECT_OK(s);
    ColumnFamilyData* cfd =
        versions_->GetColumnFamilySet()->GetColumnFamily(cf_name);
    EXPECT_NE(nullptr, cfd);
    return cfd;
  }

  Env* mem_env_;
  Env* env_;
  std::shared_ptr<Env> env_guard_;
  std::shared_ptr<FileSystem> fs_;
  const std::string dbname_;
  EnvOptions env_options_;
  Options options_;
  ImmutableDBOptions db_options_;
  ColumnFamilyOptions cf_options_;
  ImmutableOptions immutable_options_;
  MutableCFOptions mutable_cf_options_;
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  WriteBufferManager write_buffer_manager_;
  std::shared_ptr<VersionSet> versions_;
  std::shared_ptr<ReactiveVersionSet> reactive_versions_;
  InstrumentedMutex mutex_;
  std::atomic<bool> shutting_down_;
  std::shared_ptr<mock::MockTableFactory> mock_table_factory_;
  std::vector<ColumnFamilyDescriptor> column_families_;
};

const std::string VersionSetTestBase::kColumnFamilyName1 = "alice";
const std::string VersionSetTestBase::kColumnFamilyName2 = "bob";
const std::string VersionSetTestBase::kColumnFamilyName3 = "charles";

class VersionSetTest : public VersionSetTestBase, public testing::Test {
 public:
  VersionSetTest() : VersionSetTestBase("version_set_test") {}
};

TEST_F(VersionSetTest, SameColumnFamilyGroupCommit) {
  NewDB();
  const int kGroupSize = 5;
  autovector<VersionEdit> edits;
  for (int i = 0; i != kGroupSize; ++i) {
    edits.emplace_back(VersionEdit());
  }
  autovector<ColumnFamilyData*> cfds;
  autovector<const MutableCFOptions*> all_mutable_cf_options;
  autovector<autovector<VersionEdit*>> edit_lists;
  for (int i = 0; i != kGroupSize; ++i) {
    cfds.emplace_back(versions_->GetColumnFamilySet()->GetDefault());
    all_mutable_cf_options.emplace_back(&mutable_cf_options_);
    autovector<VersionEdit*> edit_list;
    edit_list.emplace_back(&edits[i]);
    edit_lists.emplace_back(edit_list);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  int count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:SameColumnFamily", [&](void* arg) {
        uint32_t* cf_id = reinterpret_cast<uint32_t*>(arg);
        EXPECT_EQ(0u, *cf_id);
        ++count;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  mutex_.Lock();
  Status s =
      versions_->LogAndApply(cfds, all_mutable_cf_options, edit_lists, &mutex_);
  mutex_.Unlock();
  EXPECT_OK(s);
  EXPECT_EQ(kGroupSize - 1, count);
}

TEST_F(VersionSetTest, PersistBlobFileStateInNewManifest) {
  // Initialize the database and add a couple of blob files, one with some
  // garbage in it, and one without any garbage.
  NewDB();

  assert(versions_);
  assert(versions_->GetColumnFamilySet());

  ColumnFamilyData* const cfd = versions_->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  Version* const version = cfd->current();
  assert(version);

  VersionStorageInfo* const storage_info = version->storage_info();
  assert(storage_info);

  {
    constexpr uint64_t blob_file_number = 123;
    constexpr uint64_t total_blob_count = 456;
    constexpr uint64_t total_blob_bytes = 77777777;
    constexpr char checksum_method[] = "SHA1";
    constexpr char checksum_value[] =
        "\xbd\xb7\xf3\x4a\x59\xdf\xa1\x59\x2c\xe7\xf5\x2e\x99\xf9\x8c\x57\x0c"
        "\x52\x5c\xbd";

    auto shared_meta = SharedBlobFileMetaData::Create(
        blob_file_number, total_blob_count, total_blob_bytes, checksum_method,
        checksum_value);

    constexpr uint64_t garbage_blob_count = 89;
    constexpr uint64_t garbage_blob_bytes = 1000000;

    auto meta = BlobFileMetaData::Create(
        std::move(shared_meta), BlobFileMetaData::LinkedSsts(),
        garbage_blob_count, garbage_blob_bytes);

    storage_info->AddBlobFile(std::move(meta));
  }

  {
    constexpr uint64_t blob_file_number = 234;
    constexpr uint64_t total_blob_count = 555;
    constexpr uint64_t total_blob_bytes = 66666;
    constexpr char checksum_method[] = "CRC32";
    constexpr char checksum_value[] = "\x3d\x87\xff\x57";

    auto shared_meta = SharedBlobFileMetaData::Create(
        blob_file_number, total_blob_count, total_blob_bytes, checksum_method,
        checksum_value);

    constexpr uint64_t garbage_blob_count = 0;
    constexpr uint64_t garbage_blob_bytes = 0;

    auto meta = BlobFileMetaData::Create(
        std::move(shared_meta), BlobFileMetaData::LinkedSsts(),
        garbage_blob_count, garbage_blob_bytes);

    storage_info->AddBlobFile(std::move(meta));
  }

  // Force the creation of a new manifest file and make sure metadata for
  // the blob files is re-persisted.
  size_t addition_encoded = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileAddition::EncodeTo::CustomFields",
      [&](void* /* arg */) { ++addition_encoded; });

  size_t garbage_encoded = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileGarbage::EncodeTo::CustomFields",
      [&](void* /* arg */) { ++garbage_encoded; });
  SyncPoint::GetInstance()->EnableProcessing();

  CreateNewManifest();

  ASSERT_EQ(addition_encoded, 2);
  ASSERT_EQ(garbage_encoded, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(VersionSetTest, AddLiveBlobFiles) {
  // Initialize the database and add a blob file.
  NewDB();

  assert(versions_);
  assert(versions_->GetColumnFamilySet());

  ColumnFamilyData* const cfd = versions_->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  Version* const first_version = cfd->current();
  assert(first_version);

  VersionStorageInfo* const first_storage_info = first_version->storage_info();
  assert(first_storage_info);

  constexpr uint64_t first_blob_file_number = 234;
  constexpr uint64_t first_total_blob_count = 555;
  constexpr uint64_t first_total_blob_bytes = 66666;
  constexpr char first_checksum_method[] = "CRC32";
  constexpr char first_checksum_value[] = "\x3d\x87\xff\x57";

  auto first_shared_meta = SharedBlobFileMetaData::Create(
      first_blob_file_number, first_total_blob_count, first_total_blob_bytes,
      first_checksum_method, first_checksum_value);

  constexpr uint64_t garbage_blob_count = 0;
  constexpr uint64_t garbage_blob_bytes = 0;

  auto first_meta = BlobFileMetaData::Create(
      std::move(first_shared_meta), BlobFileMetaData::LinkedSsts(),
      garbage_blob_count, garbage_blob_bytes);

  first_storage_info->AddBlobFile(first_meta);

  // Reference the version so it stays alive even after the following version
  // edit.
  first_version->Ref();

  // Get live files directly from version.
  std::vector<uint64_t> version_table_files;
  std::vector<uint64_t> version_blob_files;

  first_version->AddLiveFiles(&version_table_files, &version_blob_files);

  ASSERT_EQ(version_blob_files.size(), 1);
  ASSERT_EQ(version_blob_files[0], first_blob_file_number);

  // Create a new version containing an additional blob file.
  versions_->TEST_CreateAndAppendVersion(cfd);

  Version* const second_version = cfd->current();
  assert(second_version);
  assert(second_version != first_version);

  VersionStorageInfo* const second_storage_info =
      second_version->storage_info();
  assert(second_storage_info);

  constexpr uint64_t second_blob_file_number = 456;
  constexpr uint64_t second_total_blob_count = 100;
  constexpr uint64_t second_total_blob_bytes = 2000000;
  constexpr char second_checksum_method[] = "CRC32B";
  constexpr char second_checksum_value[] = "\x6d\xbd\xf2\x3a";

  auto second_shared_meta = SharedBlobFileMetaData::Create(
      second_blob_file_number, second_total_blob_count, second_total_blob_bytes,
      second_checksum_method, second_checksum_value);

  auto second_meta = BlobFileMetaData::Create(
      std::move(second_shared_meta), BlobFileMetaData::LinkedSsts(),
      garbage_blob_count, garbage_blob_bytes);

  second_storage_info->AddBlobFile(std::move(first_meta));
  second_storage_info->AddBlobFile(std::move(second_meta));

  // Get all live files from version set. Note that the result contains
  // duplicates.
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;

  versions_->AddLiveFiles(&all_table_files, &all_blob_files);

  ASSERT_EQ(all_blob_files.size(), 3);
  ASSERT_EQ(all_blob_files[0], first_blob_file_number);
  ASSERT_EQ(all_blob_files[1], first_blob_file_number);
  ASSERT_EQ(all_blob_files[2], second_blob_file_number);

  // Clean up previous version.
  first_version->Unref();
}

TEST_F(VersionSetTest, ObsoleteBlobFile) {
  // Initialize the database and add a blob file that is entirely garbage
  // and thus can immediately be marked obsolete.
  NewDB();

  VersionEdit edit;

  constexpr uint64_t blob_file_number = 234;
  constexpr uint64_t total_blob_count = 555;
  constexpr uint64_t total_blob_bytes = 66666;
  constexpr char checksum_method[] = "CRC32";
  constexpr char checksum_value[] = "\x3d\x87\xff\x57";

  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  edit.AddBlobFileGarbage(blob_file_number, total_blob_count, total_blob_bytes);

  mutex_.Lock();
  Status s =
      versions_->LogAndApply(versions_->GetColumnFamilySet()->GetDefault(),
                             mutable_cf_options_, &edit, &mutex_);
  mutex_.Unlock();

  ASSERT_OK(s);

  // Make sure blob files from the pending number range are not returned
  // as obsolete.
  {
    std::vector<ObsoleteFileInfo> table_files;
    std::vector<ObsoleteBlobFileInfo> blob_files;
    std::vector<std::string> manifest_files;
    constexpr uint64_t min_pending_output = blob_file_number;

    versions_->GetObsoleteFiles(&table_files, &blob_files, &manifest_files,
                                min_pending_output);

    ASSERT_TRUE(blob_files.empty());
  }

  // Make sure the blob file is returned as obsolete if it's not in the pending
  // range.
  {
    std::vector<ObsoleteFileInfo> table_files;
    std::vector<ObsoleteBlobFileInfo> blob_files;
    std::vector<std::string> manifest_files;
    constexpr uint64_t min_pending_output = blob_file_number + 1;

    versions_->GetObsoleteFiles(&table_files, &blob_files, &manifest_files,
                                min_pending_output);

    ASSERT_EQ(blob_files.size(), 1);
    ASSERT_EQ(blob_files[0].GetBlobFileNumber(), blob_file_number);
  }

  // Make sure it's not returned a second time.
  {
    std::vector<ObsoleteFileInfo> table_files;
    std::vector<ObsoleteBlobFileInfo> blob_files;
    std::vector<std::string> manifest_files;
    constexpr uint64_t min_pending_output = blob_file_number + 1;

    versions_->GetObsoleteFiles(&table_files, &blob_files, &manifest_files,
                                min_pending_output);

    ASSERT_TRUE(blob_files.empty());
  }
}

TEST_F(VersionSetTest, WalEditsNotAppliedToVersion) {
  NewDB();

  constexpr uint64_t kNumWals = 5;

  autovector<std::unique_ptr<VersionEdit>> edits;
  // Add some WALs.
  for (uint64_t i = 1; i <= kNumWals; i++) {
    edits.emplace_back(new VersionEdit);
    // WAL's size equals its log number.
    edits.back()->AddWal(i, WalMetadata(i));
  }
  // Delete the first half of the WALs.
  edits.emplace_back(new VersionEdit);
  edits.back()->DeleteWalsBefore(kNumWals / 2 + 1);

  autovector<Version*> versions;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:NewVersion",
      [&](void* arg) { versions.push_back(reinterpret_cast<Version*>(arg)); });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(LogAndApplyToDefaultCF(edits));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Since the edits are all WAL edits, no version should be created.
  ASSERT_EQ(versions.size(), 1);
  ASSERT_EQ(versions[0], nullptr);
}

// Similar to WalEditsNotAppliedToVersion, but contains a non-WAL edit.
TEST_F(VersionSetTest, NonWalEditsAppliedToVersion) {
  NewDB();

  const std::string kDBId = "db_db";
  constexpr uint64_t kNumWals = 5;

  autovector<std::unique_ptr<VersionEdit>> edits;
  // Add some WALs.
  for (uint64_t i = 1; i <= kNumWals; i++) {
    edits.emplace_back(new VersionEdit);
    // WAL's size equals its log number.
    edits.back()->AddWal(i, WalMetadata(i));
  }
  // Delete the first half of the WALs.
  edits.emplace_back(new VersionEdit);
  edits.back()->DeleteWalsBefore(kNumWals / 2 + 1);
  edits.emplace_back(new VersionEdit);
  edits.back()->SetDBId(kDBId);

  autovector<Version*> versions;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:NewVersion",
      [&](void* arg) { versions.push_back(reinterpret_cast<Version*>(arg)); });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(LogAndApplyToDefaultCF(edits));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Since the edits are all WAL edits, no version should be created.
  ASSERT_EQ(versions.size(), 1);
  ASSERT_NE(versions[0], nullptr);
}

TEST_F(VersionSetTest, WalAddition) {
  NewDB();

  constexpr WalNumber kLogNumber = 10;
  constexpr uint64_t kSizeInBytes = 111;

  // A WAL is just created.
  {
    VersionEdit edit;
    edit.AddWal(kLogNumber);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_FALSE(wals.at(kLogNumber).HasSyncedSize());
  }

  // The WAL is synced for several times before closing.
  {
    for (uint64_t size_delta = 100; size_delta > 0; size_delta /= 2) {
      uint64_t size = kSizeInBytes - size_delta;
      WalMetadata wal(size);
      VersionEdit edit;
      edit.AddWal(kLogNumber, wal);

      ASSERT_OK(LogAndApplyToDefaultCF(edit));

      const auto& wals = versions_->GetWalSet().GetWals();
      ASSERT_EQ(wals.size(), 1);
      ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
      ASSERT_TRUE(wals.at(kLogNumber).HasSyncedSize());
      ASSERT_EQ(wals.at(kLogNumber).GetSyncedSizeInBytes(), size);
    }
  }

  // The WAL is closed.
  {
    WalMetadata wal(kSizeInBytes);
    VersionEdit edit;
    edit.AddWal(kLogNumber, wal);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_TRUE(wals.at(kLogNumber).HasSyncedSize());
    ASSERT_EQ(wals.at(kLogNumber).GetSyncedSizeInBytes(), kSizeInBytes);
  }

  // Recover a new VersionSet.
  {
    std::unique_ptr<VersionSet> new_versions(
        new VersionSet(dbname_, &db_options_, env_options_, table_cache_.get(),
                       &write_buffer_manager_, &write_controller_,
                       /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                       /*db_session_id*/ ""));
    ASSERT_OK(new_versions->Recover(column_families_, /*read_only=*/false));
    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_TRUE(wals.at(kLogNumber).HasSyncedSize());
    ASSERT_EQ(wals.at(kLogNumber).GetSyncedSizeInBytes(), kSizeInBytes);
  }
}

TEST_F(VersionSetTest, WalCloseWithoutSync) {
  NewDB();

  constexpr WalNumber kLogNumber = 10;
  constexpr uint64_t kSizeInBytes = 111;
  constexpr uint64_t kSyncedSizeInBytes = kSizeInBytes / 2;

  // A WAL is just created.
  {
    VersionEdit edit;
    edit.AddWal(kLogNumber);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_FALSE(wals.at(kLogNumber).HasSyncedSize());
  }

  // The WAL is synced before closing.
  {
    WalMetadata wal(kSyncedSizeInBytes);
    VersionEdit edit;
    edit.AddWal(kLogNumber, wal);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_TRUE(wals.at(kLogNumber).HasSyncedSize());
    ASSERT_EQ(wals.at(kLogNumber).GetSyncedSizeInBytes(), kSyncedSizeInBytes);
  }

  // A new WAL with larger log number is created,
  // implicitly marking the current WAL closed.
  {
    VersionEdit edit;
    edit.AddWal(kLogNumber + 1);
    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 2);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_TRUE(wals.at(kLogNumber).HasSyncedSize());
    ASSERT_EQ(wals.at(kLogNumber).GetSyncedSizeInBytes(), kSyncedSizeInBytes);
    ASSERT_TRUE(wals.find(kLogNumber + 1) != wals.end());
    ASSERT_FALSE(wals.at(kLogNumber + 1).HasSyncedSize());
  }

  // Recover a new VersionSet.
  {
    std::unique_ptr<VersionSet> new_versions(
        new VersionSet(dbname_, &db_options_, env_options_, table_cache_.get(),
                       &write_buffer_manager_, &write_controller_,
                       /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                       /*db_session_id*/ ""));
    ASSERT_OK(new_versions->Recover(column_families_, false));
    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 2);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_TRUE(wals.at(kLogNumber).HasSyncedSize());
    ASSERT_EQ(wals.at(kLogNumber).GetSyncedSizeInBytes(), kSyncedSizeInBytes);
  }
}

TEST_F(VersionSetTest, WalDeletion) {
  NewDB();

  constexpr WalNumber kClosedLogNumber = 10;
  constexpr WalNumber kNonClosedLogNumber = 20;
  constexpr uint64_t kSizeInBytes = 111;

  // Add a non-closed and a closed WAL.
  {
    VersionEdit edit;
    edit.AddWal(kClosedLogNumber, WalMetadata(kSizeInBytes));
    edit.AddWal(kNonClosedLogNumber);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 2);
    ASSERT_TRUE(wals.find(kNonClosedLogNumber) != wals.end());
    ASSERT_TRUE(wals.find(kClosedLogNumber) != wals.end());
    ASSERT_FALSE(wals.at(kNonClosedLogNumber).HasSyncedSize());
    ASSERT_TRUE(wals.at(kClosedLogNumber).HasSyncedSize());
    ASSERT_EQ(wals.at(kClosedLogNumber).GetSyncedSizeInBytes(), kSizeInBytes);
  }

  // Delete the closed WAL.
  {
    VersionEdit edit;
    edit.DeleteWalsBefore(kNonClosedLogNumber);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kNonClosedLogNumber) != wals.end());
    ASSERT_FALSE(wals.at(kNonClosedLogNumber).HasSyncedSize());
  }

  // Recover a new VersionSet, only the non-closed WAL should show up.
  {
    std::unique_ptr<VersionSet> new_versions(
        new VersionSet(dbname_, &db_options_, env_options_, table_cache_.get(),
                       &write_buffer_manager_, &write_controller_,
                       /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                       /*db_session_id*/ ""));
    ASSERT_OK(new_versions->Recover(column_families_, false));
    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kNonClosedLogNumber) != wals.end());
    ASSERT_FALSE(wals.at(kNonClosedLogNumber).HasSyncedSize());
  }

  // Force the creation of a new MANIFEST file,
  // only the non-closed WAL should be written to the new MANIFEST.
  {
    std::vector<WalAddition> wal_additions;
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::WriteCurrentStateToManifest:SaveWal", [&](void* arg) {
          VersionEdit* edit = reinterpret_cast<VersionEdit*>(arg);
          ASSERT_TRUE(edit->IsWalAddition());
          for (auto& addition : edit->GetWalAdditions()) {
            wal_additions.push_back(addition);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();

    CreateNewManifest();

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ASSERT_EQ(wal_additions.size(), 1);
    ASSERT_EQ(wal_additions[0].GetLogNumber(), kNonClosedLogNumber);
    ASSERT_FALSE(wal_additions[0].GetMetadata().HasSyncedSize());
  }

  // Recover from the new MANIFEST, only the non-closed WAL should show up.
  {
    std::unique_ptr<VersionSet> new_versions(
        new VersionSet(dbname_, &db_options_, env_options_, table_cache_.get(),
                       &write_buffer_manager_, &write_controller_,
                       /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                       /*db_session_id*/ ""));
    ASSERT_OK(new_versions->Recover(column_families_, false));
    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kNonClosedLogNumber) != wals.end());
    ASSERT_FALSE(wals.at(kNonClosedLogNumber).HasSyncedSize());
  }
}

TEST_F(VersionSetTest, WalCreateTwice) {
  NewDB();

  constexpr WalNumber kLogNumber = 10;

  VersionEdit edit;
  edit.AddWal(kLogNumber);

  ASSERT_OK(LogAndApplyToDefaultCF(edit));

  Status s = LogAndApplyToDefaultCF(edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(s.ToString().find("WAL 10 is created more than once") !=
              std::string::npos)
      << s.ToString();
}

TEST_F(VersionSetTest, WalCreateAfterClose) {
  NewDB();

  constexpr WalNumber kLogNumber = 10;
  constexpr uint64_t kSizeInBytes = 111;

  {
    // Add a closed WAL.
    VersionEdit edit;
    edit.AddWal(kLogNumber);
    WalMetadata wal(kSizeInBytes);
    edit.AddWal(kLogNumber, wal);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));
  }

  {
    // Create the same WAL again.
    VersionEdit edit;
    edit.AddWal(kLogNumber);

    Status s = LogAndApplyToDefaultCF(edit);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("WAL 10 is created more than once") !=
                std::string::npos)
        << s.ToString();
  }
}

TEST_F(VersionSetTest, AddWalWithSmallerSize) {
  NewDB();

  constexpr WalNumber kLogNumber = 10;
  constexpr uint64_t kSizeInBytes = 111;

  {
    // Add a closed WAL.
    VersionEdit edit;
    WalMetadata wal(kSizeInBytes);
    edit.AddWal(kLogNumber, wal);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));
  }

  {
    // Add the same WAL with smaller synced size.
    VersionEdit edit;
    WalMetadata wal(kSizeInBytes / 2);
    edit.AddWal(kLogNumber, wal);

    Status s = LogAndApplyToDefaultCF(edit);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(
        s.ToString().find(
            "WAL 10 must not have smaller synced size than previous one") !=
        std::string::npos)
        << s.ToString();
  }
}

TEST_F(VersionSetTest, DeleteWalsBeforeNonExistingWalNumber) {
  NewDB();

  constexpr WalNumber kLogNumber0 = 10;
  constexpr WalNumber kLogNumber1 = 20;
  constexpr WalNumber kNonExistingNumber = 15;
  constexpr uint64_t kSizeInBytes = 111;

  {
    // Add closed WALs.
    VersionEdit edit;
    WalMetadata wal(kSizeInBytes);
    edit.AddWal(kLogNumber0, wal);
    edit.AddWal(kLogNumber1, wal);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));
  }

  {
    // Delete WALs before a non-existing WAL.
    VersionEdit edit;
    edit.DeleteWalsBefore(kNonExistingNumber);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));
  }

  // Recover a new VersionSet, WAL0 is deleted, WAL1 is not.
  {
    std::unique_ptr<VersionSet> new_versions(
        new VersionSet(dbname_, &db_options_, env_options_, table_cache_.get(),
                       &write_buffer_manager_, &write_controller_,
                       /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                       /*db_session_id*/ ""));
    ASSERT_OK(new_versions->Recover(column_families_, false));
    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kLogNumber1) != wals.end());
  }
}

TEST_F(VersionSetTest, DeleteAllWals) {
  NewDB();

  constexpr WalNumber kMaxLogNumber = 10;
  constexpr uint64_t kSizeInBytes = 111;

  {
    // Add a closed WAL.
    VersionEdit edit;
    WalMetadata wal(kSizeInBytes);
    edit.AddWal(kMaxLogNumber, wal);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));
  }

  {
    VersionEdit edit;
    edit.DeleteWalsBefore(kMaxLogNumber + 10);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));
  }

  // Recover a new VersionSet, all WALs are deleted.
  {
    std::unique_ptr<VersionSet> new_versions(
        new VersionSet(dbname_, &db_options_, env_options_, table_cache_.get(),
                       &write_buffer_manager_, &write_controller_,
                       /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                       /*db_session_id*/ ""));
    ASSERT_OK(new_versions->Recover(column_families_, false));
    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 0);
  }
}

TEST_F(VersionSetTest, AtomicGroupWithWalEdits) {
  NewDB();

  constexpr int kAtomicGroupSize = 7;
  constexpr uint64_t kNumWals = 5;
  const std::string kDBId = "db_db";

  int remaining = kAtomicGroupSize;
  autovector<std::unique_ptr<VersionEdit>> edits;
  // Add 5 WALs.
  for (uint64_t i = 1; i <= kNumWals; i++) {
    edits.emplace_back(new VersionEdit);
    // WAL's size equals its log number.
    edits.back()->AddWal(i, WalMetadata(i));
    edits.back()->MarkAtomicGroup(--remaining);
  }
  // One edit with the min log number set.
  edits.emplace_back(new VersionEdit);
  edits.back()->SetDBId(kDBId);
  edits.back()->MarkAtomicGroup(--remaining);
  // Delete the first added 4 WALs.
  edits.emplace_back(new VersionEdit);
  edits.back()->DeleteWalsBefore(kNumWals);
  edits.back()->MarkAtomicGroup(--remaining);
  ASSERT_EQ(remaining, 0);

  ASSERT_OK(LogAndApplyToDefaultCF(edits));

  // Recover a new VersionSet, the min log number and the last WAL should be
  // kept.
  {
    std::unique_ptr<VersionSet> new_versions(
        new VersionSet(dbname_, &db_options_, env_options_, table_cache_.get(),
                       &write_buffer_manager_, &write_controller_,
                       /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                       /*db_session_id*/ ""));
    std::string db_id;
    ASSERT_OK(
        new_versions->Recover(column_families_, /*read_only=*/false, &db_id));

    ASSERT_EQ(db_id, kDBId);

    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kNumWals) != wals.end());
    ASSERT_TRUE(wals.at(kNumWals).HasSyncedSize());
    ASSERT_EQ(wals.at(kNumWals).GetSyncedSizeInBytes(), kNumWals);
  }
}

class VersionSetWithTimestampTest : public VersionSetTest {
 public:
  static const std::string kNewCfName;

  explicit VersionSetWithTimestampTest() : VersionSetTest() {}

  void SetUp() override {
    NewDB();
    Options options;
    options.comparator = test::ComparatorWithU64Ts();
    cfd_ = CreateColumnFamily(kNewCfName, options);
    EXPECT_NE(nullptr, cfd_);
    EXPECT_NE(nullptr, cfd_->GetLatestMutableCFOptions());
    column_families_.emplace_back(kNewCfName, options);
  }

  void TearDown() override {
    for (auto* e : edits_) {
      delete e;
    }
    edits_.clear();
  }

  void GenVersionEditsToSetFullHistoryTsLow(
      const std::vector<uint64_t>& ts_lbs) {
    for (const auto ts_lb : ts_lbs) {
      VersionEdit* edit = new VersionEdit;
      edit->SetColumnFamily(cfd_->GetID());
      std::string ts_str = test::EncodeInt(ts_lb);
      edit->SetFullHistoryTsLow(ts_str);
      edits_.emplace_back(edit);
    }
  }

  void VerifyFullHistoryTsLow(uint64_t expected_ts_low) {
    std::unique_ptr<VersionSet> vset(
        new VersionSet(dbname_, &db_options_, env_options_, table_cache_.get(),
                       &write_buffer_manager_, &write_controller_,
                       /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                       /*db_session_id*/ ""));
    ASSERT_OK(vset->Recover(column_families_, /*read_only=*/false,
                            /*db_id=*/nullptr));
    for (auto* cfd : *(vset->GetColumnFamilySet())) {
      ASSERT_NE(nullptr, cfd);
      if (cfd->GetName() == kNewCfName) {
        ASSERT_EQ(test::EncodeInt(expected_ts_low), cfd->GetFullHistoryTsLow());
      } else {
        ASSERT_TRUE(cfd->GetFullHistoryTsLow().empty());
      }
    }
  }

  void DoTest(const std::vector<uint64_t>& ts_lbs) {
    if (ts_lbs.empty()) {
      return;
    }

    GenVersionEditsToSetFullHistoryTsLow(ts_lbs);

    Status s;
    mutex_.Lock();
    s = versions_->LogAndApply(cfd_, *(cfd_->GetLatestMutableCFOptions()),
                               edits_, &mutex_);
    mutex_.Unlock();
    ASSERT_OK(s);
    VerifyFullHistoryTsLow(*std::max_element(ts_lbs.begin(), ts_lbs.end()));
  }

 protected:
  ColumnFamilyData* cfd_{nullptr};
  // edits_ must contain and own pointers to heap-alloc VersionEdit objects.
  autovector<VersionEdit*> edits_;
};

const std::string VersionSetWithTimestampTest::kNewCfName("new_cf");

TEST_F(VersionSetWithTimestampTest, SetFullHistoryTsLbOnce) {
  constexpr uint64_t kTsLow = 100;
  DoTest({kTsLow});
}

// Simulate the application increasing full_history_ts_low.
TEST_F(VersionSetWithTimestampTest, IncreaseFullHistoryTsLb) {
  const std::vector<uint64_t> ts_lbs = {100, 101, 102, 103};
  DoTest(ts_lbs);
}

// Simulate the application trying to decrease full_history_ts_low
// unsuccessfully. If the application calls public API sequentially to
// decrease the lower bound ts, RocksDB will return an InvalidArgument
// status before involving VersionSet. Only when multiple threads trying
// to decrease the lower bound concurrently will this case ever happen. Even
// so, the lower bound cannot be decreased. The application will be notified
// via return value of the API.
TEST_F(VersionSetWithTimestampTest, TryDecreaseFullHistoryTsLb) {
  const std::vector<uint64_t> ts_lbs = {103, 102, 101, 100};
  DoTest(ts_lbs);
}

class VersionSetAtomicGroupTest : public VersionSetTestBase,
                                  public testing::Test {
 public:
  VersionSetAtomicGroupTest()
      : VersionSetTestBase("version_set_atomic_group_test") {}

  void SetUp() override {
    PrepareManifest(&column_families_, &last_seqno_, &log_writer_);
    SetupTestSyncPoints();
  }

  void SetupValidAtomicGroup(int atomic_group_size) {
    edits_.resize(atomic_group_size);
    int remaining = atomic_group_size;
    for (size_t i = 0; i != edits_.size(); ++i) {
      edits_[i].SetLogNumber(0);
      edits_[i].SetNextFile(2);
      edits_[i].MarkAtomicGroup(--remaining);
      edits_[i].SetLastSequence(last_seqno_++);
    }
    ASSERT_OK(SetCurrentFile(fs_.get(), dbname_, 1, nullptr));
  }

  void SetupIncompleteTrailingAtomicGroup(int atomic_group_size) {
    edits_.resize(atomic_group_size);
    int remaining = atomic_group_size;
    for (size_t i = 0; i != edits_.size(); ++i) {
      edits_[i].SetLogNumber(0);
      edits_[i].SetNextFile(2);
      edits_[i].MarkAtomicGroup(--remaining);
      edits_[i].SetLastSequence(last_seqno_++);
    }
    ASSERT_OK(SetCurrentFile(fs_.get(), dbname_, 1, nullptr));
  }

  void SetupCorruptedAtomicGroup(int atomic_group_size) {
    edits_.resize(atomic_group_size);
    int remaining = atomic_group_size;
    for (size_t i = 0; i != edits_.size(); ++i) {
      edits_[i].SetLogNumber(0);
      edits_[i].SetNextFile(2);
      if (i != ((size_t)atomic_group_size / 2)) {
        edits_[i].MarkAtomicGroup(--remaining);
      }
      edits_[i].SetLastSequence(last_seqno_++);
    }
    ASSERT_OK(SetCurrentFile(fs_.get(), dbname_, 1, nullptr));
  }

  void SetupIncorrectAtomicGroup(int atomic_group_size) {
    edits_.resize(atomic_group_size);
    int remaining = atomic_group_size;
    for (size_t i = 0; i != edits_.size(); ++i) {
      edits_[i].SetLogNumber(0);
      edits_[i].SetNextFile(2);
      if (i != 1) {
        edits_[i].MarkAtomicGroup(--remaining);
      } else {
        edits_[i].MarkAtomicGroup(remaining--);
      }
      edits_[i].SetLastSequence(last_seqno_++);
    }
    ASSERT_OK(SetCurrentFile(fs_.get(), dbname_, 1, nullptr));
  }

  void SetupTestSyncPoints() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->SetCallBack(
        "AtomicGroupReadBuffer::AddEdit:FirstInAtomicGroup", [&](void* arg) {
          VersionEdit* e = reinterpret_cast<VersionEdit*>(arg);
          EXPECT_EQ(edits_.front().DebugString(),
                    e->DebugString());  // compare based on value
          first_in_atomic_group_ = true;
        });
    SyncPoint::GetInstance()->SetCallBack(
        "AtomicGroupReadBuffer::AddEdit:LastInAtomicGroup", [&](void* arg) {
          VersionEdit* e = reinterpret_cast<VersionEdit*>(arg);
          EXPECT_EQ(edits_.back().DebugString(),
                    e->DebugString());  // compare based on value
          EXPECT_TRUE(first_in_atomic_group_);
          last_in_atomic_group_ = true;
        });
    SyncPoint::GetInstance()->SetCallBack(
        "VersionEditHandlerBase::Iterate:Finish", [&](void* arg) {
          num_recovered_edits_ = *reinterpret_cast<int*>(arg);
        });
    SyncPoint::GetInstance()->SetCallBack(
        "AtomicGroupReadBuffer::AddEdit:AtomicGroup",
        [&](void* /* arg */) { ++num_edits_in_atomic_group_; });
    SyncPoint::GetInstance()->SetCallBack(
        "AtomicGroupReadBuffer::AddEdit:AtomicGroupMixedWithNormalEdits",
        [&](void* arg) {
          corrupted_edit_ = *reinterpret_cast<VersionEdit*>(arg);
        });
    SyncPoint::GetInstance()->SetCallBack(
        "AtomicGroupReadBuffer::AddEdit:IncorrectAtomicGroupSize",
        [&](void* arg) {
          edit_with_incorrect_group_size_ =
              *reinterpret_cast<VersionEdit*>(arg);
        });
    SyncPoint::GetInstance()->EnableProcessing();
  }

  void AddNewEditsToLog(int num_edits) {
    for (int i = 0; i < num_edits; i++) {
      std::string record;
      edits_[i].EncodeTo(&record);
      ASSERT_OK(log_writer_->AddRecord(record));
    }
  }

  void TearDown() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    log_writer_.reset();
  }

 protected:
  std::vector<ColumnFamilyDescriptor> column_families_;
  SequenceNumber last_seqno_;
  std::vector<VersionEdit> edits_;
  bool first_in_atomic_group_ = false;
  bool last_in_atomic_group_ = false;
  int num_edits_in_atomic_group_ = 0;
  int num_recovered_edits_ = 0;
  VersionEdit corrupted_edit_;
  VersionEdit edit_with_incorrect_group_size_;
  std::unique_ptr<log::Writer> log_writer_;
};

TEST_F(VersionSetAtomicGroupTest, HandleValidAtomicGroupWithVersionSetRecover) {
  const int kAtomicGroupSize = 3;
  SetupValidAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kAtomicGroupSize);
  EXPECT_OK(versions_->Recover(column_families_, false));
  EXPECT_EQ(column_families_.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_TRUE(first_in_atomic_group_);
  EXPECT_TRUE(last_in_atomic_group_);
  EXPECT_EQ(num_initial_edits_ + kAtomicGroupSize, num_recovered_edits_);
}

TEST_F(VersionSetAtomicGroupTest,
       HandleValidAtomicGroupWithReactiveVersionSetRecover) {
  const int kAtomicGroupSize = 3;
  SetupValidAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kAtomicGroupSize);
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));
  EXPECT_EQ(column_families_.size(),
            reactive_versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_TRUE(first_in_atomic_group_);
  EXPECT_TRUE(last_in_atomic_group_);
  // The recover should clean up the replay buffer.
  EXPECT_TRUE(reactive_versions_->TEST_read_edits_in_atomic_group() == 0);
  EXPECT_TRUE(reactive_versions_->replay_buffer().size() == 0);
  EXPECT_EQ(num_initial_edits_ + kAtomicGroupSize, num_recovered_edits_);
}

TEST_F(VersionSetAtomicGroupTest,
       HandleValidAtomicGroupWithReactiveVersionSetReadAndApply) {
  const int kAtomicGroupSize = 3;
  SetupValidAtomicGroup(kAtomicGroupSize);
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));
  EXPECT_EQ(num_initial_edits_, num_recovered_edits_);
  AddNewEditsToLog(kAtomicGroupSize);
  InstrumentedMutex mu;
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  mu.Lock();
  EXPECT_OK(reactive_versions_->ReadAndApply(
      &mu, &manifest_reader, manifest_reader_status.get(), &cfds_changed));
  mu.Unlock();
  EXPECT_TRUE(first_in_atomic_group_);
  EXPECT_TRUE(last_in_atomic_group_);
  // The recover should clean up the replay buffer.
  EXPECT_TRUE(reactive_versions_->TEST_read_edits_in_atomic_group() == 0);
  EXPECT_TRUE(reactive_versions_->replay_buffer().size() == 0);
  EXPECT_EQ(kAtomicGroupSize, num_recovered_edits_);
}

TEST_F(VersionSetAtomicGroupTest,
       HandleIncompleteTrailingAtomicGroupWithVersionSetRecover) {
  const int kAtomicGroupSize = 4;
  const int kNumberOfPersistedVersionEdits = kAtomicGroupSize - 1;
  SetupIncompleteTrailingAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kNumberOfPersistedVersionEdits);
  EXPECT_OK(versions_->Recover(column_families_, false));
  EXPECT_EQ(column_families_.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_TRUE(first_in_atomic_group_);
  EXPECT_FALSE(last_in_atomic_group_);
  EXPECT_EQ(kNumberOfPersistedVersionEdits, num_edits_in_atomic_group_);
  EXPECT_EQ(num_initial_edits_, num_recovered_edits_);
}

TEST_F(VersionSetAtomicGroupTest,
       HandleIncompleteTrailingAtomicGroupWithReactiveVersionSetRecover) {
  const int kAtomicGroupSize = 4;
  const int kNumberOfPersistedVersionEdits = kAtomicGroupSize - 1;
  SetupIncompleteTrailingAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kNumberOfPersistedVersionEdits);
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));
  EXPECT_EQ(column_families_.size(),
            reactive_versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_TRUE(first_in_atomic_group_);
  EXPECT_FALSE(last_in_atomic_group_);
  EXPECT_EQ(kNumberOfPersistedVersionEdits, num_edits_in_atomic_group_);
  // Reactive version set should store the edits in the replay buffer.
  EXPECT_TRUE(reactive_versions_->TEST_read_edits_in_atomic_group() ==
              kNumberOfPersistedVersionEdits);
  EXPECT_TRUE(reactive_versions_->replay_buffer().size() == kAtomicGroupSize);
  // Write the last record. The reactive version set should now apply all
  // edits.
  std::string last_record;
  edits_[kAtomicGroupSize - 1].EncodeTo(&last_record);
  EXPECT_OK(log_writer_->AddRecord(last_record));
  InstrumentedMutex mu;
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  mu.Lock();
  EXPECT_OK(reactive_versions_->ReadAndApply(
      &mu, &manifest_reader, manifest_reader_status.get(), &cfds_changed));
  mu.Unlock();
  // Reactive version set should be empty now.
  EXPECT_TRUE(reactive_versions_->TEST_read_edits_in_atomic_group() == 0);
  EXPECT_TRUE(reactive_versions_->replay_buffer().size() == 0);
  EXPECT_EQ(num_initial_edits_, num_recovered_edits_);
}

TEST_F(VersionSetAtomicGroupTest,
       HandleIncompleteTrailingAtomicGroupWithReactiveVersionSetReadAndApply) {
  const int kAtomicGroupSize = 4;
  const int kNumberOfPersistedVersionEdits = kAtomicGroupSize - 1;
  SetupIncompleteTrailingAtomicGroup(kAtomicGroupSize);
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  // No edits in an atomic group.
  EXPECT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));
  EXPECT_EQ(column_families_.size(),
            reactive_versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_EQ(num_initial_edits_, num_recovered_edits_);
  // Write a few edits in an atomic group.
  AddNewEditsToLog(kNumberOfPersistedVersionEdits);
  InstrumentedMutex mu;
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  mu.Lock();
  EXPECT_OK(reactive_versions_->ReadAndApply(
      &mu, &manifest_reader, manifest_reader_status.get(), &cfds_changed));
  mu.Unlock();
  EXPECT_TRUE(first_in_atomic_group_);
  EXPECT_FALSE(last_in_atomic_group_);
  EXPECT_EQ(kNumberOfPersistedVersionEdits, num_edits_in_atomic_group_);
  // Reactive version set should store the edits in the replay buffer.
  EXPECT_TRUE(reactive_versions_->TEST_read_edits_in_atomic_group() ==
              kNumberOfPersistedVersionEdits);
  EXPECT_TRUE(reactive_versions_->replay_buffer().size() == kAtomicGroupSize);
}

TEST_F(VersionSetAtomicGroupTest,
       HandleCorruptedAtomicGroupWithVersionSetRecover) {
  const int kAtomicGroupSize = 4;
  SetupCorruptedAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kAtomicGroupSize);
  EXPECT_NOK(versions_->Recover(column_families_, false));
  EXPECT_EQ(column_families_.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_EQ(edits_[kAtomicGroupSize / 2].DebugString(),
            corrupted_edit_.DebugString());
}

TEST_F(VersionSetAtomicGroupTest,
       HandleCorruptedAtomicGroupWithReactiveVersionSetRecover) {
  const int kAtomicGroupSize = 4;
  SetupCorruptedAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kAtomicGroupSize);
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_NOK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                         &manifest_reporter,
                                         &manifest_reader_status));
  EXPECT_EQ(column_families_.size(),
            reactive_versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_EQ(edits_[kAtomicGroupSize / 2].DebugString(),
            corrupted_edit_.DebugString());
}

TEST_F(VersionSetAtomicGroupTest,
       HandleCorruptedAtomicGroupWithReactiveVersionSetReadAndApply) {
  const int kAtomicGroupSize = 4;
  SetupCorruptedAtomicGroup(kAtomicGroupSize);
  InstrumentedMutex mu;
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));
  // Write the corrupted edits.
  AddNewEditsToLog(kAtomicGroupSize);
  mu.Lock();
  EXPECT_NOK(reactive_versions_->ReadAndApply(
      &mu, &manifest_reader, manifest_reader_status.get(), &cfds_changed));
  mu.Unlock();
  EXPECT_EQ(edits_[kAtomicGroupSize / 2].DebugString(),
            corrupted_edit_.DebugString());
}

TEST_F(VersionSetAtomicGroupTest,
       HandleIncorrectAtomicGroupSizeWithVersionSetRecover) {
  const int kAtomicGroupSize = 4;
  SetupIncorrectAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kAtomicGroupSize);
  EXPECT_NOK(versions_->Recover(column_families_, false));
  EXPECT_EQ(column_families_.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_EQ(edits_[1].DebugString(),
            edit_with_incorrect_group_size_.DebugString());
}

TEST_F(VersionSetAtomicGroupTest,
       HandleIncorrectAtomicGroupSizeWithReactiveVersionSetRecover) {
  const int kAtomicGroupSize = 4;
  SetupIncorrectAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kAtomicGroupSize);
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_NOK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                         &manifest_reporter,
                                         &manifest_reader_status));
  EXPECT_EQ(column_families_.size(),
            reactive_versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_EQ(edits_[1].DebugString(),
            edit_with_incorrect_group_size_.DebugString());
}

TEST_F(VersionSetAtomicGroupTest,
       HandleIncorrectAtomicGroupSizeWithReactiveVersionSetReadAndApply) {
  const int kAtomicGroupSize = 4;
  SetupIncorrectAtomicGroup(kAtomicGroupSize);
  InstrumentedMutex mu;
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));
  AddNewEditsToLog(kAtomicGroupSize);
  mu.Lock();
  EXPECT_NOK(reactive_versions_->ReadAndApply(
      &mu, &manifest_reader, manifest_reader_status.get(), &cfds_changed));
  mu.Unlock();
  EXPECT_EQ(edits_[1].DebugString(),
            edit_with_incorrect_group_size_.DebugString());
}

class VersionSetTestDropOneCF : public VersionSetTestBase,
                                public testing::TestWithParam<std::string> {
 public:
  VersionSetTestDropOneCF()
      : VersionSetTestBase("version_set_test_drop_one_cf") {}
};

// This test simulates the following execution sequence
// Time  thread1                  bg_flush_thr
//  |                             Prepare version edits (e1,e2,e3) for atomic
//  |                             flush cf1, cf2, cf3
//  |    Enqueue e to drop cfi
//  |    to manifest_writers_
//  |                             Enqueue (e1,e2,e3) to manifest_writers_
//  |
//  |    Apply e,
//  |    cfi.IsDropped() is true
//  |                             Apply (e1,e2,e3),
//  |                             since cfi.IsDropped() == true, we need to
//  |                             drop ei and write the rest to MANIFEST.
//  V
//
//  Repeat the test for i = 1, 2, 3 to simulate dropping the first, middle and
//  last column family in an atomic group.
TEST_P(VersionSetTestDropOneCF, HandleDroppedColumnFamilyInAtomicGroup) {
  std::vector<ColumnFamilyDescriptor> column_families;
  SequenceNumber last_seqno;
  std::unique_ptr<log::Writer> log_writer;
  PrepareManifest(&column_families, &last_seqno, &log_writer);
  Status s = SetCurrentFile(fs_.get(), dbname_, 1, nullptr);
  ASSERT_OK(s);

  EXPECT_OK(versions_->Recover(column_families, false /* read_only */));
  EXPECT_EQ(column_families.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());

  const int kAtomicGroupSize = 3;
  const std::vector<std::string> non_default_cf_names = {
      kColumnFamilyName1, kColumnFamilyName2, kColumnFamilyName3};

  // Drop one column family
  VersionEdit drop_cf_edit;
  drop_cf_edit.DropColumnFamily();
  const std::string cf_to_drop_name(GetParam());
  auto cfd_to_drop =
      versions_->GetColumnFamilySet()->GetColumnFamily(cf_to_drop_name);
  ASSERT_NE(nullptr, cfd_to_drop);
  // Increase its refcount because cfd_to_drop is used later, and we need to
  // prevent it from being deleted.
  cfd_to_drop->Ref();
  drop_cf_edit.SetColumnFamily(cfd_to_drop->GetID());
  mutex_.Lock();
  s = versions_->LogAndApply(cfd_to_drop,
                             *cfd_to_drop->GetLatestMutableCFOptions(),
                             &drop_cf_edit, &mutex_);
  mutex_.Unlock();
  ASSERT_OK(s);

  std::vector<VersionEdit> edits(kAtomicGroupSize);
  uint32_t remaining = kAtomicGroupSize;
  size_t i = 0;
  autovector<ColumnFamilyData*> cfds;
  autovector<const MutableCFOptions*> mutable_cf_options_list;
  autovector<autovector<VersionEdit*>> edit_lists;
  for (const auto& cf_name : non_default_cf_names) {
    auto cfd = (cf_name != cf_to_drop_name)
                   ? versions_->GetColumnFamilySet()->GetColumnFamily(cf_name)
                   : cfd_to_drop;
    ASSERT_NE(nullptr, cfd);
    cfds.push_back(cfd);
    mutable_cf_options_list.emplace_back(cfd->GetLatestMutableCFOptions());
    edits[i].SetColumnFamily(cfd->GetID());
    edits[i].SetLogNumber(0);
    edits[i].SetNextFile(2);
    edits[i].MarkAtomicGroup(--remaining);
    edits[i].SetLastSequence(last_seqno++);
    autovector<VersionEdit*> tmp_edits;
    tmp_edits.push_back(&edits[i]);
    edit_lists.emplace_back(tmp_edits);
    ++i;
  }
  int called = 0;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:CheckOneAtomicGroup", [&](void* arg) {
        std::vector<VersionEdit*>* tmp_edits =
            reinterpret_cast<std::vector<VersionEdit*>*>(arg);
        EXPECT_EQ(kAtomicGroupSize - 1, tmp_edits->size());
        for (const auto e : *tmp_edits) {
          bool found = false;
          for (const auto& e2 : edits) {
            if (&e2 == e) {
              found = true;
              break;
            }
          }
          ASSERT_TRUE(found);
        }
        ++called;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  mutex_.Lock();
  s = versions_->LogAndApply(cfds, mutable_cf_options_list, edit_lists,
                             &mutex_);
  mutex_.Unlock();
  ASSERT_OK(s);
  ASSERT_EQ(1, called);
  cfd_to_drop->UnrefAndTryDelete();
}

INSTANTIATE_TEST_CASE_P(
    AtomicGroup, VersionSetTestDropOneCF,
    testing::Values(VersionSetTestBase::kColumnFamilyName1,
                    VersionSetTestBase::kColumnFamilyName2,
                    VersionSetTestBase::kColumnFamilyName3));

class EmptyDefaultCfNewManifest : public VersionSetTestBase,
                                  public testing::Test {
 public:
  EmptyDefaultCfNewManifest() : VersionSetTestBase("version_set_new_db_test") {}
  // Emulate DBImpl::NewDB()
  void PrepareManifest(std::vector<ColumnFamilyDescriptor>* /*column_families*/,
                       SequenceNumber* /*last_seqno*/,
                       std::unique_ptr<log::Writer>* log_writer) override {
    assert(log_writer != nullptr);
    VersionEdit new_db;
    new_db.SetLogNumber(0);
    const std::string manifest_path = DescriptorFileName(dbname_, 1);
    const auto& fs = env_->GetFileSystem();
    std::unique_ptr<WritableFileWriter> file_writer;
    Status s = WritableFileWriter::Create(
        fs, manifest_path, fs->OptimizeForManifestWrite(env_options_),
        &file_writer, nullptr);
    ASSERT_OK(s);
    log_writer->reset(new log::Writer(std::move(file_writer), 0, true));
    std::string record;
    ASSERT_TRUE(new_db.EncodeTo(&record));
    s = (*log_writer)->AddRecord(record);
    ASSERT_OK(s);
    // Create new column family
    VersionEdit new_cf;
    new_cf.AddColumnFamily(VersionSetTestBase::kColumnFamilyName1);
    new_cf.SetColumnFamily(1);
    new_cf.SetLastSequence(2);
    new_cf.SetNextFile(2);
    record.clear();
    ASSERT_TRUE(new_cf.EncodeTo(&record));
    s = (*log_writer)->AddRecord(record);
    ASSERT_OK(s);
  }

 protected:
  bool write_dbid_to_manifest_ = false;
  std::unique_ptr<log::Writer> log_writer_;
};

// Create db, create column family. Cf creation will switch to a new MANIFEST.
// Then reopen db, trying to recover.
TEST_F(EmptyDefaultCfNewManifest, Recover) {
  PrepareManifest(nullptr, nullptr, &log_writer_);
  log_writer_.reset();
  Status s =
      SetCurrentFile(fs_.get(), dbname_, 1, /*directory_to_fsync=*/nullptr);
  ASSERT_OK(s);
  std::string manifest_path;
  VerifyManifest(&manifest_path);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options_);
  column_families.emplace_back(VersionSetTestBase::kColumnFamilyName1,
                               cf_options_);
  std::string db_id;
  bool has_missing_table_file = false;
  s = versions_->TryRecoverFromOneManifest(
      manifest_path, column_families, false, &db_id, &has_missing_table_file);
  ASSERT_OK(s);
  ASSERT_FALSE(has_missing_table_file);
}

class VersionSetTestEmptyDb
    : public VersionSetTestBase,
      public testing::TestWithParam<
          std::tuple<bool, bool, std::vector<std::string>>> {
 public:
  static const std::string kUnknownColumnFamilyName;
  VersionSetTestEmptyDb() : VersionSetTestBase("version_set_test_empty_db") {}

 protected:
  void PrepareManifest(std::vector<ColumnFamilyDescriptor>* /*column_families*/,
                       SequenceNumber* /*last_seqno*/,
                       std::unique_ptr<log::Writer>* log_writer) override {
    assert(nullptr != log_writer);
    VersionEdit new_db;
    if (db_options_.write_dbid_to_manifest) {
      DBOptions tmp_db_options;
      tmp_db_options.env = env_;
      std::unique_ptr<DBImpl> impl(new DBImpl(tmp_db_options, dbname_));
      std::string db_id;
      impl->GetDbIdentityFromIdentityFile(&db_id);
      new_db.SetDBId(db_id);
    }
    const std::string manifest_path = DescriptorFileName(dbname_, 1);
    const auto& fs = env_->GetFileSystem();
    std::unique_ptr<WritableFileWriter> file_writer;
    Status s = WritableFileWriter::Create(
        fs, manifest_path, fs->OptimizeForManifestWrite(env_options_),
        &file_writer, nullptr);
    ASSERT_OK(s);
    {
      log_writer->reset(new log::Writer(std::move(file_writer), 0, false));
      std::string record;
      new_db.EncodeTo(&record);
      s = (*log_writer)->AddRecord(record);
      ASSERT_OK(s);
    }
  }

  std::unique_ptr<log::Writer> log_writer_;
};

const std::string VersionSetTestEmptyDb::kUnknownColumnFamilyName = "unknown";

TEST_P(VersionSetTestEmptyDb, OpenFromIncompleteManifest0) {
  db_options_.write_dbid_to_manifest = std::get<0>(GetParam());
  PrepareManifest(nullptr, nullptr, &log_writer_);
  log_writer_.reset();
  Status s =
      SetCurrentFile(fs_.get(), dbname_, 1, /*directory_to_fsync=*/nullptr);
  ASSERT_OK(s);

  std::string manifest_path;
  VerifyManifest(&manifest_path);

  bool read_only = std::get<1>(GetParam());
  const std::vector<std::string> cf_names = std::get<2>(GetParam());

  std::vector<ColumnFamilyDescriptor> column_families;
  for (const auto& cf_name : cf_names) {
    column_families.emplace_back(cf_name, cf_options_);
  }

  std::string db_id;
  bool has_missing_table_file = false;
  s = versions_->TryRecoverFromOneManifest(manifest_path, column_families,
                                           read_only, &db_id,
                                           &has_missing_table_file);
  auto iter =
      std::find(cf_names.begin(), cf_names.end(), kDefaultColumnFamilyName);
  if (iter == cf_names.end()) {
    ASSERT_TRUE(s.IsInvalidArgument());
  } else {
    ASSERT_TRUE(s.IsCorruption());
  }
}

TEST_P(VersionSetTestEmptyDb, OpenFromIncompleteManifest1) {
  db_options_.write_dbid_to_manifest = std::get<0>(GetParam());
  PrepareManifest(nullptr, nullptr, &log_writer_);
  // Only a subset of column families in the MANIFEST.
  VersionEdit new_cf1;
  new_cf1.AddColumnFamily(VersionSetTestBase::kColumnFamilyName1);
  new_cf1.SetColumnFamily(1);
  Status s;
  {
    std::string record;
    new_cf1.EncodeTo(&record);
    s = log_writer_->AddRecord(record);
    ASSERT_OK(s);
  }
  log_writer_.reset();
  s = SetCurrentFile(fs_.get(), dbname_, 1, /*directory_to_fsync=*/nullptr);
  ASSERT_OK(s);

  std::string manifest_path;
  VerifyManifest(&manifest_path);

  bool read_only = std::get<1>(GetParam());
  const std::vector<std::string>& cf_names = std::get<2>(GetParam());
  std::vector<ColumnFamilyDescriptor> column_families;
  for (const auto& cf_name : cf_names) {
    column_families.emplace_back(cf_name, cf_options_);
  }
  std::string db_id;
  bool has_missing_table_file = false;
  s = versions_->TryRecoverFromOneManifest(manifest_path, column_families,
                                           read_only, &db_id,
                                           &has_missing_table_file);
  auto iter =
      std::find(cf_names.begin(), cf_names.end(), kDefaultColumnFamilyName);
  if (iter == cf_names.end()) {
    ASSERT_TRUE(s.IsInvalidArgument());
  } else {
    ASSERT_TRUE(s.IsCorruption());
  }
}

TEST_P(VersionSetTestEmptyDb, OpenFromInCompleteManifest2) {
  db_options_.write_dbid_to_manifest = std::get<0>(GetParam());
  PrepareManifest(nullptr, nullptr, &log_writer_);
  // Write all column families but no log_number, next_file_number and
  // last_sequence.
  const std::vector<std::string> all_cf_names = {
      kDefaultColumnFamilyName, kColumnFamilyName1, kColumnFamilyName2,
      kColumnFamilyName3};
  uint32_t cf_id = 1;
  Status s;
  for (size_t i = 1; i != all_cf_names.size(); ++i) {
    VersionEdit new_cf;
    new_cf.AddColumnFamily(all_cf_names[i]);
    new_cf.SetColumnFamily(cf_id++);
    std::string record;
    ASSERT_TRUE(new_cf.EncodeTo(&record));
    s = log_writer_->AddRecord(record);
    ASSERT_OK(s);
  }
  log_writer_.reset();
  s = SetCurrentFile(fs_.get(), dbname_, 1, /*directory_to_fsync=*/nullptr);
  ASSERT_OK(s);

  std::string manifest_path;
  VerifyManifest(&manifest_path);

  bool read_only = std::get<1>(GetParam());
  const std::vector<std::string>& cf_names = std::get<2>(GetParam());
  std::vector<ColumnFamilyDescriptor> column_families;
  for (const auto& cf_name : cf_names) {
    column_families.emplace_back(cf_name, cf_options_);
  }
  std::string db_id;
  bool has_missing_table_file = false;
  s = versions_->TryRecoverFromOneManifest(manifest_path, column_families,
                                           read_only, &db_id,
                                           &has_missing_table_file);
  auto iter =
      std::find(cf_names.begin(), cf_names.end(), kDefaultColumnFamilyName);
  if (iter == cf_names.end()) {
    ASSERT_TRUE(s.IsInvalidArgument());
  } else {
    ASSERT_TRUE(s.IsCorruption());
  }
}

TEST_P(VersionSetTestEmptyDb, OpenManifestWithUnknownCF) {
  db_options_.write_dbid_to_manifest = std::get<0>(GetParam());
  PrepareManifest(nullptr, nullptr, &log_writer_);
  // Write all column families but no log_number, next_file_number and
  // last_sequence.
  const std::vector<std::string> all_cf_names = {
      kDefaultColumnFamilyName, kColumnFamilyName1, kColumnFamilyName2,
      kColumnFamilyName3};
  uint32_t cf_id = 1;
  Status s;
  for (size_t i = 1; i != all_cf_names.size(); ++i) {
    VersionEdit new_cf;
    new_cf.AddColumnFamily(all_cf_names[i]);
    new_cf.SetColumnFamily(cf_id++);
    std::string record;
    ASSERT_TRUE(new_cf.EncodeTo(&record));
    s = log_writer_->AddRecord(record);
    ASSERT_OK(s);
  }
  {
    VersionEdit tmp_edit;
    tmp_edit.SetColumnFamily(4);
    tmp_edit.SetLogNumber(0);
    tmp_edit.SetNextFile(2);
    tmp_edit.SetLastSequence(0);
    std::string record;
    ASSERT_TRUE(tmp_edit.EncodeTo(&record));
    s = log_writer_->AddRecord(record);
    ASSERT_OK(s);
  }
  log_writer_.reset();
  s = SetCurrentFile(fs_.get(), dbname_, 1, /*directory_to_fsync=*/nullptr);
  ASSERT_OK(s);

  std::string manifest_path;
  VerifyManifest(&manifest_path);

  bool read_only = std::get<1>(GetParam());
  const std::vector<std::string>& cf_names = std::get<2>(GetParam());
  std::vector<ColumnFamilyDescriptor> column_families;
  for (const auto& cf_name : cf_names) {
    column_families.emplace_back(cf_name, cf_options_);
  }
  std::string db_id;
  bool has_missing_table_file = false;
  s = versions_->TryRecoverFromOneManifest(manifest_path, column_families,
                                           read_only, &db_id,
                                           &has_missing_table_file);
  auto iter =
      std::find(cf_names.begin(), cf_names.end(), kDefaultColumnFamilyName);
  if (iter == cf_names.end()) {
    ASSERT_TRUE(s.IsInvalidArgument());
  } else {
    ASSERT_TRUE(s.IsCorruption());
  }
}

TEST_P(VersionSetTestEmptyDb, OpenCompleteManifest) {
  db_options_.write_dbid_to_manifest = std::get<0>(GetParam());
  PrepareManifest(nullptr, nullptr, &log_writer_);
  // Write all column families but no log_number, next_file_number and
  // last_sequence.
  const std::vector<std::string> all_cf_names = {
      kDefaultColumnFamilyName, kColumnFamilyName1, kColumnFamilyName2,
      kColumnFamilyName3};
  uint32_t cf_id = 1;
  Status s;
  for (size_t i = 1; i != all_cf_names.size(); ++i) {
    VersionEdit new_cf;
    new_cf.AddColumnFamily(all_cf_names[i]);
    new_cf.SetColumnFamily(cf_id++);
    std::string record;
    ASSERT_TRUE(new_cf.EncodeTo(&record));
    s = log_writer_->AddRecord(record);
    ASSERT_OK(s);
  }
  {
    VersionEdit tmp_edit;
    tmp_edit.SetLogNumber(0);
    tmp_edit.SetNextFile(2);
    tmp_edit.SetLastSequence(0);
    std::string record;
    ASSERT_TRUE(tmp_edit.EncodeTo(&record));
    s = log_writer_->AddRecord(record);
    ASSERT_OK(s);
  }
  log_writer_.reset();
  s = SetCurrentFile(fs_.get(), dbname_, 1, /*directory_to_fsync=*/nullptr);
  ASSERT_OK(s);

  std::string manifest_path;
  VerifyManifest(&manifest_path);

  bool read_only = std::get<1>(GetParam());
  const std::vector<std::string>& cf_names = std::get<2>(GetParam());
  std::vector<ColumnFamilyDescriptor> column_families;
  for (const auto& cf_name : cf_names) {
    column_families.emplace_back(cf_name, cf_options_);
  }
  std::string db_id;
  bool has_missing_table_file = false;
  s = versions_->TryRecoverFromOneManifest(manifest_path, column_families,
                                           read_only, &db_id,
                                           &has_missing_table_file);
  auto iter =
      std::find(cf_names.begin(), cf_names.end(), kDefaultColumnFamilyName);
  if (iter == cf_names.end()) {
    ASSERT_TRUE(s.IsInvalidArgument());
  } else if (read_only) {
    ASSERT_OK(s);
    ASSERT_FALSE(has_missing_table_file);
  } else if (cf_names.size() == all_cf_names.size()) {
    ASSERT_OK(s);
    ASSERT_FALSE(has_missing_table_file);
  } else if (cf_names.size() < all_cf_names.size()) {
    ASSERT_TRUE(s.IsInvalidArgument());
  } else {
    ASSERT_OK(s);
    ASSERT_FALSE(has_missing_table_file);
    ColumnFamilyData* cfd = versions_->GetColumnFamilySet()->GetColumnFamily(
        kUnknownColumnFamilyName);
    ASSERT_EQ(nullptr, cfd);
  }
}

INSTANTIATE_TEST_CASE_P(
    BestEffortRecovery, VersionSetTestEmptyDb,
    testing::Combine(
        /*write_dbid_to_manifest=*/testing::Bool(),
        /*read_only=*/testing::Bool(),
        /*cf_names=*/
        testing::Values(
            std::vector<std::string>(),
            std::vector<std::string>({kDefaultColumnFamilyName}),
            std::vector<std::string>({VersionSetTestBase::kColumnFamilyName1,
                                      VersionSetTestBase::kColumnFamilyName2,
                                      VersionSetTestBase::kColumnFamilyName3}),
            std::vector<std::string>({kDefaultColumnFamilyName,
                                      VersionSetTestBase::kColumnFamilyName1}),
            std::vector<std::string>({kDefaultColumnFamilyName,
                                      VersionSetTestBase::kColumnFamilyName1,
                                      VersionSetTestBase::kColumnFamilyName2,
                                      VersionSetTestBase::kColumnFamilyName3}),
            std::vector<std::string>(
                {kDefaultColumnFamilyName,
                 VersionSetTestBase::kColumnFamilyName1,
                 VersionSetTestBase::kColumnFamilyName2,
                 VersionSetTestBase::kColumnFamilyName3,
                 VersionSetTestEmptyDb::kUnknownColumnFamilyName}))));

class VersionSetTestMissingFiles : public VersionSetTestBase,
                                   public testing::Test {
 public:
  VersionSetTestMissingFiles()
      : VersionSetTestBase("version_set_test_missing_files"),
        block_based_table_options_(),
        table_factory_(std::make_shared<BlockBasedTableFactory>(
            block_based_table_options_)),
        internal_comparator_(
            std::make_shared<InternalKeyComparator>(options_.comparator)) {}

 protected:
  void PrepareManifest(std::vector<ColumnFamilyDescriptor>* column_families,
                       SequenceNumber* last_seqno,
                       std::unique_ptr<log::Writer>* log_writer) override {
    assert(column_families != nullptr);
    assert(last_seqno != nullptr);
    assert(log_writer != nullptr);
    const std::string manifest = DescriptorFileName(dbname_, 1);
    const auto& fs = env_->GetFileSystem();
    std::unique_ptr<WritableFileWriter> file_writer;
    Status s = WritableFileWriter::Create(
        fs, manifest, fs->OptimizeForManifestWrite(env_options_), &file_writer,
        nullptr);
    ASSERT_OK(s);
    log_writer->reset(new log::Writer(std::move(file_writer), 0, false));
    VersionEdit new_db;
    if (db_options_.write_dbid_to_manifest) {
      DBOptions tmp_db_options;
      tmp_db_options.env = env_;
      std::unique_ptr<DBImpl> impl(new DBImpl(tmp_db_options, dbname_));
      std::string db_id;
      impl->GetDbIdentityFromIdentityFile(&db_id);
      new_db.SetDBId(db_id);
    }
    {
      std::string record;
      ASSERT_TRUE(new_db.EncodeTo(&record));
      s = (*log_writer)->AddRecord(record);
      ASSERT_OK(s);
    }
    const std::vector<std::string> cf_names = {
        kDefaultColumnFamilyName, kColumnFamilyName1, kColumnFamilyName2,
        kColumnFamilyName3};
    uint32_t cf_id = 1;  // default cf id is 0
    cf_options_.table_factory = table_factory_;
    for (const auto& cf_name : cf_names) {
      column_families->emplace_back(cf_name, cf_options_);
      if (cf_name == kDefaultColumnFamilyName) {
        continue;
      }
      VersionEdit new_cf;
      new_cf.AddColumnFamily(cf_name);
      new_cf.SetColumnFamily(cf_id);
      std::string record;
      ASSERT_TRUE(new_cf.EncodeTo(&record));
      s = (*log_writer)->AddRecord(record);
      ASSERT_OK(s);

      VersionEdit cf_files;
      cf_files.SetColumnFamily(cf_id);
      cf_files.SetLogNumber(0);
      record.clear();
      ASSERT_TRUE(cf_files.EncodeTo(&record));
      s = (*log_writer)->AddRecord(record);
      ASSERT_OK(s);
      ++cf_id;
    }
    SequenceNumber seq = 2;
    {
      VersionEdit edit;
      edit.SetNextFile(7);
      edit.SetLastSequence(seq);
      std::string record;
      ASSERT_TRUE(edit.EncodeTo(&record));
      s = (*log_writer)->AddRecord(record);
      ASSERT_OK(s);
    }
    *last_seqno = seq + 1;
  }

  struct SstInfo {
    uint64_t file_number;
    std::string column_family;
    std::string key;  // the only key
    int level = 0;
    SstInfo(uint64_t file_num, const std::string& cf_name,
            const std::string& _key)
        : SstInfo(file_num, cf_name, _key, 0) {}
    SstInfo(uint64_t file_num, const std::string& cf_name,
            const std::string& _key, int lvl)
        : file_number(file_num),
          column_family(cf_name),
          key(_key),
          level(lvl) {}
  };

  // Create dummy sst, return their metadata. Note that only file name and size
  // are used.
  void CreateDummyTableFiles(const std::vector<SstInfo>& file_infos,
                             std::vector<FileMetaData>* file_metas) {
    assert(file_metas != nullptr);
    for (const auto& info : file_infos) {
      uint64_t file_num = info.file_number;
      std::string fname = MakeTableFileName(dbname_, file_num);
      std::unique_ptr<FSWritableFile> file;
      Status s = fs_->NewWritableFile(fname, FileOptions(), &file, nullptr);
      ASSERT_OK(s);
      std::unique_ptr<WritableFileWriter> fwriter(new WritableFileWriter(
          std::move(file), fname, FileOptions(), env_->GetSystemClock().get()));
      IntTblPropCollectorFactories int_tbl_prop_collector_factories;

      std::unique_ptr<TableBuilder> builder(table_factory_->NewTableBuilder(
          TableBuilderOptions(
              immutable_options_, mutable_cf_options_, *internal_comparator_,
              &int_tbl_prop_collector_factories, kNoCompression,
              CompressionOptions(),
              TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
              info.column_family, info.level),
          fwriter.get()));
      InternalKey ikey(info.key, 0, ValueType::kTypeValue);
      builder->Add(ikey.Encode(), "value");
      ASSERT_OK(builder->Finish());
      fwriter->Flush();
      uint64_t file_size = 0;
      s = fs_->GetFileSize(fname, IOOptions(), &file_size, nullptr);
      ASSERT_OK(s);
      ASSERT_NE(0, file_size);
      file_metas->emplace_back(file_num, /*file_path_id=*/0, file_size, ikey,
                               ikey, 0, 0, false, 0, 0, 0, kUnknownFileChecksum,
                               kUnknownFileChecksumFuncName);
    }
  }

  // This method updates last_sequence_.
  void WriteFileAdditionAndDeletionToManifest(
      uint32_t cf, const std::vector<std::pair<int, FileMetaData>>& added_files,
      const std::vector<std::pair<int, uint64_t>>& deleted_files) {
    VersionEdit edit;
    edit.SetColumnFamily(cf);
    for (const auto& elem : added_files) {
      int level = elem.first;
      edit.AddFile(level, elem.second);
    }
    for (const auto& elem : deleted_files) {
      int level = elem.first;
      edit.DeleteFile(level, elem.second);
    }
    edit.SetLastSequence(last_seqno_);
    ++last_seqno_;
    assert(log_writer_.get() != nullptr);
    std::string record;
    ASSERT_TRUE(edit.EncodeTo(&record));
    Status s = log_writer_->AddRecord(record);
    ASSERT_OK(s);
  }

  BlockBasedTableOptions block_based_table_options_;
  std::shared_ptr<TableFactory> table_factory_;
  std::shared_ptr<InternalKeyComparator> internal_comparator_;
  std::vector<ColumnFamilyDescriptor> column_families_;
  SequenceNumber last_seqno_;
  std::unique_ptr<log::Writer> log_writer_;
};

TEST_F(VersionSetTestMissingFiles, ManifestFarBehindSst) {
  std::vector<SstInfo> existing_files = {
      SstInfo(100, kDefaultColumnFamilyName, "a"),
      SstInfo(102, kDefaultColumnFamilyName, "b"),
      SstInfo(103, kDefaultColumnFamilyName, "c"),
      SstInfo(107, kDefaultColumnFamilyName, "d"),
      SstInfo(110, kDefaultColumnFamilyName, "e")};
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(existing_files, &file_metas);

  PrepareManifest(&column_families_, &last_seqno_, &log_writer_);
  std::vector<std::pair<int, FileMetaData>> added_files;
  for (uint64_t file_num = 10; file_num < 15; ++file_num) {
    std::string smallest_ukey = "a";
    std::string largest_ukey = "b";
    InternalKey smallest_ikey(smallest_ukey, 1, ValueType::kTypeValue);
    InternalKey largest_ikey(largest_ukey, 1, ValueType::kTypeValue);
    FileMetaData meta =
        FileMetaData(file_num, /*file_path_id=*/0, /*file_size=*/12,
                     smallest_ikey, largest_ikey, 0, 0, false, 0, 0, 0,
                     kUnknownFileChecksum, kUnknownFileChecksumFuncName);
    added_files.emplace_back(0, meta);
  }
  WriteFileAdditionAndDeletionToManifest(
      /*cf=*/0, added_files, std::vector<std::pair<int, uint64_t>>());
  std::vector<std::pair<int, uint64_t>> deleted_files;
  deleted_files.emplace_back(0, 10);
  WriteFileAdditionAndDeletionToManifest(
      /*cf=*/0, std::vector<std::pair<int, FileMetaData>>(), deleted_files);
  log_writer_.reset();
  Status s = SetCurrentFile(fs_.get(), dbname_, 1, nullptr);
  ASSERT_OK(s);
  std::string manifest_path;
  VerifyManifest(&manifest_path);
  std::string db_id;
  bool has_missing_table_file = false;
  s = versions_->TryRecoverFromOneManifest(manifest_path, column_families_,
                                           /*read_only=*/false, &db_id,
                                           &has_missing_table_file);
  ASSERT_OK(s);
  ASSERT_TRUE(has_missing_table_file);
  for (ColumnFamilyData* cfd : *(versions_->GetColumnFamilySet())) {
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    const std::vector<FileMetaData*>& files = vstorage->LevelFiles(0);
    ASSERT_TRUE(files.empty());
  }
}

TEST_F(VersionSetTestMissingFiles, ManifestAheadofSst) {
  std::vector<SstInfo> existing_files = {
      SstInfo(100, kDefaultColumnFamilyName, "a"),
      SstInfo(102, kDefaultColumnFamilyName, "b"),
      SstInfo(103, kDefaultColumnFamilyName, "c"),
      SstInfo(107, kDefaultColumnFamilyName, "d"),
      SstInfo(110, kDefaultColumnFamilyName, "e")};
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(existing_files, &file_metas);

  PrepareManifest(&column_families_, &last_seqno_, &log_writer_);
  std::vector<std::pair<int, FileMetaData>> added_files;
  for (size_t i = 3; i != 5; ++i) {
    added_files.emplace_back(0, file_metas[i]);
  }
  WriteFileAdditionAndDeletionToManifest(
      /*cf=*/0, added_files, std::vector<std::pair<int, uint64_t>>());

  added_files.clear();
  for (uint64_t file_num = 120; file_num < 130; ++file_num) {
    std::string smallest_ukey = "a";
    std::string largest_ukey = "b";
    InternalKey smallest_ikey(smallest_ukey, 1, ValueType::kTypeValue);
    InternalKey largest_ikey(largest_ukey, 1, ValueType::kTypeValue);
    FileMetaData meta =
        FileMetaData(file_num, /*file_path_id=*/0, /*file_size=*/12,
                     smallest_ikey, largest_ikey, 0, 0, false, 0, 0, 0,
                     kUnknownFileChecksum, kUnknownFileChecksumFuncName);
    added_files.emplace_back(0, meta);
  }
  WriteFileAdditionAndDeletionToManifest(
      /*cf=*/0, added_files, std::vector<std::pair<int, uint64_t>>());
  log_writer_.reset();
  Status s = SetCurrentFile(fs_.get(), dbname_, 1, nullptr);
  ASSERT_OK(s);
  std::string manifest_path;
  VerifyManifest(&manifest_path);
  std::string db_id;
  bool has_missing_table_file = false;
  s = versions_->TryRecoverFromOneManifest(manifest_path, column_families_,
                                           /*read_only=*/false, &db_id,
                                           &has_missing_table_file);
  ASSERT_OK(s);
  ASSERT_TRUE(has_missing_table_file);
  for (ColumnFamilyData* cfd : *(versions_->GetColumnFamilySet())) {
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    const std::vector<FileMetaData*>& files = vstorage->LevelFiles(0);
    if (cfd->GetName() == kDefaultColumnFamilyName) {
      ASSERT_EQ(2, files.size());
      for (const auto* fmeta : files) {
        if (fmeta->fd.GetNumber() != 107 && fmeta->fd.GetNumber() != 110) {
          ASSERT_FALSE(true);
        }
      }
    } else {
      ASSERT_TRUE(files.empty());
    }
  }
}

TEST_F(VersionSetTestMissingFiles, NoFileMissing) {
  std::vector<SstInfo> existing_files = {
      SstInfo(100, kDefaultColumnFamilyName, "a"),
      SstInfo(102, kDefaultColumnFamilyName, "b"),
      SstInfo(103, kDefaultColumnFamilyName, "c"),
      SstInfo(107, kDefaultColumnFamilyName, "d"),
      SstInfo(110, kDefaultColumnFamilyName, "e")};
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(existing_files, &file_metas);

  PrepareManifest(&column_families_, &last_seqno_, &log_writer_);
  std::vector<std::pair<int, FileMetaData>> added_files;
  for (const auto& meta : file_metas) {
    added_files.emplace_back(0, meta);
  }
  WriteFileAdditionAndDeletionToManifest(
      /*cf=*/0, added_files, std::vector<std::pair<int, uint64_t>>());
  std::vector<std::pair<int, uint64_t>> deleted_files;
  deleted_files.emplace_back(/*level=*/0, 100);
  WriteFileAdditionAndDeletionToManifest(
      /*cf=*/0, std::vector<std::pair<int, FileMetaData>>(), deleted_files);
  log_writer_.reset();
  Status s = SetCurrentFile(fs_.get(), dbname_, 1, nullptr);
  ASSERT_OK(s);
  std::string manifest_path;
  VerifyManifest(&manifest_path);
  std::string db_id;
  bool has_missing_table_file = false;
  s = versions_->TryRecoverFromOneManifest(manifest_path, column_families_,
                                           /*read_only=*/false, &db_id,
                                           &has_missing_table_file);
  ASSERT_OK(s);
  ASSERT_FALSE(has_missing_table_file);
  for (ColumnFamilyData* cfd : *(versions_->GetColumnFamilySet())) {
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    const std::vector<FileMetaData*>& files = vstorage->LevelFiles(0);
    if (cfd->GetName() == kDefaultColumnFamilyName) {
      ASSERT_EQ(existing_files.size() - deleted_files.size(), files.size());
      bool has_deleted_file = false;
      for (const auto* fmeta : files) {
        if (fmeta->fd.GetNumber() == 100) {
          has_deleted_file = true;
          break;
        }
      }
      ASSERT_FALSE(has_deleted_file);
    } else {
      ASSERT_TRUE(files.empty());
    }
  }
}

TEST_F(VersionSetTestMissingFiles, MinLogNumberToKeep2PC) {
  NewDB();

  SstInfo sst(100, kDefaultColumnFamilyName, "a");
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles({sst}, &file_metas);

  constexpr WalNumber kMinWalNumberToKeep2PC = 10;
  VersionEdit edit;
  edit.AddFile(0, file_metas[0]);
  edit.SetMinLogNumberToKeep(kMinWalNumberToKeep2PC);
  ASSERT_OK(LogAndApplyToDefaultCF(edit));
  ASSERT_EQ(versions_->min_log_number_to_keep_2pc(), kMinWalNumberToKeep2PC);

  for (int i = 0; i < 3; i++) {
    CreateNewManifest();
    ReopenDB();
    ASSERT_EQ(versions_->min_log_number_to_keep_2pc(), kMinWalNumberToKeep2PC);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
