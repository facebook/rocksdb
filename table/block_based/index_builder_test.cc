//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/index_builder.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "table/format.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class IndexBuilderTest
    : public testing::Test,
      public testing::WithParamInterface<BlockBasedTableOptions::IndexType> {
 public:
  IndexBuilderTest() : icomp_(BytewiseComparator()) {}

  std::unique_ptr<IndexBuilder> CreateIndexBuilder() {
    BlockBasedTableOptions table_options;
    BlockBasedTableOptions::IndexType index_type = GetParam();
    return std::unique_ptr<IndexBuilder>(IndexBuilder::CreateIndexBuilder(
        index_type, &icomp_, nullptr, false /* use_value_delta_encoding */,
        table_options, 0 /* ts_sz */,
        true /* persist_user_defined_timestamps */));
  }

  std::string MakeKey(int i) {
    return InternalKey(std::string("key") + std::to_string(i), 100 - i,
                       kTypeValue)
        .Encode()
        .ToString();
  }

  BlockHandle MakeBlockHandle(uint64_t offset, uint64_t size) {
    BlockHandle handle;
    handle.set_offset(offset);
    handle.set_size(size);
    return handle;
  }

  void AddEntriesToBuilder(IndexBuilder* builder, int num_entries,
                           std::vector<uint64_t>* estimates = nullptr) {
    for (int i = 1; i <= num_entries; ++i) {
      std::string key_current = MakeKey(i);
      BlockHandle handle = MakeBlockHandle(i * kBlockOffset, kBlockSize);
      std::string separator_scratch;

      if (i == num_entries) {
        // Last entry - no next key
        builder->AddIndexEntry(key_current, nullptr, handle, &separator_scratch,
                               false);
      } else {
        std::string key_next = MakeKey(i + 1);
        Slice key_next_slice(key_next);
        builder->AddIndexEntry(key_current, &key_next_slice, handle,
                               &separator_scratch, false);
      }

      if (estimates) {
        uint64_t current_estimate = builder->EstimateCurrentIndexSize();
        estimates->push_back(current_estimate);
      }
    }
  }

 protected:
  InternalKeyComparator icomp_;
  static const uint64_t kBlockOffset = 1000;
  static const uint64_t kBlockSize = 4096;
  // BlockBuilder initial overhead
  // See BlockBuilder constructor and Reset()
  static const uint64_t kBlockBuilderInitialOverhead = 2 * sizeof(uint32_t);
};

const uint64_t IndexBuilderTest::kBlockOffset;
const uint64_t IndexBuilderTest::kBlockSize;
const uint64_t IndexBuilderTest::kBlockBuilderInitialOverhead;

TEST_P(IndexBuilderTest, EstimateCurrentIndexSize) {
  auto builder = CreateIndexBuilder();
  BlockBasedTableOptions::IndexType index_type = GetParam();

  // Empty builder
  uint64_t empty_size = builder->EstimateCurrentIndexSize();
  if (index_type == BlockBasedTableOptions::kBinarySearch) {
    EXPECT_EQ(empty_size, kBlockBuilderInitialOverhead)
        << "Empty ShortenedIndexBuilder should return BlockBuilder initial "
           "overhead ("
        << kBlockBuilderInitialOverhead;
  } else {
    EXPECT_EQ(empty_size, 0) << "Other builders should return 0 when empty";
  }

  // Add one entry
  AddEntriesToBuilder(builder.get(), 1);
  uint64_t size_after_one = builder->EstimateCurrentIndexSize();

  if (index_type == BlockBasedTableOptions::kBinarySearch) {
    EXPECT_GT(size_after_one, kBlockBuilderInitialOverhead)
        << "Estimate should be greater than initial overhead";
  } else {
    // Other builders currently return 0 (which is expected)
    EXPECT_EQ(size_after_one, 0) << "Other index builders currently return 0";
  }

  // Add multiple entries and capture all estimates
  std::vector<uint64_t> estimates;
  auto new_builder = CreateIndexBuilder();
  AddEntriesToBuilder(new_builder.get(), 5, &estimates);

  // Validate reported estimates
  for (size_t i = 0; i < estimates.size(); ++i) {
    uint64_t estimate = estimates[i];

    if (index_type == BlockBasedTableOptions::kBinarySearch) {
      EXPECT_GT(estimate, 0)
          << "Estimate should be positive for " << i << " entry";
      if (i > 0) {
        EXPECT_GT(estimate, estimates[i - 1])
            << "Estimate should not decrease with more entries (entry " << i - 1
            << ": " << estimates[i - 1] << ", entry " << i << ": " << estimate
            << ")";
      }
    } else {
      EXPECT_EQ(estimate, 0) << "Other index builders currently return 0";
    }
  }

  // Multiple calls should return the same value if the builder state is not
  // modified
  uint64_t estimate1 = builder->EstimateCurrentIndexSize();
  uint64_t estimate2 = builder->EstimateCurrentIndexSize();
  uint64_t estimate3 = builder->EstimateCurrentIndexSize();

  EXPECT_EQ(estimate1, estimate2);
  EXPECT_EQ(estimate2, estimate3);

  // Test behavior after Finish() - only for builders that can be finished
  // successfully
  if (index_type == BlockBasedTableOptions::kBinarySearch) {
    uint64_t estimate_before_finish = builder->EstimateCurrentIndexSize();

    IndexBuilder::IndexBlocks index_blocks;
    Status s = builder->Finish(&index_blocks);
    EXPECT_TRUE(s.ok()) << "ShortenedIndexBuilder should finish successfully: "
                        << s.ToString();

    uint64_t estimate_after_finish = builder->EstimateCurrentIndexSize();
    EXPECT_GT(estimate_after_finish, 0);
    EXPECT_LE(estimate_before_finish, estimate_after_finish)
        << "Estimate should not decrease after finish";

    // Ensure that the actual index size is not greater than the estimated size
    // after finish is called to prevent underestimation.
    uint64_t actual_index_size = builder->IndexSize();
    EXPECT_LE(actual_index_size, estimate_after_finish)
        << "Actual index size should not be greater than estimated size: "
           "actual size:  "
        << actual_index_size << ", estimated size: " << estimate_after_finish;
  }
}

INSTANTIATE_TEST_CASE_P(
    IndexBuilderTypes, IndexBuilderTest,
    ::testing::Values(BlockBasedTableOptions::kBinarySearch,
                      BlockBasedTableOptions::kHashSearch,
                      BlockBasedTableOptions::kTwoLevelIndexSearch));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
