//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "table/block_based/block.h"

#include <algorithm>
#include <cstdio>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "table/format.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

std::string GenerateInternalKey(int primary_key, int secondary_key,
                                int padding_size, Random *rnd,
                                size_t ts_sz = 0) {
  char buf[50];
  char *p = &buf[0];
  snprintf(buf, sizeof(buf), "%6d%4d", primary_key, secondary_key);
  std::string k(p);
  if (padding_size) {
    k += rnd->RandomString(padding_size);
  }
  AppendInternalKeyFooter(&k, 0 /* seqno */, kTypeValue);
  std::string key_with_ts;
  if (ts_sz > 0) {
    PadInternalKeyWithMinTimestamp(&key_with_ts, k, ts_sz);
    return key_with_ts;
  }

  return k;
}

// Generate random key value pairs.
// The generated key will be sorted. You can tune the parameters to generated
// different kinds of test key/value pairs for different scenario.
void GenerateRandomKVs(std::vector<std::string> *keys,
                       std::vector<std::string> *values, const int from,
                       const int len, const int step = 1,
                       const int padding_size = 0,
                       const int keys_share_prefix = 1, size_t ts_sz = 0) {
  Random rnd(302);

  // generate different prefix
  for (int i = from; i < from + len; i += step) {
    // generating keys that shares the prefix
    for (int j = 0; j < keys_share_prefix; ++j) {
      // `DataBlockIter` assumes it reads only internal keys.
      keys->emplace_back(GenerateInternalKey(i, j, padding_size, &rnd, ts_sz));

      // 100 bytes values
      values->emplace_back(rnd.RandomString(100));
    }
  }
}

// Test Param 1): key use delta encoding.
// Test Param 2): user-defined timestamp test mode.
// Test Param 3): data block index type.
class BlockTest : public testing::Test,
                  public testing::WithParamInterface<
                      std::tuple<bool, test::UserDefinedTimestampTestMode,
                                 BlockBasedTableOptions::DataBlockIndexType>> {
 public:
  bool keyUseDeltaEncoding() const { return std::get<0>(GetParam()); }
  bool isUDTEnabled() const {
    return test::IsUDTEnabled(std::get<1>(GetParam()));
  }
  bool shouldPersistUDT() const {
    return test::ShouldPersistUDT(std::get<1>(GetParam()));
  }

  BlockBasedTableOptions::DataBlockIndexType dataBlockIndexType() const {
    return std::get<2>(GetParam());
  }
};

// block test
TEST_P(BlockTest, SimpleTest) {
  Random rnd(301);
  Options options = Options();
  if (isUDTEnabled()) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  }
  size_t ts_sz = options.comparator->timestamp_size();

  std::vector<std::string> keys;
  std::vector<std::string> values;
  BlockBasedTableOptions::DataBlockIndexType index_type =
      isUDTEnabled() ? BlockBasedTableOptions::kDataBlockBinarySearch
                     : dataBlockIndexType();
  BlockBuilder builder(16, keyUseDeltaEncoding(),
                       false /* use_value_delta_encoding */, index_type,
                       0.75 /* data_block_hash_table_util_ratio */, ts_sz,
                       shouldPersistUDT(), false /* is_user_key */);
  int num_records = 100000;

  GenerateRandomKVs(&keys, &values, 0, num_records, 1 /* step */,
                    0 /* padding_size */, 1 /* keys_share_prefix */, ts_sz);
  // add a bunch of records to a block
  for (int i = 0; i < num_records; i++) {
    builder.Add(keys[i], values[i]);
  }

  // read serialized contents of the block
  Slice rawblock = builder.Finish();

  // create block reader
  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents));

  // read contents of block sequentially
  int count = 0;
  InternalIterator *iter = reader.NewDataIterator(
      options.comparator, kDisableGlobalSequenceNumber, nullptr /* iter */,
      nullptr /* stats */, false /* block_contents_pinned */,
      shouldPersistUDT());
  for (iter->SeekToFirst(); iter->Valid(); count++, iter->Next()) {
    // read kv from block
    Slice k = iter->key();
    Slice v = iter->value();

    // compare with lookaside array
    ASSERT_EQ(k.ToString().compare(keys[count]), 0);
    ASSERT_EQ(v.ToString().compare(values[count]), 0);
  }
  delete iter;

  // read block contents randomly
  iter = reader.NewDataIterator(
      options.comparator, kDisableGlobalSequenceNumber, nullptr /* iter */,
      nullptr /* stats */, false /* block_contents_pinned */,
      shouldPersistUDT());
  for (int i = 0; i < num_records; i++) {
    // find a random key in the lookaside array
    int index = rnd.Uniform(num_records);
    Slice k(keys[index]);

    // search in block for this key
    iter->Seek(k);
    ASSERT_TRUE(iter->Valid());
    Slice v = iter->value();
    ASSERT_EQ(v.ToString().compare(values[index]), 0);
  }
  delete iter;
}

// return the block contents
BlockContents GetBlockContents(
    std::unique_ptr<BlockBuilder> *builder,
    const std::vector<std::string> &keys,
    const std::vector<std::string> &values, bool key_use_delta_encoding,
    size_t ts_sz, bool should_persist_udt, const int /*prefix_group_size*/ = 1,
    BlockBasedTableOptions::DataBlockIndexType dblock_index_type =
        BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch) {
  builder->reset(
      new BlockBuilder(1 /* restart interval */, key_use_delta_encoding,
                       false /* use_value_delta_encoding */, dblock_index_type,
                       0.75 /* data_block_hash_table_util_ratio */, ts_sz,
                       should_persist_udt, false /* is_user_key */));

  // Add only half of the keys
  for (size_t i = 0; i < keys.size(); ++i) {
    (*builder)->Add(keys[i], values[i]);
  }
  Slice rawblock = (*builder)->Finish();

  BlockContents contents;
  contents.data = rawblock;

  return contents;
}

void CheckBlockContents(BlockContents contents, const int max_key,
                        const std::vector<std::string> &keys,
                        const std::vector<std::string> &values,
                        bool is_udt_enabled, bool should_persist_udt) {
  const size_t prefix_size = 6;
  // create block reader
  BlockContents contents_ref(contents.data);
  Block reader1(std::move(contents));
  Block reader2(std::move(contents_ref));

  std::unique_ptr<const SliceTransform> prefix_extractor(
      NewFixedPrefixTransform(prefix_size));

  std::unique_ptr<InternalIterator> regular_iter(reader2.NewDataIterator(
      is_udt_enabled ? test::BytewiseComparatorWithU64TsWrapper()
                     : BytewiseComparator(),
      kDisableGlobalSequenceNumber, nullptr /* iter */, nullptr /* stats */,
      false /* block_contents_pinned */, should_persist_udt));

  // Seek existent keys
  for (size_t i = 0; i < keys.size(); i++) {
    regular_iter->Seek(keys[i]);
    ASSERT_OK(regular_iter->status());
    ASSERT_TRUE(regular_iter->Valid());

    Slice v = regular_iter->value();
    ASSERT_EQ(v.ToString().compare(values[i]), 0);
  }

  // Seek non-existent keys.
  // For hash index, if no key with a given prefix is not found, iterator will
  // simply be set as invalid; whereas the binary search based iterator will
  // return the one that is closest.
  for (int i = 1; i < max_key - 1; i += 2) {
    // `DataBlockIter` assumes its APIs receive only internal keys.
    auto key = GenerateInternalKey(i, 0, 0, nullptr,
                                   is_udt_enabled ? 8 : 0 /* ts_sz */);
    regular_iter->Seek(key);
    ASSERT_TRUE(regular_iter->Valid());
  }
}

// In this test case, no two key share same prefix.
TEST_P(BlockTest, SimpleIndexHash) {
  const int kMaxKey = 100000;
  size_t ts_sz = isUDTEnabled() ? 8 : 0;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  GenerateRandomKVs(&keys, &values, 0 /* first key id */,
                    kMaxKey /* last key id */, 2 /* step */,
                    8 /* padding size (8 bytes randomly generated suffix) */,
                    1 /* keys_share_prefix */, ts_sz);

  std::unique_ptr<BlockBuilder> builder;

  auto contents = GetBlockContents(
      &builder, keys, values, keyUseDeltaEncoding(), ts_sz, shouldPersistUDT(),
      1 /* prefix_group_size */,
      isUDTEnabled()
          ? BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch
          : dataBlockIndexType());

  CheckBlockContents(std::move(contents), kMaxKey, keys, values, isUDTEnabled(),
                     shouldPersistUDT());
}

TEST_P(BlockTest, IndexHashWithSharedPrefix) {
  const int kMaxKey = 100000;
  // for each prefix, there will be 5 keys starts with it.
  const int kPrefixGroup = 5;
  size_t ts_sz = isUDTEnabled() ? 8 : 0;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  // Generate keys with same prefix.
  GenerateRandomKVs(&keys, &values, 0,  // first key id
                    kMaxKey,            // last key id
                    2 /* step */,
                    10 /* padding size (8 bytes randomly generated suffix) */,
                    kPrefixGroup /* keys_share_prefix */, ts_sz);

  std::unique_ptr<BlockBuilder> builder;

  auto contents = GetBlockContents(
      &builder, keys, values, keyUseDeltaEncoding(), isUDTEnabled(),
      shouldPersistUDT(), kPrefixGroup,
      isUDTEnabled()
          ? BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch
          : dataBlockIndexType());

  CheckBlockContents(std::move(contents), kMaxKey, keys, values, isUDTEnabled(),
                     shouldPersistUDT());
}

// Param 0: key use delta encoding
// Param 1: user-defined timestamp test mode
// Param 2: data block index type. User-defined timestamp feature is not
// compatible with `kDataBlockBinaryAndHash` data block index type because the
// user comparator doesn't provide a `CanKeysWithDifferentByteContentsBeEqual`
// override. This combination is disabled.
INSTANTIATE_TEST_CASE_P(
    P, BlockTest,
    ::testing::Combine(
        ::testing::Bool(), ::testing::ValuesIn(test::GetUDTTestModes()),
        ::testing::Values(
            BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch,
            BlockBasedTableOptions::DataBlockIndexType::
                kDataBlockBinaryAndHash)));

// A slow and accurate version of BlockReadAmpBitmap that simply store
// all the marked ranges in a set.
class BlockReadAmpBitmapSlowAndAccurate {
 public:
  void Mark(size_t start_offset, size_t end_offset) {
    assert(end_offset >= start_offset);
    marked_ranges_.emplace(end_offset, start_offset);
  }

  void ResetCheckSequence() { iter_valid_ = false; }

  // Return true if any byte in this range was Marked
  // This does linear search from the previous position. When calling
  // multiple times, `offset` needs to be incremental to get correct results.
  // Call ResetCheckSequence() to reset it.
  bool IsPinMarked(size_t offset) {
    if (iter_valid_) {
      // Has existing iterator, try linear search from
      // the iterator.
      for (int i = 0; i < 64; i++) {
        if (offset < iter_->second) {
          return false;
        }
        if (offset <= iter_->first) {
          return true;
        }

        iter_++;
        if (iter_ == marked_ranges_.end()) {
          iter_valid_ = false;
          return false;
        }
      }
    }
    // Initial call or have linear searched too many times.
    // Do binary search.
    iter_ = marked_ranges_.lower_bound(
        std::make_pair(offset, static_cast<size_t>(0)));
    if (iter_ == marked_ranges_.end()) {
      iter_valid_ = false;
      return false;
    }
    iter_valid_ = true;
    return offset <= iter_->first && offset >= iter_->second;
  }

 private:
  std::set<std::pair<size_t, size_t>> marked_ranges_;
  std::set<std::pair<size_t, size_t>>::iterator iter_;
  bool iter_valid_ = false;
};

TEST_F(BlockTest, BlockReadAmpBitmap) {
  uint32_t pin_offset = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockReadAmpBitmap:rnd", [&pin_offset](void *arg) {
        pin_offset = *(static_cast<uint32_t *>(arg));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::vector<size_t> block_sizes = {
      1,                // 1 byte
      32,               // 32 bytes
      61,               // 61 bytes
      64,               // 64 bytes
      512,              // 0.5 KB
      1024,             // 1 KB
      1024 * 4,         // 4 KB
      1024 * 10,        // 10 KB
      1024 * 50,        // 50 KB
      1024 * 1024 * 4,  // 5 MB
      777,
      124653,
  };
  const size_t kBytesPerBit = 64;

  Random rnd(301);
  for (size_t block_size : block_sizes) {
    std::shared_ptr<Statistics> stats = ROCKSDB_NAMESPACE::CreateDBStatistics();
    BlockReadAmpBitmap read_amp_bitmap(block_size, kBytesPerBit, stats.get());
    BlockReadAmpBitmapSlowAndAccurate read_amp_slow_and_accurate;

    size_t needed_bits = (block_size / kBytesPerBit);
    if (block_size % kBytesPerBit != 0) {
      needed_bits++;
    }

    ASSERT_EQ(stats->getTickerCount(READ_AMP_TOTAL_READ_BYTES), block_size);

    // Generate some random entries
    std::vector<size_t> random_entry_offsets;
    for (int i = 0; i < 1000; i++) {
      random_entry_offsets.push_back(rnd.Next() % block_size);
    }
    std::sort(random_entry_offsets.begin(), random_entry_offsets.end());
    auto it =
        std::unique(random_entry_offsets.begin(), random_entry_offsets.end());
    random_entry_offsets.resize(
        std::distance(random_entry_offsets.begin(), it));

    std::vector<std::pair<size_t, size_t>> random_entries;
    for (size_t i = 0; i < random_entry_offsets.size(); i++) {
      size_t entry_start = random_entry_offsets[i];
      size_t entry_end;
      if (i + 1 < random_entry_offsets.size()) {
        entry_end = random_entry_offsets[i + 1] - 1;
      } else {
        entry_end = block_size - 1;
      }
      random_entries.emplace_back(entry_start, entry_end);
    }

    for (size_t i = 0; i < random_entries.size(); i++) {
      read_amp_slow_and_accurate.ResetCheckSequence();
      auto &current_entry = random_entries[rnd.Next() % random_entries.size()];

      read_amp_bitmap.Mark(static_cast<uint32_t>(current_entry.first),
                           static_cast<uint32_t>(current_entry.second));
      read_amp_slow_and_accurate.Mark(current_entry.first,
                                      current_entry.second);

      size_t total_bits = 0;
      for (size_t bit_idx = 0; bit_idx < needed_bits; bit_idx++) {
        total_bits += read_amp_slow_and_accurate.IsPinMarked(
            bit_idx * kBytesPerBit + pin_offset);
      }
      size_t expected_estimate_useful = total_bits * kBytesPerBit;
      size_t got_estimate_useful =
          stats->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES);
      ASSERT_EQ(expected_estimate_useful, got_estimate_useful);
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlockTest, BlockWithReadAmpBitmap) {
  Random rnd(301);
  Options options = Options();

  std::vector<std::string> keys;
  std::vector<std::string> values;
  BlockBuilder builder(16);
  int num_records = 10000;

  GenerateRandomKVs(&keys, &values, 0, num_records, 1 /* step */);
  // add a bunch of records to a block
  for (int i = 0; i < num_records; i++) {
    builder.Add(keys[i], values[i]);
  }

  Slice rawblock = builder.Finish();
  const size_t kBytesPerBit = 8;

  // Read the block sequentially using Next()
  {
    std::shared_ptr<Statistics> stats = ROCKSDB_NAMESPACE::CreateDBStatistics();

    // create block reader
    BlockContents contents;
    contents.data = rawblock;
    Block reader(std::move(contents), kBytesPerBit, stats.get());

    // read contents of block sequentially
    size_t read_bytes = 0;
    DataBlockIter *iter = reader.NewDataIterator(
        options.comparator, kDisableGlobalSequenceNumber, nullptr, stats.get());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      iter->value();
      read_bytes += iter->TEST_CurrentEntrySize();

      double semi_acc_read_amp =
          static_cast<double>(read_bytes) / rawblock.size();
      double read_amp = static_cast<double>(stats->getTickerCount(
                            READ_AMP_ESTIMATE_USEFUL_BYTES)) /
                        stats->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

      // Error in read amplification will be less than 1% if we are reading
      // sequentially
      double error_pct = fabs(semi_acc_read_amp - read_amp) * 100;
      EXPECT_LT(error_pct, 1);
    }

    delete iter;
  }

  // Read the block sequentially using Seek()
  {
    std::shared_ptr<Statistics> stats = ROCKSDB_NAMESPACE::CreateDBStatistics();

    // create block reader
    BlockContents contents;
    contents.data = rawblock;
    Block reader(std::move(contents), kBytesPerBit, stats.get());

    size_t read_bytes = 0;
    DataBlockIter *iter = reader.NewDataIterator(
        options.comparator, kDisableGlobalSequenceNumber, nullptr, stats.get());
    for (int i = 0; i < num_records; i++) {
      Slice k(keys[i]);

      // search in block for this key
      iter->Seek(k);
      iter->value();
      read_bytes += iter->TEST_CurrentEntrySize();

      double semi_acc_read_amp =
          static_cast<double>(read_bytes) / rawblock.size();
      double read_amp = static_cast<double>(stats->getTickerCount(
                            READ_AMP_ESTIMATE_USEFUL_BYTES)) /
                        stats->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

      // Error in read amplification will be less than 1% if we are reading
      // sequentially
      double error_pct = fabs(semi_acc_read_amp - read_amp) * 100;
      EXPECT_LT(error_pct, 1);
    }
    delete iter;
  }

  // Read the block randomly
  {
    std::shared_ptr<Statistics> stats = ROCKSDB_NAMESPACE::CreateDBStatistics();

    // create block reader
    BlockContents contents;
    contents.data = rawblock;
    Block reader(std::move(contents), kBytesPerBit, stats.get());

    size_t read_bytes = 0;
    DataBlockIter *iter = reader.NewDataIterator(
        options.comparator, kDisableGlobalSequenceNumber, nullptr, stats.get());
    std::unordered_set<int> read_keys;
    for (int i = 0; i < num_records; i++) {
      int index = rnd.Uniform(num_records);
      Slice k(keys[index]);

      iter->Seek(k);
      iter->value();
      if (read_keys.find(index) == read_keys.end()) {
        read_keys.insert(index);
        read_bytes += iter->TEST_CurrentEntrySize();
      }

      double semi_acc_read_amp =
          static_cast<double>(read_bytes) / rawblock.size();
      double read_amp = static_cast<double>(stats->getTickerCount(
                            READ_AMP_ESTIMATE_USEFUL_BYTES)) /
                        stats->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

      double error_pct = fabs(semi_acc_read_amp - read_amp) * 100;
      // Error in read amplification will be less than 2% if we are reading
      // randomly
      EXPECT_LT(error_pct, 2);
    }
    delete iter;
  }
}

TEST_F(BlockTest, ReadAmpBitmapPow2) {
  std::shared_ptr<Statistics> stats = ROCKSDB_NAMESPACE::CreateDBStatistics();
  ASSERT_EQ(BlockReadAmpBitmap(100, 1, stats.get()).GetBytesPerBit(), 1u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 2, stats.get()).GetBytesPerBit(), 2u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 4, stats.get()).GetBytesPerBit(), 4u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 8, stats.get()).GetBytesPerBit(), 8u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 16, stats.get()).GetBytesPerBit(), 16u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 32, stats.get()).GetBytesPerBit(), 32u);

  ASSERT_EQ(BlockReadAmpBitmap(100, 3, stats.get()).GetBytesPerBit(), 2u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 7, stats.get()).GetBytesPerBit(), 4u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 11, stats.get()).GetBytesPerBit(), 8u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 17, stats.get()).GetBytesPerBit(), 16u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 33, stats.get()).GetBytesPerBit(), 32u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 35, stats.get()).GetBytesPerBit(), 32u);
}

class IndexBlockTest
    : public testing::Test,
      public testing::WithParamInterface<
          std::tuple<bool, bool, bool, test::UserDefinedTimestampTestMode>> {
 public:
  IndexBlockTest() = default;

  bool keyIncludesSeq() const { return std::get<0>(GetParam()); }
  bool useValueDeltaEncoding() const { return std::get<1>(GetParam()); }
  bool includeFirstKey() const { return std::get<2>(GetParam()); }
  bool isUDTEnabled() const {
    return test::IsUDTEnabled(std::get<3>(GetParam()));
  }
  bool shouldPersistUDT() const {
    return test::ShouldPersistUDT(std::get<3>(GetParam()));
  }
};

// Similar to GenerateRandomKVs but for index block contents.
void GenerateRandomIndexEntries(std::vector<std::string> *separators,
                                std::vector<BlockHandle> *block_handles,
                                std::vector<std::string> *first_keys,
                                const int len, size_t ts_sz = 0,
                                bool zero_seqno = false) {
  Random rnd(42);

  // For each of `len` blocks, we need to generate a first and last key.
  // Let's generate n*2 random keys, sort them, group into consecutive pairs.
  std::set<std::string> keys;
  while ((int)keys.size() < len * 2) {
    // Keys need to be at least 8 bytes long to look like internal keys.
    std::string new_key = test::RandomKey(&rnd, 12);
    if (zero_seqno) {
      AppendInternalKeyFooter(&new_key, 0 /* seqno */, kTypeValue);
    }
    if (ts_sz > 0) {
      std::string key;
      PadInternalKeyWithMinTimestamp(&key, new_key, ts_sz);
      keys.insert(std::move(key));
    } else {
      keys.insert(std::move(new_key));
    }
  }

  uint64_t offset = 0;
  for (auto it = keys.begin(); it != keys.end();) {
    first_keys->emplace_back(*it++);
    separators->emplace_back(*it++);
    uint64_t size = rnd.Uniform(1024 * 16);
    BlockHandle handle(offset, size);
    offset += size + BlockBasedTable::kBlockTrailerSize;
    block_handles->emplace_back(handle);
  }
}

TEST_P(IndexBlockTest, IndexValueEncodingTest) {
  Random rnd(301);
  Options options = Options();
  if (isUDTEnabled()) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  }
  size_t ts_sz = options.comparator->timestamp_size();

  std::vector<std::string> separators;
  std::vector<BlockHandle> block_handles;
  std::vector<std::string> first_keys;
  const bool kUseDeltaEncoding = true;
  BlockBuilder builder(16, kUseDeltaEncoding, useValueDeltaEncoding(),
                       BlockBasedTableOptions::kDataBlockBinarySearch,
                       0.75 /* data_block_hash_table_util_ratio */, ts_sz,
                       shouldPersistUDT(), !keyIncludesSeq());

  int num_records = 100;

  GenerateRandomIndexEntries(&separators, &block_handles, &first_keys,
                             num_records, ts_sz, false /* zero_seqno */);
  BlockHandle last_encoded_handle;
  for (int i = 0; i < num_records; i++) {
    std::string first_key_to_persist_buf;
    Slice first_internal_key = first_keys[i];
    if (ts_sz > 0 && !shouldPersistUDT()) {
      StripTimestampFromInternalKey(&first_key_to_persist_buf, first_keys[i],
                                    ts_sz);
      first_internal_key = first_key_to_persist_buf;
    }
    IndexValue entry(block_handles[i], first_internal_key);
    std::string encoded_entry;
    std::string delta_encoded_entry;
    entry.EncodeTo(&encoded_entry, includeFirstKey(), nullptr);
    if (useValueDeltaEncoding() && i > 0) {
      entry.EncodeTo(&delta_encoded_entry, includeFirstKey(),
                     &last_encoded_handle);
    }
    last_encoded_handle = entry.handle;
    const Slice delta_encoded_entry_slice(delta_encoded_entry);

    if (keyIncludesSeq()) {
      builder.Add(separators[i], encoded_entry, &delta_encoded_entry_slice);
    } else {
      const Slice user_key = ExtractUserKey(separators[i]);
      builder.Add(user_key, encoded_entry, &delta_encoded_entry_slice);
    }
  }

  // read serialized contents of the block
  Slice rawblock = builder.Finish();

  // create block reader
  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents));

  const bool kTotalOrderSeek = true;
  IndexBlockIter *kNullIter = nullptr;
  Statistics *kNullStats = nullptr;
  // read contents of block sequentially
  InternalIteratorBase<IndexValue> *iter = reader.NewIndexIterator(
      options.comparator, kDisableGlobalSequenceNumber, kNullIter, kNullStats,
      kTotalOrderSeek, includeFirstKey(), keyIncludesSeq(),
      !useValueDeltaEncoding(), false /* block_contents_pinned */,
      shouldPersistUDT());
  iter->SeekToFirst();
  for (int index = 0; index < num_records; ++index) {
    ASSERT_TRUE(iter->Valid());

    Slice k = iter->key();
    IndexValue v = iter->value();

    if (keyIncludesSeq()) {
      EXPECT_EQ(separators[index], k.ToString());
    } else {
      const Slice user_key = ExtractUserKey(separators[index]);
      EXPECT_EQ(user_key, k);
    }
    EXPECT_EQ(block_handles[index].offset(), v.handle.offset());
    EXPECT_EQ(block_handles[index].size(), v.handle.size());
    EXPECT_EQ(includeFirstKey() ? first_keys[index] : "",
              v.first_internal_key.ToString());

    iter->Next();
  }
  delete iter;

  // read block contents randomly
  iter = reader.NewIndexIterator(
      options.comparator, kDisableGlobalSequenceNumber, kNullIter, kNullStats,
      kTotalOrderSeek, includeFirstKey(), keyIncludesSeq(),
      !useValueDeltaEncoding(), false /* block_contents_pinned */,
      shouldPersistUDT());
  for (int i = 0; i < num_records * 2; i++) {
    // find a random key in the lookaside array
    int index = rnd.Uniform(num_records);
    Slice k(separators[index]);

    // search in block for this key
    iter->Seek(k);
    ASSERT_TRUE(iter->Valid());
    IndexValue v = iter->value();
    if (keyIncludesSeq()) {
      EXPECT_EQ(separators[index], iter->key().ToString());
    } else {
      const Slice user_key = ExtractUserKey(separators[index]);
      EXPECT_EQ(user_key, iter->key());
    }
    EXPECT_EQ(block_handles[index].offset(), v.handle.offset());
    EXPECT_EQ(block_handles[index].size(), v.handle.size());
    EXPECT_EQ(includeFirstKey() ? first_keys[index] : "",
              v.first_internal_key.ToString());
  }
  delete iter;
}

// Param 0: key includes sequence number (whether to use user key or internal
// key as key entry in index block).
// Param 1: use value delta encoding
// Param 2: include first key
// Param 3: user-defined timestamp test mode
INSTANTIATE_TEST_CASE_P(
    P, IndexBlockTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool(), ::testing::Bool(),
                       ::testing::ValuesIn(test::GetUDTTestModes())));

class BlockPerKVChecksumTest : public DBTestBase {
 public:
  BlockPerKVChecksumTest()
      : DBTestBase("block_per_kv_checksum", /*env_do_fsync=*/false) {}

  template <typename TBlockIter>
  void TestIterateForward(std::unique_ptr<TBlockIter> &biter,
                          size_t &verification_count) {
    while (biter->Valid()) {
      verification_count = 0;
      biter->Next();
      if (biter->Valid()) {
        ASSERT_GE(verification_count, 1);
      }
    }
  }

  template <typename TBlockIter>
  void TestIterateBackward(std::unique_ptr<TBlockIter> &biter,
                           size_t &verification_count) {
    while (biter->Valid()) {
      verification_count = 0;
      biter->Prev();
      if (biter->Valid()) {
        ASSERT_GE(verification_count, 1);
      }
    }
  }

  template <typename TBlockIter>
  void TestSeekToFirst(std::unique_ptr<TBlockIter> &biter,
                       size_t &verification_count) {
    verification_count = 0;
    biter->SeekToFirst();
    ASSERT_GE(verification_count, 1);
    TestIterateForward(biter, verification_count);
  }

  template <typename TBlockIter>
  void TestSeekToLast(std::unique_ptr<TBlockIter> &biter,
                      size_t &verification_count) {
    verification_count = 0;
    biter->SeekToLast();
    ASSERT_GE(verification_count, 1);
    TestIterateBackward(biter, verification_count);
  }

  template <typename TBlockIter>
  void TestSeekForPrev(std::unique_ptr<TBlockIter> &biter,
                       size_t &verification_count, std::string k) {
    verification_count = 0;
    biter->SeekForPrev(k);
    ASSERT_GE(verification_count, 1);
    TestIterateBackward(biter, verification_count);
  }

  template <typename TBlockIter>
  void TestSeek(std::unique_ptr<TBlockIter> &biter, size_t &verification_count,
                std::string k) {
    verification_count = 0;
    biter->Seek(k);
    ASSERT_GE(verification_count, 1);
    TestIterateForward(biter, verification_count);
  }

  bool VerifyChecksum(uint32_t checksum_len, const char *checksum_ptr,
                      const Slice &key, const Slice &val) {
    if (!checksum_len) {
      return checksum_ptr == nullptr;
    }
    return ProtectionInfo64().ProtectKV(key, val).Verify(
        static_cast<uint8_t>(checksum_len), checksum_ptr);
  }
};

TEST_F(BlockPerKVChecksumTest, EmptyBlock) {
  // Tests that empty block code path is not broken by per kv checksum.
  BlockBuilder builder(
      16 /* block_restart_interval */, true /* use_delta_encoding */,
      false /* use_value_delta_encoding */,
      BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch);
  Slice raw_block = builder.Finish();
  BlockContents contents;
  contents.data = raw_block;

  std::unique_ptr<Block_kData> data_block;
  Options options = Options();
  BlockBasedTableOptions tbo;
  std::shared_ptr<Compressor> compressor =
      BuiltinCompressor::GetCompressor(kNoCompression);
  uint8_t protection_bytes_per_key = 8;
  BlockCreateContext create_context{&tbo,
                                    nullptr,
                                    nullptr /* statistics */,
                                    compressor,
                                    protection_bytes_per_key,
                                    options.comparator};
  create_context.Create(&data_block, std::move(contents));
  std::unique_ptr<DataBlockIter> biter{data_block->NewDataIterator(
      options.comparator, kDisableGlobalSequenceNumber)};
  biter->SeekToFirst();
  ASSERT_FALSE(biter->Valid());
  ASSERT_OK(biter->status());
  Random rnd(33);
  biter->SeekForGet(GenerateInternalKey(1, 1, 10, &rnd));
  ASSERT_FALSE(biter->Valid());
  ASSERT_OK(biter->status());
  biter->SeekToLast();
  ASSERT_FALSE(biter->Valid());
  ASSERT_OK(biter->status());
  biter->Seek(GenerateInternalKey(1, 1, 10, &rnd));
  ASSERT_FALSE(biter->Valid());
  ASSERT_OK(biter->status());
  biter->SeekForPrev(GenerateInternalKey(1, 1, 10, &rnd));
  ASSERT_FALSE(biter->Valid());
  ASSERT_OK(biter->status());
}

TEST_F(BlockPerKVChecksumTest, UnsupportedOptionValue) {
  Options options = Options();
  options.block_protection_bytes_per_key = 128;
  Destroy(options);
  ASSERT_TRUE(TryReopen(options).IsNotSupported());
}

TEST_F(BlockPerKVChecksumTest, InitializeProtectionInfo) {
  // Make sure that the checksum construction code path does not break
  // when the block is itself already corrupted.
  Options options = Options();
  BlockBasedTableOptions tbo;
  std::shared_ptr<Compressor> compressor =
      BuiltinCompressor::GetCompressor(kNoCompression);
  uint8_t protection_bytes_per_key = 8;
  BlockCreateContext create_context{
      &tbo,       nullptr /* ioptions */,   nullptr /* statistics */,
      compressor, protection_bytes_per_key, options.comparator};

  {
    std::string invalid_content = "1";
    Slice raw_block = invalid_content;
    BlockContents contents;
    contents.data = raw_block;
    std::unique_ptr<Block_kData> data_block;
    create_context.Create(&data_block, std::move(contents));
    std::unique_ptr<DataBlockIter> iter{data_block->NewDataIterator(
        options.comparator, kDisableGlobalSequenceNumber)};
    ASSERT_TRUE(iter->status().IsCorruption());
  }
  {
    std::string invalid_content = "1";
    Slice raw_block = invalid_content;
    BlockContents contents;
    contents.data = raw_block;
    std::unique_ptr<Block_kIndex> index_block;
    create_context.Create(&index_block, std::move(contents));
    std::unique_ptr<IndexBlockIter> iter{index_block->NewIndexIterator(
        options.comparator, kDisableGlobalSequenceNumber, nullptr, nullptr,
        true, false, true, true)};
    ASSERT_TRUE(iter->status().IsCorruption());
  }
  {
    std::string invalid_content = "1";
    Slice raw_block = invalid_content;
    BlockContents contents;
    contents.data = raw_block;
    std::unique_ptr<Block_kMetaIndex> meta_block;
    create_context.Create(&meta_block, std::move(contents));
    std::unique_ptr<MetaBlockIter> iter{meta_block->NewMetaIterator(true)};
    ASSERT_TRUE(iter->status().IsCorruption());
  }
}

TEST_F(BlockPerKVChecksumTest, ApproximateMemory) {
  // Tests that ApproximateMemoryUsage() includes memory used by block kv
  // checksum.
  const int kNumRecords = 20;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  GenerateRandomKVs(&keys, &values, 0, kNumRecords, 1 /* step */,
                    24 /* padding_size */);
  std::unique_ptr<BlockBuilder> builder;
  auto generate_block_content = [&]() {
    builder = std::make_unique<BlockBuilder>(16 /* restart_interval */);
    for (int i = 0; i < kNumRecords; ++i) {
      builder->Add(keys[i], values[i]);
    }
    Slice raw_block = builder->Finish();
    BlockContents contents;
    contents.data = raw_block;
    return contents;
  };

  Options options = Options();
  BlockBasedTableOptions tbo;
  std::shared_ptr<Compressor> compressor =
      BuiltinCompressor::GetCompressor(kNoCompression);
  uint8_t protection_bytes_per_key = 8;
  BlockCreateContext with_checksum_create_context{
      &tbo,       nullptr /* ioptions */,   nullptr /* statistics */,
      compressor, protection_bytes_per_key, options.comparator,
      true /* index_value_is_full */};
  BlockCreateContext create_context{&tbo,
                                    nullptr /* ioptions */,
                                    nullptr /* statistics */,
                                    compressor,
                                    0,
                                    options.comparator,
                                    true /* index_value_is_full */};

  {
    std::unique_ptr<Block_kData> data_block;
    create_context.Create(&data_block, generate_block_content());
    size_t block_memory = data_block->ApproximateMemoryUsage();
    std::unique_ptr<Block_kData> with_checksum_data_block;
    with_checksum_create_context.Create(&with_checksum_data_block,
                                        generate_block_content());
    ASSERT_GT(with_checksum_data_block->ApproximateMemoryUsage() - block_memory,
              100);
  }

  {
    std::unique_ptr<Block_kData> meta_block;
    create_context.Create(&meta_block, generate_block_content());
    size_t block_memory = meta_block->ApproximateMemoryUsage();
    std::unique_ptr<Block_kData> with_checksum_meta_block;
    with_checksum_create_context.Create(&with_checksum_meta_block,
                                        generate_block_content());
    // Rough comparison to avoid flaky test due to memory allocation alignment.
    ASSERT_GT(with_checksum_meta_block->ApproximateMemoryUsage() - block_memory,
              100);
  }

  {
    // Index block has different contents.
    std::vector<std::string> separators;
    std::vector<BlockHandle> block_handles;
    std::vector<std::string> first_keys;
    GenerateRandomIndexEntries(&separators, &block_handles, &first_keys,
                               kNumRecords);
    auto generate_index_content = [&]() {
      builder = std::make_unique<BlockBuilder>(16 /* restart_interval */);
      BlockHandle last_encoded_handle;
      for (int i = 0; i < kNumRecords; ++i) {
        IndexValue entry(block_handles[i], first_keys[i]);
        std::string encoded_entry;
        std::string delta_encoded_entry;
        entry.EncodeTo(&encoded_entry, false, nullptr);
        last_encoded_handle = entry.handle;
        const Slice delta_encoded_entry_slice(delta_encoded_entry);
        builder->Add(separators[i], encoded_entry, &delta_encoded_entry_slice);
      }
      Slice raw_block = builder->Finish();
      BlockContents contents;
      contents.data = raw_block;
      return contents;
    };

    std::unique_ptr<Block_kIndex> index_block;
    create_context.Create(&index_block, generate_index_content());
    size_t block_memory = index_block->ApproximateMemoryUsage();
    std::unique_ptr<Block_kIndex> with_checksum_index_block;
    with_checksum_create_context.Create(&with_checksum_index_block,
                                        generate_index_content());
    ASSERT_GT(
        with_checksum_index_block->ApproximateMemoryUsage() - block_memory,
        100);
  }
}

std::string GetDataBlockIndexTypeStr(
    BlockBasedTableOptions::DataBlockIndexType t) {
  return t == BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch
             ? "BinarySearch"
             : "BinaryAndHash";
}

class DataBlockKVChecksumTest
    : public BlockPerKVChecksumTest,
      public testing::WithParamInterface<std::tuple<
          BlockBasedTableOptions::DataBlockIndexType,
          uint8_t /* block_protection_bytes_per_key */,
          uint32_t /* restart_interval*/, bool /* use_delta_encoding */>> {
 public:
  DataBlockKVChecksumTest() = default;

  BlockBasedTableOptions::DataBlockIndexType GetDataBlockIndexType() const {
    return std::get<0>(GetParam());
  }
  uint8_t GetChecksumLen() const { return std::get<1>(GetParam()); }
  uint32_t GetRestartInterval() const { return std::get<2>(GetParam()); }
  bool GetUseDeltaEncoding() const { return std::get<3>(GetParam()); }

  std::unique_ptr<Block_kData> GenerateDataBlock(
      std::vector<std::string> &keys, std::vector<std::string> &values,
      int num_record) {
    BlockBasedTableOptions tbo;
    std::shared_ptr<Compressor> compressor =
        BuiltinCompressor::GetCompressor(kNoCompression);
    BlockCreateContext create_context{
        &tbo,       nullptr /* statistics */, nullptr /* ioptions */,
        compressor, GetChecksumLen(),         Options().comparator};
    builder_ = std::make_unique<BlockBuilder>(
        static_cast<int>(GetRestartInterval()),
        GetUseDeltaEncoding() /* use_delta_encoding */,
        false /* use_value_delta_encoding */, GetDataBlockIndexType());
    for (int i = 0; i < num_record; i++) {
      builder_->Add(keys[i], values[i]);
    }
    Slice raw_block = builder_->Finish();
    BlockContents contents;
    contents.data = raw_block;
    std::unique_ptr<Block_kData> data_block;
    create_context.Create(&data_block, std::move(contents));
    return data_block;
  }

  std::unique_ptr<BlockBuilder> builder_;
};

INSTANTIATE_TEST_CASE_P(
    P, DataBlockKVChecksumTest,
    ::testing::Combine(
        ::testing::Values(
            BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch,
            BlockBasedTableOptions::DataBlockIndexType::
                kDataBlockBinaryAndHash),
        ::testing::Values(0, 1, 2, 4, 8) /* protection_bytes_per_key */,
        ::testing::Values(1, 2, 3, 8, 16) /* restart_interval */,
        ::testing::Values(false, true)) /* delta_encoding */,
    [](const testing::TestParamInfo<std::tuple<
           BlockBasedTableOptions::DataBlockIndexType, uint8_t, uint32_t, bool>>
           &args) {
      std::ostringstream oss;
      oss << GetDataBlockIndexTypeStr(std::get<0>(args.param))
          << "ProtectionPerKey" << std::to_string(std::get<1>(args.param))
          << "RestartInterval" << std::to_string(std::get<2>(args.param))
          << "DeltaEncode" << std::to_string(std::get<3>(args.param));
      return oss.str();
    });

TEST_P(DataBlockKVChecksumTest, ChecksumConstructionAndVerification) {
  uint8_t protection_bytes_per_key = GetChecksumLen();
  std::vector<int> num_restart_intervals = {1, 16};
  for (const auto num_restart_interval : num_restart_intervals) {
    const int kNumRecords =
        num_restart_interval * static_cast<int>(GetRestartInterval());
    std::vector<std::string> keys;
    std::vector<std::string> values;
    GenerateRandomKVs(&keys, &values, 0, kNumRecords + 1, 1 /* step */,
                      24 /* padding_size */);
    SyncPoint::GetInstance()->DisableProcessing();
    std::unique_ptr<Block_kData> data_block =
        GenerateDataBlock(keys, values, kNumRecords);

    const char *checksum_ptr = data_block->TEST_GetKVChecksum();
    // Check checksum of correct length is generated
    for (int i = 0; i < kNumRecords; i++) {
      ASSERT_TRUE(VerifyChecksum(protection_bytes_per_key,
                                 checksum_ptr + i * protection_bytes_per_key,
                                 keys[i], values[i]));
    }
    std::vector<SequenceNumber> seqnos{kDisableGlobalSequenceNumber, 0};

    // Could just use a boolean flag. Use a counter here just to keep open the
    // possibility of checking the exact number of verifications in the future.
    size_t verification_count = 0;
    // The SyncPoint is placed before checking checksum_len == 0 in
    // Block::VerifyChecksum(). So verification count is incremented even with
    // protection_bytes_per_key = 0. No actual checksum computation is done in
    // that case (see Block::VerifyChecksum()).
    SyncPoint::GetInstance()->SetCallBack(
        "Block::VerifyChecksum::checksum_len",
        [&verification_count, protection_bytes_per_key](void *checksum_len) {
          ASSERT_EQ((*static_cast<uint8_t *>(checksum_len)),
                    protection_bytes_per_key);
          ++verification_count;
        });
    SyncPoint::GetInstance()->EnableProcessing();

    for (const auto seqno : seqnos) {
      std::unique_ptr<DataBlockIter> biter{
          data_block->NewDataIterator(Options().comparator, seqno)};

      // SeekForGet() some key that does not exist
      biter->SeekForGet(keys[kNumRecords]);
      TestIterateForward(biter, verification_count);

      verification_count = 0;
      biter->SeekForGet(keys[kNumRecords / 2]);
      ASSERT_GE(verification_count, 1);
      TestIterateForward(biter, verification_count);

      TestSeekToFirst(biter, verification_count);
      TestSeekToLast(biter, verification_count);
      TestSeekForPrev(biter, verification_count, keys[kNumRecords / 2]);
      TestSeek(biter, verification_count, keys[kNumRecords / 2]);
    }
  }
}

class IndexBlockKVChecksumTest
    : public BlockPerKVChecksumTest,
      public testing::WithParamInterface<
          std::tuple<BlockBasedTableOptions::DataBlockIndexType, uint8_t,
                     uint32_t, bool, bool>> {
 public:
  IndexBlockKVChecksumTest() = default;

  BlockBasedTableOptions::DataBlockIndexType GetDataBlockIndexType() const {
    return std::get<0>(GetParam());
  }
  uint8_t GetChecksumLen() const { return std::get<1>(GetParam()); }
  uint32_t GetRestartInterval() const { return std::get<2>(GetParam()); }
  bool UseValueDeltaEncoding() const { return std::get<3>(GetParam()); }
  bool IncludeFirstKey() const { return std::get<4>(GetParam()); }

  std::unique_ptr<Block_kIndex> GenerateIndexBlock(
      std::vector<std::string> &separators,
      std::vector<BlockHandle> &block_handles,
      std::vector<std::string> &first_keys, int num_record) {
    Options options = Options();
    BlockBasedTableOptions tbo;
    std::shared_ptr<Compressor> compressor =
        BuiltinCompressor::GetCompressor(kNoCompression);
    uint8_t protection_bytes_per_key = GetChecksumLen();
    BlockCreateContext create_context{
        &tbo,
        nullptr /* ioptions */,
        nullptr /* statistics */,
        compressor,
        protection_bytes_per_key,
        options.comparator,
        !UseValueDeltaEncoding() /* value_is_full */,
        IncludeFirstKey()};
    builder_ = std::make_unique<BlockBuilder>(
        static_cast<int>(GetRestartInterval()), true /* use_delta_encoding */,
        UseValueDeltaEncoding() /* use_value_delta_encoding */,
        GetDataBlockIndexType());
    BlockHandle last_encoded_handle;
    for (int i = 0; i < num_record; i++) {
      IndexValue entry(block_handles[i], first_keys[i]);
      std::string encoded_entry;
      std::string delta_encoded_entry;
      entry.EncodeTo(&encoded_entry, IncludeFirstKey(), nullptr);
      if (UseValueDeltaEncoding() && i > 0) {
        entry.EncodeTo(&delta_encoded_entry, IncludeFirstKey(),
                       &last_encoded_handle);
      }

      last_encoded_handle = entry.handle;
      const Slice delta_encoded_entry_slice(delta_encoded_entry);
      builder_->Add(separators[i], encoded_entry, &delta_encoded_entry_slice);
    }
    // read serialized contents of the block
    Slice raw_block = builder_->Finish();
    // create block reader
    BlockContents contents;
    contents.data = raw_block;
    std::unique_ptr<Block_kIndex> index_block;

    create_context.Create(&index_block, std::move(contents));
    return index_block;
  }

  std::unique_ptr<BlockBuilder> builder_;
};

INSTANTIATE_TEST_CASE_P(
    P, IndexBlockKVChecksumTest,
    ::testing::Combine(
        ::testing::Values(
            BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch,
            BlockBasedTableOptions::DataBlockIndexType::
                kDataBlockBinaryAndHash),
        ::testing::Values(0, 1, 2, 4, 8), ::testing::Values(1, 3, 8, 16),
        ::testing::Values(true, false), ::testing::Values(true, false)),
    [](const testing::TestParamInfo<
        std::tuple<BlockBasedTableOptions::DataBlockIndexType, uint8_t,
                   uint32_t, bool, bool>> &args) {
      std::ostringstream oss;
      oss << GetDataBlockIndexTypeStr(std::get<0>(args.param)) << "ProtBytes"
          << std::to_string(std::get<1>(args.param)) << "RestartInterval"
          << std::to_string(std::get<2>(args.param)) << "ValueDeltaEncode"
          << std::to_string(std::get<3>(args.param)) << "IncludeFirstKey"
          << std::to_string(std::get<4>(args.param));
      return oss.str();
    });

TEST_P(IndexBlockKVChecksumTest, ChecksumConstructionAndVerification) {
  Options options = Options();
  uint8_t protection_bytes_per_key = GetChecksumLen();
  std::vector<int> num_restart_intervals = {1, 16};
  std::vector<SequenceNumber> seqnos{kDisableGlobalSequenceNumber, 10001};

  for (const auto num_restart_interval : num_restart_intervals) {
    const int kNumRecords =
        num_restart_interval * static_cast<int>(GetRestartInterval());
    for (const auto seqno : seqnos) {
      std::vector<std::string> separators;
      std::vector<BlockHandle> block_handles;
      std::vector<std::string> first_keys;
      GenerateRandomIndexEntries(&separators, &block_handles, &first_keys,
                                 kNumRecords, 0 /* ts_sz */,
                                 seqno != kDisableGlobalSequenceNumber);
      SyncPoint::GetInstance()->DisableProcessing();
      std::unique_ptr<Block_kIndex> index_block = GenerateIndexBlock(
          separators, block_handles, first_keys, kNumRecords);
      IndexBlockIter *kNullIter = nullptr;
      Statistics *kNullStats = nullptr;
      // read contents of block sequentially
      std::unique_ptr<IndexBlockIter> biter{index_block->NewIndexIterator(
          options.comparator, seqno, kNullIter, kNullStats,
          true /* total_order_seek */, IncludeFirstKey() /* have_first_key */,
          true /* key_includes_seq */,
          !UseValueDeltaEncoding() /* value_is_full */,
          true /* block_contents_pinned*/,
          true /* user_defined_timestamps_persisted */,
          nullptr /* prefix_index */)};
      biter->SeekToFirst();
      const char *checksum_ptr = index_block->TEST_GetKVChecksum();
      // Check checksum of correct length is generated
      for (int i = 0; i < kNumRecords; i++) {
        // Obtaining the actual content written as value to index block is not
        // trivial: delta-encoded value is only persisted when not at block
        // restart point and that keys share some byte (see more in
        // BlockBuilder::AddWithLastKeyImpl()). So here we just do verification
        // using value from iterator unlike tests for DataBlockIter or
        // MetaBlockIter.
        ASSERT_TRUE(VerifyChecksum(protection_bytes_per_key, checksum_ptr,
                                   biter->key(), biter->raw_value()));
      }

      size_t verification_count = 0;
      // The SyncPoint is placed before checking checksum_len == 0 in
      // Block::VerifyChecksum(). To make the testing code below simpler and not
      // having to differentiate 0 vs non-0 checksum_len, we do an explicit
      // assert checking on checksum_len here.
      SyncPoint::GetInstance()->SetCallBack(
          "Block::VerifyChecksum::checksum_len",
          [&verification_count, protection_bytes_per_key](void *checksum_len) {
            ASSERT_EQ((*static_cast<uint8_t *>(checksum_len)),
                      protection_bytes_per_key);
            ++verification_count;
          });
      SyncPoint::GetInstance()->EnableProcessing();

      TestSeekToFirst(biter, verification_count);
      TestSeekToLast(biter, verification_count);
      TestSeek(biter, verification_count, first_keys[kNumRecords / 2]);
    }
  }
}

class MetaIndexBlockKVChecksumTest
    : public BlockPerKVChecksumTest,
      public testing::WithParamInterface<
          uint8_t /* block_protection_bytes_per_key */> {
 public:
  MetaIndexBlockKVChecksumTest() = default;
  uint8_t GetChecksumLen() const { return GetParam(); }
  uint32_t GetRestartInterval() const { return 1; }

  std::unique_ptr<Block_kMetaIndex> GenerateMetaIndexBlock(
      std::vector<std::string> &keys, std::vector<std::string> &values,
      int num_record) {
    Options options = Options();
    BlockBasedTableOptions tbo;
    std::shared_ptr<Compressor> compressor =
        BuiltinCompressor::GetCompressor(kNoCompression);
    uint8_t protection_bytes_per_key = GetChecksumLen();
    BlockCreateContext create_context{
        &tbo,       nullptr /* ioptions */,   nullptr /* statistics */,
        compressor, protection_bytes_per_key, options.comparator};
    builder_ =
        std::make_unique<BlockBuilder>(static_cast<int>(GetRestartInterval()));
    // add a bunch of records to a block
    for (int i = 0; i < num_record; i++) {
      builder_->Add(keys[i], values[i]);
    }
    Slice raw_block = builder_->Finish();
    BlockContents contents;
    contents.data = raw_block;
    std::unique_ptr<Block_kMetaIndex> meta_block;
    create_context.Create(&meta_block, std::move(contents));
    return meta_block;
  }

  std::unique_ptr<BlockBuilder> builder_;
};

INSTANTIATE_TEST_CASE_P(P, MetaIndexBlockKVChecksumTest,
                        ::testing::Values(0, 1, 2, 4, 8),
                        [](const testing::TestParamInfo<uint8_t> &args) {
                          std::ostringstream oss;
                          oss << "ProtBytes" << std::to_string(args.param);
                          return oss.str();
                        });

TEST_P(MetaIndexBlockKVChecksumTest, ChecksumConstructionAndVerification) {
  Options options = Options();
  BlockBasedTableOptions tbo;
  std::shared_ptr<Compressor> compressor =
      BuiltinCompressor::GetCompressor(kNoCompression);
  uint8_t protection_bytes_per_key = GetChecksumLen();
  BlockCreateContext create_context{
      &tbo,       nullptr /* ioptions */,   nullptr /* statistics */,
      compressor, protection_bytes_per_key, options.comparator};
  std::vector<int> num_restart_intervals = {1, 16};
  for (const auto num_restart_interval : num_restart_intervals) {
    const int kNumRecords = num_restart_interval * GetRestartInterval();
    std::vector<std::string> keys;
    std::vector<std::string> values;
    GenerateRandomKVs(&keys, &values, 0, kNumRecords + 1, 1 /* step */,
                      24 /* padding_size */);
    SyncPoint::GetInstance()->DisableProcessing();
    std::unique_ptr<Block_kMetaIndex> meta_block =
        GenerateMetaIndexBlock(keys, values, kNumRecords);
    const char *checksum_ptr = meta_block->TEST_GetKVChecksum();
    // Check checksum of correct length is generated
    for (int i = 0; i < kNumRecords; i++) {
      ASSERT_TRUE(VerifyChecksum(protection_bytes_per_key,
                                 checksum_ptr + i * protection_bytes_per_key,
                                 keys[i], values[i]));
    }

    size_t verification_count = 0;
    // The SyncPoint is placed before checking checksum_len == 0 in
    // Block::VerifyChecksum(). To make the testing code below simpler and not
    // having to differentiate 0 vs non-0 checksum_len, we do an explicit assert
    // checking on checksum_len here.
    SyncPoint::GetInstance()->SetCallBack(
        "Block::VerifyChecksum::checksum_len",
        [&verification_count, protection_bytes_per_key](void *checksum_len) {
          ASSERT_EQ((*static_cast<uint8_t *>(checksum_len)),
                    protection_bytes_per_key);
          ++verification_count;
        });
    SyncPoint::GetInstance()->EnableProcessing();
    // Check that block iterator does checksum verification
    std::unique_ptr<MetaBlockIter> biter{
        meta_block->NewMetaIterator(true /* block_contents_pinned */)};
    TestSeekToFirst(biter, verification_count);
    TestSeekToLast(biter, verification_count);
    TestSeek(biter, verification_count, keys[kNumRecords / 2]);
    TestSeekForPrev(biter, verification_count, keys[kNumRecords / 2]);
  }
}

class DataBlockKVChecksumCorruptionTest : public DataBlockKVChecksumTest {
 public:
  DataBlockKVChecksumCorruptionTest() = default;

  std::unique_ptr<DataBlockIter> GenerateDataBlockIter(
      std::vector<std::string> &keys, std::vector<std::string> &values,
      int num_record) {
    // During Block construction, we may create block iter to initialize per kv
    // checksum. Disable syncpoint that may be created for block iter methods.
    SyncPoint::GetInstance()->DisableProcessing();
    block_ = GenerateDataBlock(keys, values, num_record);
    std::unique_ptr<DataBlockIter> biter{block_->NewDataIterator(
        Options().comparator, kDisableGlobalSequenceNumber)};
    SyncPoint::GetInstance()->EnableProcessing();
    return biter;
  }

 protected:
  std::unique_ptr<Block_kData> block_;
};

TEST_P(DataBlockKVChecksumCorruptionTest, CorruptEntry) {
  std::vector<int> num_restart_intervals = {1, 3};
  for (const auto num_restart_interval : num_restart_intervals) {
    const int kNumRecords =
        num_restart_interval * static_cast<int>(GetRestartInterval());
    std::vector<std::string> keys;
    std::vector<std::string> values;
    GenerateRandomKVs(&keys, &values, 0, kNumRecords + 1, 1 /* step */,
                      24 /* padding_size */);
    SyncPoint::GetInstance()->SetCallBack(
        "BlockIter::UpdateKey::value", [](void *arg) {
          char *value = static_cast<char *>(arg);
          // values generated by GenerateRandomKVs are of length 100
          ++value[10];
        });

    // Purely for reducing the number of lines of code.
    typedef std::unique_ptr<DataBlockIter> IterPtr;
    typedef void(IterAPI)(IterPtr & iter, std::string &);

    std::string seek_key = keys[kNumRecords / 2];
    auto test_seek = [&](IterAPI iter_api) {
      IterPtr biter = GenerateDataBlockIter(keys, values, kNumRecords);
      ASSERT_OK(biter->status());
      iter_api(biter, seek_key);
      ASSERT_FALSE(biter->Valid());
      ASSERT_TRUE(biter->status().IsCorruption());
    };

    test_seek([](IterPtr &iter, std::string &) { iter->SeekToFirst(); });
    test_seek([](IterPtr &iter, std::string &) { iter->SeekToLast(); });
    test_seek([](IterPtr &iter, std::string &k) { iter->Seek(k); });
    test_seek([](IterPtr &iter, std::string &k) { iter->SeekForPrev(k); });
    test_seek([](IterPtr &iter, std::string &k) { iter->SeekForGet(k); });

    typedef void (DataBlockIter::*IterStepAPI)();
    auto test_step = [&](IterStepAPI iter_api, std::string &k) {
      IterPtr biter = GenerateDataBlockIter(keys, values, kNumRecords);
      SyncPoint::GetInstance()->DisableProcessing();
      biter->Seek(k);
      ASSERT_TRUE(biter->Valid());
      ASSERT_OK(biter->status());
      SyncPoint::GetInstance()->EnableProcessing();
      std::invoke(iter_api, biter);
      ASSERT_FALSE(biter->Valid());
      ASSERT_TRUE(biter->status().IsCorruption());
    };

    if (kNumRecords > 1) {
      test_step(&DataBlockIter::Prev, seek_key);
      test_step(&DataBlockIter::Next, seek_key);
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    P, DataBlockKVChecksumCorruptionTest,
    ::testing::Combine(
        ::testing::Values(
            BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch,
            BlockBasedTableOptions::DataBlockIndexType::
                kDataBlockBinaryAndHash),
        ::testing::Values(4, 8) /* block_protection_bytes_per_key */,
        ::testing::Values(1, 3, 8, 16) /* restart_interval */,
        ::testing::Values(false, true)),
    [](const testing::TestParamInfo<std::tuple<
           BlockBasedTableOptions::DataBlockIndexType, uint8_t, uint32_t, bool>>
           &args) {
      std::ostringstream oss;
      oss << GetDataBlockIndexTypeStr(std::get<0>(args.param)) << "ProtBytes"
          << std::to_string(std::get<1>(args.param)) << "RestartInterval"
          << std::to_string(std::get<2>(args.param)) << "DeltaEncode"
          << std::to_string(std::get<3>(args.param));
      return oss.str();
    });

class IndexBlockKVChecksumCorruptionTest : public IndexBlockKVChecksumTest {
 public:
  IndexBlockKVChecksumCorruptionTest() = default;

  std::unique_ptr<IndexBlockIter> GenerateIndexBlockIter(
      std::vector<std::string> &separators,
      std::vector<BlockHandle> &block_handles,
      std::vector<std::string> &first_keys, int num_record,
      SequenceNumber seqno) {
    SyncPoint::GetInstance()->DisableProcessing();
    block_ =
        GenerateIndexBlock(separators, block_handles, first_keys, num_record);
    std::unique_ptr<IndexBlockIter> biter{block_->NewIndexIterator(
        Options().comparator, seqno, nullptr, nullptr,
        true /* total_order_seek */, IncludeFirstKey() /* have_first_key */,
        true /* key_includes_seq */,
        !UseValueDeltaEncoding() /* value_is_full */,
        true /* block_contents_pinned */,
        true /* user_defined_timestamps_persisted */,
        nullptr /* prefix_index */)};
    SyncPoint::GetInstance()->EnableProcessing();
    return biter;
  }

 protected:
  std::unique_ptr<Block_kIndex> block_;
};

INSTANTIATE_TEST_CASE_P(
    P, IndexBlockKVChecksumCorruptionTest,
    ::testing::Combine(
        ::testing::Values(
            BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch,
            BlockBasedTableOptions::DataBlockIndexType::
                kDataBlockBinaryAndHash),
        ::testing::Values(4, 8) /* block_protection_bytes_per_key */,
        ::testing::Values(1, 3, 8, 16) /* restart_interval */,
        ::testing::Values(true, false), ::testing::Values(true, false)),
    [](const testing::TestParamInfo<
        std::tuple<BlockBasedTableOptions::DataBlockIndexType, uint8_t,
                   uint32_t, bool, bool>> &args) {
      std::ostringstream oss;
      oss << GetDataBlockIndexTypeStr(std::get<0>(args.param)) << "ProtBytes"
          << std::to_string(std::get<1>(args.param)) << "RestartInterval"
          << std::to_string(std::get<2>(args.param)) << "ValueDeltaEncode"
          << std::to_string(std::get<3>(args.param)) << "IncludeFirstKey"
          << std::to_string(std::get<4>(args.param));
      return oss.str();
    });

TEST_P(IndexBlockKVChecksumCorruptionTest, CorruptEntry) {
  std::vector<int> num_restart_intervals = {1, 3};
  std::vector<SequenceNumber> seqnos{kDisableGlobalSequenceNumber, 10001};

  for (const auto num_restart_interval : num_restart_intervals) {
    const int kNumRecords =
        num_restart_interval * static_cast<int>(GetRestartInterval());
    for (const auto seqno : seqnos) {
      std::vector<std::string> separators;
      std::vector<BlockHandle> block_handles;
      std::vector<std::string> first_keys;
      GenerateRandomIndexEntries(&separators, &block_handles, &first_keys,
                                 kNumRecords, 0 /* ts_sz */,
                                 seqno != kDisableGlobalSequenceNumber);
      SyncPoint::GetInstance()->SetCallBack(
          "BlockIter::UpdateKey::value", [](void *arg) {
            char *value = static_cast<char *>(arg);
            // value can be delta-encoded with different lengths, so we corrupt
            // first bytes here to be safe
            ++value[0];
          });

      typedef std::unique_ptr<IndexBlockIter> IterPtr;
      typedef void(IterAPI)(IterPtr & iter, std::string &);
      std::string seek_key = first_keys[kNumRecords / 2];
      auto test_seek = [&](IterAPI iter_api) {
        std::unique_ptr<IndexBlockIter> biter = GenerateIndexBlockIter(
            separators, block_handles, first_keys, kNumRecords, seqno);
        ASSERT_OK(biter->status());
        iter_api(biter, seek_key);
        ASSERT_FALSE(biter->Valid());
        ASSERT_TRUE(biter->status().IsCorruption());
      };
      test_seek([](IterPtr &iter, std::string &) { iter->SeekToFirst(); });
      test_seek([](IterPtr &iter, std::string &) { iter->SeekToLast(); });
      test_seek([](IterPtr &iter, std::string &k) { iter->Seek(k); });

      typedef void (IndexBlockIter::*IterStepAPI)();
      auto test_step = [&](IterStepAPI iter_api, std::string &k) {
        std::unique_ptr<IndexBlockIter> biter = GenerateIndexBlockIter(
            separators, block_handles, first_keys, kNumRecords, seqno);
        SyncPoint::GetInstance()->DisableProcessing();
        biter->Seek(k);
        ASSERT_TRUE(biter->Valid());
        ASSERT_OK(biter->status());
        SyncPoint::GetInstance()->EnableProcessing();
        std::invoke(iter_api, biter);
        ASSERT_FALSE(biter->Valid());
        ASSERT_TRUE(biter->status().IsCorruption());
      };
      if (kNumRecords > 1) {
        test_step(&IndexBlockIter::Prev, seek_key);
        test_step(&IndexBlockIter::Next, seek_key);
      }
    }
  }
}

class MetaIndexBlockKVChecksumCorruptionTest
    : public MetaIndexBlockKVChecksumTest {
 public:
  MetaIndexBlockKVChecksumCorruptionTest() = default;

  std::unique_ptr<MetaBlockIter> GenerateMetaIndexBlockIter(
      std::vector<std::string> &keys, std::vector<std::string> &values,
      int num_record) {
    SyncPoint::GetInstance()->DisableProcessing();
    block_ = GenerateMetaIndexBlock(keys, values, num_record);
    std::unique_ptr<MetaBlockIter> biter{
        block_->NewMetaIterator(true /* block_contents_pinned */)};
    SyncPoint::GetInstance()->EnableProcessing();
    return biter;
  }

 protected:
  std::unique_ptr<Block_kMetaIndex> block_;
};

INSTANTIATE_TEST_CASE_P(
    P, MetaIndexBlockKVChecksumCorruptionTest,
    ::testing::Values(4, 8) /* block_protection_bytes_per_key */,
    [](const testing::TestParamInfo<uint8_t> &args) {
      std::ostringstream oss;
      oss << "ProtBytes" << std::to_string(args.param);
      return oss.str();
    });

TEST_P(MetaIndexBlockKVChecksumCorruptionTest, CorruptEntry) {
  Options options = Options();
  std::vector<int> num_restart_intervals = {1, 3};
  for (const auto num_restart_interval : num_restart_intervals) {
    const int kNumRecords =
        num_restart_interval * static_cast<int>(GetRestartInterval());
    std::vector<std::string> keys;
    std::vector<std::string> values;
    GenerateRandomKVs(&keys, &values, 0, kNumRecords + 1, 1 /* step */,
                      24 /* padding_size */);
    SyncPoint::GetInstance()->SetCallBack(
        "BlockIter::UpdateKey::value", [](void *arg) {
          char *value = static_cast<char *>(arg);
          // values generated by GenerateRandomKVs are of length 100
          ++value[10];
        });

    typedef std::unique_ptr<MetaBlockIter> IterPtr;
    typedef void(IterAPI)(IterPtr & iter, std::string &);
    typedef void (MetaBlockIter::*IterStepAPI)();
    std::string seek_key = keys[kNumRecords / 2];
    auto test_seek = [&](IterAPI iter_api) {
      IterPtr biter = GenerateMetaIndexBlockIter(keys, values, kNumRecords);
      ASSERT_OK(biter->status());
      iter_api(biter, seek_key);
      ASSERT_FALSE(biter->Valid());
      ASSERT_TRUE(biter->status().IsCorruption());
    };

    test_seek([](IterPtr &iter, std::string &) { iter->SeekToFirst(); });
    test_seek([](IterPtr &iter, std::string &) { iter->SeekToLast(); });
    test_seek([](IterPtr &iter, std::string &k) { iter->Seek(k); });
    test_seek([](IterPtr &iter, std::string &k) { iter->SeekForPrev(k); });

    auto test_step = [&](IterStepAPI iter_api, const std::string &k) {
      IterPtr biter = GenerateMetaIndexBlockIter(keys, values, kNumRecords);
      SyncPoint::GetInstance()->DisableProcessing();
      biter->Seek(k);
      ASSERT_TRUE(biter->Valid());
      ASSERT_OK(biter->status());
      SyncPoint::GetInstance()->EnableProcessing();
      std::invoke(iter_api, biter);
      ASSERT_FALSE(biter->Valid());
      ASSERT_TRUE(biter->status().IsCorruption());
    };

    if (kNumRecords > 1) {
      test_step(&MetaBlockIter::Prev, seek_key);
      test_step(&MetaBlockIter::Next, seek_key);
    }
  }
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char **argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
