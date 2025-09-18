//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/block_based_table_reader.h"

#include <cmath>
#include <memory>
#include <string>

#include "cache/cache_reservation_manager.h"
#include "db/db_test_util.h"
#include "db/table_properties_collector.h"
#include "file/file_util.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/partitioned_index_iterator.h"
#include "table/format.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"

// Enable io_uring support for this test
extern "C" bool RocksDbIOUringEnable() { return true; }

namespace ROCKSDB_NAMESPACE {

class BlockBasedTableReaderBaseTest : public testing::Test {
 public:
  static constexpr int kBytesPerEntry = 256;
  // 16 = (default block size) 4 * 1024 / kBytesPerEntry
  static constexpr int kEntriesPerBlock = 16;

 protected:
  // Prepare key-value pairs to occupy multiple blocks.
  // Each (key, value) pair is `kBytesPerEntry` byte, every kEntriesPerBlock
  // pairs constitute 1 block.
  // If mixed_with_human_readable_string_value == true,
  // then adjacent blocks contain values with different compression
  // complexity: human readable strings are easier to compress than random
  // strings. key is an internal key.
  // When ts_sz > 0 and `same_key_diff_ts` is true, this
  // function generate keys with the same user provided key, with different
  // user defined timestamps and different sequence number to differentiate them
  static std::vector<std::pair<std::string, std::string>> GenerateKVMap(
      int num_block = 2, bool mixed_with_human_readable_string_value = false,
      size_t ts_sz = 0, bool same_key_diff_ts = false,
      const Comparator* comparator = BytewiseComparator()) {
    std::vector<std::pair<std::string, std::string>> kv;

    SequenceNumber seq_no = 0;
    uint64_t current_udt = 0;
    if (same_key_diff_ts) {
      // These numbers are based on the number of keys to create + an arbitrary
      // buffer number (100) to avoid overflow.
      current_udt = kEntriesPerBlock * num_block + 100;
      seq_no = kEntriesPerBlock * num_block + 100;
    }
    Random rnd(101);
    uint32_t key = 0;
    // To make each (key, value) pair occupy exactly kBytesPerEntry bytes.
    int value_size = kBytesPerEntry - (8 + static_cast<int>(ts_sz) +
                                       static_cast<int>(kNumInternalBytes));
    for (int block = 0; block < num_block; block++) {
      for (int i = 0; i < kEntriesPerBlock; i++) {
        char k[9] = {0};
        // Internal key is constructed directly from this key,
        // and internal key size is required to be >= 8 bytes,
        // so use %08u as the format string.
        snprintf(k, sizeof(k), "%08u", key);
        std::string v;
        if (mixed_with_human_readable_string_value) {
          v = (block % 2) ? rnd.HumanReadableString(value_size)
                          : rnd.RandomString(value_size);
        } else {
          v = rnd.RandomString(value_size);
        }
        std::string user_key = std::string(k);
        if (ts_sz > 0) {
          if (same_key_diff_ts) {
            PutFixed64(&user_key, current_udt);
            current_udt -= 1;
          } else {
            PutFixed64(&user_key, 0);
          }
        }
        InternalKey internal_key(user_key, seq_no, ValueType::kTypeValue);
        kv.emplace_back(internal_key.Encode().ToString(), v);
        if (same_key_diff_ts) {
          seq_no -= 1;
        } else {
          key++;
        }
      }
    }
    auto comparator_name = std::string(comparator->Name());
    if (comparator_name.find("Reverse") != std::string::npos) {
      std::reverse(kv.begin(), kv.end());
    }
    return kv;
  }

  void SetUp() override {
    SetupSyncPointsToMockDirectIO();
    test_dir_ = test::PerThreadDBPath("block_based_table_reader_test");
    env_ = Env::Default();
    fs_ = FileSystem::Default();
    ASSERT_OK(fs_->CreateDir(test_dir_, IOOptions(), nullptr));
    ConfigureTableFactory();
  }

  virtual void ConfigureTableFactory() = 0;

  void TearDown() override { EXPECT_OK(DestroyDir(env_, test_dir_)); }

  // Creates a table with the specificied key value pairs (kv).
  void CreateTable(const std::string& table_name,
                   const ImmutableOptions& ioptions,
                   const CompressionType& compression_type,
                   const std::vector<std::pair<std::string, std::string>>& kv,
                   uint32_t compression_parallel_threads = 1,
                   uint32_t compression_dict_bytes = 0) {
    std::unique_ptr<WritableFileWriter> writer;
    NewFileWriter(table_name, &writer);

    InternalKeyComparator comparator(ioptions.user_comparator);
    ColumnFamilyOptions cf_options;
    cf_options.comparator = ioptions.user_comparator;
    cf_options.prefix_extractor = options_.prefix_extractor;
    MutableCFOptions moptions(cf_options);
    CompressionOptions compression_opts;
    compression_opts.parallel_threads = compression_parallel_threads;
    // Enable compression dictionary and set a buffering limit that is the same
    // as each block's size.
    compression_opts.max_dict_bytes = compression_dict_bytes;
    compression_opts.max_dict_buffer_bytes = compression_dict_bytes;
    InternalTblPropCollFactories factories;
    const ReadOptions read_options;
    const WriteOptions write_options;
    std::unique_ptr<TableBuilder> table_builder(
        options_.table_factory->NewTableBuilder(
            TableBuilderOptions(ioptions, moptions, read_options, write_options,
                                comparator, &factories, compression_type,
                                compression_opts, 0 /* column_family_id */,
                                kDefaultColumnFamilyName, -1 /* level */,
                                kUnknownNewestKeyTime),
            writer.get()));

    // Build table.
    for (auto it = kv.begin(); it != kv.end(); it++) {
      std::string v = it->second;
      table_builder->Add(it->first, v);
    }
    ASSERT_OK(table_builder->Finish());
  }

  void NewBlockBasedTableReader(const FileOptions& foptions,
                                const ImmutableOptions& ioptions,
                                const InternalKeyComparator& comparator,
                                const std::string& table_name,
                                std::unique_ptr<BlockBasedTable>* table,
                                bool prefetch_index_and_filter_in_cache = true,
                                Status* status = nullptr,
                                bool user_defined_timestamps_persisted = true) {
    const MutableCFOptions moptions(options_);
    TableReaderOptions table_reader_options = TableReaderOptions(
        ioptions, moptions.prefix_extractor, moptions.compression_manager.get(),
        foptions, comparator, 0 /* block_protection_bytes_per_key */,
        false /* _skip_filters */, false /* _immortal */,
        false /* _force_direct_prefetch */, -1 /* _level */,
        nullptr /* _block_cache_tracer */,
        0 /* _max_file_size_for_l0_meta_pin */, "" /* _cur_db_session_id */,
        table_num_++ /* _cur_file_num */, {} /* _unique_id */,
        0 /* _largest_seqno */, 0 /* _tail_size */,
        user_defined_timestamps_persisted);

    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(table_name, foptions, &file, ioptions.statistics.get());

    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(Path(table_name), &file_size));

    ReadOptions read_opts;
    read_opts.verify_checksums = true;
    std::unique_ptr<TableReader> general_table;
    Status s = options_.table_factory->NewTableReader(
        read_opts, table_reader_options, std::move(file), file_size,
        &general_table, prefetch_index_and_filter_in_cache);

    if (s.ok()) {
      table->reset(static_cast<BlockBasedTable*>(general_table.release()));
    }

    if (status) {
      *status = s;
    } else {
      ASSERT_OK(s);
    }
  }

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }

  std::string test_dir_;
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  Options options_;
  uint64_t table_num_{0};

 private:
  void WriteToFile(const std::string& content, const std::string& filename) {
    std::unique_ptr<FSWritableFile> f;
    ASSERT_OK(fs_->NewWritableFile(Path(filename), FileOptions(), &f, nullptr));
    ASSERT_OK(f->Append(content, IOOptions(), nullptr));
    ASSERT_OK(f->Close(IOOptions(), nullptr));
  }

  void NewFileWriter(const std::string& filename,
                     std::unique_ptr<WritableFileWriter>* writer) {
    std::string path = Path(filename);
    EnvOptions env_options;
    FileOptions foptions;
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(fs_->NewWritableFile(path, foptions, &file, nullptr));
    writer->reset(new WritableFileWriter(std::move(file), path, env_options));
  }

  void NewFileReader(const std::string& filename, const FileOptions& opt,
                     std::unique_ptr<RandomAccessFileReader>* reader,
                     Statistics* stats = nullptr) {
    std::string path = Path(filename);
    std::unique_ptr<FSRandomAccessFile> f;
    ASSERT_OK(fs_->NewRandomAccessFile(path, opt, &f, nullptr));
    reader->reset(new RandomAccessFileReader(std::move(f), path,
                                             env_->GetSystemClock().get(),
                                             /*io_tracer=*/nullptr,
                                             /*stats=*/stats));
  }
};

// Param 1: compression type
// Param 2: whether to use direct reads
// Param 3: Block Based Table Index type
// Param 4: BBTO no_block_cache option
// Param 5: test mode for the user-defined timestamp feature
// Param 6: number of parallel compression threads
// Param 7: CompressionOptions.max_dict_bytes and
//          CompressionOptions.max_dict_buffer_bytes to enable/disable
//          compression dictionary.
// Param 8: test mode to specify the pattern for generating key / value. When
//          true, generate keys with the same user provided key, different
//          user-defined timestamps (if udt enabled), different sequence
//          numbers. This test mode is used for testing `Get`. When false,
//          generate keys with different user provided key, same user-defined
//          timestamps (if udt enabled), same sequence number. This test mode is
//          used for testing `Get`, `MultiGet`, and `NewIterator`.
// Param 9: test both the default comparator and a reverse comparator.
class BlockBasedTableReaderTest
    : public BlockBasedTableReaderBaseTest,
      public testing::WithParamInterface<
          std::tuple<CompressionType, bool, BlockBasedTableOptions::IndexType,
                     bool, test::UserDefinedTimestampTestMode, uint32_t,
                     uint32_t, bool, const Comparator*>> {
 protected:
  void SetUp() override {
    compression_type_ = std::get<0>(GetParam());
    use_direct_reads_ = std::get<1>(GetParam());
    test::UserDefinedTimestampTestMode udt_test_mode = std::get<4>(GetParam());
    udt_enabled_ = test::IsUDTEnabled(udt_test_mode);
    persist_udt_ = test::ShouldPersistUDT(udt_test_mode);
    compression_parallel_threads_ = std::get<5>(GetParam());
    compression_dict_bytes_ = std::get<6>(GetParam());
    same_key_diff_ts_ = std::get<7>(GetParam());
    comparator_ = std::get<8>(GetParam());
    BlockBasedTableReaderBaseTest::SetUp();
  }

  void ConfigureTableFactory() override {
    BlockBasedTableOptions opts;
    opts.index_type = std::get<2>(GetParam());
    opts.no_block_cache = std::get<3>(GetParam());
    opts.filter_policy.reset(NewBloomFilterPolicy(10, false));
    opts.partition_filters =
        opts.index_type ==
        BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    opts.metadata_cache_options.partition_pinning = PinningTier::kAll;
    options_.table_factory.reset(
        static_cast<BlockBasedTableFactory*>(NewBlockBasedTableFactory(opts)));
    options_.prefix_extractor =
        std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(3));
  }

  CompressionType compression_type_;
  bool use_direct_reads_;
  bool udt_enabled_;
  bool persist_udt_;
  uint32_t compression_parallel_threads_;
  uint32_t compression_dict_bytes_;
  bool same_key_diff_ts_;
  const Comparator* comparator_{};
};

class BlockBasedTableReaderGetTest : public BlockBasedTableReaderTest {};

TEST_P(BlockBasedTableReaderGetTest, Get) {
  Options options;
  if (udt_enabled_) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  }
  options.persist_user_defined_timestamps = persist_udt_;
  size_t ts_sz = options.comparator->timestamp_size();
  std::vector<std::pair<std::string, std::string>> kv =
      BlockBasedTableReaderBaseTest::GenerateKVMap(
          100 /* num_block */,
          true /* mixed_with_human_readable_string_value */, ts_sz,
          same_key_diff_ts_);

  std::string table_name = "BlockBasedTableReaderGetTest_Get" +
                           CompressionTypeToString(compression_type_);

  ImmutableOptions ioptions(options);
  CreateTable(table_name, ioptions, compression_type_, kv,
              compression_parallel_threads_, compression_dict_bytes_);

  std::unique_ptr<BlockBasedTable> table;
  FileOptions foptions;
  foptions.use_direct_reads = use_direct_reads_;
  InternalKeyComparator comparator(options.comparator);
  NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &table,
                           true /* prefetch_index_and_filter_in_cache */,
                           nullptr /* status */, persist_udt_);

  ReadOptions read_opts;
  ASSERT_OK(
      table->VerifyChecksum(read_opts, TableReaderCaller::kUserVerifyChecksum));

  for (size_t i = 0; i < kv.size(); i += 1) {
    Slice key = kv[i].first;
    Slice lkey = key;
    std::string lookup_ikey;
    if (udt_enabled_ && !persist_udt_) {
      // When user-defined timestamps are collapsed to be the minimum timestamp,
      // we also read with the minimum timestamp to be able to retrieve each
      // value.
      ReplaceInternalKeyWithMinTimestamp(&lookup_ikey, key, ts_sz);
      lkey = lookup_ikey;
    }
    // Reading the first entry in a block caches the whole block.
    if (i % kEntriesPerBlock == 0) {
      ASSERT_FALSE(table->TEST_KeyInCache(read_opts, lkey.ToString()));
    } else {
      ASSERT_TRUE(table->TEST_KeyInCache(read_opts, lkey.ToString()));
    }
    PinnableSlice value;
    GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, ExtractUserKey(key), &value,
                           nullptr, nullptr, nullptr, nullptr,
                           true /* do_merge */, nullptr, nullptr, nullptr,
                           nullptr, nullptr, nullptr);
    ASSERT_OK(table->Get(read_opts, lkey, &get_context, nullptr));
    ASSERT_EQ(value.ToString(), kv[i].second);
    ASSERT_TRUE(table->TEST_KeyInCache(read_opts, lkey.ToString()));
  }
}

// Tests MultiGet in both direct IO and non-direct IO mode.
// The keys should be in cache after MultiGet.
TEST_P(BlockBasedTableReaderTest, MultiGet) {
  Options options;
  ReadOptions read_opts;
  std::string dummy_ts(sizeof(uint64_t), '\0');
  Slice read_timestamp = dummy_ts;
  if (udt_enabled_) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    read_opts.timestamp = &read_timestamp;
  }
  options.persist_user_defined_timestamps = persist_udt_;
  size_t ts_sz = options.comparator->timestamp_size();
  std::vector<std::pair<std::string, std::string>> kv =
      BlockBasedTableReaderBaseTest::GenerateKVMap(
          100 /* num_block */,
          true /* mixed_with_human_readable_string_value */, ts_sz);

  // Prepare keys, values, and statuses for MultiGet.
  autovector<Slice, MultiGetContext::MAX_BATCH_SIZE> keys;
  autovector<Slice, MultiGetContext::MAX_BATCH_SIZE> keys_without_timestamps;
  autovector<PinnableSlice, MultiGetContext::MAX_BATCH_SIZE> values;
  autovector<Status, MultiGetContext::MAX_BATCH_SIZE> statuses;
  autovector<const std::string*, MultiGetContext::MAX_BATCH_SIZE>
      expected_values;
  {
    const int step =
        static_cast<int>(kv.size()) / MultiGetContext::MAX_BATCH_SIZE;
    auto it = kv.begin();
    for (int i = 0; i < MultiGetContext::MAX_BATCH_SIZE; i++) {
      keys.emplace_back(it->first);
      if (ts_sz > 0) {
        Slice ukey_without_ts =
            ExtractUserKeyAndStripTimestamp(it->first, ts_sz);
        keys_without_timestamps.push_back(ukey_without_ts);
      } else {
        keys_without_timestamps.emplace_back(ExtractUserKey(it->first));
      }
      values.emplace_back();
      statuses.emplace_back();
      expected_values.push_back(&(it->second));
      std::advance(it, step);
    }
  }

  std::string table_name = "BlockBasedTableReaderTest_MultiGet" +
                           CompressionTypeToString(compression_type_);

  ImmutableOptions ioptions(options);
  CreateTable(table_name, ioptions, compression_type_, kv,
              compression_parallel_threads_, compression_dict_bytes_);

  std::unique_ptr<BlockBasedTable> table;
  FileOptions foptions;
  foptions.use_direct_reads = use_direct_reads_;
  InternalKeyComparator comparator(options.comparator);
  NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &table,
                           true /* bool prefetch_index_and_filter_in_cache */,
                           nullptr /* status */, persist_udt_);

  ASSERT_OK(
      table->VerifyChecksum(read_opts, TableReaderCaller::kUserVerifyChecksum));

  // Ensure that keys are not in cache before MultiGet.
  for (auto& key : keys) {
    ASSERT_FALSE(table->TEST_KeyInCache(read_opts, key.ToString()));
  }

  // Prepare MultiGetContext.
  autovector<GetContext, MultiGetContext::MAX_BATCH_SIZE> get_context;
  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  for (size_t i = 0; i < keys.size(); ++i) {
    get_context.emplace_back(options.comparator, nullptr, nullptr, nullptr,
                             GetContext::kNotFound, ExtractUserKey(keys[i]),
                             &values[i], nullptr, nullptr, nullptr, nullptr,
                             true /* do_merge */, nullptr, nullptr, nullptr,
                             nullptr, nullptr, nullptr);
    key_context.emplace_back(nullptr, keys_without_timestamps[i], &values[i],
                             nullptr, nullptr, &statuses.back());
    key_context.back().get_context = &get_context.back();
  }
  for (auto& key_ctx : key_context) {
    sorted_keys.emplace_back(&key_ctx);
  }
  MultiGetContext ctx(&sorted_keys, 0, sorted_keys.size(), 0, read_opts,
                      fs_.get(), nullptr);

  // Execute MultiGet.
  MultiGetContext::Range range = ctx.GetMultiGetRange();
  PerfContext* perf_ctx = get_perf_context();
  perf_ctx->Reset();
  table->MultiGet(read_opts, &range, nullptr);

  ASSERT_GE(perf_ctx->block_read_count - perf_ctx->index_block_read_count -
                perf_ctx->filter_block_read_count -
                perf_ctx->compression_dict_block_read_count,
            1);
  ASSERT_GE(perf_ctx->block_read_byte, 1);

  for (const Status& status : statuses) {
    ASSERT_OK(status);
  }
  // Check that keys are in cache after MultiGet.
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(table->TEST_KeyInCache(read_opts, keys[i]));
    ASSERT_EQ(values[i].ToString(), *expected_values[i]);
  }
}

TEST_P(BlockBasedTableReaderTest, NewIterator) {
  Options options;
  ReadOptions read_opts;
  std::string dummy_ts(sizeof(uint64_t), '\0');
  Slice read_timestamp = dummy_ts;
  if (udt_enabled_) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    read_opts.timestamp = &read_timestamp;
  }
  options.persist_user_defined_timestamps = persist_udt_;
  size_t ts_sz = options.comparator->timestamp_size();
  std::vector<std::pair<std::string, std::string>> kv =
      BlockBasedTableReaderBaseTest::GenerateKVMap(
          100 /* num_block */,
          true /* mixed_with_human_readable_string_value */, ts_sz);

  std::string table_name = "BlockBasedTableReaderTest_NewIterator" +
                           CompressionTypeToString(compression_type_);

  ImmutableOptions ioptions(options);
  CreateTable(table_name, ioptions, compression_type_, kv,
              compression_parallel_threads_, compression_dict_bytes_);

  std::unique_ptr<BlockBasedTable> table;
  FileOptions foptions;
  foptions.use_direct_reads = use_direct_reads_;
  InternalKeyComparator comparator(options.comparator);
  NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &table,
                           true /* bool prefetch_index_and_filter_in_cache */,
                           nullptr /* status */, persist_udt_);
  ASSERT_OK(
      table->VerifyChecksum(read_opts, TableReaderCaller::kUserVerifyChecksum));

  std::unique_ptr<InternalIterator> iter;
  iter.reset(table->NewIterator(
      read_opts, options_.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  // Test forward scan.
  ASSERT_TRUE(!iter->Valid());
  iter->SeekToFirst();
  ASSERT_OK(iter->status());
  for (auto kv_iter = kv.begin(); kv_iter != kv.end(); kv_iter++) {
    ASSERT_EQ(iter->key().ToString(), kv_iter->first);
    ASSERT_EQ(iter->value().ToString(), kv_iter->second);
    iter->Next();
    ASSERT_OK(iter->status());
  }
  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());

  // Test backward scan.
  iter->SeekToLast();
  ASSERT_OK(iter->status());
  for (auto kv_iter = kv.rbegin(); kv_iter != kv.rend(); kv_iter++) {
    ASSERT_EQ(iter->key().ToString(), kv_iter->first);
    ASSERT_EQ(iter->value().ToString(), kv_iter->second);
    iter->Prev();
    ASSERT_OK(iter->status());
  }
  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());
}

class ChargeTableReaderTest
    : public BlockBasedTableReaderBaseTest,
      public testing::WithParamInterface<
          CacheEntryRoleOptions::Decision /* charge_table_reader_mem */> {
 protected:
  static std::size_t CalculateMaxTableReaderNumBeforeCacheFull(
      std::size_t cache_capacity, std::size_t approx_table_reader_mem) {
    // To make calculation easier for testing
    assert(cache_capacity % CacheReservationManagerImpl<
                                CacheEntryRole::kBlockBasedTableReader>::
                                GetDummyEntrySize() ==
               0 &&
           cache_capacity >= 2 * CacheReservationManagerImpl<
                                     CacheEntryRole::kBlockBasedTableReader>::
                                     GetDummyEntrySize());

    // We need to subtract 1 for max_num_dummy_entry to account for dummy
    // entries' overhead, assumed the overhead is no greater than 1 dummy entry
    // size
    std::size_t max_num_dummy_entry =
        (size_t)std::floor((
            1.0 * cache_capacity /
            CacheReservationManagerImpl<
                CacheEntryRole::kBlockBasedTableReader>::GetDummyEntrySize())) -
        1;
    std::size_t cache_capacity_rounded_to_dummy_entry_multiples =
        max_num_dummy_entry *
        CacheReservationManagerImpl<
            CacheEntryRole::kBlockBasedTableReader>::GetDummyEntrySize();
    std::size_t max_table_reader_num_capped = static_cast<std::size_t>(
        std::floor(1.0 * cache_capacity_rounded_to_dummy_entry_multiples /
                   approx_table_reader_mem));

    return max_table_reader_num_capped;
  }

  void SetUp() override {
    // To cache and re-use the same kv map and compression type in the test
    // suite for elimiating variance caused by these two factors
    kv_ = BlockBasedTableReaderBaseTest::GenerateKVMap();
    compression_type_ = CompressionType::kNoCompression;

    table_reader_charge_tracking_cache_ = std::make_shared<
        TargetCacheChargeTrackingCache<
            CacheEntryRole::kBlockBasedTableReader>>((NewLRUCache(
        4 * CacheReservationManagerImpl<
                CacheEntryRole::kBlockBasedTableReader>::GetDummyEntrySize(),
        0 /* num_shard_bits */, true /* strict_capacity_limit */)));

    // To ApproximateTableReaderMem() without being affected by
    // the feature of charging its memory, we turn off the feature
    charge_table_reader_ = CacheEntryRoleOptions::Decision::kDisabled;
    BlockBasedTableReaderBaseTest::SetUp();
    approx_table_reader_mem_ = ApproximateTableReaderMem();

    // Now we condtionally turn on the feature to test
    charge_table_reader_ = GetParam();
    ConfigureTableFactory();
  }

  void ConfigureTableFactory() override {
    BlockBasedTableOptions table_options;
    table_options.cache_usage_options.options_overrides.insert(
        {CacheEntryRole::kBlockBasedTableReader,
         {/*.charged = */ charge_table_reader_}});
    table_options.block_cache = table_reader_charge_tracking_cache_;

    table_options.cache_index_and_filter_blocks = false;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    table_options.partition_filters = true;
    table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;

    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }

  CacheEntryRoleOptions::Decision charge_table_reader_;
  std::shared_ptr<
      TargetCacheChargeTrackingCache<CacheEntryRole::kBlockBasedTableReader>>
      table_reader_charge_tracking_cache_;
  std::size_t approx_table_reader_mem_;
  std::vector<std::pair<std::string, std::string>> kv_;
  CompressionType compression_type_;

 private:
  std::size_t ApproximateTableReaderMem() {
    std::size_t approx_table_reader_mem = 0;

    std::string table_name = "table_for_approx_table_reader_mem";
    ImmutableOptions ioptions(options_);
    CreateTable(table_name, ioptions, compression_type_, kv_);

    std::unique_ptr<BlockBasedTable> table;
    Status s;
    NewBlockBasedTableReader(
        FileOptions(), ImmutableOptions(options_),
        InternalKeyComparator(options_.comparator), table_name, &table,
        false /* prefetch_index_and_filter_in_cache */, &s);
    assert(s.ok());

    approx_table_reader_mem = table->ApproximateMemoryUsage();
    assert(approx_table_reader_mem > 0);
    return approx_table_reader_mem;
  }
};

INSTANTIATE_TEST_CASE_P(
    ChargeTableReaderTest, ChargeTableReaderTest,
    ::testing::Values(CacheEntryRoleOptions::Decision::kEnabled,
                      CacheEntryRoleOptions::Decision::kDisabled));

TEST_P(ChargeTableReaderTest, Basic) {
  const std::size_t max_table_reader_num_capped =
      ChargeTableReaderTest::CalculateMaxTableReaderNumBeforeCacheFull(
          table_reader_charge_tracking_cache_->GetCapacity(),
          approx_table_reader_mem_);

  // Acceptable estimtation errors coming from
  // 1. overstimate max_table_reader_num_capped due to # dummy entries is high
  // and results in metadata charge overhead greater than 1 dummy entry size
  // (violating our assumption in calculating max_table_reader_num_capped)
  // 2. overestimate/underestimate max_table_reader_num_capped due to the gap
  // between ApproximateTableReaderMem() and actual table reader mem
  std::size_t max_table_reader_num_capped_upper_bound =
      (std::size_t)(max_table_reader_num_capped * 1.05);
  std::size_t max_table_reader_num_capped_lower_bound =
      (std::size_t)(max_table_reader_num_capped * 0.95);
  std::size_t max_table_reader_num_uncapped =
      (std::size_t)(max_table_reader_num_capped * 1.1);
  ASSERT_GT(max_table_reader_num_uncapped,
            max_table_reader_num_capped_upper_bound)
      << "We need `max_table_reader_num_uncapped` > "
         "`max_table_reader_num_capped_upper_bound` to differentiate cases "
         "between "
         "charge_table_reader_ == kDisabled and == kEnabled)";

  Status s = Status::OK();
  std::size_t opened_table_reader_num = 0;
  std::string table_name;
  std::vector<std::unique_ptr<BlockBasedTable>> tables;
  ImmutableOptions ioptions(options_);
  // Keep creating BlockBasedTableReader till hiting the memory limit based on
  // cache capacity and creation fails (when charge_table_reader_ ==
  // kEnabled) or reaching a specfied big number of table readers (when
  // charge_table_reader_ == kDisabled)
  while (s.ok() && opened_table_reader_num < max_table_reader_num_uncapped) {
    table_name = "table_" + std::to_string(opened_table_reader_num);
    CreateTable(table_name, ioptions, compression_type_, kv_);
    tables.push_back(std::unique_ptr<BlockBasedTable>());
    NewBlockBasedTableReader(
        FileOptions(), ImmutableOptions(options_),
        InternalKeyComparator(options_.comparator), table_name, &tables.back(),
        false /* prefetch_index_and_filter_in_cache */, &s);
    if (s.ok()) {
      ++opened_table_reader_num;
    }
  }

  if (charge_table_reader_ == CacheEntryRoleOptions::Decision::kEnabled) {
    EXPECT_TRUE(s.IsMemoryLimit()) << "s: " << s.ToString();
    EXPECT_TRUE(s.ToString().find(
                    kCacheEntryRoleToCamelString[static_cast<std::uint32_t>(
                        CacheEntryRole::kBlockBasedTableReader)]) !=
                std::string::npos);
    EXPECT_TRUE(s.ToString().find("memory limit based on cache capacity") !=
                std::string::npos);

    EXPECT_GE(opened_table_reader_num, max_table_reader_num_capped_lower_bound);
    EXPECT_LE(opened_table_reader_num, max_table_reader_num_capped_upper_bound);

    std::size_t updated_max_table_reader_num_capped =
        ChargeTableReaderTest::CalculateMaxTableReaderNumBeforeCacheFull(
            table_reader_charge_tracking_cache_->GetCapacity() / 2,
            approx_table_reader_mem_);

    // Keep deleting BlockBasedTableReader to lower down memory usage from the
    // memory limit to make the next creation succeeds
    while (opened_table_reader_num >= updated_max_table_reader_num_capped) {
      tables.pop_back();
      --opened_table_reader_num;
    }
    table_name = "table_for_successful_table_reader_open";
    CreateTable(table_name, ioptions, compression_type_, kv_);
    tables.push_back(std::unique_ptr<BlockBasedTable>());
    NewBlockBasedTableReader(
        FileOptions(), ImmutableOptions(options_),
        InternalKeyComparator(options_.comparator), table_name, &tables.back(),
        false /* prefetch_index_and_filter_in_cache */, &s);
    EXPECT_TRUE(s.ok()) << s.ToString();

    tables.clear();
    EXPECT_EQ(table_reader_charge_tracking_cache_->GetCacheCharge(), 0);
  } else {
    EXPECT_TRUE(s.ok() &&
                opened_table_reader_num == max_table_reader_num_uncapped)
        << "s: " << s.ToString() << " opened_table_reader_num: "
        << std::to_string(opened_table_reader_num);
    EXPECT_EQ(table_reader_charge_tracking_cache_->GetCacheCharge(), 0);
  }
}

class StrictCapacityLimitReaderTest : public BlockBasedTableReaderTest {
 public:
  StrictCapacityLimitReaderTest() : BlockBasedTableReaderTest() {}

 protected:
  void ConfigureTableFactory() override {
    BlockBasedTableOptions table_options;

    table_options.block_cache = std::make_shared<
        TargetCacheChargeTrackingCache<CacheEntryRole::kBlockBasedTableReader>>(
        (NewLRUCache(4 * 1024, 0 /* num_shard_bits */,
                     true /* strict_capacity_limit */)));

    table_options.cache_index_and_filter_blocks = false;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    table_options.partition_filters = true;
    table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;

    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }
};

TEST_P(StrictCapacityLimitReaderTest, Get) {
  // Test that we get error status when we exceed
  // the strict_capacity_limit
  Options options;
  size_t ts_sz = options.comparator->timestamp_size();
  std::vector<std::pair<std::string, std::string>> kv =
      BlockBasedTableReaderBaseTest::GenerateKVMap(
          2 /* num_block */, true /* mixed_with_human_readable_string_value */,
          ts_sz, false);

  std::string table_name = "StrictCapacityLimitReaderTest_Get" +
                           CompressionTypeToString(compression_type_);

  ImmutableOptions ioptions(options);
  CreateTable(table_name, ioptions, compression_type_, kv);

  std::unique_ptr<BlockBasedTable> table;
  FileOptions foptions;
  foptions.use_direct_reads = true;
  InternalKeyComparator comparator(options.comparator);
  NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &table,
                           true /* prefetch_index_and_filter_in_cache */,
                           nullptr /* status */);

  ReadOptions read_opts;
  ASSERT_OK(
      table->VerifyChecksum(read_opts, TableReaderCaller::kUserVerifyChecksum));

  bool hit_memory_limit = false;
  for (size_t i = 0; i < kv.size(); i += 1) {
    Slice key = kv[i].first;
    Slice lkey = key;
    std::string lookup_ikey;
    // Reading the first entry in a block caches the whole block.
    if (i % kEntriesPerBlock == 0) {
      ASSERT_FALSE(table->TEST_KeyInCache(read_opts, lkey.ToString()));
    } else if (!hit_memory_limit) {
      ASSERT_TRUE(table->TEST_KeyInCache(read_opts, lkey.ToString()));
    }
    PinnableSlice value;
    GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, ExtractUserKey(key), &value,
                           nullptr, nullptr, nullptr, nullptr,
                           true /* do_merge */, nullptr, nullptr, nullptr,
                           nullptr, nullptr, nullptr);
    Status s = table->Get(read_opts, lkey, &get_context, nullptr);
    if (!s.ok()) {
      EXPECT_TRUE(s.IsMemoryLimit());
      EXPECT_TRUE(s.ToString().find("Memory limit reached: Insert failed due "
                                    "to LRU cache being full") !=
                  std::string::npos);
      hit_memory_limit = true;
    } else {
      ASSERT_EQ(value.ToString(), kv[i].second);
      ASSERT_TRUE(table->TEST_KeyInCache(read_opts, lkey.ToString()));
    }
  }

  ASSERT_TRUE(hit_memory_limit);
}

TEST_P(StrictCapacityLimitReaderTest, MultiGet) {
  // Test that we get error status when we exceed
  // the strict_capacity_limit
  Options options;
  ReadOptions read_opts;
  std::string dummy_ts(sizeof(uint64_t), '\0');
  Slice read_timestamp = dummy_ts;
  if (udt_enabled_) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    read_opts.timestamp = &read_timestamp;
  }
  options.persist_user_defined_timestamps = persist_udt_;
  size_t ts_sz = options.comparator->timestamp_size();
  std::vector<std::pair<std::string, std::string>> kv =
      BlockBasedTableReaderBaseTest::GenerateKVMap(
          2 /* num_block */, true /* mixed_with_human_readable_string_value */,
          ts_sz);

  // Prepare keys, values, and statuses for MultiGet.
  autovector<Slice, MultiGetContext::MAX_BATCH_SIZE> keys;
  autovector<Slice, MultiGetContext::MAX_BATCH_SIZE> keys_without_timestamps;
  autovector<PinnableSlice, MultiGetContext::MAX_BATCH_SIZE> values;
  autovector<Status, MultiGetContext::MAX_BATCH_SIZE> statuses;
  autovector<const std::string*, MultiGetContext::MAX_BATCH_SIZE>
      expected_values;
  {
    const int step =
        static_cast<int>(kv.size()) / MultiGetContext::MAX_BATCH_SIZE;
    auto it = kv.begin();
    for (int i = 0; i < MultiGetContext::MAX_BATCH_SIZE; i++) {
      keys.emplace_back(it->first);
      if (ts_sz > 0) {
        Slice ukey_without_ts =
            ExtractUserKeyAndStripTimestamp(it->first, ts_sz);
        keys_without_timestamps.push_back(ukey_without_ts);
      } else {
        keys_without_timestamps.emplace_back(ExtractUserKey(it->first));
      }
      values.emplace_back();
      statuses.emplace_back();
      expected_values.push_back(&(it->second));
      std::advance(it, step);
    }
  }

  std::string table_name = "StrictCapacityLimitReaderTest_MultiGet" +
                           CompressionTypeToString(compression_type_);

  ImmutableOptions ioptions(options);
  CreateTable(table_name, ioptions, compression_type_, kv,
              compression_parallel_threads_, compression_dict_bytes_);

  std::unique_ptr<BlockBasedTable> table;
  FileOptions foptions;
  foptions.use_direct_reads = use_direct_reads_;
  InternalKeyComparator comparator(options.comparator);
  NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &table,
                           true /* bool prefetch_index_and_filter_in_cache */,
                           nullptr /* status */, persist_udt_);

  ASSERT_OK(
      table->VerifyChecksum(read_opts, TableReaderCaller::kUserVerifyChecksum));

  // Ensure that keys are not in cache before MultiGet.
  for (auto& key : keys) {
    ASSERT_FALSE(table->TEST_KeyInCache(read_opts, key.ToString()));
  }

  // Prepare MultiGetContext.
  autovector<GetContext, MultiGetContext::MAX_BATCH_SIZE> get_context;
  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  for (size_t i = 0; i < keys.size(); ++i) {
    get_context.emplace_back(options.comparator, nullptr, nullptr, nullptr,
                             GetContext::kNotFound, ExtractUserKey(keys[i]),
                             &values[i], nullptr, nullptr, nullptr, nullptr,
                             true /* do_merge */, nullptr, nullptr, nullptr,
                             nullptr, nullptr, nullptr);
    key_context.emplace_back(nullptr, keys_without_timestamps[i], &values[i],
                             nullptr, nullptr, &statuses.back());
    key_context.back().get_context = &get_context.back();
  }
  for (auto& key_ctx : key_context) {
    sorted_keys.emplace_back(&key_ctx);
  }
  MultiGetContext ctx(&sorted_keys, 0, sorted_keys.size(), 0, read_opts,
                      fs_.get(), nullptr);

  // Execute MultiGet.
  MultiGetContext::Range range = ctx.GetMultiGetRange();
  PerfContext* perf_ctx = get_perf_context();
  perf_ctx->Reset();
  table->MultiGet(read_opts, &range, nullptr);

  ASSERT_GE(perf_ctx->block_read_count - perf_ctx->index_block_read_count -
                perf_ctx->filter_block_read_count -
                perf_ctx->compression_dict_block_read_count,
            1);
  ASSERT_GE(perf_ctx->block_read_byte, 1);

  bool hit_memory_limit = false;
  for (const Status& status : statuses) {
    if (!status.ok()) {
      EXPECT_TRUE(status.IsMemoryLimit());
      hit_memory_limit = true;
    }
  }
  ASSERT_TRUE(hit_memory_limit);
}

class BlockBasedTableReaderTestVerifyChecksum
    : public BlockBasedTableReaderTest {
 public:
  BlockBasedTableReaderTestVerifyChecksum() : BlockBasedTableReaderTest() {}
};

TEST_P(BlockBasedTableReaderTestVerifyChecksum, ChecksumMismatch) {
  Options options;
  ReadOptions read_opts;
  std::string dummy_ts(sizeof(uint64_t), '\0');
  Slice read_timestamp = dummy_ts;
  if (udt_enabled_) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    read_opts.timestamp = &read_timestamp;
  }
  options.persist_user_defined_timestamps = persist_udt_;
  size_t ts_sz = options.comparator->timestamp_size();
  std::vector<std::pair<std::string, std::string>> kv =
      BlockBasedTableReaderBaseTest::GenerateKVMap(
          800 /* num_block */,
          false /* mixed_with_human_readable_string_value=*/, ts_sz);

  options.statistics = CreateDBStatistics();
  ImmutableOptions ioptions(options);
  std::string table_name =
      "BlockBasedTableReaderTest" + CompressionTypeToString(compression_type_);
  CreateTable(table_name, ioptions, compression_type_, kv,
              compression_parallel_threads_, compression_dict_bytes_);

  std::unique_ptr<BlockBasedTable> table;
  FileOptions foptions;
  foptions.use_direct_reads = use_direct_reads_;
  InternalKeyComparator comparator(options.comparator);
  NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &table,
                           true /* bool prefetch_index_and_filter_in_cache */,
                           nullptr /* status */, persist_udt_);

  // Use the top level iterator to find the offset/size of the first
  // 2nd level index block and corrupt the block
  IndexBlockIter iiter_on_stack;
  BlockCacheLookupContext context{TableReaderCaller::kUserVerifyChecksum};
  InternalIteratorBase<IndexValue>* iiter = table->NewIndexIterator(
      read_opts, /*need_upper_bound_check=*/false, &iiter_on_stack,
      /*get_context=*/nullptr, &context);
  std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr = std::unique_ptr<InternalIteratorBase<IndexValue>>(iiter);
  }
  ASSERT_OK(iiter->status());
  iiter->SeekToFirst();
  BlockHandle handle = static_cast<PartitionedIndexIterator*>(iiter)
                           ->index_iter_->value()
                           .handle;
  table.reset();

  // Corrupt the block pointed to by handle
  ASSERT_OK(test::CorruptFile(options.env, Path(table_name),
                              static_cast<int>(handle.offset()), 128));

  NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &table,
                           true /* bool prefetch_index_and_filter_in_cache */,
                           nullptr /* status */, persist_udt_);
  ASSERT_EQ(0,
            options.statistics->getTickerCount(BLOCK_CHECKSUM_MISMATCH_COUNT));
  Status s =
      table->VerifyChecksum(read_opts, TableReaderCaller::kUserVerifyChecksum);
  ASSERT_EQ(1,
            options.statistics->getTickerCount(BLOCK_CHECKSUM_MISMATCH_COUNT));
  ASSERT_EQ(s.code(), Status::kCorruption);
}

// TODO: test no block cache case
TEST_P(BlockBasedTableReaderTest, MultiScanPrepare) {
  std::ostringstream param_trace;
  param_trace << "[MultiScanPrepare] Test params: " << "CompressionType="
              << CompressionTypeToString(compression_type_)
              << ", UseDirectReads=" << (use_direct_reads_ ? "true" : "false")
              << ", UDTEnabled=" << (udt_enabled_ ? "true" : "false")
              << ", PersistUDT=" << (persist_udt_ ? "true" : "false")
              << ", CompressionParallelThreads="
              << compression_parallel_threads_
              << ", CompressionDictBytes=" << compression_dict_bytes_
              << ", SameKeyDiffTs=" << (same_key_diff_ts_ ? "true" : "false");
  SCOPED_TRACE(param_trace.str());

  for (bool fill_cache : {false, true}) {
    SCOPED_TRACE(std::string("fill_cache=") + std::to_string(fill_cache));
    for (bool use_async_io : {false,
#ifdef ROCKSDB_IOURING_PRESENT
                              true
#endif
         }) {
      SCOPED_TRACE(std::string("use_async_io=") + std::to_string(use_async_io));
      Options options;
      options.statistics = CreateDBStatistics();
      options.comparator = comparator_;
      std::shared_ptr<FileSystem> fs = options.env->GetFileSystem();
      ReadOptions read_opts;
      read_opts.fill_cache = fill_cache;
      size_t ts_sz = options.comparator->timestamp_size();
      std::vector<std::pair<std::string, std::string>> kv =
          BlockBasedTableReaderBaseTest::GenerateKVMap(
              100 /* num_block */,
              true /* mixed_with_human_readable_string_value */, ts_sz,
              same_key_diff_ts_, comparator_);
      std::string table_name = "BlockBasedTableReaderTest_NewIterator" +
                               CompressionTypeToString(compression_type_) +
                               "_async" + std::to_string(use_async_io);
      ImmutableOptions ioptions(options);
      CreateTable(table_name, ioptions, compression_type_, kv,
                  compression_parallel_threads_, compression_dict_bytes_);

      std::unique_ptr<BlockBasedTable> table;
      FileOptions foptions;
      foptions.use_direct_reads = use_direct_reads_;
      InternalKeyComparator comparator(options.comparator);
      NewBlockBasedTableReader(
          foptions, ioptions, comparator, table_name, &table,
          true /* bool prefetch_index_and_filter_in_cache */,
          nullptr /* status */, persist_udt_);

      // 1. Should coalesce into a single I/O
      std::unique_ptr<InternalIterator> iter;
      iter.reset(table->NewIterator(
          read_opts, options_.prefix_extractor.get(), /*arena=*/nullptr,
          /*skip_filters=*/false, TableReaderCaller::kUncategorized));

      MultiScanArgs scan_options(comparator_);
      scan_options.use_async_io = use_async_io;
      scan_options.insert(ExtractUserKey(kv[0].first),
                          ExtractUserKey(kv[kEntriesPerBlock].first));
      scan_options.insert(ExtractUserKey(kv[2 * kEntriesPerBlock].first),
                          ExtractUserKey(kv[3 * kEntriesPerBlock].first));
      auto read_count_before =
          options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT);

      iter->Prepare(&scan_options);
      iter->Seek(kv[0].first);
      for (size_t i = 0; i < kEntriesPerBlock + 1; ++i) {
        ASSERT_TRUE(iter->status().ok()) << iter->status().ToString();
        ASSERT_TRUE(iter->Valid()) << i;
        ASSERT_EQ(iter->key().ToString(), kv[i].first);
        iter->Next();
      }
      // Iter may still be valid after scan range. Upper layer (DBIter) handles
      // exact upper bound checking. So we don't check !iter->Valid() here.
      ASSERT_OK(iter->status());
      iter->Seek(kv[2 * kEntriesPerBlock].first);
      for (size_t i = 2 * kEntriesPerBlock; i < 3 * kEntriesPerBlock; ++i) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key().ToString(), kv[i].first);
        iter->Next();
      }
      ASSERT_OK(iter->status());
      auto read_count_after =
          options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT);
      ASSERT_EQ(read_count_before + 1, read_count_after);

      // 2. No IO coalesce, should do MultiRead/ReadAsync with 2 read requests.
      iter.reset(table->NewIterator(
          read_opts, options_.prefix_extractor.get(), /*arena=*/nullptr,
          /*skip_filters=*/false, TableReaderCaller::kUncategorized));
      scan_options = MultiScanArgs(comparator_);
      scan_options.insert(ExtractUserKey(kv[70 * kEntriesPerBlock].first),
                          ExtractUserKey(kv[75 * kEntriesPerBlock].first));
      scan_options.insert(ExtractUserKey(kv[90 * kEntriesPerBlock].first),
                          ExtractUserKey(kv[95 * kEntriesPerBlock].first));

      read_count_before =
          options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT);
      iter->Prepare(&scan_options);

      iter->Seek(kv[70 * kEntriesPerBlock].first);
      for (size_t i = 70 * kEntriesPerBlock; i < 75 * kEntriesPerBlock; ++i) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key().ToString(), kv[i].first);
        iter->Next();
      }
      ASSERT_OK(iter->status());
      iter->Seek(kv[90 * kEntriesPerBlock].first);
      for (size_t i = 90 * kEntriesPerBlock; i < 95 * kEntriesPerBlock; ++i) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key().ToString(), kv[i].first);
        iter->Next();
      }
      ASSERT_OK(iter->status());

      read_count_after =
          options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT);
      ASSERT_EQ(read_count_before + 2, read_count_after);

      iter.reset(table->NewIterator(
          read_opts, options_.prefix_extractor.get(), /*arena=*/nullptr,
          /*skip_filters=*/false, TableReaderCaller::kUncategorized));

      // 3. Tests I/O excludes blocks already in cache.
      // Reading blocks from 50-99
      // From reads above, blocks 70-75 and 90-95 already in cache
      // So we should read 50-70 76-89 96-99 in three I/Os.
      // If fill_cache is false, then we'll do one giant I/O.
      scan_options = MultiScanArgs(comparator_);
      scan_options.use_async_io = use_async_io;
      scan_options.insert(ExtractUserKey(kv[50 * kEntriesPerBlock].first));
      read_count_before =
          options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT);
      iter->Prepare(&scan_options);
      read_count_after =
          options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT);
      if (!use_async_io) {
        if (!fill_cache) {
          ASSERT_EQ(read_count_before + 1, read_count_after);
        } else {
          ASSERT_EQ(read_count_before + 3, read_count_after);
        }
      } else {
        // stat is recorded in async callback which happens in Poll(), and
        // Poll() happens during scanning.
        ASSERT_EQ(read_count_before, read_count_after);
      }

      iter->Seek(kv[50 * kEntriesPerBlock].first);
      for (size_t i = 50 * kEntriesPerBlock; i < 100 * kEntriesPerBlock; ++i) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key().ToString(), kv[i].first);
        iter->Next();
      }
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
      read_count_after =
          options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT);
      if (!fill_cache) {
        ASSERT_EQ(read_count_before + 1, read_count_after);
      } else {
        ASSERT_EQ(read_count_before + 3, read_count_after);
      }

      // 4. Check cases when Seek key does not match start key in ScanOptions
      iter.reset(table->NewIterator(
          read_opts, options_.prefix_extractor.get(), /*arena=*/nullptr,
          /*skip_filters=*/false, TableReaderCaller::kUncategorized));
      scan_options = MultiScanArgs(comparator_);
      scan_options.use_async_io = use_async_io;
      scan_options.insert(ExtractUserKey(kv[10 * kEntriesPerBlock].first),
                          ExtractUserKey(kv[20 * kEntriesPerBlock].first));
      scan_options.insert(ExtractUserKey(kv[30 * kEntriesPerBlock].first),
                          ExtractUserKey(kv[40 * kEntriesPerBlock].first));
      iter->Prepare(&scan_options);
      // Match start key
      iter->Seek(kv[10 * kEntriesPerBlock].first);
      for (size_t i = 10 * kEntriesPerBlock; i < 20 * kEntriesPerBlock; ++i) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key().ToString(), kv[i].first);
        iter->Next();
      }
      ASSERT_OK(iter->status());

      // Does not match start key of the second ScanOptions.
      iter->Seek(kv[50 * kEntriesPerBlock + 1].first);
      for (size_t i = 50 * kEntriesPerBlock + 1; i < 100 * kEntriesPerBlock;
           ++i) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key().ToString(), kv[i].first);
        iter->Next();
      }
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());

      iter.reset(table->NewIterator(
          read_opts, options_.prefix_extractor.get(), /*arena=*/nullptr,
          /*skip_filters=*/false, TableReaderCaller::kUncategorized));
      scan_options = MultiScanArgs(comparator_);
      scan_options.use_async_io = use_async_io;
      scan_options.insert(ExtractUserKey(kv[10 * kEntriesPerBlock].first));
      scan_options.insert(ExtractUserKey(kv[11 * kEntriesPerBlock].first));
      iter->Prepare(&scan_options);
      // Does not match the first ScanOptions.
      iter->SeekToFirst();
      for (size_t i = 0; i < kEntriesPerBlock; ++i) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key().ToString(), kv[i].first);
        iter->Next();
      }
      ASSERT_OK(iter->status());
      iter->Seek(kv[10 * kEntriesPerBlock].first);
      for (size_t i = 10 * kEntriesPerBlock; i < 12 * kEntriesPerBlock; ++i) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key().ToString(), kv[i].first);
        iter->Next();
      }
      ASSERT_OK(iter->status());
    }
  }
}

TEST_P(BlockBasedTableReaderTest, MultiScanPrefetchSizeLimit) {
  if (compression_type_ != kNoCompression) {
    // This test relies on block sizes to be close to what's set in option.
    ROCKSDB_GTEST_BYPASS("This test assumes no compression.");
    return;
  }
  Options options;
  options.comparator = comparator_;
  ReadOptions read_opts;
  size_t ts_sz = options.comparator->timestamp_size();

  // Generate data that spans multiple blocks
  std::vector<std::pair<std::string, std::string>> kv =
      BlockBasedTableReaderBaseTest::GenerateKVMap(
          20 /* num_block */, true /* mixed_with_human_readable_string_value */,
          ts_sz, same_key_diff_ts_, comparator_);

  std::string table_name = "BlockBasedTableReaderTest_PrefetchSizeLimit" +
                           CompressionTypeToString(compression_type_);

  ImmutableOptions ioptions(options);
  CreateTable(table_name, ioptions, compression_type_, kv,
              compression_parallel_threads_, compression_dict_bytes_);

  std::unique_ptr<BlockBasedTable> table;
  FileOptions foptions;
  foptions.use_direct_reads = use_direct_reads_;
  InternalKeyComparator comparator(options.comparator);
  NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &table,
                           true /* bool prefetch_index_and_filter_in_cache */,
                           nullptr /* status */, persist_udt_);

  // Default block size is 4KB
  //
  // Tests when no block is loaded
  {
    std::unique_ptr<InternalIterator> iter;
    iter.reset(table->NewIterator(
        read_opts, options_.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));

    MultiScanArgs scan_options(comparator_);
    scan_options.max_prefetch_size = 1024;  // less than block size
    scan_options.insert(ExtractUserKey(kv[0].first),
                        ExtractUserKey(kv[5].first));

    iter->Prepare(&scan_options);

    // Should be able to scan the first block, but not more
    iter->Seek(kv[0].first);
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsPrefetchLimitReached());
  }

  // Some blocks are loaded
  {
    std::unique_ptr<InternalIterator> iter;
    iter.reset(table->NewIterator(
        read_opts, options_.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));

    MultiScanArgs scan_options(comparator_);
    scan_options.max_prefetch_size = 9 * 1024;  // 9KB - 2 blocks with buffer
    scan_options.insert(ExtractUserKey(kv[1 * kEntriesPerBlock].first),
                        ExtractUserKey(kv[8 * kEntriesPerBlock].first));

    iter->Prepare(&scan_options);
    iter->Seek(kv[1 * kEntriesPerBlock].first);
    size_t scanned_keys = 0;

    // Should be able to scan up to 2 blocks worth of data
    while (iter->Valid()) {
      ASSERT_EQ(iter->key().ToString(),
                kv[scanned_keys + 1 * kEntriesPerBlock].first);
      iter->Next();
      scanned_keys++;
    }

    ASSERT_TRUE(iter->status().IsPrefetchLimitReached());
    ASSERT_EQ(scanned_keys, 2 * kEntriesPerBlock);
  }

  // Tests with some block loaded in cache already:
  // Blocks 1 and 2 are already in cache by the above test.
  // Here we try blocks 0 - 5, with prefetch limit to 3 blocks, and expect to
  // read 3 blocks.
  {
    std::unique_ptr<InternalIterator> iter;
    iter.reset(table->NewIterator(
        read_opts, options_.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));

    MultiScanArgs scan_options(comparator_);
    scan_options.max_prefetch_size = 3 * 4 * 1024 + 1024;  // 3 blocks + 1KB
    scan_options.insert(ExtractUserKey(kv[0].first),
                        ExtractUserKey(kv[5 * kEntriesPerBlock].first));

    iter->Prepare(&scan_options);
    iter->Seek(kv[0].first);
    size_t scanned_keys = 0;
    // Should only read 3 blocks (blocks 0, 1, 2)
    // already cached.
    while (iter->Valid()) {
      ASSERT_EQ(iter->key().ToString(), kv[scanned_keys].first);
      iter->Next();
      scanned_keys++;
    }
    ASSERT_TRUE(iter->status().IsPrefetchLimitReached());
    ASSERT_EQ(scanned_keys, 3 * kEntriesPerBlock);
  }

  // Multiple scan ranges with prefetch limit
  {
    std::unique_ptr<InternalIterator> iter;
    iter.reset(table->NewIterator(
        read_opts, options_.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));

    MultiScanArgs scan_options(comparator_);
    scan_options.max_prefetch_size = 5 * 4 * 1024 + 1024;  // 5 blocks + 1KB
    // Will read 5 entries from first scan range, and 4 blocks from the second
    // scan range
    scan_options.insert(ExtractUserKey(kv[0].first),
                        ExtractUserKey(kv[5].first));
    scan_options.insert(ExtractUserKey(kv[12 * kEntriesPerBlock].first),
                        ExtractUserKey(kv[17 * kEntriesPerBlock].first));
    scan_options.insert(ExtractUserKey(kv[18 * kEntriesPerBlock].first),
                        ExtractUserKey(kv[19 * kEntriesPerBlock].first));

    iter->Prepare(&scan_options);

    iter->Seek(kv[0].first);
    size_t scanned_keys = 0;
    size_t key_idx = 0;
    while (iter->Valid()) {
      ASSERT_EQ(iter->key().ToString(), kv[key_idx].first);
      iter->Next();
      scanned_keys++;
      key_idx++;
      if (key_idx == 5) {
        iter->Seek(kv[12 * kEntriesPerBlock].first);
        key_idx = 12 * kEntriesPerBlock;
      }
    }
    ASSERT_EQ(scanned_keys, 5 + 4 * kEntriesPerBlock);
    ASSERT_TRUE(iter->status().IsPrefetchLimitReached());
  }

  // Prefetch limit is big enough for all scan ranges.
  {
    std::unique_ptr<InternalIterator> iter;
    iter.reset(table->NewIterator(
        read_opts, options_.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));

    MultiScanArgs scan_options(comparator_);
    scan_options.max_prefetch_size = 10 * 1024 * 1024;  // 10MB
    scan_options.insert(ExtractUserKey(kv[0].first),
                        ExtractUserKey(kv[5].first));
    scan_options.insert(ExtractUserKey(kv[8 * kEntriesPerBlock].first),
                        ExtractUserKey(kv[12 * kEntriesPerBlock].first));
    scan_options.insert(ExtractUserKey(kv[18 * kEntriesPerBlock].first),
                        ExtractUserKey(kv[19 * kEntriesPerBlock].first));

    iter->Prepare(&scan_options);

    iter->Seek(kv[0].first);
    size_t scanned_keys = 0;
    size_t key_idx = 0;
    // Scan first range
    while (iter->Valid() && key_idx < 5) {
      ASSERT_EQ(iter->key().ToString(), kv[key_idx].first);
      iter->Next();
      scanned_keys++;
      key_idx++;
    }
    // Move to second range
    iter->Seek(kv[8 * kEntriesPerBlock].first);
    key_idx = 8 * kEntriesPerBlock;
    while (iter->Valid() && key_idx < 12 * kEntriesPerBlock) {
      ASSERT_EQ(iter->key().ToString(), kv[key_idx].first);
      iter->Next();
      scanned_keys++;
      key_idx++;
    }
    // Move to third range
    iter->Seek(kv[18 * kEntriesPerBlock].first);
    key_idx = 18 * kEntriesPerBlock;
    while (iter->Valid() && key_idx < 19 * kEntriesPerBlock) {
      ASSERT_EQ(iter->key().ToString(), kv[key_idx].first);
      iter->Next();
      scanned_keys++;
      key_idx++;
    }
    // Should not hit prefetch limit
    ASSERT_OK(iter->status());
    ASSERT_EQ(scanned_keys, 5 + 4 * kEntriesPerBlock + 1 * kEntriesPerBlock);
  }
}

// Param 1: compression type
// Param 2: whether to use direct reads
// Param 3: Block Based Table Index type, partitioned filters are also enabled
//          when index type is kTwoLevelIndexSearch
// Param 4: BBTO no_block_cache option
// Param 5: test mode for the user-defined timestamp feature
// Param 6: number of parallel compression threads
// Param 7: CompressionOptions.max_dict_bytes and
//          CompressionOptions.max_dict_buffer_bytes. This enable/disables
//          compression dictionary.
// Param 8: test mode to specify the pattern for generating key / value pairs.
INSTANTIATE_TEST_CASE_P(
    BlockBasedTableReaderTest, BlockBasedTableReaderTest,
    ::testing::Combine(
        ::testing::ValuesIn(GetSupportedCompressions()), ::testing::Bool(),
        ::testing::Values(
            BlockBasedTableOptions::IndexType::kBinarySearch,
            BlockBasedTableOptions::IndexType::kHashSearch,
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch,
            BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey),
        ::testing::Values(false), ::testing::ValuesIn(test::GetUDTTestModes()),
        ::testing::Values(1, 2), ::testing::Values(0, 4096),
        ::testing::Values(false),
        ::testing::Values(BytewiseComparator(), ReverseBytewiseComparator())));
INSTANTIATE_TEST_CASE_P(
    BlockBasedTableReaderGetTest, BlockBasedTableReaderGetTest,
    ::testing::Combine(
        ::testing::ValuesIn(GetSupportedCompressions()), ::testing::Bool(),
        ::testing::Values(
            BlockBasedTableOptions::IndexType::kBinarySearch,
            BlockBasedTableOptions::IndexType::kHashSearch,
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch,
            BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey),
        ::testing::Values(false), ::testing::ValuesIn(test::GetUDTTestModes()),
        ::testing::Values(1, 2), ::testing::Values(0, 4096),
        ::testing::Values(false, true),
        ::testing::Values(BytewiseComparator(), ReverseBytewiseComparator())));
INSTANTIATE_TEST_CASE_P(
    StrictCapacityLimitReaderTest, StrictCapacityLimitReaderTest,
    ::testing::Combine(
        ::testing::ValuesIn(GetSupportedCompressions()), ::testing::Bool(),
        ::testing::Values(
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch),
        ::testing::Values(false), ::testing::ValuesIn(test::GetUDTTestModes()),
        ::testing::Values(1, 2), ::testing::Values(0),
        ::testing::Values(false, true),
        ::testing::Values(BytewiseComparator(), ReverseBytewiseComparator())));
INSTANTIATE_TEST_CASE_P(
    VerifyChecksum, BlockBasedTableReaderTestVerifyChecksum,
    ::testing::Combine(
        ::testing::ValuesIn(GetSupportedCompressions()),
        ::testing::Values(false),
        ::testing::Values(
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch),
        ::testing::Values(true), ::testing::ValuesIn(test::GetUDTTestModes()),
        ::testing::Values(1, 2), ::testing::Values(0), ::testing::Values(false),
        ::testing::Values(BytewiseComparator(), ReverseBytewiseComparator())));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
