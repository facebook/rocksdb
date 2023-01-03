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
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/partitioned_index_iterator.h"
#include "table/format.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class BlockBasedTableReaderBaseTest : public testing::Test {
 protected:
  // Prepare key-value pairs to occupy multiple blocks.
  // Each value is 256B, every 16 pairs constitute 1 block.
  // If mixed_with_human_readable_string_value == true,
  // then adjacent blocks contain values with different compression
  // complexity: human readable strings are easier to compress than random
  // strings.
  static std::map<std::string, std::string> GenerateKVMap(
      int num_block = 100,
      bool mixed_with_human_readable_string_value = false) {
    std::map<std::string, std::string> kv;

    Random rnd(101);
    uint32_t key = 0;
    for (int block = 0; block < num_block; block++) {
      for (int i = 0; i < 16; i++) {
        char k[9] = {0};
        // Internal key is constructed directly from this key,
        // and internal key size is required to be >= 8 bytes,
        // so use %08u as the format string.
        snprintf(k, sizeof(k), "%08u", key);
        std::string v;
        if (mixed_with_human_readable_string_value) {
          v = (block % 2) ? rnd.HumanReadableString(256)
                          : rnd.RandomString(256);
        } else {
          v = rnd.RandomString(256);
        }
        kv[std::string(k)] = v;
        key++;
      }
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
                   const CompressionType& compression_type,
                   const std::map<std::string, std::string>& kv) {
    std::unique_ptr<WritableFileWriter> writer;
    NewFileWriter(table_name, &writer);

    // Create table builder.
    ImmutableOptions ioptions(options_);
    InternalKeyComparator comparator(options_.comparator);
    ColumnFamilyOptions cf_options;
    MutableCFOptions moptions(cf_options);
    IntTblPropCollectorFactories factories;
    std::unique_ptr<TableBuilder> table_builder(
        options_.table_factory->NewTableBuilder(
            TableBuilderOptions(ioptions, moptions, comparator, &factories,
                                compression_type, CompressionOptions(),
                                0 /* column_family_id */,
                                kDefaultColumnFamilyName, -1 /* level */),
            writer.get()));

    // Build table.
    for (auto it = kv.begin(); it != kv.end(); it++) {
      std::string k = ToInternalKey(it->first);
      std::string v = it->second;
      table_builder->Add(k, v);
    }
    ASSERT_OK(table_builder->Finish());
  }

  void NewBlockBasedTableReader(const FileOptions& foptions,
                                const ImmutableOptions& ioptions,
                                const InternalKeyComparator& comparator,
                                const std::string& table_name,
                                std::unique_ptr<BlockBasedTable>* table,
                                bool prefetch_index_and_filter_in_cache = true,
                                Status* status = nullptr) {
    const MutableCFOptions moptions(options_);
    TableReaderOptions table_reader_options = TableReaderOptions(
        ioptions, moptions.prefix_extractor, EnvOptions(), comparator);

    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(table_name, foptions, &file);

    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(Path(table_name), &file_size));

    std::unique_ptr<TableReader> general_table;
    Status s = options_.table_factory->NewTableReader(
        ReadOptions(), table_reader_options, std::move(file), file_size,
        &general_table, prefetch_index_and_filter_in_cache);

    if (s.ok()) {
      table->reset(reinterpret_cast<BlockBasedTable*>(general_table.release()));
    }

    if (status) {
      *status = s;
    }
  }

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }

  std::string test_dir_;
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  Options options_;

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
                     std::unique_ptr<RandomAccessFileReader>* reader) {
    std::string path = Path(filename);
    std::unique_ptr<FSRandomAccessFile> f;
    ASSERT_OK(fs_->NewRandomAccessFile(path, opt, &f, nullptr));
    reader->reset(new RandomAccessFileReader(std::move(f), path,
                                             env_->GetSystemClock().get()));
  }

  std::string ToInternalKey(const std::string& key) {
    InternalKey internal_key(key, 0, ValueType::kTypeValue);
    return internal_key.Encode().ToString();
  }
};

class BlockBasedTableReaderTest
    : public BlockBasedTableReaderBaseTest,
      public testing::WithParamInterface<std::tuple<
          CompressionType, bool, BlockBasedTableOptions::IndexType, bool>> {
 protected:
  void SetUp() override {
    compression_type_ = std::get<0>(GetParam());
    use_direct_reads_ = std::get<1>(GetParam());
    BlockBasedTableReaderBaseTest::SetUp();
  }

  void ConfigureTableFactory() override {
    BlockBasedTableOptions opts;
    opts.index_type = std::get<2>(GetParam());
    opts.no_block_cache = std::get<3>(GetParam());
    options_.table_factory.reset(
        static_cast<BlockBasedTableFactory*>(NewBlockBasedTableFactory(opts)));
  }

  CompressionType compression_type_;
  bool use_direct_reads_;
};

// Tests MultiGet in both direct IO and non-direct IO mode.
// The keys should be in cache after MultiGet.
TEST_P(BlockBasedTableReaderTest, MultiGet) {
  std::map<std::string, std::string> kv =
      BlockBasedTableReaderBaseTest::GenerateKVMap(
          100 /* num_block */,
          true /* mixed_with_human_readable_string_value */);

  // Prepare keys, values, and statuses for MultiGet.
  autovector<Slice, MultiGetContext::MAX_BATCH_SIZE> keys;
  autovector<PinnableSlice, MultiGetContext::MAX_BATCH_SIZE> values;
  autovector<Status, MultiGetContext::MAX_BATCH_SIZE> statuses;
  {
    const int step =
        static_cast<int>(kv.size()) / MultiGetContext::MAX_BATCH_SIZE;
    auto it = kv.begin();
    for (int i = 0; i < MultiGetContext::MAX_BATCH_SIZE; i++) {
      keys.emplace_back(it->first);
      values.emplace_back();
      statuses.emplace_back();
      std::advance(it, step);
    }
  }

  std::string table_name =
      "BlockBasedTableReaderTest" + CompressionTypeToString(compression_type_);
  CreateTable(table_name, compression_type_, kv);

  std::unique_ptr<BlockBasedTable> table;
  Options options;
  ImmutableOptions ioptions(options);
  FileOptions foptions;
  foptions.use_direct_reads = use_direct_reads_;
  InternalKeyComparator comparator(options.comparator);
  NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &table);

  // Ensure that keys are not in cache before MultiGet.
  for (auto& key : keys) {
    ASSERT_FALSE(table->TEST_KeyInCache(ReadOptions(), key));
  }

  // Prepare MultiGetContext.
  autovector<GetContext, MultiGetContext::MAX_BATCH_SIZE> get_context;
  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  for (size_t i = 0; i < keys.size(); ++i) {
    get_context.emplace_back(BytewiseComparator(), nullptr, nullptr, nullptr,
                             GetContext::kNotFound, keys[i], &values[i],
                             nullptr, nullptr, nullptr, nullptr,
                             true /* do_merge */, nullptr, nullptr, nullptr,
                             nullptr, nullptr, nullptr);
    key_context.emplace_back(nullptr, keys[i], &values[i], nullptr,
                             &statuses.back());
    key_context.back().get_context = &get_context.back();
  }
  for (auto& key_ctx : key_context) {
    sorted_keys.emplace_back(&key_ctx);
  }
  MultiGetContext ctx(&sorted_keys, 0, sorted_keys.size(), 0, ReadOptions(),
                      fs_.get(), nullptr);

  // Execute MultiGet.
  MultiGetContext::Range range = ctx.GetMultiGetRange();
  PerfContext* perf_ctx = get_perf_context();
  perf_ctx->Reset();
  table->MultiGet(ReadOptions(), &range, nullptr);

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
    ASSERT_TRUE(table->TEST_KeyInCache(ReadOptions(), keys[i]));
    ASSERT_EQ(values[i].ToString(), kv[keys[i].ToString()]);
  }
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
  std::map<std::string, std::string> kv_;
  CompressionType compression_type_;

 private:
  std::size_t ApproximateTableReaderMem() {
    std::size_t approx_table_reader_mem = 0;

    std::string table_name = "table_for_approx_table_reader_mem";
    CreateTable(table_name, compression_type_, kv_);

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
  // Keep creating BlockBasedTableReader till hiting the memory limit based on
  // cache capacity and creation fails (when charge_table_reader_ ==
  // kEnabled) or reaching a specfied big number of table readers (when
  // charge_table_reader_ == kDisabled)
  while (s.ok() && opened_table_reader_num < max_table_reader_num_uncapped) {
    table_name = "table_" + std::to_string(opened_table_reader_num);
    CreateTable(table_name, compression_type_, kv_);
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
    CreateTable(table_name, compression_type_, kv_);
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

class BlockBasedTableReaderTestVerifyChecksum
    : public BlockBasedTableReaderTest {
 public:
  BlockBasedTableReaderTestVerifyChecksum() : BlockBasedTableReaderTest() {}
};

TEST_P(BlockBasedTableReaderTestVerifyChecksum, ChecksumMismatch) {
  std::map<std::string, std::string> kv =
      BlockBasedTableReaderBaseTest::GenerateKVMap(800 /* num_block */);

  std::string table_name =
      "BlockBasedTableReaderTest" + CompressionTypeToString(compression_type_);
  CreateTable(table_name, compression_type_, kv);

  std::unique_ptr<BlockBasedTable> table;
  Options options;
  ImmutableOptions ioptions(options);
  FileOptions foptions;
  foptions.use_direct_reads = use_direct_reads_;
  InternalKeyComparator comparator(options.comparator);
  NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &table);

  // Use the top level iterator to find the offset/size of the first
  // 2nd level index block and corrupt the block
  IndexBlockIter iiter_on_stack;
  BlockCacheLookupContext context{TableReaderCaller::kUserVerifyChecksum};
  InternalIteratorBase<IndexValue>* iiter = table->NewIndexIterator(
      ReadOptions(), /*disable_prefix_seek=*/false, &iiter_on_stack,
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

  NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &table);
  Status s = table->VerifyChecksum(ReadOptions(),
                                   TableReaderCaller::kUserVerifyChecksum);
  ASSERT_EQ(s.code(), Status::kCorruption);
}

// Param 1: compression type
// Param 2: whether to use direct reads
// Param 3: Block Based Table Index type
// Param 4: BBTO no_block_cache option
#ifdef ROCKSDB_LITE
// Skip direct I/O tests in lite mode since direct I/O is unsupported.
INSTANTIATE_TEST_CASE_P(
    MultiGet, BlockBasedTableReaderTest,
    ::testing::Combine(
        ::testing::ValuesIn(GetSupportedCompressions()),
        ::testing::Values(false),
        ::testing::Values(BlockBasedTableOptions::IndexType::kBinarySearch),
        ::testing::Values(false)));
#else   // ROCKSDB_LITE
INSTANTIATE_TEST_CASE_P(
    MultiGet, BlockBasedTableReaderTest,
    ::testing::Combine(
        ::testing::ValuesIn(GetSupportedCompressions()), ::testing::Bool(),
        ::testing::Values(BlockBasedTableOptions::IndexType::kBinarySearch),
        ::testing::Values(false)));
#endif  // ROCKSDB_LITE
INSTANTIATE_TEST_CASE_P(
    VerifyChecksum, BlockBasedTableReaderTestVerifyChecksum,
    ::testing::Combine(
        ::testing::ValuesIn(GetSupportedCompressions()),
        ::testing::Values(false),
        ::testing::Values(
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch),
        ::testing::Values(true)));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
