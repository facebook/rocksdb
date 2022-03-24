//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/block_based_table_reader.h"

#include "db/db_test_util.h"
#include "db/table_properties_collector.h"
#include "file/file_util.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "port/stack_trace.h"
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

class BlockBasedTableReaderTest
    : public testing::Test,
      public testing::WithParamInterface<std::tuple<
          CompressionType, bool, BlockBasedTableOptions::IndexType, bool>> {
 protected:
  CompressionType compression_type_;
  bool use_direct_reads_;

  void SetUp() override {
    BlockBasedTableOptions::IndexType index_type;
    bool no_block_cache;
    std::tie(compression_type_, use_direct_reads_, index_type, no_block_cache) =
        GetParam();

    SetupSyncPointsToMockDirectIO();
    test_dir_ = test::PerThreadDBPath("block_based_table_reader_test");
    env_ = Env::Default();
    fs_ = FileSystem::Default();
    ASSERT_OK(fs_->CreateDir(test_dir_, IOOptions(), nullptr));

    BlockBasedTableOptions opts;
    opts.index_type = index_type;
    opts.no_block_cache = no_block_cache;
    table_factory_.reset(
        static_cast<BlockBasedTableFactory*>(NewBlockBasedTableFactory(opts)));
  }

  void TearDown() override { EXPECT_OK(DestroyDir(env_, test_dir_)); }

  // Creates a table with the specificied key value pairs (kv).
  void CreateTable(const std::string& table_name,
                   const CompressionType& compression_type,
                   const std::map<std::string, std::string>& kv) {
    std::unique_ptr<WritableFileWriter> writer;
    NewFileWriter(table_name, &writer);

    // Create table builder.
    Options options;
    ImmutableOptions ioptions(options);
    InternalKeyComparator comparator(options.comparator);
    ColumnFamilyOptions cf_options;
    MutableCFOptions moptions(cf_options);
    IntTblPropCollectorFactories factories;
    std::unique_ptr<TableBuilder> table_builder(table_factory_->NewTableBuilder(
        TableBuilderOptions(ioptions, moptions, comparator, &factories,
                            compression_type, CompressionOptions(),
                            0 /* column_family_id */, kDefaultColumnFamilyName,
                            -1 /* level */),
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
                                std::unique_ptr<BlockBasedTable>* table) {
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(table_name, foptions, &file);

    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(Path(table_name), &file_size));

    std::unique_ptr<TableReader> table_reader;
    ReadOptions ro;
    const auto* table_options =
        table_factory_->GetOptions<BlockBasedTableOptions>();
    ASSERT_NE(table_options, nullptr);
    ASSERT_OK(BlockBasedTable::Open(ro, ioptions, EnvOptions(), *table_options,
                                    comparator, std::move(file), file_size,
                                    &table_reader));

    table->reset(reinterpret_cast<BlockBasedTable*>(table_reader.release()));
  }

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }

  const std::shared_ptr<FileSystem>& fs() const { return fs_; }

 private:
  std::string test_dir_;
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  std::unique_ptr<BlockBasedTableFactory> table_factory_;

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

// Tests MultiGet in both direct IO and non-direct IO mode.
// The keys should be in cache after MultiGet.
TEST_P(BlockBasedTableReaderTest, MultiGet) {
  // Prepare key-value pairs to occupy multiple blocks.
  // Each value is 256B, every 16 pairs constitute 1 block.
  // Adjacent blocks contain values with different compression complexity:
  // human readable strings are easier to compress than random strings.
  std::map<std::string, std::string> kv;
  {
    Random rnd(101);
    uint32_t key = 0;
    for (int block = 0; block < 100; block++) {
      for (int i = 0; i < 16; i++) {
        char k[9] = {0};
        // Internal key is constructed directly from this key,
        // and internal key size is required to be >= 8 bytes,
        // so use %08u as the format string.
        sprintf(k, "%08u", key);
        std::string v;
        if (block % 2) {
          v = rnd.HumanReadableString(256);
        } else {
          v = rnd.RandomString(256);
        }
        kv[std::string(k)] = v;
        key++;
      }
    }
  }

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
    get_context.emplace_back(
        BytewiseComparator(), nullptr, nullptr, nullptr, GetContext::kNotFound,
        keys[i], &values[i], nullptr, nullptr, nullptr, true /* do_merge */,
        nullptr, nullptr, nullptr, nullptr, nullptr, nullptr);
    key_context.emplace_back(nullptr, keys[i], &values[i], nullptr,
                             &statuses.back());
    key_context.back().get_context = &get_context.back();
  }
  for (auto& key_ctx : key_context) {
    sorted_keys.emplace_back(&key_ctx);
  }
  MultiGetContext ctx(&sorted_keys, 0, sorted_keys.size(), 0, ReadOptions());

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

class BlockBasedTableReaderResOnlyCache : public CacheWrapper {
 public:
  explicit BlockBasedTableReaderResOnlyCache(std::shared_ptr<Cache> target)
      : CacheWrapper(std::move(target)) {}

  using Cache::Insert;
  Status Insert(const Slice& key, void* value, size_t charge,
                void (*deleter)(const Slice& key, void* value),
                Handle** handle = nullptr,
                Priority priority = Priority::LOW) override {
    if (deleter == kNoopDeleterForBlockBasedTableReader) {
      return target_->Insert(key, value, charge, deleter, handle, priority);
    } else {
      return Status::OK();
    }
  }

  using Cache::Release;
  bool Release(Handle* handle, bool force_erase = false) override {
    auto deleter = GetDeleter(handle);
    if (deleter == kNoopDeleterForBlockBasedTableReader) {
      return target_->Release(handle, force_erase);
    } else {
      return true;
    }
  }

 private:
  static const Cache::DeleterFn kNoopDeleterForBlockBasedTableReader;
};

const Cache::DeleterFn
    BlockBasedTableReaderResOnlyCache::kNoopDeleterForBlockBasedTableReader =
        CacheReservationManager::TEST_GetNoopDeleterForRole<
            CacheEntryRole::kBlockBasedTableReader>();

class BlockBasedTableReaderCapMemoryTest
    : public testing::Test,
      public testing::WithParamInterface<
          bool /* reserve_table_builder_memory */> {
 protected:
  void SetUp() override {
    env_ = Env::Default();
    fs_ = FileSystem::Default();
    test_dir_ = test::PerThreadDBPath("block_based_table_reader_test");
    ASSERT_OK(fs_->CreateDir(test_dir_, IOOptions(), nullptr));

    reserve_table_reader_memory_ = GetParam();
  }

  void TearDown() override { EXPECT_OK(DestroyDir(env_, test_dir_)); }

  void ConfugreOptions(std::size_t reserve_table_reader_memory,
                       std::size_t cache_capacity = 1024) {
    BlockBasedTableOptions table_options;

    table_reader_res_only_cache_.reset(new BlockBasedTableReaderResOnlyCache(
        NewLRUCache(cache_capacity, 0 /* num_shard_bits */,
                    true /* strict_capacity_limit */)));
    table_options.block_cache = table_reader_res_only_cache_;

    table_options.reserve_table_reader_memory = reserve_table_reader_memory;

    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    table_options.partition_filters = true;
    table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;

    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }

  void CreateTable(const std::string& table_name) {
    std::unique_ptr<WritableFileWriter> writer;
    NewFileWriter(table_name, &writer);

    ImmutableOptions ioptions(options_);
    InternalKeyComparator comparator(options_.comparator);
    ColumnFamilyOptions cf_options;
    MutableCFOptions moptions(cf_options);
    IntTblPropCollectorFactories factories;
    std::unique_ptr<TableBuilder> table_builder(
        options_.table_factory->NewTableBuilder(
            TableBuilderOptions(ioptions, moptions, comparator, &factories,
                                CompressionType::kNoCompression,
                                CompressionOptions(), 0 /* column_family_id */,
                                kDefaultColumnFamilyName, -1 /* level */),
            writer.get()));

    Random rnd(101);
    for (uint32_t i = 0; i < 100; ++i) {
      char k[9] = {0};
      // Internal key is constructed directly from this key,
      // and internal key size is required to be >= 8 bytes,
      // so use %08u as the format string.
      sprintf(k, "%08u", i);
      std::string key = ToInternalKey(k);
      std::string val = rnd.RandomString(256);
      table_builder->Add(key, val);
    }

    ASSERT_OK(table_builder->Finish());
  }

  void NewBlockBasedTableReader(const std::string& table_name,
                                std::unique_ptr<TableReader>* table_reader,
                                Status* create_table_reader_status) {
    assert(create_table_reader_status);

    ReadOptions ro;

    const ImmutableOptions ioptions(options_);
    const MutableCFOptions moptions(options_);
    const InternalKeyComparator ikc(options_.comparator);
    TableReaderOptions table_reader_options = TableReaderOptions(
        ioptions, moptions.prefix_extractor, EnvOptions(), ikc);

    std::unique_ptr<RandomAccessFileReader> file_reader;
    const FileOptions foptions;
    NewFileReader(table_name, foptions, &file_reader);
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(Path(table_name), &file_size));

    (*create_table_reader_status) = options_.table_factory->NewTableReader(
        table_reader_options, std::move(file_reader), file_size, table_reader,
        false);
  }

  std::size_t ApproximateTableReaderMem() {
    ConfugreOptions(false /* reserve_table_reader_mem */);

    std::size_t approx_table_reader_mem = 0;
    std::string table_name = "table_for_approx_table_reader_mem";
    CreateTable(table_name);
    std::unique_ptr<TableReader> table_reader;
    Status s;
    NewBlockBasedTableReader(table_name, &table_reader, &s);
    assert(s.ok());
    approx_table_reader_mem = table_reader->ApproximateMemoryUsage();
    assert(approx_table_reader_mem > 0);
    return approx_table_reader_mem;
  }

  static std::size_t ApproximateMaxTableReaderNumBeforeCacheFull(
      std::size_t cache_capacity, std::size_t approx_table_reader_mem) {
    // To make calculation easier for testing
    assert(cache_capacity % CacheReservationManager::GetDummyEntrySize() == 0);

    // We need to subtract 1 for max_num_dummy_entry to account for dummy
    // entry's overhead
    std::size_t max_num_dummy_entry =
        (size_t)std::floor((1.0 * cache_capacity /
                            CacheReservationManager::GetDummyEntrySize())) -
        1;
    std::size_t cache_capacity_rounded_to_dummy_entry_multiples =
        max_num_dummy_entry * CacheReservationManager::GetDummyEntrySize();
    std::size_t max_table_reader_num = static_cast<std::size_t>(
        std::floor(1.0 * cache_capacity_rounded_to_dummy_entry_multiples /
                   approx_table_reader_mem));

    return max_table_reader_num;
  }

  bool reserve_table_reader_memory_;
  std::shared_ptr<BlockBasedTableReaderResOnlyCache>
      table_reader_res_only_cache_;

 private:
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

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }

  std::string ToInternalKey(const std::string& key) {
    InternalKey internal_key(key, 0, ValueType::kTypeValue);
    return internal_key.Encode().ToString();
  }

  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  std::string test_dir_;
  Options options_;
};

INSTANTIATE_TEST_CASE_P(CapMemoryUsageUnderCacheCapacity,
                        BlockBasedTableReaderCapMemoryTest, ::testing::Bool());

TEST_P(BlockBasedTableReaderCapMemoryTest, CapMemoryUsageUnderCacheCapacity) {
  std::size_t cache_capacity =
      10 * CacheReservationManager::GetDummyEntrySize();
  std::size_t mem_per_table_reader = ApproximateTableReaderMem();
  std::size_t max_table_reader_num = BlockBasedTableReaderCapMemoryTest::
      ApproximateMaxTableReaderNumBeforeCacheFull(cache_capacity,
                                                  mem_per_table_reader);

  ConfugreOptions(true, cache_capacity);
  std::vector<std::unique_ptr<TableReader>> table_reader_objects;
  Status s = Status::OK();
  std::size_t opened_table_reader_num = 0;
  std::string table_name;

  // Keep creating BlockBasedTableReader till hiting the memory limit based on
  // cache capacity and creation fails
  while (s.ok()) {
    table_name = "table_" + std::to_string(opened_table_reader_num);
    CreateTable(table_name);
    table_reader_objects.push_back(std::unique_ptr<TableReader>());
    NewBlockBasedTableReader(table_name, &(table_reader_objects.back()), &s);
    if (s.ok()) {
      ++opened_table_reader_num;
    }
  }
  EXPECT_TRUE(s.IsMemoryLimit());
  EXPECT_TRUE(s.ToString().find("memory limit based on cache capacity") !=
              std::string::npos);
  EXPECT_GE(opened_table_reader_num, max_table_reader_num * 9 / 10);
  EXPECT_LE(opened_table_reader_num, max_table_reader_num);

  std::size_t updated_max_table_reader_num =
      BlockBasedTableReaderCapMemoryTest::
          ApproximateMaxTableReaderNumBeforeCacheFull(cache_capacity / 2,
                                                      mem_per_table_reader);

  // Keep deleting BlockBasedTableReader to lower down memory usage from limit
  // so that next creation succeeds
  while (opened_table_reader_num >= updated_max_table_reader_num) {
    table_reader_objects.pop_back();
    --opened_table_reader_num;
  }
  table_name = "table_for_successful_table_reader_open";
  CreateTable(table_name);
  table_reader_objects.push_back(std::unique_ptr<TableReader>());
  NewBlockBasedTableReader(table_name, &(table_reader_objects.back()), &s);
  EXPECT_TRUE(s.ok()) << s.ToString();

  table_reader_objects.clear();
  EXPECT_EQ(table_reader_res_only_cache_->GetPinnedUsage(), 0);
}

class BlockBasedTableReaderTestVerifyChecksum
    : public BlockBasedTableReaderTest {
 public:
  BlockBasedTableReaderTestVerifyChecksum() : BlockBasedTableReaderTest() {}
};

TEST_P(BlockBasedTableReaderTestVerifyChecksum, ChecksumMismatch) {
  // Prepare key-value pairs to occupy multiple blocks.
  // Each value is 256B, every 16 pairs constitute 1 block.
  // Adjacent blocks contain values with different compression complexity:
  // human readable strings are easier to compress than random strings.
  Random rnd(101);
  std::map<std::string, std::string> kv;
  {
    uint32_t key = 0;
    for (int block = 0; block < 800; block++) {
      for (int i = 0; i < 16; i++) {
        char k[9] = {0};
        // Internal key is constructed directly from this key,
        // and internal key size is required to be >= 8 bytes,
        // so use %08u as the format string.
        sprintf(k, "%08u", key);
        std::string v = rnd.RandomString(256);
        kv[std::string(k)] = v;
        key++;
      }
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
