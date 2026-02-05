//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/io_dispatcher.h"

#include <memory>
#include <mutex>

#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"

// Enable io_uring support for this test
extern "C" bool RocksDbIOUringEnable() { return true; }

// Check if io_uring is available at compile time
#ifdef ROCKSDB_IOURING_PRESENT
static constexpr bool kIOUringPresent = true;
#else
static constexpr bool kIOUringPresent = false;
#endif

namespace ROCKSDB_NAMESPACE {

// Represents a single read operation recorded by the tracking file system
struct ReadOp {
  enum Type { kMultiRead, kReadAsync };
  Type type;
  // For MultiRead: contains all (offset, len) pairs in the request
  // For ReadAsync: contains a single (offset, len) pair
  std::vector<std::pair<uint64_t, size_t>> requests;
};

// Forward declaration
class ReadTrackingFS;

// Wrapper around FSRandomAccessFile that tracks read operations
class ReadTrackingRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
 public:
  ReadTrackingRandomAccessFile(std::unique_ptr<FSRandomAccessFile>&& file,
                               ReadTrackingFS* fs)
      : FSRandomAccessFileOwnerWrapper(std::move(file)), fs_(fs) {}

  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override;

  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override;

 private:
  ReadTrackingFS* fs_;
};

// FileSystem wrapper that tracks all read operations for verification
class ReadTrackingFS : public FileSystemWrapper {
 public:
  explicit ReadTrackingFS(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}

  static const char* kClassName() { return "ReadTrackingFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
    if (s.ok()) {
      result->reset(new ReadTrackingRandomAccessFile(std::move(file), this));
    }
    return s;
  }

  // Record a MultiRead operation
  void RecordMultiRead(const std::vector<std::pair<uint64_t, size_t>>& reqs) {
    std::lock_guard<std::mutex> lock(mutex_);
    ReadOp op;
    op.type = ReadOp::kMultiRead;
    op.requests = reqs;
    read_ops_.push_back(std::move(op));
  }

  // Record a ReadAsync operation
  void RecordReadAsync(uint64_t offset, size_t len) {
    std::lock_guard<std::mutex> lock(mutex_);
    ReadOp op;
    op.type = ReadOp::kReadAsync;
    op.requests.push_back({offset, len});
    read_ops_.push_back(std::move(op));
  }

  // Get all recorded read operations
  std::vector<ReadOp> GetReadOps() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return read_ops_;
  }

  // Clear recorded read operations
  void ClearReadOps() {
    std::lock_guard<std::mutex> lock(mutex_);
    read_ops_.clear();
  }

  // Get count of MultiRead operations
  size_t GetMultiReadCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    for (const auto& op : read_ops_) {
      if (op.type == ReadOp::kMultiRead) {
        count++;
      }
    }
    return count;
  }

  // Get count of ReadAsync operations
  size_t GetReadAsyncCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    for (const auto& op : read_ops_) {
      if (op.type == ReadOp::kReadAsync) {
        count++;
      }
    }
    return count;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<ReadOp> read_ops_;
};

IOStatus ReadTrackingRandomAccessFile::MultiRead(FSReadRequest* reqs,
                                                 size_t num_reqs,
                                                 const IOOptions& options,
                                                 IODebugContext* dbg) {
  // Record the read operation before executing it
  std::vector<std::pair<uint64_t, size_t>> recorded_reqs;
  recorded_reqs.reserve(num_reqs);
  for (size_t i = 0; i < num_reqs; i++) {
    recorded_reqs.push_back({reqs[i].offset, reqs[i].len});
  }
  fs_->RecordMultiRead(recorded_reqs);

  // Delegate to underlying file
  return target()->MultiRead(reqs, num_reqs, options, dbg);
}

IOStatus ReadTrackingRandomAccessFile::ReadAsync(
    FSReadRequest& req, const IOOptions& opts,
    std::function<void(FSReadRequest&, void*)> cb, void* cb_arg,
    void** io_handle, IOHandleDeleter* del_fn, IODebugContext* dbg) {
  // Record the read operation before executing it
  fs_->RecordReadAsync(req.offset, req.len);

  // Delegate to underlying file
  return target()->ReadAsync(req, opts, cb, cb_arg, io_handle, del_fn, dbg);
}

class IODispatcherTest : public DBTestBase {
 public:
  IODispatcherTest()
      : DBTestBase("io_dispatcher_test", /*env_do_fsync=*/false) {}

  ~IODispatcherTest() override {
    // Close any open tables
    for (auto& table : tables_) {
      table.reset();
    }
    tables_.clear();
  }

  // Helper to collect block handles from a table
  // We use TEST_GetDataBlockHandle to get handles for specific keys
  // Since we know the keys we inserted, we can collect their block handles
  Status CollectBlockHandles(BlockBasedTable* table, size_t num_keys,
                             std::vector<BlockHandle>* block_handles_out) {
    block_handles_out->clear();

    ReadOptions read_options;
    std::unordered_set<uint64_t> seen_offsets;

    // Iterate through all keys and get their block handles
    // We collect unique block handles (same block might contain multiple keys)
    IndexBlockIter iiter_on_stack;
    BlockCacheLookupContext context{TableReaderCaller::kUserVerifyChecksum};
    auto iiter = table->NewIndexIterator(read_options, false, &iiter_on_stack,
                                         nullptr, &context);
    std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
    if (iiter != &iiter_on_stack) {
      iiter_unique_ptr.reset(iiter);
    }

    // Position the iterator at the first entry
    iiter->SeekToFirst();

    while (iiter->Valid()) {
      auto handle = iiter->value().handle;
      if (seen_offsets.find(handle.offset()) == seen_offsets.end()) {
        block_handles_out->push_back(handle);
        seen_offsets.insert(handle.offset());
        if (block_handles_out->size() >= num_keys) {
          break;
        }
      }
      iiter->Next();
    }

    return Status::OK();
  }

  std::string test_dir_{};
  Env* env_{};
  std::shared_ptr<FileSystem> base_fs_;
  std::shared_ptr<ReadTrackingFS> tracking_fs_;

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }

  void SetUp() override {
    SetupSyncPointsToMockDirectIO();
    test_dir_ = test::PerThreadDBPath("block_based_table_reader_test");
    env_ = Env::Default();
    base_fs_ = FileSystem::Default();
    tracking_fs_ = std::make_shared<ReadTrackingFS>(base_fs_);
    ASSERT_OK(base_fs_->CreateDir(test_dir_, IOOptions(), nullptr));
  }

  void TearDown() override { EXPECT_OK(DestroyDir(env_, test_dir_)); }

  void NewFileWriter(const std::string& filename,
                     std::unique_ptr<WritableFileWriter>* writer) {
    std::string path = Path(filename);
    EnvOptions env_options;
    FileOptions foptions;
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(base_fs_->NewWritableFile(path, foptions, &file, nullptr));
    writer->reset(new WritableFileWriter(std::move(file), path, env_options));
  }

  void NewFileReader(const std::string& filename, const FileOptions& opt,
                     std::unique_ptr<RandomAccessFileReader>* reader,
                     Statistics* stats = nullptr) {
    std::string path = Path(filename);
    std::unique_ptr<FSRandomAccessFile> f;
    // Use tracking_fs_ to record read operations
    ASSERT_OK(tracking_fs_->NewRandomAccessFile(path, opt, &f, nullptr));
    reader->reset(new RandomAccessFileReader(std::move(f), path,
                                             env_->GetSystemClock().get(),
                                             /*io_tracer=*/nullptr,
                                             /*stats=*/stats));
  }

  std::vector<std::shared_ptr<Statistics>> all_stats_;
  std::vector<std::unique_ptr<BlockBasedTable>> tables_;

  // Options must be stored as member variables to avoid use-after-scope
  // The BlockBasedTable keeps references to these options
  std::vector<std::unique_ptr<ImmutableOptions>> all_ioptions_;
  std::vector<std::unique_ptr<EnvOptions>> all_env_options_;

  // Helper to create an SST file and open it as a table
  // Following pattern from table_test.cc TableConstructor
  Status CreateAndOpenSST(int num_blocks,
                          std::unique_ptr<BlockBasedTable>* table,
                          std::vector<BlockHandle>* block_handles_out) {
    // Create options - store in member variables to avoid use-after-scope
    // The BlockBasedTable will keep references to these options
    Options options{};
    options.statistics = nullptr;
    BlockBasedTableOptions table_options;
    table_options.block_cache = NewLRUCache(8 * 1024 * 1024);
    table_options.block_size = 16 * 1024;
    table_options.no_block_cache = false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    // Store these in member variables so they outlive the function
    auto ioptions = std::make_unique<ImmutableOptions>(options);
    auto moptions = MutableCFOptions{options};
    InternalKeyComparator internal_comparator(options.comparator);

    // Create in-memory file using StringSink (like table_test.cc)
    auto table_name = "test_table";
    std::unique_ptr<WritableFileWriter> file_writer;
    NewFileWriter(table_name, &file_writer);

    // Create table builder
    std::string column_family_name;
    const ReadOptions read_options;
    const WriteOptions write_options;
    std::vector<std::unique_ptr<InternalTblPropCollFactory>>
        int_tbl_prop_coll_factories;
    TableBuilderOptions builder_options(
        *ioptions, moptions, read_options, write_options, internal_comparator,
        &int_tbl_prop_coll_factories, kNoCompression, options.compression_opts,
        0 /* column_family_id */, column_family_name, -1 /* level */,
        kUnknownNewestKeyTime);

    std::unique_ptr<TableBuilder> builder(
        options.table_factory->NewTableBuilder(builder_options,
                                               file_writer.get()));

    Status s;
    auto rnd = Random::GetTLSInstance();
    // Add keys to the table
    // 10k * 1Kib = ~10MiB
    for (int i = 0; i < 10000; i++) {
      std::string value = rnd->RandomString(2 << 10);
      InternalKey ikey(Key(i), i, kTypeValue);
      builder->Add(ikey.Encode(), value);
    }
    s = builder->Finish();
    if (!s.ok()) {
      return s;
    }

    uint64_t file_size = builder->FileSize();

    IOOptions io_options;
    s = file_writer->Flush(io_options);
    if (!s.ok()) {
      return s;
    }

    // Now open the file for reading using StringSource (like table_test.cc)
    std::unique_ptr<RandomAccessFileReader> file;
    FileOptions foptions;
    foptions.use_direct_reads = false;

    NewFileReader(table_name, foptions, &file, nullptr);

    // Store EnvOptions and InternalKeyComparator to avoid use-after-scope
    auto soptions = std::make_unique<EnvOptions>();
    BlockCacheTracer block_cache_tracer;
    std::unique_ptr<TableReader> table_reader;

    auto ikc = InternalKeyComparator(options.comparator);
    TableReaderOptions reader_options(*ioptions, moptions.prefix_extractor,
                                      moptions.compression_manager.get(),
                                      *soptions, ikc,
                                      0 /* block_protection_bytes_per_key */);

    s = options.table_factory->NewTableReader(reader_options, std::move(file),
                                              file_size, &table_reader);

    if (!s.ok()) {
      return s;
    }

    table->reset(static_cast<BlockBasedTable*>(table_reader.release()));

    // Collect actual block handles from the table's index
    // This is similar to how block_based_table_iterator.cc CollectBlockHandles
    // works
    s = CollectBlockHandles(table->get(), num_blocks, block_handles_out);
    if (!s.ok()) {
      return s;
    }

    // Store all options in member variables to keep them alive
    all_ioptions_.push_back(std::move(ioptions));
    all_env_options_.push_back(std::move(soptions));

    return Status::OK();
  }

  static uint64_t cur_file_num_;
};

uint64_t IODispatcherTest::cur_file_num_ = 1;

TEST_F(IODispatcherTest, BasicSSTRead) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(50, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_NE(table, nullptr);
  ASSERT_GT(block_handles.size(), 0);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  ReadOptions read_options;
  // Only use async IO when io_uring is available
  job->job_options.read_options.async_io = kIOUringPresent;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // Read blocks using the new ReadSet API and verify they are valid
  // ReadIndex will poll for async IO completion internally, no need to sleep
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);

    // Verify the block has reasonable content
    const Block* block_ptr = block.GetValue();
    ASSERT_GT(block_ptr->size(), 0);
  }

  // Verify statistics - some blocks should have been read asynchronously
  // Note: actual counts depend on cache behavior and IO completion
  uint64_t total_reads = read_set->GetNumSyncReads() +
                         read_set->GetNumAsyncReads() +
                         read_set->GetNumCacheHits();
  ASSERT_EQ(total_reads, block_handles.size());
}

TEST_F(IODispatcherTest, MultipleSSTFiles) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::vector<std::shared_ptr<ReadSet>> read_sets;
  std::vector<std::vector<BlockHandle>> all_block_handles;

  // Create and submit jobs for multiple SST files
  for (int i = 0; i < 3; i++) {
    std::unique_ptr<BlockBasedTable> table;
    std::vector<BlockHandle> block_handles;

    Status s = CreateAndOpenSST(30 + i * 10, &table, &block_handles);
    ASSERT_OK(s);

    auto job = std::make_shared<IOJob>();
    job->block_handles = block_handles;
    job->table = table.get();
    tables_.push_back(std::move(table));

    all_block_handles.push_back(block_handles);
    std::shared_ptr<ReadSet> read_set;
    s = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(s);
    read_sets.push_back(read_set);
  }

  // Verify all ReadSets can read their blocks successfully
  // ReadIndex will poll for async IO completion internally, no need to sleep
  for (size_t i = 0; i < read_sets.size(); ++i) {
    for (size_t j = 0; j < all_block_handles[i].size(); ++j) {
      CachableEntry<Block> block;
      Status read_status = read_sets[i]->ReadIndex(j, &block);
      ASSERT_OK(read_status);
      ASSERT_NE(block.GetValue(), nullptr);
    }
  }
}

TEST_F(IODispatcherTest, StatisticsTracking) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(30, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_NE(table, nullptr);
  ASSERT_GT(block_handles.size(), 0);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  // Only use async IO when io_uring is available
  job->job_options.read_options.async_io = kIOUringPresent;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // Read all blocks - ReadIndex handles polling for async IO completion
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }

  // Read the same blocks again - should all be cache hits now
  std::shared_ptr<ReadSet> read_set2;
  s = dispatcher->SubmitJob(job, &read_set2);
  ASSERT_OK(s);

  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set2->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }

  // After reading all blocks, verify statistics
  uint64_t num_sync = read_set->GetNumSyncReads();
  uint64_t num_async = read_set->GetNumAsyncReads();
  uint64_t num_cache = read_set->GetNumCacheHits();

  // Total reads should equal number of blocks
  uint64_t total_reads = num_sync + num_async + num_cache;
  ASSERT_EQ(total_reads, block_handles.size());
}
TEST_F(IODispatcherTest, AsyncAndSyncRead) {
  // This test verifies the difference between async_io=true and async_io=false
  // by checking the statistics after reading all blocks.
  // Only test async_io=true when io_uring is available.
  std::vector<bool> async_modes = {false};
  if (kIOUringPresent) {
    async_modes.push_back(true);
  }

  for (auto async : async_modes) {
    std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

    std::unique_ptr<BlockBasedTable> table;
    std::vector<BlockHandle> block_handles;
    Status s = CreateAndOpenSST(40, &table, &block_handles);
    ASSERT_OK(s);
    ASSERT_NE(table, nullptr);
    ASSERT_GT(block_handles.size(), 0);

    auto job = std::make_shared<IOJob>();
    job->block_handles = block_handles;
    job->table = table.get();
    ReadOptions read_options;
    // Ensure we don't use cache for this test - we want fresh reads
    read_options.fill_cache = false;
    job->job_options.read_options.async_io = async;

    std::shared_ptr<ReadSet> read_set;
    s = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(s);
    ASSERT_NE(read_set, nullptr);

    // Read all blocks - ReadIndex handles polling for async IO internally
    for (size_t i = 0; i < block_handles.size(); ++i) {
      CachableEntry<Block> block;
      Status read_status = read_set->ReadIndex(i, &block);
      ASSERT_OK(read_status);
      ASSERT_NE(block.GetValue(), nullptr);

      // Verify the block has reasonable content
      const Block* block_ptr = block.GetValue();
      ASSERT_GT(block_ptr->size(), 0);
    }

    // Verify statistics
    uint64_t num_sync = read_set->GetNumSyncReads();
    uint64_t num_async = read_set->GetNumAsyncReads();
    uint64_t num_cache = read_set->GetNumCacheHits();

    // Total reads should equal number of blocks
    uint64_t total_reads = num_sync + num_async + num_cache;
    EXPECT_EQ(total_reads, block_handles.size());

    // When async_io is false, we always expect sync reads
    if (!async) {
      EXPECT_GT(num_sync, 0) << "Expected sync reads when async_io=false";
      EXPECT_EQ(num_async, 0) << "Expected no async reads when async_io=false";
    }
    // When async_io is true:
    // - If io_uring is available, we expect async reads
    // - If io_uring is NOT available, ReadAsync returns NotSupported and
    //   we fall back to sync reads. This is valid behavior.
    // So we only verify that ALL blocks were read (checked above).
  }
}

TEST_F(IODispatcherTest, VerifyBlockContent) {
  // Test that blocks retrieved through ReadSet contain the correct data
  // that was written to the SST file
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(50, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_NE(table, nullptr);
  ASSERT_GT(block_handles.size(), 0);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  ReadOptions read_options;
  job->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // Read each block and verify its content
  int t = 0;
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block_entry;
    Status read_status = read_set->ReadIndex(i, &block_entry);
    ASSERT_OK(read_status);
    ASSERT_NE(block_entry.GetValue(), nullptr);

    Block* block = block_entry.GetValue();
    ASSERT_GT(block->size(), 0);

    // Create an iterator to walk through the block's keys
    // We use InternalKeyComparator for data blocks
    InternalKeyComparator internal_comparator(BytewiseComparator());
    std::unique_ptr<DataBlockIter> iter(block->NewDataIterator(
        internal_comparator.user_comparator(), kDisableGlobalSequenceNumber));

    // Iterate through all keys in this block
    size_t num_keys_in_block = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      num_keys_in_block++;

      // Verify key is not empty
      ASSERT_GT(iter->key().size(), 0)
          << "Block " << i << " contains empty key";

      // Verify value is not empty (we wrote 1KB values)
      ASSERT_GT(iter->value().size(), 2 ^ 10)
          << "Block " << i << " contains empty value";

      // Parse the internal key
      ParsedInternalKey parsed_key;
      Status parse_status =
          ParseInternalKey(iter->key(), &parsed_key, true /* log_err */);
      ASSERT_OK(parse_status) << "Failed to parse internal key in block " << i;

      // Verify the key matches the expected format from CreateAndOpenSST
      // Keys are created with Key(i) which generates keys like "key000000"
      std::string user_key = parsed_key.user_key.ToString();
      auto check = Key(t);
      t++;
      ASSERT_TRUE(user_key.find("key") == 0)
          << "Unexpected key format in block " << i << ": " << user_key;

      ASSERT_EQ(check.c_str(), user_key);

      // Verify value type is correct (should be kTypeValue)
      ASSERT_EQ(parsed_key.type, kTypeValue)
          << "Unexpected value type in block " << i;
    }

    // Verify iterator status after iteration
    ASSERT_OK(iter->status()) << "Iterator error in block " << i;

    // Each block should contain at least one key
    ASSERT_GT(num_keys_in_block, 0) << "Block " << i << " contains no keys";
  }
}

// We want to test here that even when we DONT read from the readset that all
// pinned blocks will be unpinned.
TEST_F(IODispatcherTest, ReadSetDestroysUnpinsBlocks) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(30, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(block_handles.size(), 30);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  ReadOptions read_options;
  job->job_options.read_options.async_io =
      false;  // Use sync IO so blocks are pinned immediately

  auto* rep = table->get_rep();
  auto cache = rep->table_options.block_cache.get();
  ASSERT_NE(cache, nullptr);

  auto initial_pinned_usage = cache->GetPinnedUsage();
  ASSERT_EQ(initial_pinned_usage, 0);

  {
    std::shared_ptr<ReadSet> read_set;
    Status t = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(t);
    ASSERT_NE(read_set, nullptr);

    // With sync IO, blocks are already pinned in read_set->pinned_blocks_
    // We do NOT call read_set->Read() - blocks should remain in pinned_blocks_

    // At this point, blocks should be pinned in the ReadSet
    auto pinned_usage_with_blocks = cache->GetPinnedUsage();
    ASSERT_GT(pinned_usage_with_blocks, initial_pinned_usage)
        << "Expected pinned usage to increase after SubmitJob, but "
        << "initial=" << initial_pinned_usage
        << " current=" << pinned_usage_with_blocks;

    // ReadSet goes out of scope here, its destructor should unpin all blocks
  }

  // ReadSet destroyed - all blocks should be unpinned
  auto final_pinned_usage = cache->GetPinnedUsage();
  ASSERT_EQ(final_pinned_usage, initial_pinned_usage)
      << "Expected pinned usage to return to initial value after ReadSet "
      << "destruction, but initial=" << initial_pinned_usage
      << " final=" << final_pinned_usage;
}


// Test that verifies the coalescing logic: adjacent blocks within the
// coalesce threshold should be combined into a single read request.
TEST_F(IODispatcherTest, VerifyCoalescing) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  // Get many blocks so we can test coalescing behavior
  Status s = CreateAndOpenSST(50, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_NE(table, nullptr);
  ASSERT_GE(block_handles.size(), 20);

  tracking_fs_->ClearReadOps();

  // Test coalescing with sync reads (uses MultiRead)
  {
    auto job = std::make_shared<IOJob>();
    // Use a subset of adjacent blocks
    std::vector<BlockHandle> adjacent_blocks;
    for (size_t i = 0; i < 10 && i < block_handles.size(); ++i) {
      adjacent_blocks.push_back(block_handles[i]);
    }
    job->block_handles = adjacent_blocks;
    job->table = table.get();
    job->job_options.read_options.async_io = false;
    // Set a large coalesce threshold so all adjacent blocks are combined
    job->job_options.io_coalesce_threshold = 1024 * 1024;  // 1MB

    std::shared_ptr<ReadSet> read_set;
    s = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(s);

    for (size_t i = 0; i < adjacent_blocks.size(); ++i) {
      CachableEntry<Block> block;
      Status read_status = read_set->ReadIndex(i, &block);
      ASSERT_OK(read_status);
      ASSERT_NE(block.GetValue(), nullptr);
    }

    // With a large coalesce threshold and adjacent blocks, we expect
    // all blocks to be coalesced into a single MultiRead request
    auto read_ops = tracking_fs_->GetReadOps();
    size_t multiread_count = 0;
    size_t total_requests_in_multireads = 0;
    for (const auto& op : read_ops) {
      if (op.type == ReadOp::kMultiRead) {
        multiread_count++;
        total_requests_in_multireads += op.requests.size();
      }
    }

    // Adjacent blocks should be coalesced into a single read request
    // (assuming they're within the coalesce threshold)
    EXPECT_EQ(multiread_count, 1)
        << "Expected 1 MultiRead call with coalesced blocks";
    EXPECT_EQ(total_requests_in_multireads, 1)
        << "Expected all adjacent blocks to be coalesced into 1 request";
  }

  tracking_fs_->ClearReadOps();

  // Test with zero coalesce threshold and non-adjacent blocks
  // Non-adjacent blocks (with gaps) should NOT be coalesced with threshold=0
  {
    // Create new table to avoid cache hits
    std::unique_ptr<BlockBasedTable> table2;
    std::vector<BlockHandle> block_handles2;
    s = CreateAndOpenSST(50, &table2, &block_handles2);
    ASSERT_OK(s);
    ASSERT_GE(block_handles2.size(), 20);

    tracking_fs_->ClearReadOps();

    auto job = std::make_shared<IOJob>();
    // Skip every other block to create gaps between requested blocks
    // This ensures there are gaps that won't be bridged with threshold=0
    std::vector<BlockHandle> non_adjacent_blocks;
    for (size_t i = 0;
         i < block_handles2.size() && non_adjacent_blocks.size() < 5; i += 2) {
      non_adjacent_blocks.push_back(block_handles2[i]);
    }
    job->block_handles = non_adjacent_blocks;
    job->table = table2.get();
    job->job_options.read_options.async_io = false;
    // Set zero coalesce threshold - blocks with gaps should not be coalesced
    job->job_options.io_coalesce_threshold = 0;

    std::shared_ptr<ReadSet> read_set;
    s = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(s);

    for (size_t i = 0; i < non_adjacent_blocks.size(); ++i) {
      CachableEntry<Block> block;
      Status read_status = read_set->ReadIndex(i, &block);
      ASSERT_OK(read_status);
      ASSERT_NE(block.GetValue(), nullptr);
    }

    // With zero coalesce threshold and non-adjacent blocks (with gaps),
    // each block should be a separate request
    auto read_ops = tracking_fs_->GetReadOps();
    size_t total_requests_in_multireads = 0;
    for (const auto& op : read_ops) {
      if (op.type == ReadOp::kMultiRead) {
        total_requests_in_multireads += op.requests.size();
      }
    }

    // Each non-adjacent block should be a separate request since there are
    // gaps between them and threshold=0 means no gap tolerance
    EXPECT_EQ(total_requests_in_multireads, non_adjacent_blocks.size())
        << "Expected each non-adjacent block to be a separate request with "
           "zero coalesce threshold";
  }
}

// Test that verifies the read request offsets and lengths match the
// expected block handles.
TEST_F(IODispatcherTest, VerifyReadRequestDetails) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(10, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_NE(table, nullptr);
  ASSERT_GE(block_handles.size(), 5);

  tracking_fs_->ClearReadOps();

  // Use just a few non-adjacent blocks to avoid coalescing
  std::vector<BlockHandle> test_blocks;
  // Pick every other block to ensure they're not adjacent
  for (size_t i = 0; i < block_handles.size(); i += 2) {
    test_blocks.push_back(block_handles[i]);
  }

  auto job = std::make_shared<IOJob>();
  job->block_handles = test_blocks;
  job->table = table.get();
  job->job_options.read_options.async_io = false;
  // Small coalesce threshold to minimize coalescing for this test
  job->job_options.io_coalesce_threshold = 0;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);

  for (size_t i = 0; i < test_blocks.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
  }

  // Verify the read requests match the block handles
  auto read_ops = tracking_fs_->GetReadOps();
  std::unordered_set<uint64_t> expected_offsets;
  for (const auto& handle : test_blocks) {
    expected_offsets.insert(handle.offset());
  }

  std::unordered_set<uint64_t> actual_offsets;
  for (const auto& op : read_ops) {
    if (op.type == ReadOp::kMultiRead) {
      for (const auto& req : op.requests) {
        actual_offsets.insert(req.first);
      }
    }
  }

  // Verify all expected offsets were read
  for (const auto& expected : expected_offsets) {
    EXPECT_TRUE(actual_offsets.count(expected) > 0)
        << "Expected read at offset " << expected << " but it was not found";
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
