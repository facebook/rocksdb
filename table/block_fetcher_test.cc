//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_fetcher.h"
#include "db/table_properties_collector.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "table/block_based/binary_search_index_reader.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/format.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {
namespace {

class CountedMemoryAllocator : public MemoryAllocator {
 public:
  const char* Name() const override { return "CountedMemoryAllocator"; }

  void* Allocate(size_t size) override {
    num_allocations_++;
    return static_cast<void*>(new char[size]);
  }

  void Deallocate(void* p) override {
    num_deallocations_++;
    delete[] static_cast<char*>(p);
  }

  int GetNumAllocations() const { return num_allocations_; }
  int GetNumDeallocations() const { return num_deallocations_; }

 private:
  int num_allocations_ = 0;
  int num_deallocations_ = 0;
};

struct MemcpyStats {
  int num_stack_buf_memcpy = 0;
  int num_heap_buf_memcpy = 0;
  int num_compressed_buf_memcpy = 0;
};

struct BufAllocationStats {
  int num_heap_buf_allocations = 0;
  int num_compressed_buf_allocations = 0;
};

struct TestStats {
  MemcpyStats memcpy_stats;
  BufAllocationStats buf_allocation_stats;
};

class BlockFetcherTest : public testing::Test {
 protected:
  void SetUp() override {
    test::ResetTmpDirForDirectIO();
    test_dir_ = test::PerThreadDBPath("block_fetcher_test");
    env_ = Env::Default();
    fs_ = FileSystem::Default();
    ASSERT_OK(fs_->CreateDir(test_dir_, IOOptions(), nullptr));
    is_direct_io_supported_ = DetectDirectIOSupport();
  }

  void TearDown() override { EXPECT_OK(test::DestroyDir(env_, test_dir_)); }

  bool IsDirectIOSupported() const { return is_direct_io_supported_; }

  void AssertSameBlock(const BlockContents& block1,
                       const BlockContents& block2) {
    ASSERT_EQ(block1.data.ToString(), block2.data.ToString());
  }

  // Creates a table with kv pairs (i, i) where i ranges from 0 to 9, inclusive.
  void CreateTable(const std::string& table_name,
                   const CompressionType& compression_type) {
    std::unique_ptr<WritableFileWriter> writer;
    NewFileWriter(table_name, &writer);

    // Create table builder.
    Options options;
    ImmutableCFOptions ioptions(options);
    InternalKeyComparator comparator(options.comparator);
    ColumnFamilyOptions cf_options;
    MutableCFOptions moptions(cf_options);
    std::vector<std::unique_ptr<IntTblPropCollectorFactory>> factories;
    std::unique_ptr<TableBuilder> table_builder(table_factory_.NewTableBuilder(
        TableBuilderOptions(ioptions, moptions, comparator, &factories,
                            compression_type, 0 /* sample_for_compression */,
                            CompressionOptions(), false /* skip_filters */,
                            kDefaultColumnFamilyName, -1 /* level */),
        0 /* column_family_id */, writer.get()));

    // Build table.
    for (int i = 0; i < 9; i++) {
      std::string key = ToInternalKey(std::to_string(i));
      std::string value = std::to_string(i);
      table_builder->Add(key, value);
    }
    ASSERT_OK(table_builder->Finish());
  }

  void FetchIndexBlock(const std::string& table_name, bool use_direct_io,
                       CountedMemoryAllocator* heap_buf_allocator,
                       CountedMemoryAllocator* compressed_buf_allocator,
                       MemcpyStats* memcpy_stats, BlockContents* index_block) {
    FileOptions fopt;
    fopt.use_direct_reads = use_direct_io;
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(table_name, fopt, &file);

    // Get handle of the index block.
    Footer footer;
    ReadFooter(file.get(), &footer);
    const BlockHandle& index_handle = footer.index_handle();

    CompressionType compression_type;
    FetchBlock(file.get(), index_handle, BlockType::kIndex,
               false /* compressed */, false /* do_uncompress */,
               heap_buf_allocator, compressed_buf_allocator, index_block,
               memcpy_stats, &compression_type);
    ASSERT_EQ(compression_type, CompressionType::kNoCompression);
  }

  // Fetches the first data block in both direct IO and non-direct IO mode.
  //
  // compressed: whether the data blocks are compressed;
  // do_uncompress: whether the data blocks should be uncompressed on fetching.
  // compression_type: the expected compression type.
  //
  // Expects:
  // Block contents are the same.
  // Bufferr allocation and memory copy statistics are expected.
  void TestFetchDataBlock(const std::string& table_name_prefix, bool compressed,
                          bool do_uncompress,
                          const TestStats& expected_non_direct_io_stats,
                          const TestStats& expected_direct_io_stats) {
    if (!IsDirectIOSupported()) {
      printf("Skip this test since direct IO is not supported\n");
      return;
    }

    for (CompressionType compression_type : GetSupportedCompressions()) {
      bool do_compress = compression_type != kNoCompression;
      if (compressed != do_compress) continue;
      std::string compression_type_str =
          CompressionTypeToString(compression_type);

      std::string table_name = table_name_prefix + compression_type_str;
      CreateTable(table_name, compression_type);

      CompressionType expected_compression_type_after_fetch =
          (compressed && !do_uncompress) ? compression_type : kNoCompression;

      BlockContents blocks[2];
      MemcpyStats memcpy_stats[2];
      CountedMemoryAllocator heap_buf_allocators[2];
      CountedMemoryAllocator compressed_buf_allocators[2];
      for (bool use_direct_io : {false, true}) {
        FetchFirstDataBlock(
            table_name, use_direct_io, compressed, do_uncompress,
            expected_compression_type_after_fetch,
            &heap_buf_allocators[use_direct_io],
            &compressed_buf_allocators[use_direct_io], &blocks[use_direct_io],
            &memcpy_stats[use_direct_io]);
      }

      AssertSameBlock(blocks[0], blocks[1]);

      // Check memcpy and buffer allocation statistics.
      for (bool use_direct_io : {false, true}) {
        const TestStats& expected_stats = use_direct_io
                                              ? expected_direct_io_stats
                                              : expected_non_direct_io_stats;

        ASSERT_EQ(memcpy_stats[use_direct_io].num_stack_buf_memcpy,
                  expected_stats.memcpy_stats.num_stack_buf_memcpy);
        ASSERT_EQ(memcpy_stats[use_direct_io].num_heap_buf_memcpy,
                  expected_stats.memcpy_stats.num_heap_buf_memcpy);
        ASSERT_EQ(memcpy_stats[use_direct_io].num_compressed_buf_memcpy,
                  expected_stats.memcpy_stats.num_compressed_buf_memcpy);

        ASSERT_EQ(heap_buf_allocators[use_direct_io].GetNumAllocations(),
                  expected_stats.buf_allocation_stats.num_heap_buf_allocations);
        ASSERT_EQ(
            compressed_buf_allocators[use_direct_io].GetNumAllocations(),
            expected_stats.buf_allocation_stats.num_compressed_buf_allocations);

        // The allocated buffers are not deallocated until
        // the block content is deleted.
        ASSERT_EQ(heap_buf_allocators[use_direct_io].GetNumDeallocations(), 0);
        ASSERT_EQ(
            compressed_buf_allocators[use_direct_io].GetNumDeallocations(), 0);
        blocks[use_direct_io].allocation.reset();
        ASSERT_EQ(heap_buf_allocators[use_direct_io].GetNumDeallocations(),
                  expected_stats.buf_allocation_stats.num_heap_buf_allocations);
        ASSERT_EQ(
            compressed_buf_allocators[use_direct_io].GetNumDeallocations(),
            expected_stats.buf_allocation_stats.num_compressed_buf_allocations);
      }
    }
  }

 private:
  std::string test_dir_;
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  BlockBasedTableFactory table_factory_;
  bool is_direct_io_supported_;

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }

  void WriteToFile(const std::string& content, const std::string& filename) {
    std::unique_ptr<FSWritableFile> f;
    ASSERT_OK(fs_->NewWritableFile(Path(filename), FileOptions(), &f, nullptr));
    ASSERT_OK(f->Append(content, IOOptions(), nullptr));
    ASSERT_OK(f->Close(IOOptions(), nullptr));
  }

  bool DetectDirectIOSupport() {
    WriteToFile("", ".direct");
    FileOptions opt;
    opt.use_direct_reads = true;
    std::unique_ptr<FSRandomAccessFile> f;
    auto s = fs_->NewRandomAccessFile(Path(".direct"), opt, &f, nullptr);
    return s.ok();
  }

  void NewFileWriter(const std::string& filename,
                     std::unique_ptr<WritableFileWriter>* writer) {
    std::string path = Path(filename);
    EnvOptions env_options;
    std::unique_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(path, &file, env_options));
    writer->reset(new WritableFileWriter(
        NewLegacyWritableFileWrapper(std::move(file)), path, env_options));
  }

  void NewFileReader(const std::string& filename, const FileOptions& opt,
                     std::unique_ptr<RandomAccessFileReader>* reader) {
    std::string path = Path(filename);
    std::unique_ptr<FSRandomAccessFile> f;
    ASSERT_OK(fs_->NewRandomAccessFile(path, opt, &f, nullptr));
    reader->reset(new RandomAccessFileReader(std::move(f), path, env_));
  }

  void NewTableReader(const ImmutableCFOptions& ioptions,
                      const FileOptions& foptions,
                      const InternalKeyComparator& comparator,
                      const std::string& table_name,
                      std::unique_ptr<BlockBasedTable>* table) {
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(table_name, foptions, &file);

    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(Path(table_name), &file_size));

    std::unique_ptr<TableReader> table_reader;
    ASSERT_OK(BlockBasedTable::Open(ioptions, EnvOptions(),
                                    table_factory_.table_options(), comparator,
                                    std::move(file), file_size, &table_reader));

    table->reset(reinterpret_cast<BlockBasedTable*>(table_reader.release()));
  }

  std::string ToInternalKey(const std::string& key) {
    InternalKey internal_key(key, 0, ValueType::kTypeValue);
    return internal_key.Encode().ToString();
  }

  void ReadFooter(RandomAccessFileReader* file, Footer* footer) {
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file->file_name(), &file_size));
    ReadFooterFromFile(file, nullptr /* prefetch_buffer */, file_size, footer,
                       kBlockBasedTableMagicNumber);
  }

  // NOTE: compression_type returns the compression type of the fetched block
  // contents, so if the block is fetched and uncompressed, then it's
  // kNoCompression.
  void FetchBlock(RandomAccessFileReader* file, const BlockHandle& block,
                  BlockType block_type, bool compressed, bool do_uncompress,
                  MemoryAllocator* heap_buf_allocator,
                  MemoryAllocator* compressed_buf_allocator,
                  BlockContents* contents, MemcpyStats* stats,
                  CompressionType* compresstion_type) {
    Options options;
    ImmutableCFOptions ioptions(options);
    ReadOptions roptions;
    PersistentCacheOptions persistent_cache_options;
    Footer footer;
    ReadFooter(file, &footer);
    std::unique_ptr<BlockFetcher> fetcher(new BlockFetcher(
        file, nullptr /* prefetch_buffer */, footer, roptions, block, contents,
        ioptions, do_uncompress, compressed, block_type,
        UncompressionDict::GetEmptyDict(), persistent_cache_options,
        heap_buf_allocator, compressed_buf_allocator));

    ASSERT_OK(fetcher->ReadBlockContents());

    stats->num_stack_buf_memcpy = fetcher->TEST_GetNumStackBufMemcpy();
    stats->num_heap_buf_memcpy = fetcher->TEST_GetNumHeapBufMemcpy();
    stats->num_compressed_buf_memcpy =
        fetcher->TEST_GetNumCompressedBufMemcpy();

    *compresstion_type = fetcher->get_compression_type();
  }

  // NOTE: expected_compression_type is the expected compression
  // type of the fetched block content, if the block is uncompressed,
  // then the expected compression type is kNoCompression.
  void FetchFirstDataBlock(const std::string& table_name, bool use_direct_io,
                           bool compressed, bool do_uncompress,
                           CompressionType expected_compression_type,
                           MemoryAllocator* heap_buf_allocator,
                           MemoryAllocator* compressed_buf_allocator,
                           BlockContents* block, MemcpyStats* memcpy_stats) {
    if (use_direct_io && !IsDirectIOSupported()) {
      printf("Skip this test since direct IO is not supported\n");
      return;
    }

    Options options;
    ImmutableCFOptions ioptions(options);
    InternalKeyComparator comparator(options.comparator);

    FileOptions foptions;
    foptions.use_direct_reads = use_direct_io;

    // Get block handle for the first data block.
    std::unique_ptr<BlockBasedTable> table;
    NewTableReader(ioptions, foptions, comparator, table_name, &table);

    std::unique_ptr<BlockBasedTable::IndexReader> index_reader;
    ASSERT_OK(BinarySearchIndexReader::Create(
        table.get(), nullptr /* prefetch_buffer */, false /* use_cache */,
        false /* prefetch */, false /* pin */, nullptr /* lookup_context */,
        &index_reader));

    std::unique_ptr<InternalIteratorBase<IndexValue>> iter(
        index_reader->NewIterator(
            ReadOptions(), false /* disable_prefix_seek */, nullptr /* iter */,
            nullptr /* get_context */, nullptr /* lookup_context */));
    ASSERT_OK(iter->status());
    iter->SeekToFirst();
    BlockHandle first_block_handle = iter->value().handle;

    // Fetch first data block.
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(table_name, foptions, &file);
    CompressionType compression_type;
    FetchBlock(file.get(), first_block_handle, BlockType::kData, compressed,
               do_uncompress, heap_buf_allocator, compressed_buf_allocator,
               block, memcpy_stats, &compression_type);
    ASSERT_EQ(compression_type, expected_compression_type);
  }
};

// Fetch index block under both direct IO and non-direct IO.
// Expects:
// the index block contents are the same for both read modes.
TEST_F(BlockFetcherTest, FetchIndexBlock) {
  if (!IsDirectIOSupported()) {
    printf("Skip this test since direct IO is not supported\n");
    return;
  }

  for (CompressionType compression : GetSupportedCompressions()) {
    std::string table_name =
        "FetchIndexBlock" + CompressionTypeToString(compression);
    CreateTable(table_name, compression);

    CountedMemoryAllocator allocator;
    MemcpyStats memcpy_stats;
    BlockContents indexes[2];
    for (bool use_direct_io : {false, true}) {
      FetchIndexBlock(table_name, use_direct_io, &allocator, &allocator,
                      &memcpy_stats, &indexes[use_direct_io]);
    }
    AssertSameBlock(indexes[0], indexes[1]);
  }
}

// Data blocks are not compressed,
// fetch data block under both direct IO and non-direct IO.
// Expects:
// 1. in non-direct IO mode, allocate a heap buffer and memcpy the block
//    into the buffer;
// 2. in direct IO mode, allocate a heap buffer and memcpy from the
//    direct IO buffer to the heap buffer.
TEST_F(BlockFetcherTest, FetchUncompressedDataBlock) {
  MemcpyStats memcpy_stats;
  memcpy_stats.num_heap_buf_memcpy = 1;

  BufAllocationStats buf_allocation_stats;
  buf_allocation_stats.num_heap_buf_allocations = 1;

  TestStats expected_stats{memcpy_stats, buf_allocation_stats};

  TestFetchDataBlock("FetchUncompressedDataBlock", false, false, expected_stats,
                     expected_stats);
}

// Data blocks are compressed,
// fetch data block under both direct IO and non-direct IO,
// but do not uncompress.
// Expects:
// 1. in non-direct IO mode, allocate a compressed buffer and memcpy the block
//    into the buffer;
// 2. in direct IO mode, allocate a compressed buffer and memcpy from the
//    direct IO buffer to the compressed buffer.
TEST_F(BlockFetcherTest, FetchCompressedDataBlock) {
  MemcpyStats memcpy_stats;
  memcpy_stats.num_compressed_buf_memcpy = 1;

  BufAllocationStats buf_allocation_stats;
  buf_allocation_stats.num_compressed_buf_allocations = 1;

  TestStats expected_stats{memcpy_stats, buf_allocation_stats};

  TestFetchDataBlock("FetchCompressedDataBlock", true, false, expected_stats,
                     expected_stats);
}

// Data blocks are compressed,
// fetch and uncompress data block under both direct IO and non-direct IO.
// Expects:
// 1. in non-direct IO mode, since the block is small, so it's first memcpyed
//    to the stack buffer, then a heap buffer is allocated and the block is
//    uncompressed into the heap.
// 2. in direct IO mode mode, allocate a heap buffer, then directly uncompress
//    and memcpy from the direct IO buffer to the heap buffer.
TEST_F(BlockFetcherTest, FetchAndUncompressCompressedDataBlock) {
  TestStats expected_non_direct_io_stats;
  {
    MemcpyStats memcpy_stats;
    memcpy_stats.num_stack_buf_memcpy = 1;
    memcpy_stats.num_heap_buf_memcpy = 1;

    BufAllocationStats buf_allocation_stats;
    buf_allocation_stats.num_heap_buf_allocations = 1;
    buf_allocation_stats.num_compressed_buf_allocations = 0;

    expected_non_direct_io_stats = {memcpy_stats, buf_allocation_stats};
  }

  TestStats expected_direct_io_stats;
  {
    MemcpyStats memcpy_stats;
    memcpy_stats.num_heap_buf_memcpy = 1;

    BufAllocationStats buf_allocation_stats;
    buf_allocation_stats.num_heap_buf_allocations = 1;

    expected_direct_io_stats = {memcpy_stats, buf_allocation_stats};
  }

  TestFetchDataBlock("FetchAndUncompressCompressedDataBlock", true, true,
                     expected_non_direct_io_stats, expected_direct_io_stats);
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
