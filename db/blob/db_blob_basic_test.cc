//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>

#include "cache/compressed_secondary_cache.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_sequential_reader.h"
#include "db/column_family.h"
#include "db/db_test_util.h"
#include "db/db_with_timestamp_test_util.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/sst_file_manager_impl.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/file_system.h"
#include "rocksdb/lazy_wide_columns.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/utilities/replayer.h"
#include "table/embedded_blob_sst.h"
#include "test_util/sync_point.h"
#include "util/compression.h"
#include "util/defer.h"
#include "util/file_checksum_helper.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class DBBlobBasicTest : public DBTestBase {
 protected:
  DBBlobBasicTest()
      : DBTestBase("db_blob_basic_test", /* env_do_fsync */ false) {}

  Options GetDefaultOptions() {
    Options options = DBTestBase::GetDefaultOptions();
    BlockBasedTableOptions table_options;
    table_options.format_version = 7;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    return options;
  }
};

class BlobFileChecksumCapturingFS : public FileSystemWrapper {
 public:
  explicit BlobFileChecksumCapturingFS(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}

  static const char* kClassName() { return "BlobFileChecksumCapturingFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    if (fname.find(".blob") != std::string::npos) {
      std::lock_guard<std::mutex> lock(mu_);
      file_checksum_ = opts.file_checksum;
      file_checksum_func_name_ = opts.file_checksum_func_name;
      ++capture_count_;
    }
    return target()->NewRandomAccessFile(fname, opts, result, dbg);
  }

  std::string GetFileChecksum() {
    std::lock_guard<std::mutex> lock(mu_);
    return file_checksum_;
  }

  std::string GetFileChecksumFuncName() {
    std::lock_guard<std::mutex> lock(mu_);
    return file_checksum_func_name_;
  }

  int GetCaptureCount() {
    std::lock_guard<std::mutex> lock(mu_);
    return capture_count_;
  }

 private:
  std::mutex mu_;
  std::string file_checksum_;
  std::string file_checksum_func_name_;
  int capture_count_ = 0;
};

class BlobGCAccountingListener : public EventListener {
 public:
  void OnBlobFileCreated(const BlobFileCreationInfo& info) override {
    if (info.reason == BlobFileCreationReason::kCompaction &&
        info.status.ok()) {
      blob_count_.fetch_add(info.total_blob_count, std::memory_order_relaxed);
      blob_bytes_.fetch_add(info.total_blob_bytes, std::memory_order_relaxed);
    }
  }

  void OnCompactionCompleted(DB* /*db*/,
                             const CompactionJobInfo& info) override {
    for (const BlobFileGarbageInfo& garbage : info.blob_file_garbage_infos) {
      garbage_file_number_.store(garbage.blob_file_number,
                                 std::memory_order_relaxed);
      garbage_blob_count_.store(garbage.garbage_blob_count,
                                std::memory_order_relaxed);
      garbage_blob_bytes_.store(garbage.garbage_blob_bytes,
                                std::memory_order_relaxed);
    }
  }

  uint64_t blob_count() const {
    return blob_count_.load(std::memory_order_relaxed);
  }
  uint64_t blob_bytes() const {
    return blob_bytes_.load(std::memory_order_relaxed);
  }
  uint64_t garbage_file_number() const {
    return garbage_file_number_.load(std::memory_order_relaxed);
  }
  uint64_t garbage_blob_count() const {
    return garbage_blob_count_.load(std::memory_order_relaxed);
  }
  uint64_t garbage_blob_bytes() const {
    return garbage_blob_bytes_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<uint64_t> blob_count_{0};
  std::atomic<uint64_t> blob_bytes_{0};
  std::atomic<uint64_t> garbage_file_number_{0};
  std::atomic<uint64_t> garbage_blob_count_{0};
  std::atomic<uint64_t> garbage_blob_bytes_{0};
};

class BlobGCTestCompactionService : public CompactionService {
 public:
  const char* Name() const override { return "BlobGCTestCompactionService"; }
};

TEST_F(DBBlobBasicTest, GetBlob) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  ASSERT_OK(Put(key, blob_value));

  ASSERT_OK(Flush());

  ASSERT_EQ(Get(key), blob_value);

  // Try again with no I/O allowed. The table and the necessary blocks should
  // already be in their respective caches; however, the blob itself can only be
  // read from the blob file, so the read should return Incomplete.
  ReadOptions read_options;
  read_options.read_tier = kBlockCacheTier;

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result)
                  .IsIncomplete());
}

TEST_F(DBBlobBasicTest, BlobFileChecksumInFileOptions) {
  auto capturing_fs =
      std::make_shared<BlobFileChecksumCapturingFS>(env_->GetFileSystem());
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, capturing_fs));

  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = env.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  Defer close_db([this]() { Close(); });
  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";
  ASSERT_OK(Put(key, blob_value));
  ASSERT_OK(Flush());

  std::vector<ColumnFamilyMetaData> column_family_metadata;
  db_->GetAllColumnFamilyMetaData(&column_family_metadata);
  ASSERT_EQ(column_family_metadata.size(), 1);
  ASSERT_EQ(column_family_metadata[0].blob_files.size(), 1);
  const BlobMetaData& blob_metadata = column_family_metadata[0].blob_files[0];
  ASSERT_FALSE(blob_metadata.checksum_value.empty());
  ASSERT_EQ(blob_metadata.checksum_method, "FileChecksumCrc32c");

  ASSERT_EQ(Get(key), blob_value);

  ASSERT_GT(capturing_fs->GetCaptureCount(), 0);
  ASSERT_EQ(capturing_fs->GetFileChecksum(), blob_metadata.checksum_value);
  ASSERT_EQ(capturing_fs->GetFileChecksumFuncName(),
            blob_metadata.checksum_method);
}

TEST_F(DBBlobBasicTest, IndirectIdentityBlobSurvivesReopen) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);
  ASSERT_OK(Put("key1", "blob-value-1"));
  ASSERT_OK(Put("key2", "blob-value-2"));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("key1"), "blob-value-1");
  ASSERT_EQ(Get("key2"), "blob-value-2");

  Reopen(options);
  ASSERT_EQ(Get("key1"), "blob-value-1");
  ASSERT_EQ(Get("key2"), "blob-value-2");
}

TEST_F(DBBlobBasicTest, IndirectIdentityPrepopulationRespectsCacheCapacity) {
  LRUCacheOptions cache_options;
  cache_options.capacity = 1024;
  cache_options.num_shard_bits = 0;
  cache_options.strict_capacity_limit = true;
  cache_options.metadata_charge_policy = kDontChargeCacheMetadata;

  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;
  options.blob_cache = NewLRUCache(cache_options);
  options.statistics = CreateDBStatistics();

  Reopen(options);
  ASSERT_OK(Put(std::string(4096, 'k'), "blob-value"));
  ASSERT_OK(Flush());

  EXPECT_GT(options.blob_cache->GetUsage(), 0U);
  EXPECT_LE(options.blob_cache->GetUsage(), cache_options.capacity);
  EXPECT_EQ(options.statistics->getTickerCount(BLOB_DB_CACHE_ADD), 1U);
  EXPECT_EQ(options.statistics->getTickerCount(BLOB_DB_CACHE_ADD_FAILURES), 0U);
}

TEST_F(DBBlobBasicTest, PersistedIndirectionRejectsCompactionService) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);
  ASSERT_OK(Put("key", "blob-value"));
  ASSERT_OK(Flush());

  options.enable_blob_indirection = false;
  options.compaction_service = std::make_shared<BlobGCTestCompactionService>();
  const Status status = TryReopen(options);
  EXPECT_TRUE(status.IsNotSupported()) << status.ToString();
}

TEST_F(DBBlobBasicTest, PersistedIndirectionRejectsBlobDirectWrite) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);
  ASSERT_OK(Put("key", "blob-value"));
  ASSERT_OK(Flush());

  options.enable_blob_indirection = false;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  const Status status = TryReopen(options);
  EXPECT_TRUE(status.IsNotSupported()) << status.ToString();
}

TEST_F(DBBlobBasicTest, BlobIndirectionRejectsAdaptivePlainWriter) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;

  BlockBasedTableOptions block_options;
  block_options.format_version = 7;
  auto block_reader =
      std::shared_ptr<TableFactory>(NewBlockBasedTableFactory(block_options));
  auto plain_writer =
      std::shared_ptr<TableFactory>(NewPlainTableFactory(PlainTableOptions()));
  options.table_factory.reset(
      NewAdaptiveTableFactory(plain_writer, block_reader, plain_writer,
                              /*cuckoo_table_factory=*/nullptr));

  SstFileWriter writer(EnvOptions(), options);
  SstFileWriterEmbeddedBlobOptions embedded_options;
  Status status = writer.OpenWithEmbeddedBlobs(dbname_ + "/adaptive_plain.sst",
                                               embedded_options);
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();

  status = TryReopen(options);
  EXPECT_TRUE(status.IsNotSupported()) << status.ToString();
}

TEST_F(DBBlobBasicTest, LegacyBlobGCRemainsEnabledAfterIndirectionOptIn) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  const std::string live_value(1000, 'a');
  ASSERT_OK(Put("key0", live_value));
  ASSERT_OK(Put("key1", std::string(1000, 'b')));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> direct_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(direct_blob_files.size(), 1U);

  ASSERT_OK(Delete("key1"));
  ASSERT_OK(Flush());

  options.enable_blob_indirection = true;
  Reopen(options);
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kForce;
  compact_options.blob_garbage_collection_age_cutoff = 1.0;
  ASSERT_OK(db_->CompactRange(compact_options, /*begin=*/nullptr,
                              /*end=*/nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForPurge());

  const std::vector<uint64_t> indirect_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(indirect_blob_files.size(), 1U);
  EXPECT_NE(indirect_blob_files.front(), direct_blob_files.front());
  EXPECT_EQ(Get("key0"), live_value);
  EXPECT_EQ(Get("key1"), "NOT_FOUND");

  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);
  const auto meta = cfd->current()->storage_info()->GetBlobFileMetaDataByOrigin(
      indirect_blob_files.front());
  ASSERT_NE(meta, nullptr);
  EXPECT_TRUE(meta->HasIndirectionInfo());
}

TEST_F(DBBlobBasicTest, StandaloneBlobGCDoesNotRewriteSsts) {
  Options options = GetDefaultOptions();
  auto listener = std::make_shared<BlobGCAccountingListener>();
  options.listeners.emplace_back(listener);
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  // Keep legacy compaction-coupled relocation disabled while allowing the
  // compaction to meter newly dead blob references.
  options.blob_garbage_collection_age_cutoff = 0.0;
  options.blob_garbage_collection_force_threshold = 0.3;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;
  options.create_if_missing = true;
  options.blob_cache = NewLRUCache(1 << 20);
  options.statistics = CreateDBStatistics();

  Reopen(options);
  constexpr size_t kValueSize = 4096;
  BlobLogHeader blob_log_header(/*column_family_id=*/0, kNoCompression,
                                /*has_ttl=*/false, ExpirationRange());
  std::string zeroth_value;
  blob_log_header.EncodeTo(&zeroth_value);
  zeroth_value.resize(kValueSize, 'z');
  const std::string first_value(kValueSize, 'a');
  const std::string second_value(kValueSize, 'b');
  ASSERT_OK(Put("key0", zeroth_value));
  ASSERT_OK(Put("key1", first_value));
  ASSERT_OK(Put("key2", second_value));
  ASSERT_OK(Flush());
  const std::string third_value(kValueSize, 'c');
  ASSERT_OK(Put("key3", third_value));
  ASSERT_OK(Flush());

  const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(original_blob_files.size(), 2);

  ASSERT_OK(Delete("key1"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, /*begin=*/nullptr,
                              /*end=*/nullptr));

  auto get_sst_names = [this]() {
    std::vector<LiveFileMetaData> live_files;
    db_->GetLiveFilesMetaData(&live_files);
    std::vector<std::string> names;
    for (const LiveFileMetaData& file : live_files) {
      if (file.file_type == kTableFile) {
        names.push_back(file.name);
      }
    }
    std::sort(names.begin(), names.end());
    return names;
  };
  const std::vector<std::string> ssts_before_gc = get_sst_names();
  ASSERT_EQ(ssts_before_gc.size(), 1);

  {
    ColumnFamilyData* const cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    ASSERT_NE(cfd, nullptr);
    const auto source_meta =
        cfd->current()->storage_info()->GetBlobFileMetaDataByOrigin(
            original_blob_files.front());
    ASSERT_NE(source_meta, nullptr);
    EXPECT_TRUE(source_meta->HasIndirectionInfo());
    EXPECT_FALSE(source_meta->GetLinkedSsts().empty());
    EXPECT_EQ(source_meta->GetTotalBlobCount(), 3);
    EXPECT_EQ(source_meta->GetGarbageBlobCount(), 1);
    EXPECT_GT(source_meta->GetGarbageBlobBytes(), 0);
    const auto gc_candidate =
        cfd->current()->storage_info()->BlobFileForStandaloneGC();
    ASSERT_NE(gc_candidate, nullptr);
    EXPECT_EQ(gc_candidate->GetOriginFileNumber(), original_blob_files.front());
  }
  options.statistics->Reset().PermitUncheckedError();
  ASSERT_OK(dbfull()->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  EXPECT_EQ(options.statistics->getTickerCount(BLOB_DB_CACHE_ADD), 0U);

  const std::vector<uint64_t> relocated_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(relocated_blob_files.size(), 2);
  EXPECT_EQ(std::find(relocated_blob_files.begin(), relocated_blob_files.end(),
                      original_blob_files.front()),
            relocated_blob_files.end());
  EXPECT_NE(std::find(relocated_blob_files.begin(), relocated_blob_files.end(),
                      original_blob_files.back()),
            relocated_blob_files.end());
  EXPECT_EQ(get_sst_names(), ssts_before_gc);
  EXPECT_EQ(Get("key1"), "NOT_FOUND");
  EXPECT_EQ(Get("key0"), zeroth_value);
  EXPECT_EQ(Get("key2"), second_value);
  EXPECT_EQ(Get("key3"), third_value);

  const uint64_t first_carrier = *std::max_element(relocated_blob_files.begin(),
                                                   relocated_blob_files.end());
  const std::string first_carrier_path = BlobFileName(dbname_, first_carrier);
  ASSERT_OK(env_->FileExists(first_carrier_path));
  uint64_t first_carrier_size = 0;
  ASSERT_OK(env_->GetFileSize(first_carrier_path, &first_carrier_size));
  ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "true"}}));
  ASSERT_OK(Delete("key2"));
  ASSERT_OK(Flush());
  // Legacy age-based compaction GC must not relocate indirect references by
  // comparing their logical origin against physical carrier file numbers.
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, /*begin=*/nullptr,
                              /*end=*/nullptr));
  EXPECT_EQ(listener->garbage_file_number(), first_carrier);
  EXPECT_EQ(listener->garbage_blob_count(), 1);
  EXPECT_EQ(listener->garbage_blob_bytes(),
            BlobLogRecord::kHeaderSize + 4 + kValueSize);
  const std::vector<std::string> ssts_before_second_gc = get_sst_names();

  ASSERT_OK(dbfull()->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  const std::vector<uint64_t> twice_relocated_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(twice_relocated_blob_files.size(), 2);
  EXPECT_EQ(std::find(twice_relocated_blob_files.begin(),
                      twice_relocated_blob_files.end(), first_carrier),
            twice_relocated_blob_files.end());
  EXPECT_TRUE(
      env_->FileExists(BlobFileName(dbname_, first_carrier)).IsNotFound());
  EXPECT_EQ(get_sst_names(), ssts_before_second_gc);
  EXPECT_EQ(Get("key0"), zeroth_value);
  EXPECT_EQ(Get("key2"), "NOT_FOUND");
  EXPECT_EQ(options.statistics->getTickerCount(BLOB_DB_GC_NUM_KEYS_RELOCATED),
            3);
  EXPECT_EQ(options.statistics->getTickerCount(BLOB_DB_GC_BYTES_RELOCATED),
            3 * kValueSize);
  const uint64_t second_carrier = *std::max_element(
      twice_relocated_blob_files.begin(), twice_relocated_blob_files.end());
  uint64_t second_carrier_size = 0;
  ASSERT_OK(env_->GetFileSize(BlobFileName(dbname_, second_carrier),
                              &second_carrier_size));
  EXPECT_EQ(listener->blob_count(), 3);
  EXPECT_EQ(listener->blob_bytes(), first_carrier_size + second_carrier_size);

  Reopen(options);
  EXPECT_EQ(Get("key0"), zeroth_value);
  EXPECT_EQ(Get("key1"), "NOT_FOUND");
  EXPECT_EQ(Get("key2"), "NOT_FOUND");
  EXPECT_EQ(Get("key3"), third_value);
}

TEST_F(DBBlobBasicTest, RepeatedStandaloneBlobGCShrinksLargeKeyCarrier) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.0;
  options.blob_garbage_collection_force_threshold = 0.3;
  options.min_blob_size = 0;
  options.blob_file_size = 4 << 20;
  options.disable_auto_compactions = true;

  Reopen(options);
  constexpr size_t kBlobCount = 150;
  constexpr size_t kFirstGarbageCount = 50;
  constexpr size_t kSecondGarbageCount = 40;
  constexpr size_t kKeySize = 8 << 10;
  constexpr size_t kValueSize = 1 << 10;
  std::vector<std::string> keys;
  keys.reserve(kBlobCount);
  for (size_t i = 0; i < kBlobCount; ++i) {
    std::string key = "key-" + std::to_string(i);
    key.resize(kKeySize, static_cast<char>('a' + i % 26));
    keys.emplace_back(std::move(key));
    ASSERT_OK(Put(keys.back(), std::string(kValueSize, 'v')));
  }
  ASSERT_OK(Flush());
  const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(original_blob_files.size(), 1U);

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  for (size_t i = 0; i < kFirstGarbageCount; ++i) {
    ASSERT_OK(Delete(keys[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(compact_options, /*begin=*/nullptr,
                              /*end=*/nullptr));
  ASSERT_OK(dbfull()->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(dbfull()->TEST_WaitForPurge());

  const std::vector<uint64_t> first_carrier_files = GetBlobFileNumbers();
  ASSERT_EQ(first_carrier_files.size(), 1U);
  const uint64_t first_carrier = first_carrier_files.front();
  ASSERT_NE(first_carrier, original_blob_files.front());
  uint64_t first_carrier_size = 0;
  ASSERT_OK(env_->GetFileSize(BlobFileName(dbname_, first_carrier),
                              &first_carrier_size));

  ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "true"}}));
  for (size_t i = kFirstGarbageCount;
       i < kFirstGarbageCount + kSecondGarbageCount; ++i) {
    ASSERT_OK(Delete(keys[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(compact_options, /*begin=*/nullptr,
                              /*end=*/nullptr));

  {
    ColumnFamilyData* const cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    ASSERT_NE(cfd, nullptr);
    const auto candidate =
        cfd->current()->storage_info()->BlobFileForStandaloneGC();
    ASSERT_NE(candidate, nullptr);
    EXPECT_EQ(candidate->GetBlobFileNumber(), first_carrier);
  }

  ASSERT_OK(dbfull()->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  const std::vector<uint64_t> second_carrier_files = GetBlobFileNumbers();
  ASSERT_EQ(second_carrier_files.size(), 1U);
  const uint64_t second_carrier = second_carrier_files.front();
  EXPECT_NE(second_carrier, first_carrier);
  uint64_t second_carrier_size = 0;
  ASSERT_OK(env_->GetFileSize(BlobFileName(dbname_, second_carrier),
                              &second_carrier_size));
  EXPECT_LT(second_carrier_size, first_carrier_size);
  EXPECT_TRUE(
      env_->FileExists(BlobFileName(dbname_, first_carrier)).IsNotFound());
  for (size_t i = 0; i < kFirstGarbageCount + kSecondGarbageCount; ++i) {
    EXPECT_EQ(Get(keys[i]), "NOT_FOUND");
  }
  for (size_t i = kFirstGarbageCount + kSecondGarbageCount; i < kBlobCount;
       ++i) {
    EXPECT_EQ(Get(keys[i]), std::string(kValueSize, 'v'));
  }
}

TEST_F(DBBlobBasicTest, BestEffortsRecoveryOpensBlobGCCarrier) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.0;
  options.blob_garbage_collection_force_threshold = 0.3;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;
  options.block_protection_bytes_per_key = 8;

  Reopen(options);
  const std::string live_value(4096, 'a');
  ASSERT_OK(Put("live", live_value));
  ASSERT_OK(Put("dead", std::string(4096, 'b')));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(original_blob_files.size(), 1U);

  ASSERT_OK(Delete("dead"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, /*begin=*/nullptr,
                              /*end=*/nullptr));

  std::atomic<bool> full_purge_ran{false};
  std::atomic<bool> temp_survived{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:OutputCreated", [&](void* arg) {
        const uint64_t output_file_number = *static_cast<uint64_t*>(arg);
        const std::string temp_path = TempFileName(dbname_, output_file_number);
        if (!env_->FileExists(temp_path).ok()) {
          return;
        }

        JobContext job_context(0);
        dbfull()->TEST_LockMutex();
        dbfull()->FindObsoleteFiles(&job_context, /*force=*/true,
                                    /*no_full_scan=*/false);
        dbfull()->TEST_UnlockMutex();
        dbfull()->PurgeObsoleteFiles(job_context);
        job_context.Clean();

        temp_survived.store(env_->FileExists(temp_path).ok(),
                            std::memory_order_release);
        full_purge_ran.store(true, std::memory_order_release);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  EXPECT_TRUE(full_purge_ran.load(std::memory_order_acquire));
  EXPECT_TRUE(temp_survived.load(std::memory_order_acquire));

  const std::vector<uint64_t> carrier_files = GetBlobFileNumbers();
  ASSERT_EQ(carrier_files.size(), 1U);
  ASSERT_NE(carrier_files.front(), original_blob_files.front());
  ASSERT_TRUE(
      env_->FileExists(BlobFileName(dbname_, original_blob_files.front()))
          .IsNotFound());

  std::atomic<bool> saw_expected_block_protection{false};
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::CreateCarrier:BlockProtectionBytesPerKey",
      [&](void* arg) {
        saw_expected_block_protection.store(
            *static_cast<uint8_t*>(arg) ==
                options.block_protection_bytes_per_key,
            std::memory_order_release);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  options.best_efforts_recovery = true;
  Reopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  EXPECT_TRUE(saw_expected_block_protection.load(std::memory_order_acquire));
  EXPECT_EQ(Get("live"), live_value);
  EXPECT_EQ(Get("dead"), "NOT_FOUND");

  BlockBasedTableOptions adaptive_block_options;
  adaptive_block_options.format_version = 7;
  auto adaptive_block_factory = std::shared_ptr<TableFactory>(
      NewBlockBasedTableFactory(adaptive_block_options));
  options.table_factory.reset(NewAdaptiveTableFactory(
      adaptive_block_factory, adaptive_block_factory,
      /*plain_table_factory=*/nullptr, /*cuckoo_table_factory=*/nullptr));
  options.best_efforts_recovery = false;
  options.enable_blob_indirection = false;
  Reopen(options);
  EXPECT_EQ(Get("live"), live_value);
  EXPECT_EQ(Get("dead"), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, IndirectLazyRangeReadsVerifyChecksums) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.0;
  options.blob_garbage_collection_force_threshold = 0.3;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;
  options.max_open_files = -1;

  Reopen(options);
  const std::string first_value(4096, 'a');
  const std::string second_value(4096, 'b');
  const std::string dead_first_value(4096, 'x');
  const std::string dead_second_value(4096, 'y');
  const WideColumns live_columns{{kDefaultWideColumnName, first_value},
                                 {"meta", second_value}};
  const WideColumns dead_columns{{kDefaultWideColumnName, dead_first_value},
                                 {"meta", dead_second_value}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "",
                           live_columns));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "dead",
                           dead_columns));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(original_blob_files.size(), 1U);

  const auto verify_ranges = [&](bool bounded, bool async_io) {
    ReadOptions read_options;
    ASSERT_TRUE(read_options.verify_checksums);
    const std::string upper_bound_key = EncodeBlobGcCarrierKey(1);
    const Slice upper_bound(upper_bound_key);
    read_options.iterate_upper_bound = bounded ? &upper_bound : nullptr;
    read_options.async_io = async_io;

    LazyWideColumns scalar;
    ASSERT_OK(db_->GetEntityLazy(read_options, db_->DefaultColumnFamily(), "",
                                 &scalar));
    ASSERT_EQ(scalar.size(), 2U);
    PinnableSlice scalar_result;
    ASSERT_OK(scalar.ResolveColumnRange(scalar[0], /*offset=*/17,
                                        /*length=*/101, &scalar_result));
    EXPECT_EQ(scalar_result, first_value.substr(17, 101));

    LazyWideColumns batch;
    ASSERT_OK(db_->GetEntityLazy(read_options, db_->DefaultColumnFamily(), "",
                                 &batch));
    ASSERT_EQ(batch.size(), 2U);
    std::array<PinnableSlice, 2> results;
    std::array<Status, 2> statuses;
    std::vector<LazyColumnReadRequest> requests(2);
    requests[0].column = &batch[0];
    requests[0].offset = 29;
    requests[0].length = 83;
    requests[0].result = &results[0];
    requests[0].status = &statuses[0];
    requests[1].column = &batch[1];
    requests[1].offset = 41;
    requests[1].length = 97;
    requests[1].result = &results[1];
    requests[1].status = &statuses[1];
    ASSERT_OK(batch.MultiResolve(requests));
    ASSERT_OK(statuses[0]);
    EXPECT_EQ(results[0], first_value.substr(29, 83));
    ASSERT_OK(statuses[1]);
    EXPECT_EQ(results[1], second_value.substr(41, 97));
  };

  {
    SCOPED_TRACE("identity route");
    verify_ranges(/*bounded=*/false, /*async_io=*/false);
  }

  ASSERT_OK(Delete("dead"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, /*begin=*/nullptr,
                              /*end=*/nullptr));
  ASSERT_OK(dbfull()->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(dbfull()->TEST_WaitForPurge());

  const std::vector<uint64_t> carrier_files = GetBlobFileNumbers();
  ASSERT_EQ(carrier_files.size(), 1U);
  ASSERT_NE(carrier_files.front(), original_blob_files.front());
  {
    SCOPED_TRACE("carrier route with cold async lookup");
    verify_ranges(/*bounded=*/false, /*async_io=*/true);
  }
  {
    SCOPED_TRACE("carrier route with foreign user bound");
    verify_ranges(/*bounded=*/true, /*async_io=*/false);
  }
}

TEST_F(DBBlobBasicTest, StandaloneBlobGCBatchesRootCensus) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.0;
  options.blob_garbage_collection_force_threshold = 0.3;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;

  Reopen(options);
  constexpr size_t kValueSize = 4096;
  const std::string first_live(kValueSize, 'a');
  const std::string second_live(kValueSize, 'b');
  ASSERT_OK(Put("first-live", first_live));
  ASSERT_OK(Put("first-dead", std::string(kValueSize, 'x')));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("second-live", second_live));
  ASSERT_OK(Put("second-dead", std::string(kValueSize, 'y')));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(original_blob_files.size(), 2U);

  ASSERT_OK(Delete("first-dead"));
  ASSERT_OK(Delete("second-dead"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, /*begin=*/nullptr,
                              /*end=*/nullptr));

  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);
  ASSERT_EQ(
      cfd->current()->storage_info()->BlobFilesForStandaloneGCCensus().size(),
      2U);

  std::atomic<uint64_t> census_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:CensusStarted",
      [&](void*) { census_count.fetch_add(1, std::memory_order_relaxed); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  EXPECT_EQ(census_count.load(std::memory_order_relaxed), 1U);
  const std::vector<uint64_t> relocated_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(relocated_blob_files.size(), 2U);
  for (uint64_t original : original_blob_files) {
    EXPECT_EQ(std::find(relocated_blob_files.begin(),
                        relocated_blob_files.end(), original),
              relocated_blob_files.end());
  }
  EXPECT_EQ(Get("first-live"), first_live);
  EXPECT_EQ(Get("second-live"), second_live);
  EXPECT_EQ(Get("first-dead"), "NOT_FOUND");
  EXPECT_EQ(Get("second-dead"), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, StandaloneBlobGCRejectsReservedRouteBeforeCensus) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.0;
  options.blob_garbage_collection_force_threshold = 0.3;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);
  ASSERT_OK(Put("live", std::string(1000, 'a')));
  ASSERT_OK(Put("dead", std::string(1000, 'b')));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("dead"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, /*begin=*/nullptr,
                              /*end=*/nullptr));

  std::atomic<bool> first_validation{true};
  std::atomic<bool> early_rejection{false};
  std::atomic<bool> census_before_rejection{false};
  std::atomic<uint64_t> census_count{0};
  std::atomic<uint64_t> output_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:RouteReserved", [&](void* arg) {
        if (first_validation.exchange(false, std::memory_order_acq_rel)) {
          *static_cast<bool*>(arg) = true;
          early_rejection.store(true, std::memory_order_release);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:CensusStarted", [&](void*) {
        if (!early_rejection.load(std::memory_order_acquire)) {
          census_before_rejection.store(true, std::memory_order_release);
        }
        census_count.fetch_add(1, std::memory_order_relaxed);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:OutputCreated",
      [&](void*) { output_count.fetch_add(1, std::memory_order_relaxed); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  EXPECT_TRUE(early_rejection.load(std::memory_order_acquire));
  EXPECT_FALSE(census_before_rejection.load(std::memory_order_acquire));
  EXPECT_EQ(census_count.load(std::memory_order_relaxed), 1U);
  EXPECT_EQ(output_count.load(std::memory_order_relaxed), 1U);
  EXPECT_EQ(Get("live"), std::string(1000, 'a'));
  EXPECT_EQ(Get("dead"), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, StandaloneBlobGCRelocatesWideColumnBlobs) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.0;
  options.blob_garbage_collection_force_threshold = 0.3;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;

  Reopen(options);
  const std::string live_value(1000, 'a');
  const std::string live_meta(1000, 'b');
  const std::string deleted_value(1000, 'c');
  const std::string deleted_meta(1000, 'd');
  const WideColumns live_columns{{kDefaultWideColumnName, live_value},
                                 {"meta", live_meta}};
  const WideColumns deleted_columns{{kDefaultWideColumnName, deleted_value},
                                    {"meta", deleted_meta}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "key0",
                           live_columns));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "key1",
                           deleted_columns));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(original_blob_files.size(), 1);

  ASSERT_OK(Delete("key1"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, /*begin=*/nullptr,
                              /*end=*/nullptr));

  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);
  const auto candidate =
      cfd->current()->storage_info()->BlobFileForStandaloneGC();
  ASSERT_NE(candidate, nullptr);
  EXPECT_EQ(candidate->GetOriginFileNumber(), original_blob_files.front());
  EXPECT_EQ(candidate->GetGarbageBlobCount(), 2);

  ASSERT_OK(dbfull()->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(dbfull()->TEST_WaitForPurge());

  const std::vector<uint64_t> relocated_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(relocated_blob_files.size(), 1);
  EXPECT_NE(relocated_blob_files.front(), original_blob_files.front());

  PinnableWideColumns result;
  ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "key0",
                           &result));
  EXPECT_EQ(result.columns(), live_columns);
  result.Reset();
  EXPECT_TRUE(
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "key1", &result)
          .IsNotFound());
}

TEST_F(DBBlobBasicTest, StandaloneBlobGCHonorsCompactionAbort) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.0;
  options.blob_garbage_collection_force_threshold = 0.3;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;

  struct AbortCase {
    const char* sync_point;
    const char* abort_flag_sync_point;
    bool abort_all;
  };
  const std::array<AbortCase, 2> abort_cases{{
      {"DBImpl::RunStandaloneBlobGC:DuringCensus",
       "DBImpl::AbortAllCompactions:FlagSet", true},
      {"DBImpl::RunStandaloneBlobGC:DuringRelocation",
       "DBImpl::AbortCompactions:FlagSet", false},
  }};

  constexpr size_t kValueSize = 4096;
  for (const AbortCase& abort_case : abort_cases) {
    DestroyAndReopen(options);
    ASSERT_OK(Put("key0", std::string(kValueSize, 'a')));
    ASSERT_OK(Put("key1", std::string(kValueSize, 'b')));
    ASSERT_OK(Put("key2", std::string(kValueSize, 'c')));
    ASSERT_OK(Flush());
    ASSERT_OK(Delete("key1"));
    ASSERT_OK(Flush());

    CompactRangeOptions compact_options;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    compact_options.blob_garbage_collection_policy =
        BlobGarbageCollectionPolicy::kDisable;
    ASSERT_OK(db_->CompactRange(compact_options, /*begin=*/nullptr,
                                /*end=*/nullptr));
    const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
    ASSERT_EQ(original_blob_files.size(), 1U);
    const uint64_t original_blob_file = original_blob_files.front();

    std::atomic<bool> start_abort{false};
    std::atomic<bool> abort_flag_set{false};
    std::atomic<bool> abort_triggered{false};
    std::atomic<uint64_t> output_file_number{0};
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::RunStandaloneBlobGC:OutputCreated", [&](void* arg) {
          output_file_number.store(*static_cast<uint64_t*>(arg),
                                   std::memory_order_release);
        });
    SyncPoint::GetInstance()->SetCallBack(
        abort_case.abort_flag_sync_point,
        [&](void*) { abort_flag_set.store(true, std::memory_order_release); });
    SyncPoint::GetInstance()->SetCallBack(abort_case.sync_point, [&](void*) {
      if (!abort_triggered.exchange(true, std::memory_order_acq_rel)) {
        start_abort.store(true, std::memory_order_release);
        while (!abort_flag_set.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
      }
    });
    SyncPoint::GetInstance()->EnableProcessing();

    std::thread abort_thread([&]() {
      while (!start_abort.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      if (abort_case.abort_all) {
        dbfull()->AbortAllCompactions();
      } else {
        dbfull()->AbortCompactions(db_->DefaultColumnFamily());
      }
    });

    ASSERT_OK(dbfull()->EnableAutoCompaction({db_->DefaultColumnFamily()}));
    abort_thread.join();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ASSERT_TRUE(abort_triggered.load(std::memory_order_acquire));
    const uint64_t aborted_output =
        output_file_number.load(std::memory_order_acquire);
    if (aborted_output != 0) {
      EXPECT_TRUE(
          env_->FileExists(BlobFileName(dbname_, aborted_output)).IsNotFound());
    }
    EXPECT_EQ(Get("key0"), std::string(kValueSize, 'a'));
    EXPECT_EQ(Get("key1"), "NOT_FOUND");
    EXPECT_EQ(Get("key2"), std::string(kValueSize, 'c'));

    if (abort_case.abort_all) {
      dbfull()->ResumeAllCompactions();
    } else {
      dbfull()->ResumeCompactions(db_->DefaultColumnFamily());
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    const std::vector<uint64_t> resumed_blob_files = GetBlobFileNumbers();
    EXPECT_EQ(std::find(resumed_blob_files.begin(), resumed_blob_files.end(),
                        original_blob_file),
              resumed_blob_files.end());
  }
}

TEST_F(DBBlobBasicTest, StandaloneBlobGCDeletesOutputWhenColumnFamilyDrops) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto* const sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Options options = GetDefaultOptions();
  options.sst_file_manager = sst_file_manager;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.0;
  options.blob_garbage_collection_force_threshold = 0.3;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"target"}, options);

  ASSERT_OK(Put(1, "key0", std::string(1000, 'a')));
  ASSERT_OK(Put(1, "key1", std::string(1000, 'b')));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Delete(1, "key1"));
  ASSERT_OK(Flush(1));

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, handles_[1], /*begin=*/nullptr,
                              /*end=*/nullptr));

  std::atomic<uint64_t> output_file_number{0};
  std::atomic<bool> drop_callback_ran{false};
  std::atomic<bool> drop_succeeded{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:OutputCreated", [&](void* arg) {
        output_file_number.store(*static_cast<uint64_t*>(arg),
                                 std::memory_order_release);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:AfterBlobFileSync", [&](void*) {
        if (!drop_callback_ran.exchange(true, std::memory_order_acq_rel)) {
          drop_succeeded.store(db_->DropColumnFamily(handles_[1]).ok(),
                               std::memory_order_release);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->EnableAutoCompaction({handles_[1]}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(drop_callback_ran.load(std::memory_order_acquire));
  ASSERT_TRUE(drop_succeeded.load(std::memory_order_acquire));
  const uint64_t unpublished_file_number =
      output_file_number.load(std::memory_order_acquire);
  ASSERT_NE(unpublished_file_number, 0U);
  const std::string unpublished_path =
      BlobFileName(dbname_, unpublished_file_number);
  EXPECT_TRUE(env_->FileExists(unpublished_path).IsNotFound());
  EXPECT_EQ(sfm->GetTrackedFiles().count(unpublished_path), 0U);
}

TEST_F(DBBlobBasicTest, StandaloneBlobGCKeepsAmbiguousManifestOutput) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.0;
  options.blob_garbage_collection_force_threshold = 0.3;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;

  Reopen(options);
  const std::string live_value(4096, 'a');
  ASSERT_OK(Put("live", live_value));
  ASSERT_OK(Put("dead", std::string(4096, 'b')));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("dead"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, /*begin=*/nullptr,
                              /*end=*/nullptr));

  std::atomic<uint64_t> output_file_number{0};
  std::atomic<bool> gc_manifest_started{false};
  std::atomic<bool> manifest_error_injected{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:OutputCreated", [&](void* arg) {
        output_file_number.store(*static_cast<uint64_t*>(arg),
                                 std::memory_order_release);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:BeforeManifest", [&](void*) {
        gc_manifest_started.store(true, std::memory_order_release);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        if (gc_manifest_started.load(std::memory_order_acquire) &&
            !manifest_error_injected.exchange(true,
                                              std::memory_order_acq_rel)) {
          *static_cast<IOStatus*>(arg) =
              IOStatus::IOError("injected ambiguous MANIFEST error");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_NOK(dbfull()->TEST_WaitForCompact());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(manifest_error_injected.load(std::memory_order_acquire));
  const uint64_t carrier_file_number =
      output_file_number.load(std::memory_order_acquire);
  ASSERT_NE(carrier_file_number, 0U);
  const autovector<uint64_t> quarantined_files =
      dbfull()->TEST_GetFilesToQuarantine();
  EXPECT_NE(std::find(quarantined_files.begin(), quarantined_files.end(),
                      carrier_file_number),
            quarantined_files.end());
  ASSERT_OK(env_->FileExists(BlobFileName(dbname_, carrier_file_number)));

  // The injected error happened after SyncManifest, so recovery can replay the
  // new route. Its quarantined carrier must still be present for that route.
  Reopen(options);
  EXPECT_EQ(Get("live"), live_value);
  EXPECT_EQ(Get("dead"), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, QueuedBlobGCDropDoesNotOutliveColumnFamilyCache) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.0;
  options.blob_garbage_collection_force_threshold = 0.3;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"target"}, options);

  ASSERT_OK(Put(1, "key0", std::string(1000, 'a')));
  ASSERT_OK(Put(1, "key1", std::string(1000, 'b')));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Delete(1, "key1"));
  ASSERT_OK(Flush(1));

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, handles_[1], /*begin=*/nullptr,
                              /*end=*/nullptr));

  std::atomic<bool> worker_blocked{false};
  std::atomic<bool> release_worker{false};
  std::atomic<bool> output_created{false};
  SyncPoint::GetInstance()->SetCallBack("DBImpl::BGWorkCompaction", [&](void*) {
    worker_blocked.store(true, std::memory_order_release);
    while (!release_worker.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:OutputCreated",
      [&](void*) { output_created.store(true, std::memory_order_release); });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->EnableAutoCompaction({handles_[1]}));
  while (!worker_blocked.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[1]));
  handles_.resize(1);
  release_worker.store(true, std::memory_order_release);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_FALSE(output_created.load(std::memory_order_acquire));
}

TEST_F(DBBlobBasicTest, BlobFileWritableFileMaxBufferSize) {
  constexpr uint64_t kBlobWriterBufferSize = 128 * 1024;

  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.writable_file_max_buffer_size = 1024 * 1024;
  options.blob_file_writable_file_max_buffer_size = kBlobWriterBufferSize;

  Reopen(options);

  std::atomic<int> matching_blob_writer_count{0};
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::WritableFileWriter:0", [&](void* arg) {
        const uint64_t max_buffer_size =
            static_cast<uint64_t>(reinterpret_cast<uintptr_t>(arg));
        if (max_buffer_size == kBlobWriterBufferSize) {
          ++matching_blob_writer_count;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key", std::string(1024, 'v')));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_GE(matching_blob_writer_count.load(), 1);
}

TEST_F(DBBlobBasicTest,
       DirectWriteBlobFileWritableFileMaxBufferSizeSetOptions) {
  constexpr uint64_t kBlobWriterBufferSize = 64 * 1024;

  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  options.min_blob_size = 0;
  options.blob_file_size = 200;
  options.writable_file_max_buffer_size = 1024 * 1024;

  Reopen(options);

  std::atomic<int> matching_blob_writer_count{0};
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::WritableFileWriter:0", [&](void* arg) {
        const uint64_t max_buffer_size =
            static_cast<uint64_t>(reinterpret_cast<uintptr_t>(arg));
        if (max_buffer_size == kBlobWriterBufferSize) {
          ++matching_blob_writer_count;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("first", std::string(300, 'a')));
  ASSERT_OK(db_->SetOptions({{"blob_file_writable_file_max_buffer_size",
                              std::to_string(kBlobWriterBufferSize)}}));
  ASSERT_OK(Put("second", std::string(300, 'b')));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_GE(matching_blob_writer_count.load(), 1);
}

TEST_F(DBBlobBasicTest, EmptyValueNotStoredAsBlob) {
  // Regression test for crash when empty blob value is evicted to
  // CompressedSecondaryCache (T261142690). Empty values should always be
  // stored inline in the SST, never as blobs, even with min_blob_size=0.
  // A BlobIndex for an empty value is strictly larger than the value itself,
  // so storing it as a blob is pure overhead.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);

  // Write an empty value and a non-empty value.
  ASSERT_OK(Put("empty_key", ""));
  constexpr char blob_value[] = "blob_value";
  ASSERT_OK(Put("nonempty_key", blob_value));
  ASSERT_OK(Flush());

  // Both values should be readable.
  ASSERT_EQ(Get("empty_key"), "");
  ASSERT_EQ(Get("nonempty_key"), blob_value);

  // The empty value should be stored inline (readable from block cache
  // without blob file I/O), while the non-empty value requires blob I/O.
  ReadOptions ro;
  ro.read_tier = kBlockCacheTier;

  PinnableSlice result;
  ASSERT_OK(db_->Get(ro, db_->DefaultColumnFamily(), "empty_key", &result));
  ASSERT_EQ(result, "");

  result.Reset();
  ASSERT_TRUE(db_->Get(ro, db_->DefaultColumnFamily(), "nonempty_key", &result)
                  .IsIncomplete());
}

TEST_F(DBBlobBasicTest, GetBlobFromCache) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 2 << 20;  // 2MB
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.enable_blob_files = true;
  options.blob_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  ASSERT_OK(Put(key, blob_value));

  ASSERT_OK(Flush());

  ReadOptions read_options;

  read_options.fill_cache = false;

  {
    PinnableSlice result;

    read_options.read_tier = kReadAllTier;
    ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, blob_value);

    result.Reset();
    read_options.read_tier = kBlockCacheTier;

    // Try again with no I/O allowed. Since we didn't re-fill the cache, the
    // blob itself can only be read from the blob file, so the read should
    // return Incomplete.
    ASSERT_TRUE(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result)
                    .IsIncomplete());
    ASSERT_TRUE(result.empty());
  }

  read_options.fill_cache = true;

  {
    PinnableSlice result;

    read_options.read_tier = kReadAllTier;
    ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, blob_value);

    result.Reset();
    read_options.read_tier = kBlockCacheTier;

    // Try again with no I/O allowed. The table and the necessary blocks/blobs
    // should already be in their respective caches.
    ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, blob_value);
  }
}

TEST_F(DBBlobBasicTest, IterateBlobsFromCache) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 2 << 20;  // 2MB
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.enable_blob_files = true;
  options.blob_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  options.statistics = CreateDBStatistics();

  Reopen(options);

  int num_blobs = 5;
  std::vector<std::string> keys;
  std::vector<std::string> blobs;

  for (int i = 0; i < num_blobs; ++i) {
    keys.push_back("key" + std::to_string(i));
    blobs.push_back("blob" + std::to_string(i));
    ASSERT_OK(Put(keys[i], blobs[i]));
  }
  ASSERT_OK(Flush());

  ReadOptions read_options;

  {
    read_options.fill_cache = false;
    read_options.read_tier = kReadAllTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key().ToString(), keys[i]);
      ASSERT_EQ(iter->value().ToString(), blobs[i]);
      ++i;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
    ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD), 0);
  }

  {
    read_options.fill_cache = false;
    read_options.read_tier = kBlockCacheTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    // Try again with no I/O allowed. Since we didn't re-fill the cache,
    // the blob itself can only be read from the blob file, so iter->Valid()
    // should be false.
    iter->SeekToFirst();
    ASSERT_NOK(iter->status());
    ASSERT_FALSE(iter->Valid());
    ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD), 0);
  }

  {
    read_options.fill_cache = true;
    read_options.read_tier = kReadAllTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    // Read blobs from the file and refill the cache.
    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key().ToString(), keys[i]);
      ASSERT_EQ(iter->value().ToString(), blobs[i]);
      ++i;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
    ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD),
              num_blobs);
  }

  {
    read_options.fill_cache = false;
    read_options.read_tier = kBlockCacheTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    // Try again with no I/O allowed. The table and the necessary blocks/blobs
    // should already be in their respective caches.
    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key().ToString(), keys[i]);
      ASSERT_EQ(iter->value().ToString(), blobs[i]);
      ++i;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
    ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD), 0);
  }
}

TEST_F(DBBlobBasicTest, IterateBlobsFromCachePinning) {
  constexpr size_t min_blob_size = 6;

  Options options = GetDefaultOptions();

  LRUCacheOptions cache_options;
  cache_options.capacity = 2048;
  cache_options.num_shard_bits = 0;
  cache_options.metadata_charge_policy = kDontChargeCacheMetadata;

  options.blob_cache = NewLRUCache(cache_options);
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;

  Reopen(options);

  // Put then iterate over three key-values. The second value is below the size
  // limit and is thus stored inline; the other two are stored separately as
  // blobs. We expect to have something pinned in the cache iff we are
  // positioned on a blob.

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "long_value";
  static_assert(sizeof(first_value) - 1 >= min_blob_size,
                "first_value too short to be stored as blob");

  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "short";
  static_assert(sizeof(second_value) - 1 < min_blob_size,
                "second_value too long to be inlined");

  ASSERT_OK(Put(second_key, second_value));

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "other_long_value";
  static_assert(sizeof(third_value) - 1 >= min_blob_size,
                "third_value too short to be stored as blob");

  ASSERT_OK(Put(third_key, third_value));

  ASSERT_OK(Flush());

  {
    ReadOptions read_options;
    read_options.fill_cache = true;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), first_key);
    ASSERT_EQ(iter->value(), first_value);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), second_key);
    ASSERT_EQ(iter->value(), second_value);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), third_key);
    ASSERT_EQ(iter->value(), third_value);

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  {
    ReadOptions read_options;
    read_options.fill_cache = false;
    read_options.read_tier = kBlockCacheTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), first_key);
    ASSERT_EQ(iter->value(), first_value);
    ASSERT_GT(options.blob_cache->GetPinnedUsage(), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), second_key);
    ASSERT_EQ(iter->value(), second_value);
    ASSERT_EQ(options.blob_cache->GetPinnedUsage(), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), third_key);
    ASSERT_EQ(iter->value(), third_value);
    ASSERT_GT(options.blob_cache->GetPinnedUsage(), 0);

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(options.blob_cache->GetPinnedUsage(), 0);
  }

  {
    ReadOptions read_options;
    read_options.fill_cache = false;
    read_options.read_tier = kBlockCacheTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), third_key);
    ASSERT_EQ(iter->value(), third_value);
    ASSERT_GT(options.blob_cache->GetPinnedUsage(), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), second_key);
    ASSERT_EQ(iter->value(), second_value);
    ASSERT_EQ(options.blob_cache->GetPinnedUsage(), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), first_key);
    ASSERT_EQ(iter->value(), first_value);
    ASSERT_GT(options.blob_cache->GetPinnedUsage(), 0);

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(options.blob_cache->GetPinnedUsage(), 0);
  }
}

TEST_F(DBBlobBasicTest, IterateBlobsAllowUnpreparedValue) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;

  Reopen(options);

  constexpr size_t num_blobs = 5;
  std::vector<std::string> keys;
  std::vector<std::string> blobs;

  for (size_t i = 0; i < num_blobs; ++i) {
    keys.emplace_back("key" + std::to_string(i));
    blobs.emplace_back("blob" + std::to_string(i));
    ASSERT_OK(Put(keys[i], blobs[i]));
  }

  ASSERT_OK(Flush());

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

  {
    size_t i = 0;

    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_EQ(iter->key(), keys[i]);
      ASSERT_TRUE(iter->value().empty());
      ASSERT_OK(iter->status());

      ASSERT_TRUE(iter->PrepareValue());

      ASSERT_EQ(iter->key(), keys[i]);
      ASSERT_EQ(iter->value(), blobs[i]);
      ASSERT_OK(iter->status());

      ++i;
    }

    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
  }

  {
    size_t i = 0;

    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      ASSERT_EQ(iter->key(), keys[num_blobs - 1 - i]);
      ASSERT_TRUE(iter->value().empty());
      ASSERT_OK(iter->status());

      ASSERT_TRUE(iter->PrepareValue());

      ASSERT_EQ(iter->key(), keys[num_blobs - 1 - i]);
      ASSERT_EQ(iter->value(), blobs[num_blobs - 1 - i]);
      ASSERT_OK(iter->status());

      ++i;
    }

    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
  }

  {
    size_t i = 1;

    for (iter->Seek(keys[i]); iter->Valid(); iter->Next()) {
      ASSERT_EQ(iter->key(), keys[i]);
      ASSERT_TRUE(iter->value().empty());
      ASSERT_OK(iter->status());

      ASSERT_TRUE(iter->PrepareValue());

      ASSERT_EQ(iter->key(), keys[i]);
      ASSERT_EQ(iter->value(), blobs[i]);
      ASSERT_OK(iter->status());

      ++i;
    }

    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
  }

  {
    size_t i = 1;

    for (iter->SeekForPrev(keys[num_blobs - 1 - i]); iter->Valid();
         iter->Prev()) {
      ASSERT_EQ(iter->key(), keys[num_blobs - 1 - i]);
      ASSERT_TRUE(iter->value().empty());
      ASSERT_OK(iter->status());

      ASSERT_TRUE(iter->PrepareValue());

      ASSERT_EQ(iter->key(), keys[num_blobs - 1 - i]);
      ASSERT_EQ(iter->value(), blobs[num_blobs - 1 - i]);
      ASSERT_OK(iter->status());

      ++i;
    }

    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
  }
}

TEST_F(DBBlobBasicTest, MultiGetBlobs) {
  constexpr size_t min_blob_size = 6;

  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;

  Reopen(options);

  // Put then retrieve three key-values. The first value is below the size limit
  // and is thus stored inline; the other two are stored separately as blobs.
  constexpr size_t num_keys = 3;

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "short";
  static_assert(sizeof(first_value) - 1 < min_blob_size,
                "first_value too long to be inlined");

  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "long_value";
  static_assert(sizeof(second_value) - 1 >= min_blob_size,
                "second_value too short to be stored as blob");

  ASSERT_OK(Put(second_key, second_value));

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "other_long_value";
  static_assert(sizeof(third_value) - 1 >= min_blob_size,
                "third_value too short to be stored as blob");

  ASSERT_OK(Put(third_key, third_value));

  ASSERT_OK(Flush());

  ReadOptions read_options;

  std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }

  // Try again with no I/O allowed. The table and the necessary blocks should
  // already be in their respective caches. The first (inlined) value should be
  // successfully read; however, the two blob values could only be read from the
  // blob file, so for those the read should return Incomplete.
  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_TRUE(statuses[1].IsIncomplete());

    ASSERT_TRUE(statuses[2].IsIncomplete());
  }
}

TEST_F(DBBlobBasicTest, MultiGetBlobsFromCache) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 2 << 20;  // 2MB
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  constexpr size_t min_blob_size = 6;
  options.min_blob_size = min_blob_size;
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.blob_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  DestroyAndReopen(options);

  // Put then retrieve three key-values. The first value is below the size limit
  // and is thus stored inline; the other two are stored separately as blobs.
  constexpr size_t num_keys = 3;

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "short";
  static_assert(sizeof(first_value) - 1 < min_blob_size,
                "first_value too long to be inlined");

  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "long_value";
  static_assert(sizeof(second_value) - 1 >= min_blob_size,
                "second_value too short to be stored as blob");

  ASSERT_OK(Put(second_key, second_value));

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "other_long_value";
  static_assert(sizeof(third_value) - 1 >= min_blob_size,
                "third_value too short to be stored as blob");

  ASSERT_OK(Put(third_key, third_value));

  ASSERT_OK(Flush());

  ReadOptions read_options;
  read_options.fill_cache = false;

  std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }

  // Try again with no I/O allowed. The first (inlined) value should be
  // successfully read; however, the two blob values could only be read from the
  // blob file, so for those the read should return Incomplete.
  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_TRUE(statuses[1].IsIncomplete());

    ASSERT_TRUE(statuses[2].IsIncomplete());
  }

  // Fill the cache when reading blobs from the blob file.
  read_options.read_tier = kReadAllTier;
  read_options.fill_cache = true;

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }

  // Try again with no I/O allowed. All blobs should be successfully read from
  // the cache.
  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }
}

TEST_F(DBBlobBasicTest, MultiGetWithDirectIO) {
  Options options = GetDefaultOptions();

  // First, create an external SST file ["b"].
  const std::string file_path = dbname_ + "/test.sst";
  {
    SstFileWriter sst_file_writer(EnvOptions(), GetDefaultOptions());
    Status s = sst_file_writer.Open(file_path);
    ASSERT_OK(s);
    ASSERT_OK(sst_file_writer.Put("b", "b_value"));
    ASSERT_OK(sst_file_writer.Finish());
  }

  options.enable_blob_files = true;
  options.min_blob_size = 1000;
  options.use_direct_reads = true;
  options.allow_ingest_behind = true;

  // Open DB with fixed-prefix sst-partitioner so that compaction will cut
  // new table file when encountering a new key whose 1-byte prefix changes.
  constexpr size_t key_len = 1;
  options.sst_partitioner_factory =
      NewSstPartitionerFixedPrefixFactory(key_len);

  Status s = TryReopen(options);
  if (s.IsInvalidArgument()) {
    ROCKSDB_GTEST_SKIP("This test requires direct IO support");
    return;
  }
  ASSERT_OK(s);

  constexpr size_t num_keys = 3;
  constexpr size_t blob_size = 3000;

  constexpr char first_key[] = "a";
  const std::string first_blob(blob_size, 'a');
  ASSERT_OK(Put(first_key, first_blob));

  constexpr char second_key[] = "b";
  const std::string second_blob(2 * blob_size, 'b');
  ASSERT_OK(Put(second_key, second_blob));

  constexpr char third_key[] = "d";
  const std::string third_blob(blob_size, 'd');
  ASSERT_OK(Put(third_key, third_blob));

  // first_blob, second_blob and third_blob in the same blob file.
  //      SST                    Blob file
  // L0  ["a",    "b",    "d"]   |'aaaa', 'bbbb', 'dddd'|
  //       |       |       |         ^       ^        ^
  //       |       |       |         |       |        |
  //       |       |       +---------|-------|--------+
  //       |       +-----------------|-------+
  //       +-------------------------+
  ASSERT_OK(Flush());

  constexpr char fourth_key[] = "c";
  const std::string fourth_blob(blob_size, 'c');
  ASSERT_OK(Put(fourth_key, fourth_blob));
  // fourth_blob in another blob file.
  //      SST                    Blob file                 SST     Blob file
  // L0  ["a",    "b",    "d"]   |'aaaa', 'bbbb', 'dddd'|  ["c"]   |'cccc'|
  //       |       |       |         ^       ^        ^      |       ^
  //       |       |       |         |       |        |      |       |
  //       |       |       +---------|-------|--------+      +-------+
  //       |       +-----------------|-------+
  //       +-------------------------+
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));

  // Due to the above sst partitioner, we get 4 L1 files. The blob files are
  // unchanged.
  //                             |'aaaa', 'bbbb', 'dddd'|  |'cccc'|
  //                                 ^       ^     ^         ^
  //                                 |       |     |         |
  // L0                              |       |     |         |
  // L1  ["a"]   ["b"]   ["c"]       |       |   ["d"]       |
  //       |       |       |         |       |               |
  //       |       |       +---------|-------|---------------+
  //       |       +-----------------|-------+
  //       +-------------------------+
  ASSERT_EQ(4, NumTableFilesAtLevel(/*level=*/1));

  {
    // Ingest the external SST file into bottommost level.
    std::vector<std::string> ext_files{file_path};
    IngestExternalFileOptions opts;
    opts.ingest_behind = true;
    ASSERT_OK(
        db_->IngestExternalFile(db_->DefaultColumnFamily(), ext_files, opts));
  }

  // Now the database becomes as follows.
  //                             |'aaaa', 'bbbb', 'dddd'|  |'cccc'|
  //                                 ^       ^     ^         ^
  //                                 |       |     |         |
  // L0                              |       |     |         |
  // L1  ["a"]   ["b"]   ["c"]       |       |   ["d"]       |
  //       |       |       |         |       |               |
  //       |       |       +---------|-------|---------------+
  //       |       +-----------------|-------+
  //       +-------------------------+
  //
  // L6          ["b"]

  {
    // Compact ["b"] to bottommost level.
    Slice begin = Slice(second_key);
    Slice end = Slice(second_key);
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(cro, &begin, &end));
  }

  //                             |'aaaa', 'bbbb', 'dddd'|  |'cccc'|
  //                                 ^       ^     ^         ^
  //                                 |       |     |         |
  // L0                              |       |     |         |
  // L1  ["a"]           ["c"]       |       |   ["d"]       |
  //       |               |         |       |               |
  //       |               +---------|-------|---------------+
  //       |       +-----------------|-------+
  //       +-------|-----------------+
  //               |
  // L6          ["b"]
  ASSERT_EQ(3, NumTableFilesAtLevel(/*level=*/1));
  ASSERT_EQ(1, NumTableFilesAtLevel(/*level=*/6));

  bool called = false;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::MultiRead:AlignedReqs", [&](void* arg) {
        auto* aligned_reqs = static_cast<std::vector<FSReadRequest>*>(arg);
        assert(aligned_reqs);
        ASSERT_EQ(1, aligned_reqs->size());
        called = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::array<Slice, num_keys> keys{{first_key, third_key, second_key}};

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    // The MultiGet(), when constructing the KeyContexts, will process the keys
    // in such order: a, d, b. The reason is that ["a"] and ["d"] are in L1,
    // while ["b"] resides in L6.
    // Consequently, the original FSReadRequest list prepared by
    // Version::MultiGetblob() will be for "a", "d" and "b". It is unsorted as
    // follows:
    //
    // ["a", offset=30, len=3033],
    // ["d", offset=9096, len=3033],
    // ["b", offset=3063, len=6033]
    //
    // If we do not sort them before calling MultiRead() in DirectIO, then the
    // underlying IO merging logic will yield two requests.
    //
    // [offset=0, len=4096] (for "a")
    // [offset=0, len=12288] (result of merging the request for "d" and "b")
    //
    // We need to sort them in Version::MultiGetBlob() so that the underlying
    // IO merging logic in DirectIO mode works as expected. The correct
    // behavior will be one aligned request:
    //
    // [offset=0, len=12288]

    db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ASSERT_TRUE(called);

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_blob);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], third_blob);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], second_blob);
  }
}

TEST_F(DBBlobBasicTest, MultiGetBlobsFromMultipleFiles) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 2 << 20;  // 2MB
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.min_blob_size = 0;
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.blob_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  Reopen(options);

  constexpr size_t kNumBlobFiles = 3;
  constexpr size_t kNumBlobsPerFile = 3;
  constexpr size_t kNumKeys = kNumBlobsPerFile * kNumBlobFiles;

  std::vector<std::string> key_strs;
  std::vector<std::string> value_strs;
  for (size_t i = 0; i < kNumBlobFiles; ++i) {
    for (size_t j = 0; j < kNumBlobsPerFile; ++j) {
      std::string key = "key" + std::to_string(i) + "_" + std::to_string(j);
      std::string value =
          "value_as_blob" + std::to_string(i) + "_" + std::to_string(j);
      ASSERT_OK(Put(key, value));
      key_strs.push_back(key);
      value_strs.push_back(value);
    }
    ASSERT_OK(Flush());
  }
  assert(key_strs.size() == kNumKeys);
  std::array<Slice, kNumKeys> keys;
  for (size_t i = 0; i < keys.size(); ++i) {
    keys[i] = key_strs[i];
  }

  ReadOptions read_options;
  read_options.read_tier = kReadAllTier;
  read_options.fill_cache = false;

  {
    std::array<PinnableSlice, kNumKeys> values;
    std::array<Status, kNumKeys> statuses;
    db_->MultiGet(read_options, db_->DefaultColumnFamily(), kNumKeys,
                  keys.data(), values.data(), statuses.data());

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(value_strs[i], values[i]);
    }
  }

  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, kNumKeys> values;
    std::array<Status, kNumKeys> statuses;
    db_->MultiGet(read_options, db_->DefaultColumnFamily(), kNumKeys,
                  keys.data(), values.data(), statuses.data());

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_TRUE(statuses[i].IsIncomplete());
      ASSERT_TRUE(values[i].empty());
    }
  }

  read_options.read_tier = kReadAllTier;
  read_options.fill_cache = true;

  {
    std::array<PinnableSlice, kNumKeys> values;
    std::array<Status, kNumKeys> statuses;
    db_->MultiGet(read_options, db_->DefaultColumnFamily(), kNumKeys,
                  keys.data(), values.data(), statuses.data());

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(value_strs[i], values[i]);
    }
  }

  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, kNumKeys> values;
    std::array<Status, kNumKeys> statuses;
    db_->MultiGet(read_options, db_->DefaultColumnFamily(), kNumKeys,
                  keys.data(), values.data(), statuses.data());

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(value_strs[i], values[i]);
    }
  }
}

TEST_F(DBBlobBasicTest, GetBlob_CorruptIndex) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  ASSERT_OK(Put(key, blob));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(
      "Version::Get::TamperWithBlobIndex", [](void* arg) {
        Slice* const blob_index = static_cast<Slice*>(arg);
        assert(blob_index);
        assert(!blob_index->empty());
        blob_index->remove_prefix(1);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBBlobBasicTest, MultiGetBlob_CorruptIndex) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;

  DestroyAndReopen(options);

  constexpr size_t kNumOfKeys = 3;
  std::array<std::string, kNumOfKeys> key_strs;
  std::array<std::string, kNumOfKeys> value_strs;
  std::array<Slice, kNumOfKeys + 1> keys;
  for (size_t i = 0; i < kNumOfKeys; ++i) {
    key_strs[i] = "foo" + std::to_string(i);
    value_strs[i] = "blob_value" + std::to_string(i);
    ASSERT_OK(Put(key_strs[i], value_strs[i]));
    keys[i] = key_strs[i];
  }

  constexpr char key[] = "key";
  constexpr char blob[] = "blob";
  ASSERT_OK(Put(key, blob));
  keys[kNumOfKeys] = key;

  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(
      "Version::MultiGet::TamperWithBlobIndex", [&key](void* arg) {
        KeyContext* const key_context = static_cast<KeyContext*>(arg);
        assert(key_context);
        assert(key_context->key);

        if (*(key_context->key) == key) {
          Slice* const blob_index = key_context->value;
          assert(blob_index);
          assert(!blob_index->empty());
          blob_index->remove_prefix(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::array<PinnableSlice, kNumOfKeys + 1> values;
  std::array<Status, kNumOfKeys + 1> statuses;
  db_->MultiGet(ReadOptions(), dbfull()->DefaultColumnFamily(), kNumOfKeys + 1,
                keys.data(), values.data(), statuses.data(),
                /*sorted_input=*/false);
  for (size_t i = 0; i < kNumOfKeys + 1; ++i) {
    if (i != kNumOfKeys) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ("blob_value" + std::to_string(i), values[i]);
    } else {
      ASSERT_TRUE(statuses[i].IsCorruption());
    }
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBBlobBasicTest, MultiGetBlob_ExceedSoftLimit) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr size_t kNumOfKeys = 3;
  std::array<std::string, kNumOfKeys> key_bufs;
  std::array<std::string, kNumOfKeys> value_bufs;
  std::array<Slice, kNumOfKeys> keys;
  for (size_t i = 0; i < kNumOfKeys; ++i) {
    key_bufs[i] = "foo" + std::to_string(i);
    value_bufs[i] = "blob_value" + std::to_string(i);
    ASSERT_OK(Put(key_bufs[i], value_bufs[i]));
    keys[i] = key_bufs[i];
  }
  ASSERT_OK(Flush());

  std::array<PinnableSlice, kNumOfKeys> values;
  std::array<Status, kNumOfKeys> statuses;
  ReadOptions read_opts;
  read_opts.value_size_soft_limit = 1;
  db_->MultiGet(read_opts, dbfull()->DefaultColumnFamily(), kNumOfKeys,
                keys.data(), values.data(), statuses.data(),
                /*sorted_input=*/true);
  for (const auto& s : statuses) {
    ASSERT_TRUE(s.IsAborted());
  }
}

TEST_F(DBBlobBasicTest, GetBlob_InlinedTTLIndex) {
  constexpr uint64_t min_blob_size = 10;

  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob[] = "short";
  static_assert(sizeof(short) - 1 < min_blob_size,
                "Blob too long to be inlined");

  // Fake an inlined TTL blob index.
  std::string blob_index;

  constexpr uint64_t expiration = 1234567890;

  BlobIndex::EncodeInlinedTTL(&blob_index, expiration, blob);

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 0, key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());
}

TEST_F(DBBlobBasicTest, GetBlob_IndexWithInvalidFileNumber) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";

  // Fake a blob index referencing a non-existent blob file.
  std::string blob_index;

  constexpr uint64_t blob_file_number = 1000;
  constexpr uint64_t offset = 1234;
  constexpr uint64_t size = 5678;

  BlobIndex::EncodeBlob(&blob_index, blob_file_number, offset, size,
                        kNoCompression);

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 0, key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());
}

TEST_F(DBBlobBasicTest, GenerateIOTracing) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  std::string trace_file = dbname_ + "/io_trace_file";

  Reopen(options);
  {
    // Create IO trace file
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(
        NewFileTraceWriter(env_, EnvOptions(), trace_file, &trace_writer));
    ASSERT_OK(db_->StartIOTrace(TraceOptions(), std::move(trace_writer)));

    constexpr char key[] = "key";
    constexpr char blob_value[] = "blob_value";

    ASSERT_OK(Put(key, blob_value));
    ASSERT_OK(Flush());
    ASSERT_EQ(Get(key), blob_value);

    ASSERT_OK(db_->EndIOTrace());
    ASSERT_OK(env_->FileExists(trace_file));
  }
  {
    // Parse trace file to check file operations related to blob files are
    // recorded.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(
        NewFileTraceReader(env_, EnvOptions(), trace_file, &trace_reader));
    IOTraceReader reader(std::move(trace_reader));

    IOTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));

    // Read records.
    int blob_files_op_count = 0;
    Status status;
    while (true) {
      IOTraceRecord record;
      status = reader.ReadIOOp(&record);
      if (!status.ok()) {
        break;
      }
      if (record.file_name.find("blob") != std::string::npos) {
        blob_files_op_count++;
      }
    }
    // Assuming blob files will have Append, Close and then Read operations.
    ASSERT_GT(blob_files_op_count, 2);
  }
}

TEST_F(DBBlobBasicTest, BestEffortsRecovery_MissingNewestBlobFile) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  Reopen(options);

  ASSERT_OK(dbfull()->DisableFileDeletions());
  constexpr int kNumTableFiles = 2;
  for (int i = 0; i < kNumTableFiles; ++i) {
    for (char ch = 'a'; ch != 'c'; ++ch) {
      std::string key(1, ch);
      ASSERT_OK(Put(key, "value" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
  }

  Close();

  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  std::string blob_file_path;
  uint64_t max_blob_file_num = kInvalidBlobFileNumber;
  for (const auto& fname : files) {
    uint64_t file_num = 0;
    FileType type;
    if (ParseFileName(fname, &file_num, /*info_log_name_prefix=*/"", &type) &&
        type == kBlobFile) {
      if (file_num > max_blob_file_num) {
        max_blob_file_num = file_num;
        blob_file_path = dbname_ + "/" + fname;
      }
    }
  }
  ASSERT_OK(env_->DeleteFile(blob_file_path));

  options.best_efforts_recovery = true;
  Reopen(options);
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "a", &value));
  ASSERT_EQ("value" + std::to_string(kNumTableFiles - 2), value);
}

TEST_F(DBBlobBasicTest, GetMergeBlobWithPut) {
  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  ASSERT_OK(Put("Key1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("Key1", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("Key1", "v3"));
  ASSERT_OK(Flush());

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "Key1", &value));
  ASSERT_EQ(Get("Key1"), "v1,v2,v3");
}

TEST_F(DBBlobBasicTest, GetMergeBlobFromMemoryTier) {
  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  ASSERT_OK(Put(Key(0), "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge(Key(0), "v2"));
  ASSERT_OK(Flush());

  // Regular `Get()` loads data block to cache.
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), Key(0), &value));
  ASSERT_EQ("v1,v2", value);

  // Base value blob is still uncached, so an in-memory read will fail.
  ReadOptions read_options;
  read_options.read_tier = kBlockCacheTier;
  ASSERT_TRUE(db_->Get(read_options, Key(0), &value).IsIncomplete());
}

TEST_F(DBBlobBasicTest, MultiGetMergeBlobWithPut) {
  constexpr size_t num_keys = 3;

  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  ASSERT_OK(Put("Key0", "v0_0"));
  ASSERT_OK(Put("Key1", "v1_0"));
  ASSERT_OK(Put("Key2", "v2_0"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("Key0", "v0_1"));
  ASSERT_OK(Merge("Key1", "v1_1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("Key0", "v0_2"));
  ASSERT_OK(Flush());

  std::array<Slice, num_keys> keys{{"Key0", "Key1", "Key2"}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                keys.data(), values.data(), statuses.data());

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], "v0_0,v0_1,v0_2");

  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1], "v1_0,v1_1");

  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], "v2_0");
}

TEST_F(DBBlobBasicTest, Properties) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key1[] = "key1";
  constexpr size_t key1_size = sizeof(key1) - 1;

  constexpr char key2[] = "key2";
  constexpr size_t key2_size = sizeof(key2) - 1;

  constexpr char key3[] = "key3";
  constexpr size_t key3_size = sizeof(key3) - 1;

  constexpr char blob[] = "00000000000000";
  constexpr size_t blob_size = sizeof(blob) - 1;

  constexpr char longer_blob[] = "00000000000000000000";
  constexpr size_t longer_blob_size = sizeof(longer_blob) - 1;

  ASSERT_OK(Put(key1, blob));
  ASSERT_OK(Put(key2, longer_blob));
  ASSERT_OK(Flush());

  constexpr size_t first_blob_file_expected_size =
      BlobLogHeader::kSize +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(key1_size) + blob_size +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(key2_size) +
      longer_blob_size + BlobLogFooter::kSize;

  ASSERT_OK(Put(key3, blob));
  ASSERT_OK(Flush());

  constexpr size_t second_blob_file_expected_size =
      BlobLogHeader::kSize +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(key3_size) + blob_size +
      BlobLogFooter::kSize;

  constexpr size_t total_expected_size =
      first_blob_file_expected_size + second_blob_file_expected_size;

  // Number of blob files
  uint64_t num_blob_files = 0;
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kNumBlobFiles, &num_blob_files));
  ASSERT_EQ(num_blob_files, 2);

  // Total size of live blob files
  uint64_t live_blob_file_size = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kLiveBlobFileSize,
                                  &live_blob_file_size));
  ASSERT_EQ(live_blob_file_size, total_expected_size);

  // Total amount of garbage in live blob files
  {
    uint64_t live_blob_file_garbage_size = 0;
    ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kLiveBlobFileGarbageSize,
                                    &live_blob_file_garbage_size));
    ASSERT_EQ(live_blob_file_garbage_size, 0);
  }

  // Total size of all blob files across all versions
  // Note: this should be the same as above since we only have one
  // version at this point.
  uint64_t total_blob_file_size = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kTotalBlobFileSize,
                                  &total_blob_file_size));
  ASSERT_EQ(total_blob_file_size, total_expected_size);

  // Delete key2 to create some garbage
  ASSERT_OK(Delete(key2));
  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  constexpr size_t expected_garbage_size =
      BlobLogRecord::CalculateAdjustmentForRecordHeader(key2_size) +
      longer_blob_size;

  constexpr double expected_space_amp =
      static_cast<double>(total_expected_size) /
      (total_expected_size - expected_garbage_size);

  // Blob file stats
  std::string blob_stats;
  ASSERT_TRUE(db_->GetProperty(DB::Properties::kBlobStats, &blob_stats));

  std::ostringstream oss;
  oss << "Number of blob files: 2\nTotal size of blob files: "
      << total_expected_size
      << "\nTotal size of garbage in blob files: " << expected_garbage_size
      << "\nBlob file space amplification: " << expected_space_amp << '\n';

  ASSERT_EQ(blob_stats, oss.str());

  // Total amount of garbage in live blob files
  {
    uint64_t live_blob_file_garbage_size = 0;
    ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kLiveBlobFileGarbageSize,
                                    &live_blob_file_garbage_size));
    ASSERT_EQ(live_blob_file_garbage_size, expected_garbage_size);
  }
}

TEST_F(DBBlobBasicTest, PropertiesMultiVersion) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key1[] = "key1";
  constexpr char key2[] = "key2";
  constexpr char key3[] = "key3";

  constexpr size_t key_size = sizeof(key1) - 1;
  static_assert(sizeof(key2) - 1 == key_size, "unexpected size: key2");
  static_assert(sizeof(key3) - 1 == key_size, "unexpected size: key3");

  constexpr char blob[] = "0000000000";
  constexpr size_t blob_size = sizeof(blob) - 1;

  ASSERT_OK(Put(key1, blob));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(key2, blob));
  ASSERT_OK(Flush());

  // Create an iterator to keep the current version alive
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  ASSERT_OK(iter->status());

  // Note: the Delete and subsequent compaction results in the first blob file
  // not making it to the final version. (It is still part of the previous
  // version kept alive by the iterator though.) On the other hand, the Put
  // results in a third blob file.
  ASSERT_OK(Delete(key1));
  ASSERT_OK(Put(key3, blob));
  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  // Total size of all blob files across all versions: between the two versions,
  // we should have three blob files of the same size with one blob each.
  // The version kept alive by the iterator contains the first and the second
  // blob file, while the final version contains the second and the third blob
  // file. (The second blob file is thus shared by the two versions but should
  // be counted only once.)
  uint64_t total_blob_file_size = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kTotalBlobFileSize,
                                  &total_blob_file_size));
  ASSERT_EQ(total_blob_file_size,
            3 * (BlobLogHeader::kSize +
                 BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                 blob_size + BlobLogFooter::kSize));
}

class DBBlobBasicIOErrorTest : public DBBlobBasicTest,
                               public testing::WithParamInterface<std::string> {
 protected:
  DBBlobBasicIOErrorTest() : sync_point_(GetParam()) {
    fault_injection_env_.reset(new FaultInjectionTestEnv(env_));
  }
  ~DBBlobBasicIOErrorTest() { Close(); }

  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env_;
  std::string sync_point_;
};

class DBBlobBasicIOErrorMultiGetTest : public DBBlobBasicIOErrorTest {
 public:
  DBBlobBasicIOErrorMultiGetTest() : DBBlobBasicIOErrorTest() {}
};

INSTANTIATE_TEST_CASE_P(DBBlobBasicTest, DBBlobBasicIOErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileReader::OpenFile:NewRandomAccessFile",
                            "BlobFileReader::GetBlob:ReadFromFile"}));

INSTANTIATE_TEST_CASE_P(DBBlobBasicTest, DBBlobBasicIOErrorMultiGetTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileReader::OpenFile:NewRandomAccessFile",
                            "BlobFileReader::MultiGetBlob:ReadFromFile"}));

TEST_P(DBBlobBasicIOErrorTest, GetBlob_IOError) {
  Options options;
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  ASSERT_OK(Put(key, blob_value));

  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBBlobBasicIOErrorTest, GetEntityMergeWithBlobBaseIOError) {
  // Goal: verify GetEntity preserves injected blob-read IOErrors when merge
  // reads a blob-backed base value, instead of laundering them into Corruption.
  // The test writes a blob-backed base value plus a merge operand, then injects
  // an IOError at blob read time and checks both GetEntity and Get see it.
  Options options;
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char base_value[] = "base_value";

  ASSERT_OK(Put(key, base_value));
  ASSERT_OK(Flush());

  ASSERT_OK(Merge(key, "merge_operand"));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableWideColumns entity_result;
  Status s = db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                            &entity_result);
  ASSERT_TRUE(s.IsIOError()) << "Expected IOError but got: " << s.ToString();

  PinnableSlice get_result;
  s = db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &get_result);
  ASSERT_TRUE(s.IsIOError()) << "Expected IOError but got: " << s.ToString();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBBlobBasicIOErrorMultiGetTest, MultiGetBlobs_IOError) {
  Options options = GetDefaultOptions();
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr size_t num_keys = 2;

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";

  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";

  ASSERT_OK(Put(second_key, second_value));

  ASSERT_OK(Flush());

  std::array<Slice, num_keys> keys{{first_key, second_key}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                keys.data(), values.data(), statuses.data());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(statuses[0].IsIOError());
  ASSERT_TRUE(statuses[1].IsIOError());
}

TEST_P(DBBlobBasicIOErrorMultiGetTest, MultipleBlobFiles) {
  Options options = GetDefaultOptions();
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr size_t num_keys = 2;

  constexpr char key1[] = "key1";
  constexpr char value1[] = "blob1";

  ASSERT_OK(Put(key1, value1));
  ASSERT_OK(Flush());

  constexpr char key2[] = "key2";
  constexpr char value2[] = "blob2";

  ASSERT_OK(Put(key2, value2));
  ASSERT_OK(Flush());

  std::array<Slice, num_keys> keys{{key1, key2}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;

  bool first_blob_file = true;
  SyncPoint::GetInstance()->SetCallBack(
      sync_point_, [&first_blob_file, this](void* /* arg */) {
        if (first_blob_file) {
          first_blob_file = false;
          return;
        }
        fault_injection_env_->SetFilesystemActive(false,
                                                  Status::IOError(sync_point_));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                keys.data(), values.data(), statuses.data());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(value1, values[0]);
  ASSERT_TRUE(statuses[1].IsIOError());
}

TEST_F(DBBlobBasicTest, MultiGetFindTable_IOError) {
  // Repro test for a specific bug where `MultiGet()` would fail to open a table
  // in `FindTable()` and then proceed to return raw blob handles for the other
  // keys.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  // Force no table cache so every read will preload the SST file.
  dbfull()->TEST_table_cache()->SetCapacity(0);

  constexpr size_t num_keys = 2;

  constexpr char key1[] = "key1";
  constexpr char value1[] = "blob1";

  ASSERT_OK(Put(key1, value1));
  ASSERT_OK(Flush());

  constexpr char key2[] = "key2";
  constexpr char value2[] = "blob2";

  ASSERT_OK(Put(key2, value2));
  ASSERT_OK(Flush());

  std::atomic<int> num_files_opened = 0;
  // This test would be more realistic if we injected an `IOError` from the
  // `FileSystem`
  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::MultiGet:FindTable", [&](void* status) {
        num_files_opened++;
        if (num_files_opened == 2) {
          Status* s = static_cast<Status*>(status);
          *s = Status::IOError();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::array<Slice, num_keys> keys{{key1, key2}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;
  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                keys.data(), values.data(), statuses.data());

  ASSERT_TRUE(statuses[0].IsIOError());
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(value2, values[1]);
}

namespace {

class ReadBlobCompactionFilter : public CompactionFilter {
 public:
  ReadBlobCompactionFilter() = default;
  const char* Name() const override {
    return "rocksdb.compaction.filter.read.blob";
  }
  CompactionFilter::Decision FilterV2(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice& existing_value, std::string* new_value,
      std::string* /*skip_until*/) const override {
    if (value_type != CompactionFilter::ValueType::kValue) {
      return CompactionFilter::Decision::kKeep;
    }
    assert(new_value);
    new_value->assign(existing_value.data(), existing_value.size());
    return CompactionFilter::Decision::kChangeValue;
  }
};

}  // anonymous namespace

TEST_P(DBBlobBasicIOErrorTest, CompactionFilterReadBlob_IOError) {
  Options options = GetDefaultOptions();
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new ReadBlobCompactionFilter);
  options.compaction_filter = compaction_filter_guard.get();

  DestroyAndReopen(options);
  constexpr char key[] = "foo";
  constexpr char blob_value[] = "foo_blob_value";
  ASSERT_OK(Put(key, blob_value));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                                /*end=*/nullptr)
                  .IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBBlobBasicIOErrorTest, IterateBlobsAllowUnpreparedValue_IOError) {
  Options options;
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  ASSERT_OK(Put(key, blob_value));

  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->SeekToFirst();

  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), key);
  ASSERT_TRUE(iter->value().empty());
  ASSERT_OK(iter->status());

  ASSERT_FALSE(iter->PrepareValue());

  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBBlobBasicTest, WarmCacheWithBlobsDuringFlush) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 1 << 25;
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.blob_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  options.enable_blob_files = true;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  DestroyAndReopen(options);

  constexpr size_t kNumBlobs = 10;
  constexpr size_t kValueSize = 100;

  std::string value(kValueSize, 'a');

  for (size_t i = 1; i <= kNumBlobs; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(Put(std::to_string(i + kNumBlobs), value));  // Add some overlap
    ASSERT_OK(Flush());
    ASSERT_EQ(i * 2, options.statistics->getTickerCount(BLOB_DB_CACHE_ADD));
    ASSERT_EQ(value, Get(std::to_string(i)));
    ASSERT_EQ(value, Get(std::to_string(i + kNumBlobs)));
    ASSERT_EQ(0, options.statistics->getTickerCount(BLOB_DB_CACHE_MISS));
    ASSERT_EQ(i * 2, options.statistics->getTickerCount(BLOB_DB_CACHE_HIT));
  }

  // Verify compaction not counted
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  EXPECT_EQ(kNumBlobs * 2,
            options.statistics->getTickerCount(BLOB_DB_CACHE_ADD));
}

TEST_F(DBBlobBasicTest, DynamicallyWarmCacheDuringFlush) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 1 << 25;
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.blob_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  options.enable_blob_files = true;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  DestroyAndReopen(options);

  constexpr size_t kNumBlobs = 10;
  constexpr size_t kValueSize = 100;

  std::string value(kValueSize, 'a');

  for (size_t i = 1; i <= 5; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(Put(std::to_string(i + kNumBlobs), value));  // Add some overlap
    ASSERT_OK(Flush());
    ASSERT_EQ(2, options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD));

    ASSERT_EQ(value, Get(std::to_string(i)));
    ASSERT_EQ(value, Get(std::to_string(i + kNumBlobs)));
    ASSERT_EQ(0, options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD));
    ASSERT_EQ(0,
              options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_MISS));
    ASSERT_EQ(2, options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_HIT));
  }

  ASSERT_OK(dbfull()->SetOptions({{"prepopulate_blob_cache", "kDisable"}}));

  for (size_t i = 6; i <= kNumBlobs; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(Put(std::to_string(i + kNumBlobs), value));  // Add some overlap
    ASSERT_OK(Flush());
    ASSERT_EQ(0, options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD));

    ASSERT_EQ(value, Get(std::to_string(i)));
    ASSERT_EQ(value, Get(std::to_string(i + kNumBlobs)));
    ASSERT_EQ(2, options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD));
    ASSERT_EQ(2,
              options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_MISS));
    ASSERT_EQ(0, options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_HIT));
  }

  // Verify compaction not counted
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  EXPECT_EQ(0, options.statistics->getTickerCount(BLOB_DB_CACHE_ADD));
}

TEST_F(DBBlobBasicTest, WarmCacheWithBlobsSecondary) {
  CompressedSecondaryCacheOptions secondary_cache_opts;
  secondary_cache_opts.capacity = 1 << 20;
  secondary_cache_opts.num_shard_bits = 0;
  secondary_cache_opts.metadata_charge_policy = kDontChargeCacheMetadata;
  secondary_cache_opts.compression_type = kNoCompression;

  LRUCacheOptions primary_cache_opts;
  primary_cache_opts.capacity = 1024;
  primary_cache_opts.num_shard_bits = 0;
  primary_cache_opts.metadata_charge_policy = kDontChargeCacheMetadata;
  primary_cache_opts.secondary_cache =
      NewCompressedSecondaryCache(secondary_cache_opts);

  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();
  options.enable_blob_files = true;
  options.blob_cache = NewLRUCache(primary_cache_opts);
  options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;

  DestroyAndReopen(options);

  // Note: only one of the two blobs fit in the primary cache at any given time.
  constexpr char first_key[] = "foo";
  constexpr size_t first_blob_size = 512;
  const std::string first_blob(first_blob_size, 'a');

  constexpr char second_key[] = "bar";
  constexpr size_t second_blob_size = 768;
  const std::string second_blob(second_blob_size, 'b');

  // First blob is inserted into primary cache during flush.
  ASSERT_OK(Put(first_key, first_blob));
  ASSERT_OK(Flush());
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD), 1);

  // Second blob is inserted into primary cache during flush,
  // First blob is evicted but only a dummy handle is inserted into secondary
  // cache.
  ASSERT_OK(Put(second_key, second_blob));
  ASSERT_OK(Flush());
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD), 1);

  // First blob is inserted into primary cache.
  // Second blob is evicted but only a dummy handle is inserted into secondary
  // cache.
  ASSERT_EQ(Get(first_key), first_blob);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_MISS), 1);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_HIT), 0);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(SECONDARY_CACHE_HITS),
            0);
  // Second blob is inserted into primary cache,
  // First blob is evicted and is inserted into secondary cache.
  ASSERT_EQ(Get(second_key), second_blob);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_MISS), 1);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_HIT), 0);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(SECONDARY_CACHE_HITS),
            0);

  // First blob's dummy item is inserted into primary cache b/c of lookup.
  // Second blob is still in primary cache.
  ASSERT_EQ(Get(first_key), first_blob);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_MISS), 0);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_HIT), 1);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(SECONDARY_CACHE_HITS),
            1);

  // First blob's item is inserted into primary cache b/c of lookup.
  // Second blob is evicted and inserted into secondary cache.
  ASSERT_EQ(Get(first_key), first_blob);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_MISS), 0);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_HIT), 1);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(SECONDARY_CACHE_HITS),
            1);
}

TEST_F(DBBlobBasicTest, GetEntityBlob) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  constexpr char other_key[] = "other_key";
  constexpr char other_blob_value[] = "other_blob_value";

  ASSERT_OK(Put(key, blob_value));
  ASSERT_OK(Put(other_key, other_blob_value));

  ASSERT_OK(Flush());

  WideColumns expected_columns{{kDefaultWideColumnName, blob_value}};
  WideColumns other_expected_columns{
      {kDefaultWideColumnName, other_blob_value}};

  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), expected_columns);
  }

  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             other_key, &result));

    ASSERT_EQ(result.columns(), other_expected_columns);
  }

  {
    constexpr size_t num_keys = 2;

    std::array<Slice, num_keys> keys{{key, other_key}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(results[0].columns(), expected_columns);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(results[1].columns(), other_expected_columns);
  }
}

class DBBlobWithTimestampTest : public DBBasicTestWithTimestampBase {
 protected:
  DBBlobWithTimestampTest()
      : DBBasicTestWithTimestampBase("db_blob_with_timestamp_test") {}
};

TEST_F(DBBlobWithTimestampTest, GetBlob) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  DestroyAndReopen(options);
  WriteOptions write_opts;
  const std::string ts = Timestamp(1, 0);
  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  ASSERT_OK(db_->Put(write_opts, key, ts, blob_value));

  ASSERT_OK(Flush());

  const std::string read_ts = Timestamp(2, 0);
  Slice read_ts_slice(read_ts);
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts_slice;
  std::string value;
  ASSERT_OK(db_->Get(read_opts, key, &value));
  ASSERT_EQ(value, blob_value);
}

TEST_F(DBBlobWithTimestampTest, MultiGetBlobs) {
  constexpr size_t min_blob_size = 6;

  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  DestroyAndReopen(options);

  // Put then retrieve three key-values. The first value is below the size limit
  // and is thus stored inline; the other two are stored separately as blobs.
  constexpr size_t num_keys = 3;

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "short";
  static_assert(sizeof(first_value) - 1 < min_blob_size,
                "first_value too long to be inlined");

  DestroyAndReopen(options);
  WriteOptions write_opts;
  const std::string ts = Timestamp(1, 0);
  ASSERT_OK(db_->Put(write_opts, first_key, ts, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "long_value";
  static_assert(sizeof(second_value) - 1 >= min_blob_size,
                "second_value too short to be stored as blob");

  ASSERT_OK(db_->Put(write_opts, second_key, ts, second_value));

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "other_long_value";
  static_assert(sizeof(third_value) - 1 >= min_blob_size,
                "third_value too short to be stored as blob");

  ASSERT_OK(db_->Put(write_opts, third_key, ts, third_value));

  ASSERT_OK(Flush());

  ReadOptions read_options;
  const std::string read_ts = Timestamp(2, 0);
  Slice read_ts_slice(read_ts);
  read_options.timestamp = &read_ts_slice;
  std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }
}

TEST_F(DBBlobWithTimestampTest, GetMergeBlobWithPut) {
  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  DestroyAndReopen(options);

  WriteOptions write_opts;
  const std::string ts = Timestamp(1, 0);
  ASSERT_OK(db_->Put(write_opts, "Key1", ts, "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(
      db_->Merge(write_opts, db_->DefaultColumnFamily(), "Key1", ts, "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(
      db_->Merge(write_opts, db_->DefaultColumnFamily(), "Key1", ts, "v3"));
  ASSERT_OK(Flush());

  std::string value;
  const std::string read_ts = Timestamp(2, 0);
  Slice read_ts_slice(read_ts);
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts_slice;
  ASSERT_OK(db_->Get(read_opts, "Key1", &value));
  ASSERT_EQ(value, "v1,v2,v3");
}

TEST_F(DBBlobWithTimestampTest, MultiGetMergeBlobWithPut) {
  constexpr size_t num_keys = 3;

  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  DestroyAndReopen(options);

  WriteOptions write_opts;
  const std::string ts = Timestamp(1, 0);

  ASSERT_OK(db_->Put(write_opts, "Key0", ts, "v0_0"));
  ASSERT_OK(db_->Put(write_opts, "Key1", ts, "v1_0"));
  ASSERT_OK(db_->Put(write_opts, "Key2", ts, "v2_0"));
  ASSERT_OK(Flush());
  ASSERT_OK(
      db_->Merge(write_opts, db_->DefaultColumnFamily(), "Key0", ts, "v0_1"));
  ASSERT_OK(
      db_->Merge(write_opts, db_->DefaultColumnFamily(), "Key1", ts, "v1_1"));
  ASSERT_OK(Flush());
  ASSERT_OK(
      db_->Merge(write_opts, db_->DefaultColumnFamily(), "Key0", ts, "v0_2"));
  ASSERT_OK(Flush());

  const std::string read_ts = Timestamp(2, 0);
  Slice read_ts_slice(read_ts);
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts_slice;
  std::array<Slice, num_keys> keys{{"Key0", "Key1", "Key2"}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;

  db_->MultiGet(read_opts, db_->DefaultColumnFamily(), num_keys, keys.data(),
                values.data(), statuses.data());

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], "v0_0,v0_1,v0_2");

  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1], "v1_0,v1_1");

  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], "v2_0");
}

TEST_F(DBBlobWithTimestampTest, IterateBlobs) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  DestroyAndReopen(options);

  int num_blobs = 5;
  std::vector<std::string> keys;
  std::vector<std::string> blobs;

  WriteOptions write_opts;
  std::vector<std::string> write_timestamps = {Timestamp(1, 0),
                                               Timestamp(2, 0)};

  // For each key in ["key0", ... "keyi", ...], write two versions:
  // Timestamp(1, 0), "blobi0"
  // Timestamp(2, 0), "blobi1"
  for (int i = 0; i < num_blobs; i++) {
    keys.push_back("key" + std::to_string(i));
    blobs.push_back("blob" + std::to_string(i));
    for (size_t j = 0; j < write_timestamps.size(); j++) {
      ASSERT_OK(db_->Put(write_opts, keys[i], write_timestamps[j],
                         blobs[i] + std::to_string(j)));
    }
  }
  ASSERT_OK(Flush());

  ReadOptions read_options;
  std::vector<std::string> read_timestamps = {Timestamp(0, 0), Timestamp(3, 0)};
  Slice ts_upper_bound(read_timestamps[1]);
  read_options.timestamp = &ts_upper_bound;

  auto check_iter_entry =
      [](const Iterator* iter, const std::string& expected_key,
         const std::string& expected_ts, const std::string& expected_value,
         bool key_is_internal = true) {
        ASSERT_OK(iter->status());
        if (key_is_internal) {
          std::string expected_ukey_and_ts;
          expected_ukey_and_ts.assign(expected_key.data(), expected_key.size());
          expected_ukey_and_ts.append(expected_ts.data(), expected_ts.size());

          ParsedInternalKey parsed_ikey;
          ASSERT_OK(ParseInternalKey(iter->key(), &parsed_ikey,
                                     true /* log_err_key */));
          ASSERT_EQ(parsed_ikey.user_key, expected_ukey_and_ts);
        } else {
          ASSERT_EQ(iter->key(), expected_key);
        }
        ASSERT_EQ(iter->timestamp(), expected_ts);
        ASSERT_EQ(iter->value(), expected_value);
      };

  // Forward iterating one version of each key, get in this order:
  // [("key0", Timestamp(2, 0), "blob01"),
  //  ("key1", Timestamp(2, 0), "blob11")...]
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToFirst();
    for (int i = 0; i < num_blobs; i++) {
      check_iter_entry(iter.get(), keys[i], write_timestamps[1],
                       blobs[i] + std::to_string(1), /*key_is_internal*/ false);
      iter->Next();
    }
  }

  // Forward iteration, then reverse to backward.
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToFirst();
    for (int i = 0; i < num_blobs * 2 - 1; i++) {
      if (i < num_blobs) {
        check_iter_entry(iter.get(), keys[i], write_timestamps[1],
                         blobs[i] + std::to_string(1),
                         /*key_is_internal*/ false);
        if (i != num_blobs - 1) {
          iter->Next();
        }
      } else {
        if (i != num_blobs) {
          check_iter_entry(iter.get(), keys[num_blobs * 2 - 1 - i],
                           write_timestamps[1],
                           blobs[num_blobs * 2 - 1 - i] + std::to_string(1),
                           /*key_is_internal*/ false);
        }
        iter->Prev();
      }
    }
  }

  // Backward iterating one versions of each key, get in this order:
  // [("key4", Timestamp(2, 0), "blob41"),
  //  ("key3", Timestamp(2, 0), "blob31")...]
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    for (int i = 0; i < num_blobs; i++) {
      check_iter_entry(iter.get(), keys[num_blobs - 1 - i], write_timestamps[1],
                       blobs[num_blobs - 1 - i] + std::to_string(1),
                       /*key_is_internal*/ false);
      iter->Prev();
    }
    ASSERT_OK(iter->status());
  }

  // Backward iteration, then reverse to forward.
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    for (int i = 0; i < num_blobs * 2 - 1; i++) {
      if (i < num_blobs) {
        check_iter_entry(iter.get(), keys[num_blobs - 1 - i],
                         write_timestamps[1],
                         blobs[num_blobs - 1 - i] + std::to_string(1),
                         /*key_is_internal*/ false);
        if (i != num_blobs - 1) {
          iter->Prev();
        }
      } else {
        if (i != num_blobs) {
          check_iter_entry(iter.get(), keys[i - num_blobs], write_timestamps[1],
                           blobs[i - num_blobs] + std::to_string(1),
                           /*key_is_internal*/ false);
        }
        iter->Next();
      }
    }
  }

  Slice ts_lower_bound(read_timestamps[0]);
  read_options.iter_start_ts = &ts_lower_bound;
  // Forward iterating multiple versions of the same key, get in this order:
  // [("key0", Timestamp(2, 0), "blob01"),
  //  ("key0", Timestamp(1, 0), "blob00"),
  //  ("key1", Timestamp(2, 0), "blob11")...]
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToFirst();
    for (int i = 0; i < num_blobs; i++) {
      for (size_t j = write_timestamps.size(); j > 0; --j) {
        check_iter_entry(iter.get(), keys[i], write_timestamps[j - 1],
                         blobs[i] + std::to_string(j - 1));
        iter->Next();
      }
    }
    ASSERT_OK(iter->status());
  }

  // Backward iterating multiple versions of the same key, get in this order:
  // [("key4", Timestamp(1, 0), "blob00"),
  //  ("key4", Timestamp(2, 0), "blob01"),
  //  ("key3", Timestamp(1, 0), "blob10")...]
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    for (int i = num_blobs; i > 0; i--) {
      for (size_t j = 0; j < write_timestamps.size(); j++) {
        check_iter_entry(iter.get(), keys[i - 1], write_timestamps[j],
                         blobs[i - 1] + std::to_string(j));
        iter->Prev();
      }
    }
    ASSERT_OK(iter->status());
  }

  int upper_bound_idx = num_blobs - 2;
  int lower_bound_idx = 1;
  Slice upper_bound_slice(keys[upper_bound_idx]);
  Slice lower_bound_slice(keys[lower_bound_idx]);
  read_options.iterate_upper_bound = &upper_bound_slice;
  read_options.iterate_lower_bound = &lower_bound_slice;

  // Forward iteration with upper and lower bound.
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToFirst();
    for (int i = lower_bound_idx; i < upper_bound_idx; i++) {
      for (size_t j = write_timestamps.size(); j > 0; --j) {
        check_iter_entry(iter.get(), keys[i], write_timestamps[j - 1],
                         blobs[i] + std::to_string(j - 1));
        iter->Next();
      }
    }
    ASSERT_OK(iter->status());
  }

  // Backward iteration with upper and lower bound.
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    for (int i = upper_bound_idx; i > lower_bound_idx; i--) {
      for (size_t j = 0; j < write_timestamps.size(); j++) {
        check_iter_entry(iter.get(), keys[i - 1], write_timestamps[j],
                         blobs[i - 1] + std::to_string(j));
        iter->Prev();
      }
    }
    ASSERT_OK(iter->status());
  }
}

TEST_F(DBBlobBasicTest, GetApproximateSizesIncludingBlobFiles) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  // Write some key-value pairs with blob values and flush to create blob files.
  constexpr int kNumKeys = 1000;
  constexpr int kValueSize = 1024;

  Random rnd(301);
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
  }
  ASSERT_OK(Flush());

  // Verify blob files exist.
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  bool has_blob_files = false;
  for (const auto& f : files) {
    if (f.size() > 5 && f.substr(f.size() - 5) == ".blob") {
      has_blob_files = true;
      break;
    }
  }
  ASSERT_TRUE(has_blob_files);

  // Query the full range - all keys are covered.
  std::string start = Key(0);
  std::string end = Key(kNumKeys);
  Range r(start, end);

  // Without include_blob_files (default behavior): should not include blob
  // file sizes.
  uint64_t size_without_blobs = 0;
  {
    SizeApproximationOptions size_approx_options;
    size_approx_options.include_files = true;
    size_approx_options.include_blob_files = false;
    ASSERT_OK(db_->GetApproximateSizes(size_approx_options,
                                       db_->DefaultColumnFamily(), &r, 1,
                                       &size_without_blobs));
    ASSERT_GT(size_without_blobs, 0);
  }

  // With include_blob_files: should be strictly larger.
  {
    SizeApproximationOptions size_approx_options;
    size_approx_options.include_files = true;
    size_approx_options.include_blob_files = true;
    uint64_t size_with_blobs = 0;
    ASSERT_OK(db_->GetApproximateSizes(size_approx_options,
                                       db_->DefaultColumnFamily(), &r, 1,
                                       &size_with_blobs));
    ASSERT_GT(size_with_blobs, size_without_blobs);
  }

  // Range that doesn't overlap any data should return 0.
  {
    std::string no_start = Key(kNumKeys + 100);
    std::string no_end = Key(kNumKeys + 200);
    Range no_r(no_start, no_end);
    SizeApproximationOptions size_approx_options;
    size_approx_options.include_files = true;
    size_approx_options.include_blob_files = true;
    uint64_t no_size = 0;
    ASSERT_OK(db_->GetApproximateSizes(
        size_approx_options, db_->DefaultColumnFamily(), &no_r, 1, &no_size));
    ASSERT_EQ(no_size, 0);
  }

  // Partial range should return proportionally less blob size than full range.
  {
    SizeApproximationOptions size_approx_options;
    size_approx_options.include_files = true;
    size_approx_options.include_blob_files = true;

    uint64_t full_size = 0;
    ASSERT_OK(db_->GetApproximateSizes(
        size_approx_options, db_->DefaultColumnFamily(), &r, 1, &full_size));

    // Query roughly the first half of keys.
    std::string half_end = Key(kNumKeys / 2);
    Range half_r(start, half_end);
    uint64_t half_size = 0;
    ASSERT_OK(db_->GetApproximateSizes(size_approx_options,
                                       db_->DefaultColumnFamily(), &half_r, 1,
                                       &half_size));
    ASSERT_GT(half_size, 0);
    ASSERT_LT(half_size, full_size);
  }

  // Via SizeApproximationFlags API.
  {
    uint64_t size_flags = 0;
    ASSERT_OK(db_->GetApproximateSizes(
        db_->DefaultColumnFamily(), &r, 1, &size_flags,
        DB::SizeApproximationFlags::INCLUDE_FILES |
            DB::SizeApproximationFlags::INCLUDE_BLOB_FILES));
    ASSERT_GT(size_flags, size_without_blobs);
  }

  // Multi-range query: two non-overlapping sub-ranges should sum to
  // approximately the full-range result.
  {
    SizeApproximationOptions size_approx_options;
    size_approx_options.include_files = true;
    size_approx_options.include_blob_files = true;

    std::string mid = Key(kNumKeys / 2);
    std::string r1_start = Key(0);
    std::string r1_end = mid;
    std::string r2_start = mid;
    std::string r2_end = Key(kNumKeys);
    Range ranges[2] = {Range(r1_start, r1_end), Range(r2_start, r2_end)};
    uint64_t sizes[2] = {0, 0};
    ASSERT_OK(db_->GetApproximateSizes(
        size_approx_options, db_->DefaultColumnFamily(), ranges, 2, sizes));
    // Each sub-range should return a positive size.
    ASSERT_GT(sizes[0], 0);
    ASSERT_GT(sizes[1], 0);
    // Sum of sub-ranges should be close to the full-range result.
    uint64_t full_size = 0;
    ASSERT_OK(db_->GetApproximateSizes(
        size_approx_options, db_->DefaultColumnFamily(), &r, 1, &full_size));
    ASSERT_NEAR(static_cast<double>(sizes[0] + sizes[1]),
                static_cast<double>(full_size), full_size * 0.1);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
