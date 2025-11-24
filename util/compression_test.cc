//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Testing various compression features

#include <cstdlib>
#include <memory>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/utilities/object_registry.h"
#include "table/block_based/block_builder.h"
#include "test_util/testutil.h"
#include "util/auto_tune_compressor.h"
#include "util/coding.h"
#include "util/random.h"
#include "util/simple_mixed_compressor.h"

namespace ROCKSDB_NAMESPACE {

class DBCompressionTest : public DBTestBase {
 public:
  DBCompressionTest() : DBTestBase("compression_test", /*env_do_fsync=*/true) {}
};

TEST_F(DBCompressionTest, PresetCompressionDict) {
  // Verifies that compression ratio improves when dictionary is enabled, and
  // improves even further when the dictionary is trained by ZSTD.
  const size_t kBlockSizeBytes = 4 << 10;
  const size_t kL0FileBytes = 128 << 10;
  const size_t kApproxPerBlockOverheadBytes = 50;
  const int kNumL0Files = 5;

  Options options;
  // Make sure to use any custom env that the test is configured with.
  options.env = CurrentOptions().env;
  options.allow_concurrent_memtable_write = false;
  options.arena_block_size = kBlockSizeBytes;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kL0FileBytes / kBlockSizeBytes));
  options.num_levels = 2;
  options.target_file_size_base = kL0FileBytes;
  options.target_file_size_multiplier = 2;
  options.write_buffer_size = kL0FileBytes;
  BlockBasedTableOptions table_options;
  table_options.block_size = kBlockSizeBytes;
  std::vector<CompressionType> compression_types;
  if (Zlib_Supported()) {
    compression_types.push_back(kZlibCompression);
  }
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  compression_types.push_back(kLZ4Compression);
  compression_types.push_back(kLZ4HCCompression);
#endif  // LZ4_VERSION_NUMBER >= 10400
  if (ZSTD_Supported()) {
    compression_types.push_back(kZSTD);
  }

  enum DictionaryTypes : int {
    kWithoutDict,
    kWithDict,
    kWithZSTDfinalizeDict,
    kWithZSTDTrainedDict,
    kDictEnd,
  };

  for (auto compression_type : compression_types) {
    options.compression = compression_type;
    size_t bytes_without_dict = 0;
    size_t bytes_with_dict = 0;
    size_t bytes_with_zstd_finalize_dict = 0;
    size_t bytes_with_zstd_trained_dict = 0;
    for (int i = kWithoutDict; i < kDictEnd; i++) {
      // First iteration: compress without preset dictionary
      // Second iteration: compress with preset dictionary
      // Third iteration (zstd only): compress with zstd-trained dictionary
      //
      // To make sure the compression dictionary has the intended effect, we
      // verify the compressed size is smaller in successive iterations. Also in
      // the non-first iterations, verify the data we get out is the same data
      // we put in.
      switch (i) {
        case kWithoutDict:
          options.compression_opts.max_dict_bytes = 0;
          options.compression_opts.zstd_max_train_bytes = 0;
          break;
        case kWithDict:
          options.compression_opts.max_dict_bytes = kBlockSizeBytes;
          options.compression_opts.zstd_max_train_bytes = 0;
          break;
        case kWithZSTDfinalizeDict:
          if (compression_type != kZSTD ||
              !ZSTD_FinalizeDictionarySupported()) {
            continue;
          }
          options.compression_opts.max_dict_bytes = kBlockSizeBytes;
          options.compression_opts.zstd_max_train_bytes = kL0FileBytes;
          options.compression_opts.use_zstd_dict_trainer = false;
          break;
        case kWithZSTDTrainedDict:
          if (compression_type != kZSTD || !ZSTD_TrainDictionarySupported()) {
            continue;
          }
          options.compression_opts.max_dict_bytes = kBlockSizeBytes;
          options.compression_opts.zstd_max_train_bytes = kL0FileBytes;
          options.compression_opts.use_zstd_dict_trainer = true;
          break;
        default:
          assert(false);
      }

      options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
      CreateAndReopenWithCF({"pikachu"}, options);
      Random rnd(301);
      std::string seq_datas[10];
      for (int j = 0; j < 10; ++j) {
        seq_datas[j] =
            rnd.RandomString(kBlockSizeBytes - kApproxPerBlockOverheadBytes);
      }

      ASSERT_EQ(0, NumTableFilesAtLevel(0, 1));
      for (int j = 0; j < kNumL0Files; ++j) {
        for (size_t k = 0; k < kL0FileBytes / kBlockSizeBytes + 1; ++k) {
          auto key_num = j * (kL0FileBytes / kBlockSizeBytes) + k;
          ASSERT_OK(Put(1, Key(static_cast<int>(key_num)),
                        seq_datas[(key_num / 10) % 10]));
        }
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[1]));
        ASSERT_EQ(j + 1, NumTableFilesAtLevel(0, 1));
      }
      ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1],
                                            true /* disallow_trivial_move */));
      ASSERT_EQ(0, NumTableFilesAtLevel(0, 1));
      ASSERT_GT(NumTableFilesAtLevel(1, 1), 0);

      // Get the live sst files size
      size_t total_sst_bytes = TotalSize(1);
      if (i == kWithoutDict) {
        bytes_without_dict = total_sst_bytes;
      } else if (i == kWithDict) {
        bytes_with_dict = total_sst_bytes;
      } else if (i == kWithZSTDfinalizeDict) {
        bytes_with_zstd_finalize_dict = total_sst_bytes;
      } else if (i == kWithZSTDTrainedDict) {
        bytes_with_zstd_trained_dict = total_sst_bytes;
      }

      for (size_t j = 0; j < kNumL0Files * (kL0FileBytes / kBlockSizeBytes);
           j++) {
        ASSERT_EQ(seq_datas[(j / 10) % 10], Get(1, Key(static_cast<int>(j))));
      }
      if (i == kWithDict) {
        ASSERT_GT(bytes_without_dict, bytes_with_dict);
      } else if (i == kWithZSTDTrainedDict) {
        // In zstd compression, it is sometimes possible that using a finalized
        // dictionary does not get as good a compression ratio as raw content
        // dictionary. But using a dictionary should always get better
        // compression ratio than not using one.
        ASSERT_TRUE(bytes_with_dict > bytes_with_zstd_finalize_dict ||
                    bytes_without_dict > bytes_with_zstd_finalize_dict);
      } else if (i == kWithZSTDTrainedDict) {
        // In zstd compression, it is sometimes possible that using a trained
        // dictionary does not get as good a compression ratio as without
        // training.
        // But using a dictionary (with or without training) should always get
        // better compression ratio than not using one.
        ASSERT_TRUE(bytes_with_dict > bytes_with_zstd_trained_dict ||
                    bytes_without_dict > bytes_with_zstd_trained_dict);
      }

      DestroyAndReopen(options);
    }
  }
}

TEST_F(DBCompressionTest, PresetCompressionDictLocality) {
  if (!ZSTD_Supported()) {
    return;
  }
  // Verifies that compression dictionary is generated from local data. The
  // verification simply checks all output SSTs have different compression
  // dictionaries. We do not verify effectiveness as that'd likely be flaky in
  // the future.
  const int kNumEntriesPerFile = 1 << 10;  // 1KB
  const int kNumBytesPerEntry = 1 << 10;   // 1KB
  const int kNumFiles = 4;
  Options options = CurrentOptions();
  options.compression = kZSTD;
  options.compression_opts.max_dict_bytes = 1 << 14;        // 16KB
  options.compression_opts.zstd_max_train_bytes = 1 << 18;  // 256KB
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.target_file_size_base = kNumEntriesPerFile * kNumBytesPerEntry;
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kNumEntriesPerFile; ++j) {
      ASSERT_OK(Put(Key(i * kNumEntriesPerFile + j),
                    rnd.RandomString(kNumBytesPerEntry)));
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(1);
    ASSERT_EQ(NumTableFilesAtLevel(1), i + 1);
  }

  // Store all the dictionaries generated during a full compaction.
  std::vector<std::string> compression_dicts;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WriteCompressionDictBlock:RawDict",
      [&](void* arg) {
        compression_dicts.emplace_back(static_cast<Slice*>(arg)->ToString());
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  CompactRangeOptions compact_range_opts;
  compact_range_opts.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(compact_range_opts, nullptr, nullptr));

  // Dictionary compression should not be so good as to compress four totally
  // random files into one. If it does then there's probably something wrong
  // with the test.
  ASSERT_GT(NumTableFilesAtLevel(1), 1);

  // Furthermore, there should be one compression dictionary generated per file.
  // And they should all be different from each other.
  ASSERT_EQ(NumTableFilesAtLevel(1),
            static_cast<int>(compression_dicts.size()));
  for (size_t i = 1; i < compression_dicts.size(); ++i) {
    std::string& a = compression_dicts[i - 1];
    std::string& b = compression_dicts[i];
    size_t alen = a.size();
    size_t blen = b.size();
    ASSERT_TRUE(alen != blen || memcmp(a.data(), b.data(), alen) != 0);
  }
}

static std::string CompressibleString(Random* rnd, int len) {
  std::string r;
  test::CompressibleString(rnd, 0.8, len, &r);
  return r;
}

TEST_F(DBCompressionTest, DynamicLevelCompressionPerLevel) {
  if (!Snappy_Supported()) {
    return;
  }
  const int kNKeys = 120;
  int keys[kNKeys];
  for (int i = 0; i < kNKeys; i++) {
    keys[i] = i;
  }

  Random rnd(301);
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.db_write_buffer_size = 20480;
  options.write_buffer_size = 20480;
  options.max_write_buffer_number = 2;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 2;
  options.target_file_size_base = 20480;
  options.level_compaction_dynamic_level_bytes = true;
  options.max_bytes_for_level_base = 102400;
  options.max_bytes_for_level_multiplier = 4;
  options.max_background_compactions = 1;
  options.num_levels = 5;
  options.statistics = CreateDBStatistics();

  options.compression_per_level.resize(3);
  // No compression for L0
  options.compression_per_level[0] = kNoCompression;
  // No compression for the Ln whre L0 is compacted to
  options.compression_per_level[1] = kNoCompression;
  // Snappy compression for Ln+1
  options.compression_per_level[2] = kSnappyCompression;

  OnFileDeletionListener* listener = new OnFileDeletionListener();
  options.listeners.emplace_back(listener);

  DestroyAndReopen(options);

  // Insert more than 80K. L4 should be base level. Neither L0 nor L4 should
  // be compressed, so there shouldn't be any compression.
  for (int i = 0; i < 20; i++) {
    ASSERT_OK(Put(Key(keys[i]), CompressibleString(&rnd, 4000)));
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_EQ(NumTableFilesAtLevel(3), 0);
  ASSERT_TRUE(NumTableFilesAtLevel(0) > 0 || NumTableFilesAtLevel(4) > 0);

  // Verify there was no compression
  auto num_block_compressed =
      options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED);
  ASSERT_EQ(num_block_compressed, 0);

  // Insert 400KB and there will be some files end up in L3. According to the
  // above compression settings for each level, there will be some compression.
  ASSERT_OK(options.statistics->Reset());
  ASSERT_EQ(num_block_compressed, 0);
  for (int i = 20; i < 120; i++) {
    ASSERT_OK(Put(Key(keys[i]), CompressibleString(&rnd, 4000)));
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_GE(NumTableFilesAtLevel(3), 1);
  ASSERT_GE(NumTableFilesAtLevel(4), 1);

  // Verify there was compression
  num_block_compressed =
      options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED);
  ASSERT_GT(num_block_compressed, 0);

  // Make sure data in files in L3 is not compacted by removing all files
  // in L4 and calculate number of rows
  ASSERT_OK(dbfull()->SetOptions({
      {"disable_auto_compactions", "true"},
  }));
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);

  // Ensure that L1+ files are non-overlapping and together with L0 encompass
  // full key range between smallestkey and largestkey from CF file metadata.
  int largestkey_in_prev_level = -1;
  int keys_found = 0;
  for (int level = (int)cf_meta.levels.size() - 1; level >= 0; level--) {
    int files_in_level = (int)cf_meta.levels[level].files.size();
    int largestkey_in_prev_file = -1;
    for (int j = 0; j < files_in_level; j++) {
      int smallestkey = IdFromKey(cf_meta.levels[level].files[j].smallestkey);
      int largestkey = IdFromKey(cf_meta.levels[level].files[j].largestkey);
      int num_entries = (int)cf_meta.levels[level].files[j].num_entries;
      ASSERT_EQ(num_entries, largestkey - smallestkey + 1);
      keys_found += num_entries;
      if (level > 0) {
        if (j == 0) {
          ASSERT_GT(smallestkey, largestkey_in_prev_level);
        }
        if (j > 0) {
          ASSERT_GT(smallestkey, largestkey_in_prev_file);
        }
        if (j == files_in_level - 1) {
          largestkey_in_prev_level = largestkey;
        }
      }
      largestkey_in_prev_file = largestkey;
    }
  }
  ASSERT_EQ(keys_found, kNKeys);

  for (const auto& file : cf_meta.levels[4].files) {
    listener->SetExpectedFileName(dbname_ + file.name);
    const RangeOpt ranges(file.smallestkey, file.largestkey);
    // Given verification from above, we're guaranteed that by deleting all the
    // files in [<smallestkey>, <largestkey>] range, we're effectively deleting
    // that very single file and nothing more.
    EXPECT_OK(dbfull()->DeleteFilesInRanges(dbfull()->DefaultColumnFamily(),
                                            &ranges, true /* include_end */));
  }
  listener->VerifyMatchedCount(cf_meta.levels[4].files.size());

  int num_keys = 0;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    num_keys++;
  }
  ASSERT_OK(iter->status());

  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_GE(NumTableFilesAtLevel(3), 1);
  ASSERT_EQ(NumTableFilesAtLevel(4), 0);

  ASSERT_GT(SizeAtLevel(0) + SizeAtLevel(3), num_keys * 4000U + num_keys * 10U);
}

TEST_F(DBCompressionTest, DynamicLevelCompressionPerLevel2) {
  if (!Snappy_Supported() || !LZ4_Supported() || !Zlib_Supported()) {
    return;
  }
  const int kNKeys = 500;
  int keys[kNKeys];
  for (int i = 0; i < kNKeys; i++) {
    keys[i] = i;
  }
  RandomShuffle(std::begin(keys), std::end(keys));

  Random rnd(301);
  Options options;
  options.create_if_missing = true;
  options.db_write_buffer_size = 6000000;
  options.write_buffer_size = 600000;
  options.max_write_buffer_number = 2;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 2;
  options.soft_pending_compaction_bytes_limit = 1024 * 1024;
  options.target_file_size_base = 20;
  options.env = env_;
  options.level_compaction_dynamic_level_bytes = true;
  options.max_bytes_for_level_base = 200;
  options.max_bytes_for_level_multiplier = 8;
  options.max_background_compactions = 1;
  options.num_levels = 5;
  std::shared_ptr<mock::MockTableFactory> mtf(new mock::MockTableFactory);
  options.table_factory = mtf;

  options.compression_per_level.resize(3);
  options.compression_per_level[0] = kNoCompression;
  options.compression_per_level[1] = kLZ4Compression;
  options.compression_per_level[2] = kZlibCompression;

  DestroyAndReopen(options);
  // When base level is L4, L4 is LZ4.
  std::atomic<int> num_zlib(0);
  std::atomic<int> num_lz4(0);
  std::atomic<int> num_no(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        if (compaction->output_level() == 4) {
          ASSERT_TRUE(compaction->output_compression() == kLZ4Compression);
          num_lz4.fetch_add(1);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:output_compression", [&](void* arg) {
        auto* compression = static_cast<CompressionType*>(arg);
        ASSERT_TRUE(*compression == kNoCompression);
        num_no.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < 100; i++) {
    std::string value = rnd.RandomString(200);
    ASSERT_OK(Put(Key(keys[i]), value));
    if (i % 25 == 24) {
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
  }

  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_EQ(NumTableFilesAtLevel(3), 0);
  ASSERT_GT(NumTableFilesAtLevel(4), 0);
  ASSERT_GT(num_no.load(), 2);
  ASSERT_GT(num_lz4.load(), 0);
  int prev_num_files_l4 = NumTableFilesAtLevel(4);

  // After base level turn L4->L3, L3 becomes LZ4 and L4 becomes Zlib
  num_lz4.store(0);
  num_no.store(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        if (compaction->output_level() == 4 && compaction->start_level() == 3) {
          ASSERT_TRUE(compaction->output_compression() == kZlibCompression);
          num_zlib.fetch_add(1);
        } else {
          ASSERT_TRUE(compaction->output_compression() == kLZ4Compression);
          num_lz4.fetch_add(1);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:output_compression", [&](void* arg) {
        auto* compression = static_cast<CompressionType*>(arg);
        ASSERT_TRUE(*compression == kNoCompression);
        num_no.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 101; i < 500; i++) {
    std::string value = rnd.RandomString(200);
    ASSERT_OK(Put(Key(keys[i]), value));
    if (i % 100 == 99) {
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_GT(NumTableFilesAtLevel(3), 0);
  ASSERT_GT(NumTableFilesAtLevel(4), prev_num_files_l4);
  ASSERT_GT(num_no.load(), 2);
  ASSERT_GT(num_lz4.load(), 0);
  ASSERT_GT(num_zlib.load(), 0);
}

class PresetCompressionDictTest
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<CompressionType, bool>> {
 public:
  PresetCompressionDictTest()
      : DBTestBase("db_test2", false /* env_do_fsync */),
        compression_type_(std::get<0>(GetParam())),
        bottommost_(std::get<1>(GetParam())) {}

 protected:
  const CompressionType compression_type_;
  const bool bottommost_;
};

INSTANTIATE_TEST_CASE_P(
    DBCompressionTest, PresetCompressionDictTest,
    ::testing::Combine(::testing::ValuesIn(GetSupportedDictCompressions()),
                       ::testing::Bool()));

TEST_P(PresetCompressionDictTest, Flush) {
  // Verifies that dictionary is generated and written during flush only when
  // `ColumnFamilyOptions::compression` enables dictionary. Also verifies the
  // size of the dictionary is within expectations according to the limit on
  // buffering set by `CompressionOptions::max_dict_buffer_bytes`.
  const size_t kValueLen = 256;
  const size_t kKeysPerFile = 1 << 10;
  const size_t kDictLen = 16 << 10;
  const size_t kBlockLen = 4 << 10;

  Options options = CurrentOptions();
  if (bottommost_) {
    options.bottommost_compression = compression_type_;
    options.bottommost_compression_opts.enabled = true;
    options.bottommost_compression_opts.max_dict_bytes = kDictLen;
    options.bottommost_compression_opts.max_dict_buffer_bytes = kBlockLen;
  } else {
    options.compression = compression_type_;
    options.compression_opts.max_dict_bytes = kDictLen;
    options.compression_opts.max_dict_buffer_bytes = kBlockLen;
  }
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(kKeysPerFile));
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.block_size = kBlockLen;
  bbto.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  Random rnd(301);
  for (size_t i = 0; i <= kKeysPerFile; ++i) {
    ASSERT_OK(Put(Key(static_cast<int>(i)), rnd.RandomString(kValueLen)));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // We can use `BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT` to detect whether a
  // compression dictionary exists since dictionaries would be preloaded when
  // the flush finishes.
  if (bottommost_) {
    // Flush is never considered bottommost. This should change in the future
    // since flushed files may have nothing underneath them, like the one in
    // this test case.
    ASSERT_EQ(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        0);
  } else {
    ASSERT_GT(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        0);
    // TODO(ajkr): fix the below assertion to work with ZSTD. The expectation on
    // number of bytes needs to be adjusted in case the cached block is in
    // ZSTD's digested dictionary format.
    if (compression_type_ != kZSTD) {
      // Although we limited buffering to `kBlockLen`, there may be up to two
      // blocks of data included in the dictionary since we only check limit
      // after each block is built.
      ASSERT_LE(TestGetTickerCount(options,
                                   BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
                2 * kBlockLen);
    }
  }
}

TEST_P(PresetCompressionDictTest, CompactNonBottommost) {
  // Verifies that dictionary is generated and written during compaction to
  // non-bottommost level only when `ColumnFamilyOptions::compression` enables
  // dictionary. Also verifies the size of the dictionary is within expectations
  // according to the limit on buffering set by
  // `CompressionOptions::max_dict_buffer_bytes`.
  const size_t kValueLen = 256;
  const size_t kKeysPerFile = 1 << 10;
  const size_t kDictLen = 16 << 10;
  const size_t kBlockLen = 4 << 10;

  Options options = CurrentOptions();
  if (bottommost_) {
    options.bottommost_compression = compression_type_;
    options.bottommost_compression_opts.enabled = true;
    options.bottommost_compression_opts.max_dict_bytes = kDictLen;
    options.bottommost_compression_opts.max_dict_buffer_bytes = kBlockLen;
  } else {
    options.compression = compression_type_;
    options.compression_opts.max_dict_bytes = kDictLen;
    options.compression_opts.max_dict_buffer_bytes = kBlockLen;
  }
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.block_size = kBlockLen;
  bbto.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  Random rnd(301);
  for (size_t j = 0; j <= kKeysPerFile; ++j) {
    ASSERT_OK(Put(Key(static_cast<int>(j)), rnd.RandomString(kValueLen)));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  for (int i = 0; i < 2; ++i) {
    for (size_t j = 0; j <= kKeysPerFile; ++j) {
      ASSERT_OK(Put(Key(static_cast<int>(j)), rnd.RandomString(kValueLen)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("2,0,1", FilesPerLevel(0));

  uint64_t prev_compression_dict_bytes_inserted =
      TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT);
  // This L0->L1 compaction merges the two L0 files into L1. The produced L1
  // file is not bottommost due to the existing L2 file covering the same key-
  // range.
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_EQ("0,1,1", FilesPerLevel(0));
  // We can use `BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT` to detect whether a
  // compression dictionary exists since dictionaries would be preloaded when
  // the compaction finishes.
  if (bottommost_) {
    ASSERT_EQ(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        prev_compression_dict_bytes_inserted);
  } else {
    ASSERT_GT(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        prev_compression_dict_bytes_inserted);
    // TODO(ajkr): fix the below assertion to work with ZSTD. The expectation on
    // number of bytes needs to be adjusted in case the cached block is in
    // ZSTD's digested dictionary format.
    if (compression_type_ != kZSTD) {
      // Although we limited buffering to `kBlockLen`, there may be up to two
      // blocks of data included in the dictionary since we only check limit
      // after each block is built.
      ASSERT_LE(TestGetTickerCount(options,
                                   BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
                prev_compression_dict_bytes_inserted + 2 * kBlockLen);
    }
  }
}

TEST_P(PresetCompressionDictTest, CompactBottommost) {
  // Verifies that dictionary is generated and written during compaction to
  // non-bottommost level only when either `ColumnFamilyOptions::compression` or
  // `ColumnFamilyOptions::bottommost_compression` enables dictionary. Also
  // verifies the size of the dictionary is within expectations according to the
  // limit on buffering set by `CompressionOptions::max_dict_buffer_bytes`.
  const size_t kValueLen = 256;
  const size_t kKeysPerFile = 1 << 10;
  const size_t kDictLen = 16 << 10;
  const size_t kBlockLen = 4 << 10;

  Options options = CurrentOptions();
  if (bottommost_) {
    options.bottommost_compression = compression_type_;
    options.bottommost_compression_opts.enabled = true;
    options.bottommost_compression_opts.max_dict_bytes = kDictLen;
    options.bottommost_compression_opts.max_dict_buffer_bytes = kBlockLen;
  } else {
    options.compression = compression_type_;
    options.compression_opts.max_dict_bytes = kDictLen;
    options.compression_opts.max_dict_buffer_bytes = kBlockLen;
  }
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.block_size = kBlockLen;
  bbto.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  Random rnd(301);
  for (int i = 0; i < 2; ++i) {
    for (size_t j = 0; j <= kKeysPerFile; ++j) {
      ASSERT_OK(Put(Key(static_cast<int>(j)), rnd.RandomString(kValueLen)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("2", FilesPerLevel(0));

  uint64_t prev_compression_dict_bytes_inserted =
      TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT);
  CompactRangeOptions cro;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_GT(
      TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
      prev_compression_dict_bytes_inserted);
  // TODO(ajkr): fix the below assertion to work with ZSTD. The expectation on
  // number of bytes needs to be adjusted in case the cached block is in ZSTD's
  // digested dictionary format.
  if (compression_type_ != kZSTD) {
    // Although we limited buffering to `kBlockLen`, there may be up to two
    // blocks of data included in the dictionary since we only check limit after
    // each block is built.
    ASSERT_LE(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        prev_compression_dict_bytes_inserted + 2 * kBlockLen);
  }
}

class CompactionCompressionListener : public EventListener {
 public:
  explicit CompactionCompressionListener(Options* db_options)
      : db_options_(db_options) {}

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) override {
    // Figure out last level with files
    int bottommost_level = 0;
    for (int level = 0; level < db->NumberLevels(); level++) {
      std::string files_at_level;
      ASSERT_TRUE(
          db->GetProperty("rocksdb.num-files-at-level" + std::to_string(level),
                          &files_at_level));
      if (files_at_level != "0") {
        bottommost_level = level;
      }
    }

    if (db_options_->bottommost_compression != kDisableCompressionOption &&
        ci.output_level == bottommost_level) {
      ASSERT_EQ(ci.compression, db_options_->bottommost_compression);
    } else if (db_options_->compression_per_level.size() != 0) {
      ASSERT_EQ(ci.compression,
                db_options_->compression_per_level[ci.output_level]);
    } else {
      ASSERT_EQ(ci.compression, db_options_->compression);
    }
    max_level_checked = std::max(max_level_checked, ci.output_level);
  }

  int max_level_checked = 0;
  const Options* db_options_;
};

enum CompressionFailureType {
  kTestCompressionFail,
  kTestDecompressionFail,
  kTestDecompressionCorruption,
  kTestStartOfFinishFail,
};

class CompressionFailuresTest
    : public DBCompressionTest,
      public testing::WithParamInterface<std::tuple<
          CompressionFailureType, CompressionType, uint32_t, uint32_t>> {
 public:
  CompressionFailuresTest() {
    std::tie(compression_failure_type_, compression_type_,
             compression_max_dict_bytes_, compression_parallel_threads_) =
        GetParam();
  }

  CompressionFailureType compression_failure_type_ = kTestCompressionFail;
  CompressionType compression_type_ = kNoCompression;
  uint32_t compression_max_dict_bytes_ = 0;
  uint32_t compression_parallel_threads_ = 0;
};

INSTANTIATE_TEST_CASE_P(
    DBCompressionTest, CompressionFailuresTest,
    ::testing::Combine(::testing::Values(kTestCompressionFail,
                                         kTestDecompressionFail,
                                         kTestDecompressionCorruption,
                                         kTestStartOfFinishFail),
                       ::testing::ValuesIn(GetSupportedCompressions()),
                       ::testing::Values(0, 10), ::testing::Values(1, 4)));

TEST_P(CompressionFailuresTest, CompressionFailures) {
  if (compression_type_ == kNoCompression) {
    return;
  }

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.max_bytes_for_level_base = 1024;
  options.max_bytes_for_level_multiplier = 2;
  options.num_levels = 7;
  options.max_background_compactions = 1;
  options.target_file_size_base = 512;

  BlockBasedTableOptions table_options;
  table_options.block_size = 512;
  table_options.verify_compression = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  options.compression = compression_type_;
  options.compression_opts.parallel_threads = compression_parallel_threads_;
  options.compression_opts.max_dict_bytes = compression_max_dict_bytes_;
  options.bottommost_compression_opts.parallel_threads =
      compression_parallel_threads_;
  options.bottommost_compression_opts.max_dict_bytes =
      compression_max_dict_bytes_;

  if (compression_failure_type_ == kTestCompressionFail) {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "CompressData:TamperWithReturnValue", [](void* arg) {
          bool* ret = static_cast<bool*>(arg);
          *ret = false;
        });
  } else if (compression_failure_type_ == kTestDecompressionFail) {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "DecompressBlockData:TamperWithReturnValue", [](void* arg) {
          Status* ret = static_cast<Status*>(arg);
          ASSERT_OK(*ret);
          *ret = Status::Corruption("kTestDecompressionFail");
        });
  } else if (compression_failure_type_ == kTestDecompressionCorruption) {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "DecompressBlockData:TamperWithDecompressionOutput", [](void* arg) {
          BlockContents* contents = static_cast<BlockContents*>(arg);
          // Ensure uncompressed data != original data
          const size_t len = contents->data.size() + 1;
          std::unique_ptr<char[]> fake_data(new char[len]());
          *contents = BlockContents(std::move(fake_data), len);
        });
  } else if (compression_failure_type_ == kTestStartOfFinishFail) {
    if (compression_parallel_threads_ <= 1) {
      // skip this configuration
      return;
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTableBuilder::Finish:ParallelIOStatus", [&](void* arg) {
          *static_cast<IOStatus*>(arg) = IOStatus::Corruption("Seeded failure");
        });
  } else {
    abort();
  }

  std::map<std::string, std::string> key_value_written;

  const int kKeySize = 5;
  const int kValUnitSize = 16;
  const int kValSize = 256;
  Random rnd(405);

  Status s = Status::OK();

  DestroyAndReopen(options);
  // Write 10 random files
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 5; j++) {
      std::string key = rnd.RandomString(kKeySize);
      // Ensure good compression ratio
      std::string valueUnit = rnd.RandomString(kValUnitSize);
      std::string value;
      for (int k = 0; k < kValSize; k += kValUnitSize) {
        value += valueUnit;
      }
      s = Put(key, value);
      if (compression_failure_type_ == kTestCompressionFail) {
        key_value_written[key] = value;
        ASSERT_OK(s);
      }
    }
    s = Flush();
    if (compression_failure_type_ == kTestCompressionFail) {
      ASSERT_OK(s);
    }
    s = dbfull()->TEST_WaitForCompact();
    if (compression_failure_type_ == kTestCompressionFail) {
      ASSERT_OK(s);
    }
    if (i == 4) {
      // Make compression fail at the mid of table building
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    }
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  auto st = s.getState();
  if (compression_failure_type_ == kTestCompressionFail) {
    // Should be kNoCompression, check content consistency
    std::unique_ptr<Iterator> db_iter(db_->NewIterator(ReadOptions()));
    for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
      std::string key = db_iter->key().ToString();
      std::string value = db_iter->value().ToString();
      ASSERT_NE(key_value_written.find(key), key_value_written.end());
      ASSERT_EQ(key_value_written[key], value);
      key_value_written.erase(key);
    }
    ASSERT_OK(db_iter->status());
    ASSERT_EQ(0, key_value_written.size());
  } else if (compression_failure_type_ == kTestDecompressionFail) {
    ASSERT_EQ(s.code(), Status::kCorruption);
    ASSERT_NE(st, nullptr);
    ASSERT_EQ(std::string(st), "Could not decompress: kTestDecompressionFail");
  } else if (compression_failure_type_ == kTestDecompressionCorruption) {
    ASSERT_EQ(s.code(), Status::kCorruption);
    ASSERT_NE(st, nullptr);
    ASSERT_EQ(std::string(st),
              "Decompressed block did not match pre-compression block");
  } else if (compression_failure_type_ == kTestStartOfFinishFail) {
    ASSERT_EQ(s.code(), Status::kCorruption);
    ASSERT_NE(st, nullptr);
    ASSERT_EQ(std::string(st), "Seeded failure");
  }
}

TEST_F(DBCompressionTest, CompressionOptions) {
  if (!Zlib_Supported() || !Snappy_Supported()) {
    return;
  }

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.max_bytes_for_level_base = 100;
  options.max_bytes_for_level_multiplier = 2;
  options.num_levels = 7;
  options.max_background_compactions = 1;

  CompactionCompressionListener* listener =
      new CompactionCompressionListener(&options);
  options.listeners.emplace_back(listener);

  const int kKeySize = 5;
  const int kValSize = 20;
  Random rnd(301);

  std::vector<uint32_t> compression_parallel_threads = {1, 4};

  std::map<std::string, std::string> key_value_written;

  for (int iter = 0; iter <= 2; iter++) {
    listener->max_level_checked = 0;

    if (iter == 0) {
      // Use different compression algorithms for different levels but
      // always use Zlib for bottommost level
      options.compression_per_level = {kNoCompression,     kNoCompression,
                                       kNoCompression,     kSnappyCompression,
                                       kSnappyCompression, kSnappyCompression,
                                       kZlibCompression};
      options.compression = kNoCompression;
      options.bottommost_compression = kZlibCompression;
    } else if (iter == 1) {
      // Use Snappy except for bottommost level use ZLib
      options.compression_per_level = {};
      options.compression = kSnappyCompression;
      options.bottommost_compression = kZlibCompression;
    } else if (iter == 2) {
      // Use Snappy everywhere
      options.compression_per_level = {};
      options.compression = kSnappyCompression;
      options.bottommost_compression = kDisableCompressionOption;
    }

    for (auto num_threads : compression_parallel_threads) {
      options.compression_opts.parallel_threads = num_threads;
      options.bottommost_compression_opts.parallel_threads = num_threads;

      DestroyAndReopen(options);
      // Write 10 random files
      for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 5; j++) {
          std::string key = rnd.RandomString(kKeySize);
          std::string value = rnd.RandomString(kValSize);
          key_value_written[key] = value;
          ASSERT_OK(Put(key, value));
        }
        ASSERT_OK(Flush());
        ASSERT_OK(dbfull()->TEST_WaitForCompact());
      }

      // Make sure that we wrote enough to check all 7 levels
      ASSERT_EQ(listener->max_level_checked, 6);

      // Make sure database content is the same as key_value_written
      std::unique_ptr<Iterator> db_iter(db_->NewIterator(ReadOptions()));
      for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
        std::string key = db_iter->key().ToString();
        std::string value = db_iter->value().ToString();
        ASSERT_NE(key_value_written.find(key), key_value_written.end());
        ASSERT_EQ(key_value_written[key], value);
        key_value_written.erase(key);
      }
      ASSERT_OK(db_iter->status());
      ASSERT_EQ(0, key_value_written.size());
    }
  }
}

TEST_F(DBCompressionTest, RoundRobinManager) {
  if (ZSTD_Supported()) {
    auto mgr =
        std::make_shared<RoundRobinManager>(GetBuiltinV2CompressionManager());

    std::vector<std::string> values;
    for (bool use_wrapper : {true}) {
      SCOPED_TRACE((use_wrapper ? "With " : "No ") + std::string("wrapper"));

      Options options = CurrentOptions();
      options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
      BlockBasedTableOptions bbto;
      bbto.enable_index_compression = false;
      options.table_factory.reset(NewBlockBasedTableFactory(bbto));
      options.compression_manager = use_wrapper ? mgr : nullptr;
      DestroyAndReopen(options);

      Random rnd(301);
      constexpr int kCount = 13;

      // Highly compressible blocks, except 1 non-compressible. Half of the
      // compressible are morked for bypass and 1 marked for rejection. Values
      // are large enough to ensure just 1 k-v per block.
      for (int i = 0; i < kCount; ++i) {
        std::string value;
        if (i == 6) {
          // One non-compressible block
          value = rnd.RandomBinaryString(20000);
        } else {
          test::CompressibleString(&rnd, 0.1, 20000, &value);
        }
        values.push_back(value);
        ASSERT_OK(Put(Key(i), value));
        ASSERT_EQ(Get(Key(i)), value);
      }
      ASSERT_OK(Flush());

      // Ensure well-formed for reads
      for (int i = 0; i < kCount; ++i) {
        ASSERT_NE(Get(Key(i)), "NOT_FOUND");
        ASSERT_EQ(Get(Key(i)), values[i]);
      }
      ASSERT_EQ(Get(Key(kCount)), "NOT_FOUND");
    }
  }
}

TEST_F(DBCompressionTest, RandomMixedCompressionManager) {
  if (ZSTD_Supported()) {
    auto mgr = std::make_shared<RandomMixedCompressionManager>(
        GetBuiltinV2CompressionManager());
    std::vector<std::string> values;
    for (bool use_wrapper : {true}) {
      SCOPED_TRACE((use_wrapper ? "With " : "No ") + std::string("wrapper"));

      Options options = CurrentOptions();
      options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
      BlockBasedTableOptions bbto;
      bbto.enable_index_compression = false;
      options.table_factory.reset(NewBlockBasedTableFactory(bbto));
      options.compression_manager = use_wrapper ? mgr : nullptr;
      DestroyAndReopen(options);

      Random rnd(301);
      constexpr int kCount = 13;

      // Highly compressible blocks, except 1 non-compressible. Half of the
      // compressible are morked for bypass and 1 marked for rejection. Values
      // are large enough to ensure just 1 k-v per block.
      for (int i = 0; i < kCount; ++i) {
        std::string value;
        if (i == 6) {
          // One non-compressible block
          value = rnd.RandomBinaryString(20000);
        } else {
          test::CompressibleString(&rnd, 0.1, 20000, &value);
        }
        values.push_back(value);
        ASSERT_OK(Put(Key(i), value));
        ASSERT_EQ(Get(Key(i)), value);
      }
      ASSERT_OK(Flush());

      // Ensure well-formed for reads
      for (int i = 0; i < kCount; ++i) {
        ASSERT_NE(Get(Key(i)), "NOT_FOUND");
        ASSERT_EQ(Get(Key(i)), values[i]);
      }
      ASSERT_EQ(Get(Key(kCount)), "NOT_FOUND");
    }
  }
}

namespace {
// Template parameter to distinguish data blocks vs. v4+ index blocks
template <bool kIndexBlockV4>
static Status ValidateRocksBlock(Slice data) {
  const char* src = data.data();
  size_t srcSize = data.size();
  const char* const block_type_str =
      kIndexBlockV4 ? "Index block" : "Data block";

  // Minimum RocksDB block content size: at least 1 entry + restarts
  if (srcSize < 8) {
    return Status::Corruption(std::string(block_type_str) + " too small");
  }

  uint32_t numRestarts = DecodeFixed32(src + srcSize - sizeof(uint32_t));

  // Sanity check: num_restarts should be reasonable
  // TODO: also support data block hash index
  if (numRestarts > srcSize / 4 || numRestarts == 0) {
    return Status::Corruption(std::string("Invalid num_restarts in ") +
                              block_type_str);
  }

  size_t restartsSize = numRestarts * sizeof(uint32_t) + sizeof(uint32_t);
  if (srcSize < restartsSize) {
    return Status::Corruption(std::string(block_type_str) +
                              " too small for restarts array");
  }

  size_t entriesSize = srcSize - restartsSize;
  const char* entriesEnd = src + entriesSize;

  // Parse entries
  const char* p = src;
  while (p < entriesEnd) {
    // Parse shared_bytes varint
    uint32_t shared;
    const char* next = GetVarint32Ptr(p, entriesEnd, &shared);
    if (next == nullptr) {
      return Status::Corruption(std::string("Invalid shared_bytes varint in ") +
                                block_type_str);
    }
    p = next;

    // Parse unshared_bytes varint
    uint32_t unshared;
    next = GetVarint32Ptr(p, entriesEnd, &unshared);
    if (next == nullptr) {
      return Status::Corruption(
          std::string("Invalid unshared_bytes varint in ") + block_type_str);
    }
    p = next;

    uint32_t valueLen = 0;
    if constexpr (!kIndexBlockV4) {
      // For data blocks, parse value_length varint
      next = GetVarint32Ptr(p, entriesEnd, &valueLen);
      if (next == nullptr) {
        return Status::Corruption(
            std::string("Invalid value_length varint in ") + block_type_str);
      }
      p = next;
    }

    // Validate key delta
    if (p + unshared > entriesEnd) {
      return Status::Corruption(
          std::string("Key delta exceeds end of entries in ") + block_type_str);
    }
    p += unshared;

    if constexpr (kIndexBlockV4) {
      // For v4 index blocks, value is self-describing (varints)
      // Parse first varint (always present)
      uint32_t v1;
      next = GetVarint32Ptr(p, entriesEnd, &v1);
      if (next == nullptr) {
        return Status::Corruption(std::string("Invalid value varint in ") +
                                  block_type_str);
      }
      p = next;

      // If shared_bytes == 0, there's a second varint
      if (shared == 0) {
        uint32_t v2;
        next = GetVarint32Ptr(p, entriesEnd, &v2);
        if (next == nullptr) {
          return Status::Corruption(
              std::string("Invalid second value varint in ") + block_type_str);
        }
        p = next;
      }
    } else {
      // For data blocks, validate value
      if (p + valueLen > entriesEnd) {
        return Status::Corruption(
            std::string("Value exceeds end of entries in ") + block_type_str);
      }
      p += valueLen;
    }
  }

  return Status::OK();
}
}  // anonymous namespace

class DBCompressionTestMaybeParallel
    : public DBCompressionTest,
      public testing::WithParamInterface<std::tuple<int, bool>> {
 public:
  DBCompressionTestMaybeParallel()
      : DBCompressionTest(),
        parallel_threads_(std::get<0>(GetParam())),
        use_dict_(std::get<1>(GetParam())) {}

 protected:
  int parallel_threads_;
  bool use_dict_;
};

INSTANTIATE_TEST_CASE_P(DBCompressionTest, DBCompressionTestMaybeParallel,
                        ::testing::Combine(::testing::Values(1, 4),
                                           ::testing::Values(false, true)));

TEST_P(DBCompressionTestMaybeParallel, CompressionManagerWrapper) {
  // Test that we can use a custom CompressionManager to wrap the built-in
  // CompressionManager, thus adopting a custom *strategy* based on existing
  // algorithms. This will "mark" some blocks (in their contents) as "do not
  // compress", i.e. no attempt to compress, and some blocks as "reject
  // compression", i.e. compression attempted but rejected because of ratio
  // or otherwise. These cases are distinguishable for statistics that
  // approximate "wasted effort".
  static std::string kDoNotCompress = "do_not_compress";
  static std::string kRejectCompression = "reject_compression";

  static RelaxedAtomic<int> dataCheckedCount{0};
  static RelaxedAtomic<int> indexCheckedCount{0};
  static RelaxedAtomic<int> compressCalledCount{0};

  // We also have wrappers here to help verify that when RocksDB asks to
  // specialize the Compressor for a particular kind of block, it only passes in
  // that kind of block to ensure proper grouping of related data for
  // compression. We check this by parsing the subtly distinct schemas of data
  // blocks vs. v4+ index blocks. This also ensures that structure-aware
  // compressions like OpenZL can parse the data block and index block formats.
  struct CheckDataBlockCompressorWrapper : public CompressorWrapper {
    using CompressorWrapper::CompressorWrapper;
    const char* Name() const override { return "CheckDataBlockCompressor"; }

    std::unique_ptr<Compressor> Clone() const override {
      return std::make_unique<CheckDataBlockCompressorWrapper>(
          wrapped_->Clone());
    }

    Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                         size_t* compressed_output_size,
                         CompressionType* out_compression_type,
                         ManagedWorkingArea* working_area) override {
      dataCheckedCount.FetchAddRelaxed(1);
      // Parse and validate data block format before compressing
      Status s = ValidateRocksBlock</*kIndexBlockV4=*/false>(uncompressed_data);
      if (!s.ok()) {
        return s;
      }
      // Delegate to wrapped compressor on success
      return wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                     compressed_output_size,
                                     out_compression_type, working_area);
    }
  };

  struct CheckIndexBlockCompressorWrapper : public CompressorWrapper {
    using CompressorWrapper::CompressorWrapper;
    const char* Name() const override { return "CheckIndexBlockCompressor"; }

    std::unique_ptr<Compressor> Clone() const override {
      return std::make_unique<CheckIndexBlockCompressorWrapper>(
          wrapped_->Clone());
    }

    Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                         size_t* compressed_output_size,
                         CompressionType* out_compression_type,
                         ManagedWorkingArea* working_area) override {
      indexCheckedCount.FetchAddRelaxed(1);
      // Parse and validate index block v4 format before compressing
      Status s = ValidateRocksBlock</*kIndexBlockV4=*/true>(uncompressed_data);
      if (!s.ok()) {
        return s;
      }
      // Delegate to wrapped compressor on success
      return wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                     compressed_output_size,
                                     out_compression_type, working_area);
    }
  };

  struct MyCompressor : public CompressorWrapper {
    using CompressorWrapper::CompressorWrapper;
    const char* Name() const override { return "MyCompressor"; }

    std::unique_ptr<Compressor> Clone() const override {
      return std::make_unique<MyCompressor>(wrapped_->Clone());
    }

    Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                         size_t* compressed_output_size,
                         CompressionType* out_compression_type,
                         ManagedWorkingArea* working_area) override {
      compressCalledCount.FetchAddRelaxed(1);
      auto begin = uncompressed_data.data();
      auto end = uncompressed_data.data() + uncompressed_data.size();
      if (std::search(begin, end, kDoNotCompress.begin(),
                      kDoNotCompress.end()) != end) {
        // Do not attempt compression
        *compressed_output_size = 0;
        EXPECT_EQ(*out_compression_type, kNoCompression);
        return Status::OK();
      } else if (std::search(begin, end, kRejectCompression.begin(),
                             kRejectCompression.end()) != end) {
        // Simulate attempted & rejected compression
        *compressed_output_size = 1;
        EXPECT_EQ(*out_compression_type, kNoCompression);
        return Status::OK();
      } else {
        return wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                       compressed_output_size,
                                       out_compression_type, working_area);
      }
    }

    // Also check WorkingArea handling
    struct MyWorkingArea : public WorkingArea {
      explicit MyWorkingArea(ManagedWorkingArea&& wrapped)
          : wrapped_(std::move(wrapped)) {}
      ManagedWorkingArea wrapped_;
    };

    ManagedWorkingArea ObtainWorkingArea() override {
      ManagedWorkingArea rv{
          new MyWorkingArea{CompressorWrapper::ObtainWorkingArea()}, this};
      if (GetPreferredCompressionType() == kZSTD) {
        // ZSTD should always use WorkingArea, so this is our chance to ensure
        // CompressorWrapper::ObtainWorkingArea() is properly connected
        assert(rv.get() != nullptr);
      }
      return rv;
    }

    void ReleaseWorkingArea(WorkingArea* wa) override {
      delete static_cast<MyWorkingArea*>(wa);
    }

    std::unique_ptr<Compressor> MaybeCloneSpecialized(
        CacheEntryRole block_type,
        DictSampleArgs&& dict_samples) const override {
      std::unique_ptr<Compressor> result = std::make_unique<MyCompressor>(
          wrapped_->CloneMaybeSpecialized(block_type, std::move(dict_samples)));
      if (block_type == CacheEntryRole::kDataBlock) {
        result = std::make_unique<CheckDataBlockCompressorWrapper>(
            std::move(result));
      } else if (block_type == CacheEntryRole::kIndexBlock) {
        result = std::make_unique<CheckIndexBlockCompressorWrapper>(
            std::move(result));
      }
      return result;
    }
  };
  struct MyManager : public CompressionManagerWrapper {
    using CompressionManagerWrapper::CompressionManagerWrapper;
    const char* Name() const override { return "MyManager"; }
    std::unique_ptr<Compressor> GetCompressorForSST(
        const FilterBuildingContext& context, const CompressionOptions& opts,
        CompressionType preferred) override {
      return std::make_unique<MyCompressor>(
          wrapped_->GetCompressorForSST(context, opts, preferred));
    }
  };
  auto mgr = std::make_shared<MyManager>(GetBuiltinV2CompressionManager());

  for (CompressionType type : GetSupportedCompressions()) {
    for (bool use_wrapper : {false, true}) {
      if (type == kNoCompression) {
        continue;
      }
      SCOPED_TRACE("Compression type: " + std::to_string(type) +
                   (use_wrapper ? " with " : " no ") + "wrapper");

      Options options = CurrentOptions();
      options.compression = type;
      options.compression_opts.parallel_threads = parallel_threads_;
      options.compression_opts.max_dict_bytes = use_dict_ ? 4096 : 0;
      options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
      BlockBasedTableOptions bbto;
      bbto.enable_index_compression = true;
      bbto.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
      bbto.partition_filters = true;
      bbto.filter_policy.reset(NewBloomFilterPolicy(5));
      options.table_factory.reset(NewBlockBasedTableFactory(bbto));
      options.compression_manager = use_wrapper ? mgr : nullptr;
      DestroyAndReopen(options);

      auto PopStat = [&](Tickers t) -> uint64_t {
        return options.statistics->getAndResetTickerCount(t);
      };

      Random rnd(301);
      constexpr int kCount = 13;

      // Highly compressible blocks, except 1 non-compressible. Half of the
      // compressible are morked for bypass and 1 marked for rejection. Values
      // are large enough to ensure just 1 k-v per block.
      for (int i = 0; i < kCount; ++i) {
        std::string value;
        if (i == 6) {
          // One non-compressible block
          value = rnd.RandomBinaryString(20000);
        } else {
          test::CompressibleString(&rnd, 0.1, 20000, &value);
          if ((i % 2) == 0) {
            // Half for bypass
            value += kDoNotCompress;
          } else if (i == 7) {
            // One for rejection
            value += kRejectCompression;
          }
        }
        ASSERT_OK(Put(Key(i), value));
      }
      ASSERT_OK(Flush());

      // Index partition is compressed
      constexpr int kIdxComp = 1;
      // Top level index block is rejected for compression
      constexpr int kIdxRej = 1;

      if (use_dict_) {
        // FIXME: why don't the stats match? (for now, checking for crashes)
      } else if (use_wrapper) {
        EXPECT_EQ(kCount / 2 - 1 + kIdxComp, PopStat(NUMBER_BLOCK_COMPRESSED));
        EXPECT_EQ(kCount / 2, PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED));
        EXPECT_EQ(1 + 1 + kIdxRej, PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED));
      } else {
        EXPECT_EQ(kCount - 1 + kIdxComp, PopStat(NUMBER_BLOCK_COMPRESSED));
        EXPECT_EQ(0, PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED));
        EXPECT_EQ(1 + kIdxRej, PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED));
      }

      // Ensure well-formed for reads
      for (int i = 0; i < kCount; ++i) {
        ASSERT_NE(Get(Key(i)), "NOT_FOUND");
      }
      ASSERT_EQ(Get(Key(kCount)), "NOT_FOUND");

      // Ensure expected checks were performed
      EXPECT_EQ(indexCheckedCount.ExchangeRelaxed(0),
                use_wrapper ? kIdxComp + kIdxRej : 0);
      EXPECT_EQ(dataCheckedCount.ExchangeRelaxed(0), use_wrapper ? kCount : 0);
      // And every use of MyCompressor went through either the data block
      // checker or index block checker
      EXPECT_EQ(compressCalledCount.ExchangeRelaxed(0),
                use_wrapper ? kIdxComp + kIdxRej + kCount : 0);
    }
  }
}

namespace {
std::string UniqueName(const std::string& base) {
  static RelaxedAtomic<int> counter{0};
  return base + std::to_string(counter.FetchAddRelaxed(1));
}
}  // anonymous namespace

TEST_P(DBCompressionTestMaybeParallel, CompressionManagerCustomCompression) {
  // Test that we can use a custom CompressionManager to implement custom
  // compression algorithms, and that there are appropriate schema guard rails
  // to ensure data is not processed by the wrong algorithm.
  using Compressor8A = test::CompressorCustomAlg<kCustomCompression8A>;
  using Compressor8B = test::CompressorCustomAlg<kCustomCompression8B>;
  using Compressor8C = test::CompressorCustomAlg<kCustomCompression8C>;

  if (!Compressor8A::Supported() || !LZ4_Supported()) {
    fprintf(stderr,
            "Prerequisite compression library not supported. Skipping\n");
    return;
  }

  class MyManager : public CompressionManager {
   public:
    explicit MyManager(const std::string& compat_name)
        : compat_name_(compat_name), name_("MyManager:" + compat_name_) {}
    const char* Name() const override { return name_.c_str(); }
    const char* CompatibilityName() const override {
      return compat_name_.c_str();
    }

    bool SupportsCompressionType(CompressionType type) const override {
      return type == kCustomCompression8A || type == kCustomCompression8B ||
             type == kCustomCompression8C ||
             GetBuiltinV2CompressionManager()->SupportsCompressionType(type);
    }

    int used_compressor8A_count_ = 0;
    int used_compressor8B_count_ = 0;
    int used_compressor8C_count_ = 0;

    std::unique_ptr<Compressor> GetCompressor(const CompressionOptions& opts,
                                              CompressionType type) override {
      switch (static_cast<unsigned char>(type)) {
        case kCustomCompression8A:
          used_compressor8A_count_++;
          return std::make_unique<Compressor8A>();
        case kCustomCompression8B:
          used_compressor8B_count_++;
          return std::make_unique<Compressor8B>();
        case kCustomCompression8C:
          used_compressor8C_count_++;
          return std::make_unique<Compressor8C>();
        // Also support built-in compression algorithms
        default:
          return GetBuiltinV2CompressionManager()->GetCompressor(opts, type);
      }
    }

    std::shared_ptr<Decompressor> GetDecompressor() override {
      return std::make_shared<test::DecompressorCustomAlg>();
    }

    RelaxedAtomic<CompressionType> last_specific_decompressor_type_{
        kNoCompression};

    std::shared_ptr<Decompressor> GetDecompressorForTypes(
        const CompressionType* types_begin,
        const CompressionType* types_end) override {
      assert(types_end > types_begin);
      last_specific_decompressor_type_.StoreRelaxed(*types_begin);
      auto decomp = std::make_shared<test::DecompressorCustomAlg>();
      decomp->SetAllowedTypes(types_begin, types_end);
      return decomp;
    }

    void AddFriend(const std::shared_ptr<CompressionManager>& mgr) {
      friends_[mgr->CompatibilityName()] = mgr;
    }
    std::shared_ptr<CompressionManager> FindCompatibleCompressionManager(
        Slice compatibility_name) override {
      std::shared_ptr<CompressionManager> rv =
          CompressionManager::FindCompatibleCompressionManager(
              compatibility_name);
      if (!rv) {
        auto it = friends_.find(compatibility_name.ToString());
        if (it != friends_.end()) {
          return it->second.lock();
        }
      }
      return rv;
    }

   private:
    std::string compat_name_;
    std::string name_;
    // weak_ptr to avoid cycles
    std::map<std::string, std::weak_ptr<CompressionManager>> friends_;
  };

  // Although these compression managers are actually compatible, we must
  // respect their distinct compatibility names and treat them as incompatible
  // (or else risk processing data incorrectly)
  // NOTE: these are not registered in ObjectRegistry to test what happens
  // when the original CompressionManager might not be available, but
  // mgr_bar will be registered during the test, with different names to
  // prevent interference between iterations.
  auto mgr_foo = std::make_shared<MyManager>("Foo");
  auto mgr_bar = std::make_shared<MyManager>(UniqueName("Bar"));

  // And this one claims to be fully compatible with the built-in compression
  // manager when it's not fully compatible (for custom CompressionTypes)
  auto mgr_claim_compatible = std::make_shared<MyManager>("BuiltinV2");

  constexpr uint16_t kValueSize = 10000;

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 20;
  BlockBasedTableOptions bbto;
  bbto.enable_index_compression = false;
  bbto.format_version = 6;  // Before custom compression alg support
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  // Claims not to use custom compression (and doesn't unless setting a custom
  // CompressionType)
  options.compression_manager = mgr_claim_compatible;
  // Use a built-in compression type with dictionary support
  options.compression = kLZ4Compression;
  options.compression_opts.max_dict_bytes = use_dict_ ? kValueSize / 2 : 0;
  options.compression_opts.parallel_threads = parallel_threads_;
  DestroyAndReopen(options);

  Random rnd(404);
  std::string value;
  ASSERT_OK(Put("a", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  ASSERT_OK(Flush());

  // That data should be readable without access to the original compression
  // manager, because it used the built-in CompatibilityName and a built-in
  // CompressionType
  options.compression_manager = nullptr;
  Reopen(options);
  ASSERT_EQ(Get("a"), value);

  // Verify it was compressed
  Range r = {"a", "a0"};
  TablePropertiesCollection tables_properties;
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  EXPECT_EQ(tables_properties.begin()->second->compression_name, "LZ4");

  // Disallow setting a custom CompressionType with a CompressionManager
  // claiming to be built-in compatible.
  options.compression_manager = mgr_claim_compatible;
  options.compression = kCustomCompression8A;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kInvalidArgument);

  options.compression_manager = nullptr;
  options.compression = kCustomCompressionFE;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kInvalidArgument);
  options.compression =
      static_cast<CompressionType>(kLastBuiltinCompression + 1);
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kInvalidArgument);

  // Custom compression schema (different CompatibilityName) not supported
  // before format_version=7
  options.compression_manager = mgr_foo;
  options.compression = kLZ4Compression;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kInvalidArgument);

  // Set format version supporting custom compression
  bbto.format_version = 7;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  // Custom compression type not supported with built-in schema name, even
  // with format_version=7
  options.compression_manager = mgr_claim_compatible;
  options.compression = kCustomCompression8B;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kInvalidArgument);

  // Custom compression schema, but specifying a custom compression type it
  // doesn't support.
  options.compression_manager = mgr_foo;
  options.compression = kCustomCompressionF0;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kNotSupported);

  // Using a built-in compression type with fv=7 but named custom schema
  options.compression = kLZ4Compression;
  Reopen(options);
  ASSERT_OK(Put("b", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);
  ASSERT_EQ(Get("b"), value);

  // Verify it was compressed with LZ4
  r = {"b", "b0"};
  tables_properties.clear();
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  // Uses new format for "compression_name" property
  EXPECT_EQ(tables_properties.begin()->second->compression_name, "Foo;04;");
  EXPECT_EQ(mgr_foo->last_specific_decompressor_type_.LoadRelaxed(),
            kLZ4Compression);

  // Custom compression type
  options.compression = kCustomCompression8A;
  Reopen(options);
  ASSERT_OK(Put("c", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  EXPECT_EQ(mgr_foo->used_compressor8A_count_, 0);
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 3);
  ASSERT_EQ(Get("c"), value);
  EXPECT_EQ(mgr_foo->used_compressor8A_count_, 1);

  // Verify it was compressed with custom format
  r = {"c", "c0"};
  tables_properties.clear();
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  EXPECT_EQ(tables_properties.begin()->second->compression_name, "Foo;8A;");
  EXPECT_EQ(mgr_foo->last_specific_decompressor_type_.LoadRelaxed(),
            kCustomCompression8A);

  // Also dynamically changeable, because the compression manager will respect
  // the current setting as reported under the legacy logic
  ASSERT_OK(dbfull()->SetOptions({{"compression", "kLZ4Compression"}}));
  ASSERT_OK(Put("d", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 4);
  ASSERT_EQ(Get("d"), value);

  // Verify it was compressed with LZ4
  r = {"d", "d0"};
  tables_properties.clear();
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  EXPECT_EQ(tables_properties.begin()->second->compression_name, "Foo;04;");
  EXPECT_EQ(mgr_foo->last_specific_decompressor_type_.LoadRelaxed(),
            kLZ4Compression);

  // Dynamically changeable to custom compressions also
  ASSERT_OK(dbfull()->SetOptions({{"compression", "kCustomCompression8B"}}));
  ASSERT_OK(Put("e", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 5);
  ASSERT_EQ(Get("e"), value);

  // Verify it was compressed with custom format
  r = {"e", "e0"};
  tables_properties.clear();
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  EXPECT_EQ(tables_properties.begin()->second->compression_name, "Foo;8B;");
  EXPECT_EQ(mgr_foo->last_specific_decompressor_type_.LoadRelaxed(),
            kCustomCompression8B);

  // Fails to re-open with incompatible compression manager (can't find
  // compression manager Foo because it's not registered nor known by Bar)
  options.compression_manager = mgr_bar;
  options.compression = kLZ4Compression;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kNotSupported);

  // But should re-open if we make Bar aware of the Foo compression manager
  mgr_bar->AddFriend(mgr_foo);
  Reopen(options);

  // Can still read everything
  ASSERT_EQ(Get("a").size(), kValueSize);
  ASSERT_EQ(Get("b").size(), kValueSize);
  ASSERT_EQ(Get("c").size(), kValueSize);
  ASSERT_EQ(Get("d").size(), kValueSize);
  ASSERT_EQ(Get("e").size(), kValueSize);

  // Add a file using mgr_bar
  ASSERT_OK(Put("f", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 6);
  ASSERT_EQ(Get("f"), value);

  // Verify it was compressed appropriately
  r = {"f", "f0"};
  tables_properties.clear();
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  EXPECT_EQ(mgr_bar->last_specific_decompressor_type_.LoadRelaxed(),
            kLZ4Compression);

  // Fails to re-open with incompatible compression manager (can't find
  // compression manager Bar because it's not registered nor known by Foo)
  options.compression_manager = mgr_foo;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kNotSupported);

  // Register and re-open
  auto& library = *ObjectLibrary::Default();
  library.AddFactory<CompressionManager>(
      mgr_bar->CompatibilityName(),
      [mgr_bar](const std::string& /*uri*/,
                std::unique_ptr<CompressionManager>* guard,
                std::string* /*errmsg*/) {
        *guard = std::make_unique<MyManager>(mgr_bar->CompatibilityName());
        return guard->get();
      });
  Reopen(options);

  // Can still read everything
  ASSERT_EQ(Get("a").size(), kValueSize);
  ASSERT_EQ(Get("b").size(), kValueSize);
  ASSERT_EQ(Get("c").size(), kValueSize);
  ASSERT_EQ(Get("d").size(), kValueSize);
  ASSERT_EQ(Get("e").size(), kValueSize);
  ASSERT_EQ(Get("f").size(), kValueSize);

  // TODO: test old version of a compression manager unable to read a
  // compression type
}

TEST_F(DBCompressionTest, FailWhenCompressionNotSupportedTest) {
  CompressionType compressions[] = {kZlibCompression, kBZip2Compression,
                                    kLZ4Compression, kLZ4HCCompression,
                                    kXpressCompression};
  for (auto comp : compressions) {
    if (!CompressionTypeSupported(comp)) {
      // not supported, we should fail the Open()
      Options options = CurrentOptions();
      options.compression = comp;
      ASSERT_TRUE(!TryReopen(options).ok());
      // Try if CreateColumnFamily also fails
      options.compression = kNoCompression;
      ASSERT_OK(TryReopen(options));
      ColumnFamilyOptions cf_options(options);
      cf_options.compression = comp;
      ColumnFamilyHandle* handle;
      ASSERT_TRUE(!db_->CreateColumnFamily(cf_options, "name", &handle).ok());
    }
  }
}

class AutoSkipTestFlushBlockPolicy : public FlushBlockPolicy {
 public:
  explicit AutoSkipTestFlushBlockPolicy(const int window,
                                        const BlockBuilder& data_block_builder,
                                        std::shared_ptr<Statistics> statistics)
      : window_(window),
        num_keys_(0),
        data_block_builder_(data_block_builder),
        statistics_(statistics) {}

  bool Update(const Slice& /*key*/, const Slice& /*value*/) override {
    auto nth_window = num_keys_ / window_;
    if (data_block_builder_.empty()) {
      // First key in this block
      return false;
    }
    // Check every window
    if (num_keys_ % window_ == 0) {
      auto set_exploration = [&](void* arg) {
        bool* exploration = static_cast<bool*>(arg);
        *exploration = true;
      };
      auto unset_exploration = [&](void* arg) {
        bool* exploration = static_cast<bool*>(arg);
        *exploration = false;
      };
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      // We force exploration to set the predicted rejection ratio for odd
      // window and then test that the prediction is exploited in the even
      // window
      if (nth_window % 2 == 0) {
        SyncPoint::GetInstance()->SetCallBack(
            "AutoSkipCompressorWrapper::CompressBlock::exploitOrExplore",
            set_exploration);
      } else {
        SyncPoint::GetInstance()->SetCallBack(
            "AutoSkipCompressorWrapper::CompressBlock::exploitOrExplore",
            unset_exploration);
      }
      SyncPoint::GetInstance()->EnableProcessing();

      auto compressed_count = PopStat(NUMBER_BLOCK_COMPRESSED);
      auto bypassed_count = PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED);
      auto rejected_count = PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED);
      auto total = compressed_count + rejected_count + bypassed_count;
      int rejection_percentage, bypassed_percentage, compressed_percentage;
      if (total != 0) {
        rejection_percentage = static_cast<int>(rejected_count * 100 / total);
        bypassed_percentage = static_cast<int>(bypassed_count * 100 / total);
        compressed_percentage =
            static_cast<int>(compressed_count * 100 / total);
        // use nth window to detect test cases and set the expected
        switch (nth_window) {
          case 1:
            // In first window we only explore and thus here we verify that the
            // correct prediction has been made by the end of the window
            // Since 6 of 10 blocks are compression unfriendly, the predicted
            // rejection ratio should be 60%
            EXPECT_EQ(rejection_percentage, 60);
            EXPECT_EQ(bypassed_percentage, 0);
            EXPECT_EQ(compressed_percentage, 40);
            break;
          case 2:
            // With the rejection ratio set to 0.6 all the blocks should be
            // bypassed in next window
            EXPECT_EQ(rejection_percentage, 0);
            EXPECT_EQ(bypassed_percentage, 100);
            EXPECT_EQ(compressed_percentage, 0);
            break;
          case 3:
            // In third window we only explore and verify that the correct
            // prediction has been made by the end of the window
            // since 4 of 10 blocks are compression ufriendly, the predicted
            // rejection ratio should be 40%
            EXPECT_EQ(rejection_percentage, 40);
            EXPECT_EQ(bypassed_percentage, 0);
            EXPECT_EQ(compressed_percentage, 60);
            break;
          case 4:
            // With the rejection ratio set to 0.4 all the blocks should be
            // attempted to be compressed
            // 6 of 10 blocks are compression unfriendly and thus should be
            // rejected 4 of 10 blocks are compression friendly and thus should
            // be compressed
            EXPECT_EQ(rejection_percentage, 60);
            EXPECT_EQ(bypassed_percentage, 0);
            EXPECT_EQ(compressed_percentage, 40);
        }
      }
    }
    num_keys_++;
    return true;
  }
  uint64_t PopStat(Tickers t) { return statistics_->getAndResetTickerCount(t); }

 private:
  int window_;
  int num_keys_;
  const BlockBuilder& data_block_builder_;
  std::shared_ptr<Statistics> statistics_;
};

class AutoSkipTestFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
 public:
  explicit AutoSkipTestFlushBlockPolicyFactory(
      const int window, std::shared_ptr<Statistics> statistics)
      : window_(window), statistics_(statistics) {}

  virtual const char* Name() const override {
    return "AutoSkipTestFlushBlockPolicyFactory";
  }

  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& /*table_options*/,
      const BlockBuilder& data_block_builder) const override {
    (void)data_block_builder;
    return new AutoSkipTestFlushBlockPolicy(window_, data_block_builder,
                                            statistics_);
  }

 private:
  int window_;
  std::shared_ptr<Statistics> statistics_;
};

class DBAutoSkip : public DBTestBase {
 public:
  Options options;
  Random rnd_;
  int key_index_;
  DBAutoSkip()
      : DBTestBase("db_auto_skip", /*env_do_fsync=*/true),
        options(CurrentOptions()),
        rnd_(231),
        key_index_(0) {
    options.compression_manager = CreateAutoSkipCompressionManager();
    auto statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.statistics = statistics;
    options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
    BlockBasedTableOptions bbto;
    bbto.enable_index_compression = false;
    bbto.flush_block_policy_factory.reset(
        new AutoSkipTestFlushBlockPolicyFactory(10, statistics));
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  }

  bool CompressionFriendlyPut(const int no_of_kvs, const int size_of_value) {
    auto value = std::string(size_of_value, 'A');
    for (int i = 0; i < no_of_kvs; ++i) {
      auto status = Put(Key(key_index_), value);
      EXPECT_EQ(status.ok(), true);
      key_index_++;
    }
    return true;
  }
  bool CompressionUnfriendlyPut(const int no_of_kvs, const int size_of_value) {
    auto value = rnd_.RandomBinaryString(size_of_value);
    for (int i = 0; i < no_of_kvs; ++i) {
      auto status = Put(Key(key_index_), value);
      EXPECT_EQ(status.ok(), true);
      key_index_++;
    }
    return true;
  }
};

TEST_F(DBAutoSkip, AutoSkipCompressionManager) {
  for (uint32_t max_dict_bytes : {0, 10000}) {
    for (auto type : GetSupportedCompressions()) {
      if (type == kNoCompression) {
        continue;
      }
      options.compression = type;
      options.bottommost_compression = type;
      options.compression_opts.max_dict_bytes = max_dict_bytes;
      DestroyAndReopen(options);
      const int kValueSize = 20000;
      // This will set the rejection ratio to 60%
      CompressionUnfriendlyPut(6, kValueSize);
      CompressionFriendlyPut(4, kValueSize);
      // This will verify all the data block compressions are bypassed based on
      // previous prediction
      CompressionUnfriendlyPut(6, kValueSize);
      CompressionFriendlyPut(4, kValueSize);
      // This will set the rejection ratio to 40%
      CompressionUnfriendlyPut(4, kValueSize);
      CompressionFriendlyPut(6, kValueSize);
      // This will verify all the data block compression are attempted based on
      // previous prediction
      // Compression will be rejected for 6 compression unfriendly blocks
      // Compression will be accepted for 4 compression friendly blocks
      CompressionUnfriendlyPut(6, kValueSize);
      CompressionFriendlyPut(4, kValueSize);
      // Extra block write to ensure that the all above cases are checked
      CompressionFriendlyPut(6, kValueSize);
      CompressionFriendlyPut(4, kValueSize);
      ASSERT_OK(Flush());
    }
  }
}
class CostAwareTestFlushBlockPolicy : public FlushBlockPolicy {
 public:
  explicit CostAwareTestFlushBlockPolicy(const int window,
                                         const BlockBuilder& data_block_builder)
      : window_(window),
        num_keys_(0),
        data_block_builder_(data_block_builder) {}

  bool Update(const Slice& /*key*/, const Slice& /*value*/) override {
    auto nth_window = num_keys_ / window_;
    if (data_block_builder_.empty()) {
      // First key in this block
      return false;
    }
    // Check every window
    if (num_keys_ % window_ == 0) {
      auto get_predictor = [&](void* arg) {
        // gets the predictor and sets the mocked cpu and io cost
        predictor_ = static_cast<IOCPUCostPredictor*>(arg);
        predictor_->CPUPredictor.SetPrediction(1000);
        predictor_->IOPredictor.SetPrediction(100);
      };
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();

      // Add syncpoint to get the cpu and io cost
      SyncPoint::GetInstance()->SetCallBack(
          "CostAwareCompressor::CompressBlockAndRecord::"
          "GetPredictor",
          get_predictor);
      SyncPoint::GetInstance()->EnableProcessing();
      // use nth window to detect test cases and set the expected
      switch (nth_window) {
        case 0:
          break;
        case 1:
          // Verify that the Mocked cpu cost and io cost are predicted correctly
          auto predicted_cpu_time = predictor_->CPUPredictor.Predict();
          auto predicted_io_bytes = predictor_->IOPredictor.Predict();
          EXPECT_EQ(predicted_io_bytes, 100);
          EXPECT_EQ(predicted_cpu_time, 1000);
          break;
      }
    }
    num_keys_++;
    return true;
  }

 private:
  int window_;
  int num_keys_;
  const BlockBuilder& data_block_builder_;
  IOCPUCostPredictor* predictor_;
};
class CostAwareTestFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
 public:
  explicit CostAwareTestFlushBlockPolicyFactory(const int window)
      : window_(window) {}

  virtual const char* Name() const override {
    return "CostAwareTestFlushBlockPolicyFactory";
  }

  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& /*table_options*/,
      const BlockBuilder& data_block_builder) const override {
    (void)data_block_builder;
    return new CostAwareTestFlushBlockPolicy(window_, data_block_builder);
  }

 private:
  int window_;
};
class DBCompressionCostPredictor : public DBTestBase {
 public:
  Options options;
  DBCompressionCostPredictor()
      : DBTestBase("db_cpuio_skip", /*env_do_fsync=*/true),
        options(CurrentOptions()) {
    options.compression_manager = CreateCostAwareCompressionManager();
    auto statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.statistics = statistics;
    options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
    BlockBasedTableOptions bbto;
    bbto.enable_index_compression = false;
    bbto.flush_block_policy_factory.reset(
        new CostAwareTestFlushBlockPolicyFactory(10));
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    DestroyAndReopen(options);
  }
};
TEST_F(DBCompressionCostPredictor, CostAwareCompressorManager) {
  // making sure that the compression is supported
  if (!ZSTD_Supported()) {
    return;
  }
  const int kValueSize = 20000;
  int next_key = 0;
  Random rnd(231);
  auto value = rnd.RandomBinaryString(kValueSize);
  int window_size = 10;
  auto WindowWrite = [&]() {
    for (auto i = 0; i < window_size; ++i) {
      auto status = Put(Key(next_key), value);
      EXPECT_OK(status);
      next_key++;
    }
  };
  // This denotes the first window
  // Mocked to have specific cpu utilization and io cost
  WindowWrite();
  // check the predictor is predicting the correct cpu and io cost
  WindowWrite();
  ASSERT_OK(Flush());
}

}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
