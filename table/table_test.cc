//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/table.h"

#include <gtest/gtest.h>
#include <stddef.h>
#include <stdio.h>

#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "cache/lru_cache.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "memtable/stl_wrappers.h"
#include "monitoring/statistics.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/file_system.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/unique_id.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/flush_block_policy.h"
#include "table/block_fetcher.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/plain/plain_table_factory.h"
#include "table/scoped_arena_iterator.h"
#include "table/sst_file_writer_collectors.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding_lean.h"
#include "util/compression.h"
#include "util/file_checksum_helper.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/memory_allocators.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;
extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;

namespace {

const std::string kDummyValue(10000, 'o');

// DummyPropertiesCollector used to test BlockBasedTableProperties
class DummyPropertiesCollector : public TablePropertiesCollector {
 public:
  const char* Name() const override { return "DummyPropertiesCollector"; }

  Status Finish(UserCollectedProperties* /*properties*/) override {
    return Status::OK();
  }

  Status Add(const Slice& /*user_key*/, const Slice& /*value*/) override {
    return Status::OK();
  }

  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties{};
  }
};

class DummyPropertiesCollectorFactory1
    : public TablePropertiesCollectorFactory {
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /*context*/) override {
    return new DummyPropertiesCollector();
  }
  const char* Name() const override {
    return "DummyPropertiesCollectorFactory1";
  }
};

class DummyPropertiesCollectorFactory2
    : public TablePropertiesCollectorFactory {
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /*context*/) override {
    return new DummyPropertiesCollector();
  }
  const char* Name() const override {
    return "DummyPropertiesCollectorFactory2";
  }
};

// Return reverse of "key".
// Used to test non-lexicographic comparators.
std::string Reverse(const Slice& key) {
  auto rev = key.ToString();
  std::reverse(rev.begin(), rev.end());
  return rev;
}

class ReverseKeyComparator : public Comparator {
 public:
  const char* Name() const override {
    return "rocksdb.ReverseBytewiseComparator";
  }

  int Compare(const Slice& a, const Slice& b) const override {
    return BytewiseComparator()->Compare(Reverse(a), Reverse(b));
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    std::string s = Reverse(*start);
    std::string l = Reverse(limit);
    BytewiseComparator()->FindShortestSeparator(&s, l);
    *start = Reverse(s);
  }

  void FindShortSuccessor(std::string* key) const override {
    std::string s = Reverse(*key);
    BytewiseComparator()->FindShortSuccessor(&s);
    *key = Reverse(s);
  }
};

ReverseKeyComparator reverse_key_comparator;

void Increment(const Comparator* cmp, std::string* key) {
  if (cmp == BytewiseComparator()) {
    key->push_back('\0');
  } else {
    assert(cmp == &reverse_key_comparator);
    std::string rev = Reverse(*key);
    rev.push_back('\0');
    *key = Reverse(rev);
  }
}

const auto kUnknownColumnFamily =
    TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;

}  // namespace

// Helper class for tests to unify the interface between
// BlockBuilder/TableBuilder and Block/Table.
class Constructor {
 public:
  explicit Constructor(const Comparator* cmp)
      : data_(stl_wrappers::LessOfComparator(cmp)) {}
  virtual ~Constructor() {}

  void Add(const std::string& key, const Slice& value) {
    data_[key] = value.ToString();
  }

  // Finish constructing the data structure with all the keys that have
  // been added so far.  Returns the keys in sorted order in "*keys"
  // and stores the key/value pairs in "*kvmap"
  void Finish(const Options& options, const ImmutableOptions& ioptions,
              const MutableCFOptions& moptions,
              const BlockBasedTableOptions& table_options,
              const InternalKeyComparator& internal_comparator,
              std::vector<std::string>* keys, stl_wrappers::KVMap* kvmap) {
    last_internal_comparator_ = &internal_comparator;
    *kvmap = data_;
    keys->clear();
    for (const auto& kv : data_) {
      keys->push_back(kv.first);
    }
    data_.clear();
    Status s = FinishImpl(options, ioptions, moptions, table_options,
                          internal_comparator, *kvmap);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Construct the data structure from the data in "data"
  virtual Status FinishImpl(const Options& options,
                            const ImmutableOptions& ioptions,
                            const MutableCFOptions& moptions,
                            const BlockBasedTableOptions& table_options,
                            const InternalKeyComparator& internal_comparator,
                            const stl_wrappers::KVMap& data) = 0;

  virtual InternalIterator* NewIterator(
      const SliceTransform* prefix_extractor = nullptr) const = 0;

  virtual const stl_wrappers::KVMap& data() { return data_; }

  virtual bool IsArenaMode() const { return false; }

  virtual DB* db() const { return nullptr; }  // Overridden in DBConstructor

  virtual bool AnywayDeleteIterator() const { return false; }

 protected:
  const InternalKeyComparator* last_internal_comparator_;

 private:
  stl_wrappers::KVMap data_;
};

// A helper class that converts internal format keys into user keys
class KeyConvertingIterator : public InternalIterator {
 public:
  explicit KeyConvertingIterator(InternalIterator* iter,
                                 bool arena_mode = false)
      : iter_(iter), arena_mode_(arena_mode) {}
  ~KeyConvertingIterator() override {
    if (arena_mode_) {
      iter_->~InternalIterator();
    } else {
      delete iter_;
    }
  }
  bool Valid() const override { return iter_->Valid() && status_.ok(); }
  void Seek(const Slice& target) override {
    ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
    std::string encoded;
    AppendInternalKey(&encoded, ikey);
    iter_->Seek(encoded);
  }
  void SeekForPrev(const Slice& target) override {
    ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
    std::string encoded;
    AppendInternalKey(&encoded, ikey);
    iter_->SeekForPrev(encoded);
  }
  void SeekToFirst() override { iter_->SeekToFirst(); }
  void SeekToLast() override { iter_->SeekToLast(); }
  void Next() override { iter_->Next(); }
  void Prev() override { iter_->Prev(); }
  IterBoundCheck UpperBoundCheckResult() override {
    return iter_->UpperBoundCheckResult();
  }

  Slice key() const override {
    assert(Valid());
    ParsedInternalKey parsed_key;
    Status pik_status =
        ParseInternalKey(iter_->key(), &parsed_key, true /* log_err_key */);
    if (!pik_status.ok()) {
      status_ = pik_status;
      return Slice(status_.getState());
    }
    return parsed_key.user_key;
  }

  Slice value() const override { return iter_->value(); }
  Status status() const override {
    return status_.ok() ? iter_->status() : status_;
  }

 private:
  mutable Status status_;
  InternalIterator* iter_;
  bool arena_mode_;

  // No copying allowed
  KeyConvertingIterator(const KeyConvertingIterator&);
  void operator=(const KeyConvertingIterator&);
};

// `BlockConstructor` APIs always accept/return user keys.
class BlockConstructor : public Constructor {
 public:
  explicit BlockConstructor(const Comparator* cmp)
      : Constructor(cmp), comparator_(cmp), block_(nullptr) {}
  ~BlockConstructor() override { delete block_; }
  Status FinishImpl(const Options& /*options*/,
                    const ImmutableOptions& /*ioptions*/,
                    const MutableCFOptions& /*moptions*/,
                    const BlockBasedTableOptions& table_options,
                    const InternalKeyComparator& /*internal_comparator*/,
                    const stl_wrappers::KVMap& kv_map) override {
    delete block_;
    block_ = nullptr;
    BlockBuilder builder(table_options.block_restart_interval);

    for (const auto& kv : kv_map) {
      // `DataBlockIter` assumes it reads only internal keys. `BlockConstructor`
      // clients provide user keys, so we need to convert to internal key format
      // before writing the data block.
      ParsedInternalKey ikey(kv.first, kMaxSequenceNumber, kTypeValue);
      std::string encoded;
      AppendInternalKey(&encoded, ikey);
      builder.Add(encoded, kv.second);
    }
    // Open the block
    data_ = builder.Finish().ToString();
    BlockContents contents;
    contents.data = data_;
    block_ = new Block(std::move(contents));
    return Status::OK();
  }
  InternalIterator* NewIterator(
      const SliceTransform* /*prefix_extractor*/) const override {
    // `DataBlockIter` returns the internal keys it reads.
    // `KeyConvertingIterator` converts them to user keys before they are
    // exposed to the `BlockConstructor` clients.
    return new KeyConvertingIterator(
        block_->NewDataIterator(comparator_, kDisableGlobalSequenceNumber));
  }

 private:
  const Comparator* comparator_;
  std::string data_;
  Block* block_;

  BlockConstructor();
};

class TableConstructor : public Constructor {
 public:
  explicit TableConstructor(const Comparator* cmp,
                            bool convert_to_internal_key = false,
                            int level = -1, SequenceNumber largest_seqno = 0)
      : Constructor(cmp),
        largest_seqno_(largest_seqno),
        convert_to_internal_key_(convert_to_internal_key),
        level_(level) {
    env_ = ROCKSDB_NAMESPACE::Env::Default();
  }
  ~TableConstructor() override { Reset(); }

  Status FinishImpl(const Options& options, const ImmutableOptions& ioptions,
                    const MutableCFOptions& moptions,
                    const BlockBasedTableOptions& /*table_options*/,
                    const InternalKeyComparator& internal_comparator,
                    const stl_wrappers::KVMap& kv_map) override {
    Reset();
    soptions.use_mmap_reads = ioptions.allow_mmap_reads;
    std::unique_ptr<FSWritableFile> sink(new test::StringSink());
    file_writer_.reset(new WritableFileWriter(
        std::move(sink), "" /* don't care */, FileOptions()));
    std::unique_ptr<TableBuilder> builder;
    IntTblPropCollectorFactories int_tbl_prop_collector_factories;

    if (largest_seqno_ != 0) {
      // Pretend that it's an external file written by SstFileWriter.
      int_tbl_prop_collector_factories.emplace_back(
          new SstFileWriterPropertiesCollectorFactory(2 /* version */,
                                                      0 /* global_seqno*/));
    }

    std::string column_family_name;
    builder.reset(ioptions.table_factory->NewTableBuilder(
        TableBuilderOptions(ioptions, moptions, internal_comparator,
                            &int_tbl_prop_collector_factories,
                            options.compression, options.compression_opts,
                            kUnknownColumnFamily, column_family_name, level_),
        file_writer_.get()));

    for (const auto& kv : kv_map) {
      if (convert_to_internal_key_) {
        ParsedInternalKey ikey(kv.first, kMaxSequenceNumber, kTypeValue);
        std::string encoded;
        AppendInternalKey(&encoded, ikey);
        builder->Add(encoded, kv.second);
      } else {
        builder->Add(kv.first, kv.second);
      }
      EXPECT_OK(builder->status());
    }
    Status s = builder->Finish();
    EXPECT_OK(file_writer_->Flush());
    EXPECT_TRUE(s.ok()) << s.ToString();

    EXPECT_EQ(TEST_GetSink()->contents().size(), builder->FileSize());

    // Open the table
    file_num_ = cur_file_num_++;

    return Reopen(ioptions, moptions);
  }

  InternalIterator* NewIterator(
      const SliceTransform* prefix_extractor) const override {
    InternalIterator* iter = table_reader_->NewIterator(
        read_options_, prefix_extractor, /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized);
    if (convert_to_internal_key_) {
      return new KeyConvertingIterator(iter);
    } else {
      return iter;
    }
  }

  uint64_t ApproximateOffsetOf(const Slice& key) const {
    if (convert_to_internal_key_) {
      InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
      const Slice skey = ikey.Encode();
      return table_reader_->ApproximateOffsetOf(
          skey, TableReaderCaller::kUncategorized);
    }
    return table_reader_->ApproximateOffsetOf(
        key, TableReaderCaller::kUncategorized);
  }

  virtual Status Reopen(const ImmutableOptions& ioptions,
                        const MutableCFOptions& moptions) {
    std::unique_ptr<FSRandomAccessFile> source(new test::StringSource(
        TEST_GetSink()->contents(), file_num_, ioptions.allow_mmap_reads));

    file_reader_.reset(new RandomAccessFileReader(std::move(source), "test"));
    return ioptions.table_factory->NewTableReader(
        TableReaderOptions(ioptions, moptions.prefix_extractor, soptions,
                           *last_internal_comparator_, /*skip_filters*/ false,
                           /*immortal*/ false, false, level_,
                           &block_cache_tracer_, moptions.write_buffer_size, "",
                           file_num_, kNullUniqueId64x2, largest_seqno_),
        std::move(file_reader_), TEST_GetSink()->contents().size(),
        &table_reader_);
  }

  virtual TableReader* GetTableReader() { return table_reader_.get(); }

  bool AnywayDeleteIterator() const override {
    return convert_to_internal_key_;
  }

  void ResetTableReader() { table_reader_.reset(); }

  bool ConvertToInternalKey() { return convert_to_internal_key_; }

  test::StringSink* TEST_GetSink() {
    return static_cast<test::StringSink*>(file_writer_->writable_file());
  }

  BlockCacheTracer block_cache_tracer_;

 private:
  void Reset() {
    file_num_ = 0;
    table_reader_.reset();
    file_writer_.reset();
    file_reader_.reset();
  }

  const ReadOptions read_options_;
  uint64_t file_num_;
  std::unique_ptr<WritableFileWriter> file_writer_;
  std::unique_ptr<RandomAccessFileReader> file_reader_;
  std::unique_ptr<TableReader> table_reader_;
  SequenceNumber largest_seqno_;
  bool convert_to_internal_key_;
  int level_;

  TableConstructor();

  static uint64_t cur_file_num_;
  EnvOptions soptions;
  Env* env_;
};
uint64_t TableConstructor::cur_file_num_ = 1;

class MemTableConstructor : public Constructor {
 public:
  explicit MemTableConstructor(const Comparator* cmp, WriteBufferManager* wb)
      : Constructor(cmp),
        internal_comparator_(cmp),
        write_buffer_manager_(wb),
        table_factory_(new SkipListFactory) {
    options_.memtable_factory = table_factory_;
    ImmutableOptions ioptions(options_);
    memtable_ =
        new MemTable(internal_comparator_, ioptions, MutableCFOptions(options_),
                     wb, kMaxSequenceNumber, 0 /* column_family_id */);
    memtable_->Ref();
  }
  ~MemTableConstructor() override { delete memtable_->Unref(); }
  Status FinishImpl(const Options&, const ImmutableOptions& ioptions,
                    const MutableCFOptions& /*moptions*/,
                    const BlockBasedTableOptions& /*table_options*/,
                    const InternalKeyComparator& /*internal_comparator*/,
                    const stl_wrappers::KVMap& kv_map) override {
    delete memtable_->Unref();
    ImmutableOptions mem_ioptions(ioptions);
    memtable_ = new MemTable(internal_comparator_, mem_ioptions,
                             MutableCFOptions(options_), write_buffer_manager_,
                             kMaxSequenceNumber, 0 /* column_family_id */);
    memtable_->Ref();
    int seq = 1;
    for (const auto& kv : kv_map) {
      Status s = memtable_->Add(seq, kTypeValue, kv.first, kv.second,
                                nullptr /* kv_prot_info */);
      if (!s.ok()) {
        return s;
      }
      seq++;
    }
    return Status::OK();
  }
  InternalIterator* NewIterator(
      const SliceTransform* /*prefix_extractor*/) const override {
    return new KeyConvertingIterator(
        memtable_->NewIterator(ReadOptions(), &arena_), true);
  }

  bool AnywayDeleteIterator() const override { return true; }

  bool IsArenaMode() const override { return true; }

 private:
  mutable Arena arena_;
  InternalKeyComparator internal_comparator_;
  Options options_;
  WriteBufferManager* write_buffer_manager_;
  MemTable* memtable_;
  std::shared_ptr<SkipListFactory> table_factory_;
};

class InternalIteratorFromIterator : public InternalIterator {
 public:
  explicit InternalIteratorFromIterator(Iterator* it) : it_(it) {}
  bool Valid() const override { return it_->Valid(); }
  void Seek(const Slice& target) override { it_->Seek(target); }
  void SeekForPrev(const Slice& target) override { it_->SeekForPrev(target); }
  void SeekToFirst() override { it_->SeekToFirst(); }
  void SeekToLast() override { it_->SeekToLast(); }
  void Next() override { it_->Next(); }
  void Prev() override { it_->Prev(); }
  Slice key() const override { return it_->key(); }
  Slice value() const override { return it_->value(); }
  Status status() const override { return it_->status(); }

 private:
  std::unique_ptr<Iterator> it_;
};

class DBConstructor : public Constructor {
 public:
  explicit DBConstructor(const Comparator* cmp)
      : Constructor(cmp), comparator_(cmp) {
    db_ = nullptr;
    NewDB();
  }
  ~DBConstructor() override { delete db_; }
  Status FinishImpl(const Options& /*options*/,
                    const ImmutableOptions& /*ioptions*/,
                    const MutableCFOptions& /*moptions*/,
                    const BlockBasedTableOptions& /*table_options*/,
                    const InternalKeyComparator& /*internal_comparator*/,
                    const stl_wrappers::KVMap& kv_map) override {
    delete db_;
    db_ = nullptr;
    NewDB();
    for (const auto& kv : kv_map) {
      WriteBatch batch;
      EXPECT_OK(batch.Put(kv.first, kv.second));
      EXPECT_TRUE(db_->Write(WriteOptions(), &batch).ok());
    }
    return Status::OK();
  }

  InternalIterator* NewIterator(
      const SliceTransform* /*prefix_extractor*/) const override {
    return new InternalIteratorFromIterator(db_->NewIterator(ReadOptions()));
  }

  DB* db() const override { return db_; }

 private:
  void NewDB() {
    std::string name = test::PerThreadDBPath("table_testdb");

    Options options;
    options.comparator = comparator_;
    Status status = DestroyDB(name, options);
    ASSERT_TRUE(status.ok()) << status.ToString();

    options.create_if_missing = true;
    options.error_if_exists = true;
    options.write_buffer_size = 10000;  // Something small to force merging
    status = DB::Open(options, name, &db_);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  const Comparator* comparator_;
  DB* db_;
};

enum TestType {
  BLOCK_BASED_TABLE_TEST,
#ifndef ROCKSDB_LITE
  PLAIN_TABLE_SEMI_FIXED_PREFIX,
  PLAIN_TABLE_FULL_STR_PREFIX,
  PLAIN_TABLE_TOTAL_ORDER,
#endif  // !ROCKSDB_LITE
  BLOCK_TEST,
  MEMTABLE_TEST,
  DB_TEST
};

struct TestArgs {
  TestType type;
  bool reverse_compare;
  int restart_interval;
  CompressionType compression;
  uint32_t compression_parallel_threads;
  uint32_t format_version;
  bool use_mmap;
};

std::ostream& operator<<(std::ostream& os, const TestArgs& args) {
  os << "type: " << args.type << " reverse_compare: " << args.reverse_compare
     << " restart_interval: " << args.restart_interval
     << " compression: " << args.compression
     << " compression_parallel_threads: " << args.compression_parallel_threads
     << " format_version: " << args.format_version
     << " use_mmap: " << args.use_mmap;

  return os;
}

static std::vector<TestArgs> GenerateArgList() {
  std::vector<TestArgs> test_args;
  std::vector<TestType> test_types = {BLOCK_BASED_TABLE_TEST,
#ifndef ROCKSDB_LITE
                                      PLAIN_TABLE_SEMI_FIXED_PREFIX,
                                      PLAIN_TABLE_FULL_STR_PREFIX,
                                      PLAIN_TABLE_TOTAL_ORDER,
#endif  // !ROCKSDB_LITE
                                      BLOCK_TEST,
                                      MEMTABLE_TEST,
                                      DB_TEST};
  std::vector<bool> reverse_compare_types = {false, true};
  std::vector<int> restart_intervals = {16, 1, 1024};
  std::vector<uint32_t> compression_parallel_threads = {1, 4};

  // Only add compression if it is supported
  std::vector<std::pair<CompressionType, bool>> compression_types;
  compression_types.emplace_back(kNoCompression, false);
  if (Snappy_Supported()) {
    compression_types.emplace_back(kSnappyCompression, false);
  }
  if (Zlib_Supported()) {
    compression_types.emplace_back(kZlibCompression, false);
    compression_types.emplace_back(kZlibCompression, true);
  }
  if (BZip2_Supported()) {
    compression_types.emplace_back(kBZip2Compression, false);
    compression_types.emplace_back(kBZip2Compression, true);
  }
  if (LZ4_Supported()) {
    compression_types.emplace_back(kLZ4Compression, false);
    compression_types.emplace_back(kLZ4Compression, true);
    compression_types.emplace_back(kLZ4HCCompression, false);
    compression_types.emplace_back(kLZ4HCCompression, true);
  }
  if (XPRESS_Supported()) {
    compression_types.emplace_back(kXpressCompression, false);
    compression_types.emplace_back(kXpressCompression, true);
  }
  if (ZSTD_Supported()) {
    compression_types.emplace_back(kZSTD, false);
    compression_types.emplace_back(kZSTD, true);
  }

  for (auto test_type : test_types) {
    for (auto reverse_compare : reverse_compare_types) {
#ifndef ROCKSDB_LITE
      if (test_type == PLAIN_TABLE_SEMI_FIXED_PREFIX ||
          test_type == PLAIN_TABLE_FULL_STR_PREFIX ||
          test_type == PLAIN_TABLE_TOTAL_ORDER) {
        // Plain table doesn't use restart index or compression.
        TestArgs one_arg;
        one_arg.type = test_type;
        one_arg.reverse_compare = reverse_compare;
        one_arg.restart_interval = restart_intervals[0];
        one_arg.compression = compression_types[0].first;
        one_arg.compression_parallel_threads = 1;
        one_arg.format_version = 0;
        one_arg.use_mmap = true;
        test_args.push_back(one_arg);
        one_arg.use_mmap = false;
        test_args.push_back(one_arg);
        continue;
      }
#endif  // !ROCKSDB_LITE

      for (auto restart_interval : restart_intervals) {
        for (auto compression_type : compression_types) {
          for (auto num_threads : compression_parallel_threads) {
            TestArgs one_arg;
            one_arg.type = test_type;
            one_arg.reverse_compare = reverse_compare;
            one_arg.restart_interval = restart_interval;
            one_arg.compression = compression_type.first;
            one_arg.compression_parallel_threads = num_threads;
            one_arg.format_version = compression_type.second ? 2 : 1;
            one_arg.use_mmap = false;
            test_args.push_back(one_arg);
          }
        }
      }
    }
  }
  return test_args;
}

// In order to make all tests run for plain table format, including
// those operating on empty keys, create a new prefix transformer which
// return fixed prefix if the slice is not shorter than the prefix length,
// and the full slice if it is shorter.
class FixedOrLessPrefixTransform : public SliceTransform {
 private:
  const size_t prefix_len_;

 public:
  explicit FixedOrLessPrefixTransform(size_t prefix_len)
      : prefix_len_(prefix_len) {}

  const char* Name() const override { return "rocksdb.FixedPrefix"; }

  Slice Transform(const Slice& src) const override {
    assert(InDomain(src));
    if (src.size() < prefix_len_) {
      return src;
    }
    return Slice(src.data(), prefix_len_);
  }

  bool InDomain(const Slice& /*src*/) const override { return true; }

  bool InRange(const Slice& dst) const override {
    return (dst.size() <= prefix_len_);
  }
  bool FullLengthEnabled(size_t* /*len*/) const override { return false; }
};

class HarnessTest : public testing::Test {
 public:
  explicit HarnessTest(const TestArgs& args)
      : args_(args),
        ioptions_(options_),
        moptions_(options_),
        write_buffer_(options_.db_write_buffer_size),
        support_prev_(true),
        only_support_prefix_seek_(false) {
    options_.compression = args_.compression;
    options_.compression_opts.parallel_threads =
        args_.compression_parallel_threads;
    // Use shorter block size for tests to exercise block boundary
    // conditions more.
    if (args_.reverse_compare) {
      options_.comparator = &reverse_key_comparator;
    }

    internal_comparator_.reset(
        new test::PlainInternalKeyComparator(options_.comparator));

    options_.allow_mmap_reads = args_.use_mmap;
    switch (args_.type) {
      case BLOCK_BASED_TABLE_TEST:
        table_options_.flush_block_policy_factory.reset(
            new FlushBlockBySizePolicyFactory());
        table_options_.block_size = 256;
        table_options_.block_restart_interval = args_.restart_interval;
        table_options_.index_block_restart_interval = args_.restart_interval;
        table_options_.format_version = args_.format_version;
        options_.table_factory.reset(
            new BlockBasedTableFactory(table_options_));
        constructor_.reset(new TableConstructor(
            options_.comparator, true /* convert_to_internal_key_ */));
        internal_comparator_.reset(
            new InternalKeyComparator(options_.comparator));
        break;
// Plain table is not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
      case PLAIN_TABLE_SEMI_FIXED_PREFIX:
        support_prev_ = false;
        only_support_prefix_seek_ = true;
        options_.prefix_extractor.reset(new FixedOrLessPrefixTransform(2));
        options_.table_factory.reset(NewPlainTableFactory());
        constructor_.reset(new TableConstructor(
            options_.comparator, true /* convert_to_internal_key_ */));
        internal_comparator_.reset(
            new InternalKeyComparator(options_.comparator));
        break;
      case PLAIN_TABLE_FULL_STR_PREFIX:
        support_prev_ = false;
        only_support_prefix_seek_ = true;
        options_.prefix_extractor.reset(NewNoopTransform());
        options_.table_factory.reset(NewPlainTableFactory());
        constructor_.reset(new TableConstructor(
            options_.comparator, true /* convert_to_internal_key_ */));
        internal_comparator_.reset(
            new InternalKeyComparator(options_.comparator));
        break;
      case PLAIN_TABLE_TOTAL_ORDER:
        support_prev_ = false;
        only_support_prefix_seek_ = false;
        options_.prefix_extractor = nullptr;

        {
          PlainTableOptions plain_table_options;
          plain_table_options.user_key_len = kPlainTableVariableLength;
          plain_table_options.bloom_bits_per_key = 0;
          plain_table_options.hash_table_ratio = 0;

          options_.table_factory.reset(
              NewPlainTableFactory(plain_table_options));
        }
        constructor_.reset(new TableConstructor(
            options_.comparator, true /* convert_to_internal_key_ */));
        internal_comparator_.reset(
            new InternalKeyComparator(options_.comparator));
        break;
#endif  // !ROCKSDB_LITE
      case BLOCK_TEST:
        table_options_.block_size = 256;
        options_.table_factory.reset(
            new BlockBasedTableFactory(table_options_));
        constructor_.reset(new BlockConstructor(options_.comparator));
        break;
      case MEMTABLE_TEST:
        table_options_.block_size = 256;
        options_.table_factory.reset(
            new BlockBasedTableFactory(table_options_));
        constructor_.reset(
            new MemTableConstructor(options_.comparator, &write_buffer_));
        break;
      case DB_TEST:
        table_options_.block_size = 256;
        options_.table_factory.reset(
            new BlockBasedTableFactory(table_options_));
        constructor_.reset(new DBConstructor(options_.comparator));
        break;
    }
    ioptions_ = ImmutableOptions(options_);
    moptions_ = MutableCFOptions(options_);
  }

  void Add(const std::string& key, const std::string& value) {
    constructor_->Add(key, value);
  }

  void Test(Random* rnd) {
    std::vector<std::string> keys;
    stl_wrappers::KVMap data;
    constructor_->Finish(options_, ioptions_, moptions_, table_options_,
                         *internal_comparator_, &keys, &data);

    TestForwardScan(keys, data);
    if (support_prev_) {
      TestBackwardScan(keys, data);
    }
    TestRandomAccess(rnd, keys, data);
  }

  void TestForwardScan(const std::vector<std::string>& /*keys*/,
                       const stl_wrappers::KVMap& data) {
    InternalIterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    for (stl_wrappers::KVMap::const_iterator model_iter = data.begin();
         model_iter != data.end(); ++model_iter) {
      ASSERT_EQ(ToString(data, model_iter), ToString(iter));
      iter->Next();
      ASSERT_OK(iter->status());
    }
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    if (constructor_->IsArenaMode() && !constructor_->AnywayDeleteIterator()) {
      iter->~InternalIterator();
    } else {
      delete iter;
    }
  }

  void TestBackwardScan(const std::vector<std::string>& /*keys*/,
                        const stl_wrappers::KVMap& data) {
    InternalIterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    iter->SeekToLast();
    ASSERT_OK(iter->status());
    for (stl_wrappers::KVMap::const_reverse_iterator model_iter = data.rbegin();
         model_iter != data.rend(); ++model_iter) {
      ASSERT_EQ(ToString(data, model_iter), ToString(iter));
      iter->Prev();
      ASSERT_OK(iter->status());
    }
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    if (constructor_->IsArenaMode() && !constructor_->AnywayDeleteIterator()) {
      iter->~InternalIterator();
    } else {
      delete iter;
    }
  }

  void TestRandomAccess(Random* rnd, const std::vector<std::string>& keys,
                        const stl_wrappers::KVMap& data) {
    static const bool kVerbose = false;
    InternalIterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    stl_wrappers::KVMap::const_iterator model_iter = data.begin();
    if (kVerbose) fprintf(stderr, "---\n");
    for (int i = 0; i < 200; i++) {
      const int toss = rnd->Uniform(support_prev_ ? 5 : 3);
      switch (toss) {
        case 0: {
          if (iter->Valid()) {
            if (kVerbose) fprintf(stderr, "Next\n");
            iter->Next();
            ASSERT_OK(iter->status());
            ++model_iter;
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          }
          break;
        }

        case 1: {
          if (kVerbose) fprintf(stderr, "SeekToFirst\n");
          iter->SeekToFirst();
          ASSERT_OK(iter->status());
          model_iter = data.begin();
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }

        case 2: {
          std::string key = PickRandomKey(rnd, keys);
          model_iter = data.lower_bound(key);
          if (kVerbose)
            fprintf(stderr, "Seek '%s'\n", EscapeString(key).c_str());
          iter->Seek(Slice(key));
          ASSERT_OK(iter->status());
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }

        case 3: {
          if (iter->Valid()) {
            if (kVerbose) fprintf(stderr, "Prev\n");
            iter->Prev();
            ASSERT_OK(iter->status());
            if (model_iter == data.begin()) {
              model_iter = data.end();  // Wrap around to invalid value
            } else {
              --model_iter;
            }
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          }
          break;
        }

        case 4: {
          if (kVerbose) fprintf(stderr, "SeekToLast\n");
          iter->SeekToLast();
          ASSERT_OK(iter->status());
          if (keys.empty()) {
            model_iter = data.end();
          } else {
            std::string last = data.rbegin()->first;
            model_iter = data.lower_bound(last);
          }
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }
      }
    }
    if (constructor_->IsArenaMode() && !constructor_->AnywayDeleteIterator()) {
      iter->~InternalIterator();
    } else {
      delete iter;
    }
  }

  std::string ToString(const stl_wrappers::KVMap& data,
                       const stl_wrappers::KVMap::const_iterator& it) {
    if (it == data.end()) {
      return "END";
    } else {
      return "'" + it->first + "->" + it->second + "'";
    }
  }

  std::string ToString(const stl_wrappers::KVMap& data,
                       const stl_wrappers::KVMap::const_reverse_iterator& it) {
    if (it == data.rend()) {
      return "END";
    } else {
      return "'" + it->first + "->" + it->second + "'";
    }
  }

  std::string ToString(const InternalIterator* it) {
    if (!it->Valid()) {
      return "END";
    } else {
      return "'" + it->key().ToString() + "->" + it->value().ToString() + "'";
    }
  }

  std::string PickRandomKey(Random* rnd, const std::vector<std::string>& keys) {
    if (keys.empty()) {
      return "foo";
    } else {
      const int index = rnd->Uniform(static_cast<int>(keys.size()));
      std::string result = keys[index];
      switch (rnd->Uniform(support_prev_ ? 3 : 1)) {
        case 0:
          // Return an existing key
          break;
        case 1: {
          // Attempt to return something smaller than an existing key
          if (result.size() > 0 && result[result.size() - 1] > '\0' &&
              (!only_support_prefix_seek_ ||
               options_.prefix_extractor->Transform(result).size() <
                   result.size())) {
            result[result.size() - 1]--;
          }
          break;
        }
        case 2: {
          // Return something larger than an existing key
          Increment(options_.comparator, &result);
          break;
        }
      }
      return result;
    }
  }

  // Returns nullptr if not running against a DB
  DB* db() const { return constructor_->db(); }

 private:
  TestArgs args_;
  Options options_;
  ImmutableOptions ioptions_;
  MutableCFOptions moptions_;
  BlockBasedTableOptions table_options_;
  std::unique_ptr<Constructor> constructor_;
  WriteBufferManager write_buffer_;
  bool support_prev_;
  bool only_support_prefix_seek_;
  std::shared_ptr<InternalKeyComparator> internal_comparator_;
};

class ParameterizedHarnessTest : public HarnessTest,
                                 public testing::WithParamInterface<TestArgs> {
 public:
  ParameterizedHarnessTest() : HarnessTest(GetParam()) {}
};

INSTANTIATE_TEST_CASE_P(TableTest, ParameterizedHarnessTest,
                        ::testing::ValuesIn(GenerateArgList()));

class DBHarnessTest : public HarnessTest {
 public:
  DBHarnessTest()
      : HarnessTest(TestArgs{DB_TEST, /* reverse_compare */ false,
                             /* restart_interval */ 16, kNoCompression,
                             /* compression_parallel_threads */ 1,
                             /* format_version */ 0, /* use_mmap */ false}) {}
};

static bool Between(uint64_t val, uint64_t low, uint64_t high) {
  bool result = (val >= low) && (val <= high);
  if (!result) {
    fprintf(stderr, "Value %llu is not in range [%llu, %llu]\n",
            (unsigned long long)(val), (unsigned long long)(low),
            (unsigned long long)(high));
  }
  return result;
}

// Tests against all kinds of tables
class TableTest : public testing::Test {
 public:
  const InternalKeyComparator& GetPlainInternalComparator(
      const Comparator* comp) {
    if (!plain_internal_comparator) {
      plain_internal_comparator.reset(
          new test::PlainInternalKeyComparator(comp));
    }
    return *plain_internal_comparator;
  }
  void IndexTest(BlockBasedTableOptions table_options);

 private:
  std::unique_ptr<InternalKeyComparator> plain_internal_comparator;
};

class GeneralTableTest : public TableTest {};
class BlockBasedTableTestBase : public TableTest {};
class BlockBasedTableTest
    : public BlockBasedTableTestBase,
      virtual public ::testing::WithParamInterface<uint32_t> {
 public:
  BlockBasedTableTest() : format_(GetParam()) {
    env_ = ROCKSDB_NAMESPACE::Env::Default();
  }

  BlockBasedTableOptions GetBlockBasedTableOptions() {
    BlockBasedTableOptions options;
    options.format_version = format_;
    return options;
  }

  void SetupTracingTest(TableConstructor* c) {
    test_path_ = test::PerThreadDBPath("block_based_table_tracing_test");
    EXPECT_OK(env_->CreateDir(test_path_));
    trace_file_path_ = test_path_ + "/block_cache_trace_file";

    BlockCacheTraceWriterOptions trace_writer_opt;
    BlockCacheTraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    EXPECT_OK(NewFileTraceWriter(env_, EnvOptions(), trace_file_path_,
                                 &trace_writer));
    std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
        NewBlockCacheTraceWriter(env_->GetSystemClock().get(), trace_writer_opt,
                                 std::move(trace_writer));
    ASSERT_NE(block_cache_trace_writer, nullptr);
    // Always return Status::OK().
    assert(c->block_cache_tracer_
               .StartTrace(trace_opt, std::move(block_cache_trace_writer))
               .ok());

    {
      std::string user_key = "k01";
      InternalKey internal_key(user_key, 0, kTypeValue);
      std::string encoded_key = internal_key.Encode().ToString();
      c->Add(encoded_key, kDummyValue);
    }
    {
      std::string user_key = "k02";
      InternalKey internal_key(user_key, 0, kTypeValue);
      std::string encoded_key = internal_key.Encode().ToString();
      c->Add(encoded_key, kDummyValue);
    }
  }

  void VerifyBlockAccessTrace(
      TableConstructor* c,
      const std::vector<BlockCacheTraceRecord>& expected_records) {
    c->block_cache_tracer_.EndTrace();

    {
      std::unique_ptr<TraceReader> trace_reader;
      Status s = NewFileTraceReader(env_, EnvOptions(), trace_file_path_,
                                    &trace_reader);
      EXPECT_OK(s);
      BlockCacheTraceReader reader(std::move(trace_reader));
      BlockCacheTraceHeader header;
      EXPECT_OK(reader.ReadHeader(&header));
      uint32_t index = 0;
      while (s.ok()) {
        BlockCacheTraceRecord access;
        s = reader.ReadAccess(&access);
        if (!s.ok()) {
          break;
        }
        ASSERT_LT(index, expected_records.size());
        EXPECT_NE("", access.block_key);
        EXPECT_EQ(access.block_type, expected_records[index].block_type);
        EXPECT_GT(access.block_size, 0);
        EXPECT_EQ(access.caller, expected_records[index].caller);
        EXPECT_EQ(access.no_insert, expected_records[index].no_insert);
        EXPECT_EQ(access.is_cache_hit, expected_records[index].is_cache_hit);
        // Get
        if (access.caller == TableReaderCaller::kUserGet) {
          EXPECT_EQ(access.referenced_key,
                    expected_records[index].referenced_key);
          EXPECT_EQ(access.get_id, expected_records[index].get_id);
          EXPECT_EQ(access.get_from_user_specified_snapshot,
                    expected_records[index].get_from_user_specified_snapshot);
          if (access.block_type == TraceType::kBlockTraceDataBlock) {
            EXPECT_GT(access.referenced_data_size, 0);
            EXPECT_GT(access.num_keys_in_block, 0);
            EXPECT_EQ(access.referenced_key_exist_in_block,
                      expected_records[index].referenced_key_exist_in_block);
          }
        } else {
          EXPECT_EQ(access.referenced_key, "");
          EXPECT_EQ(access.get_id, 0);
          EXPECT_FALSE(access.get_from_user_specified_snapshot);
          EXPECT_EQ(access.referenced_data_size, 0);
          EXPECT_EQ(access.num_keys_in_block, 0);
          EXPECT_FALSE(access.referenced_key_exist_in_block);
        }
        index++;
      }
      EXPECT_EQ(index, expected_records.size());
    }
    EXPECT_OK(env_->DeleteFile(trace_file_path_));
    EXPECT_OK(env_->DeleteDir(test_path_));
  }

 protected:
  uint64_t IndexUncompressedHelper(bool indexCompress);

 private:
  uint32_t format_;
  Env* env_;
  std::string trace_file_path_;
  std::string test_path_;
};
class PlainTableTest : public TableTest {};
class TablePropertyTest : public testing::Test {};
class BBTTailPrefetchTest : public TableTest {};

// The helper class to test the file checksum
class FileChecksumTestHelper {
 public:
  FileChecksumTestHelper(bool convert_to_internal_key = false)
      : convert_to_internal_key_(convert_to_internal_key) {}
  ~FileChecksumTestHelper() {}

  void CreateWritableFile() {
    sink_ = new test::StringSink();
    std::unique_ptr<FSWritableFile> holder(sink_);
    file_writer_.reset(new WritableFileWriter(
        std::move(holder), "" /* don't care */, FileOptions()));
  }

  void SetFileChecksumGenerator(FileChecksumGenerator* checksum_generator) {
    if (file_writer_ != nullptr) {
      file_writer_->TEST_SetFileChecksumGenerator(checksum_generator);
    } else {
      delete checksum_generator;
    }
  }

  WritableFileWriter* GetFileWriter() { return file_writer_.get(); }

  Status ResetTableBuilder(std::unique_ptr<TableBuilder>&& builder) {
    assert(builder != nullptr);
    table_builder_ = std::move(builder);
    return Status::OK();
  }

  void AddKVtoKVMap(int num_entries) {
    Random rnd(test::RandomSeed());
    for (int i = 0; i < num_entries; i++) {
      std::string v = rnd.RandomString(100);
      kv_map_[test::RandomKey(&rnd, 20)] = v;
    }
  }

  Status WriteKVAndFlushTable() {
    for (const auto& kv : kv_map_) {
      if (convert_to_internal_key_) {
        ParsedInternalKey ikey(kv.first, kMaxSequenceNumber, kTypeValue);
        std::string encoded;
        AppendInternalKey(&encoded, ikey);
        table_builder_->Add(encoded, kv.second);
      } else {
        table_builder_->Add(kv.first, kv.second);
      }
      EXPECT_TRUE(table_builder_->status().ok());
    }
    Status s = table_builder_->Finish();
    EXPECT_OK(file_writer_->Flush());
    EXPECT_OK(s);

    EXPECT_EQ(sink_->contents().size(), table_builder_->FileSize());
    return s;
  }

  std::string GetFileChecksum() {
    EXPECT_OK(file_writer_->Close());
    return table_builder_->GetFileChecksum();
  }

  const char* GetFileChecksumFuncName() {
    return table_builder_->GetFileChecksumFuncName();
  }

  Status CalculateFileChecksum(FileChecksumGenerator* file_checksum_generator,
                               std::string* checksum) {
    assert(file_checksum_generator != nullptr);
    cur_file_num_ = checksum_file_num_++;
    test::StringSink* ss_rw =
        static_cast<test::StringSink*>(file_writer_->writable_file());
    std::unique_ptr<FSRandomAccessFile> source(
        new test::StringSource(ss_rw->contents()));
    file_reader_.reset(new RandomAccessFileReader(std::move(source), "test"));

    std::unique_ptr<char[]> scratch(new char[2048]);
    Slice result;
    uint64_t offset = 0;
    Status s;
    s = file_reader_->Read(IOOptions(), offset, 2048, &result, scratch.get(),
                           nullptr, Env::IO_TOTAL /* rate_limiter_priority */);
    if (!s.ok()) {
      return s;
    }
    while (result.size() != 0) {
      file_checksum_generator->Update(scratch.get(), result.size());
      offset += static_cast<uint64_t>(result.size());
      s = file_reader_->Read(IOOptions(), offset, 2048, &result, scratch.get(),
                             nullptr,
                             Env::IO_TOTAL /* rate_limiter_priority */);
      if (!s.ok()) {
        return s;
      }
    }
    EXPECT_EQ(offset, static_cast<uint64_t>(table_builder_->FileSize()));
    file_checksum_generator->Finalize();
    *checksum = file_checksum_generator->GetChecksum();
    return Status::OK();
  }

 private:
  bool convert_to_internal_key_;
  uint64_t cur_file_num_;
  std::unique_ptr<WritableFileWriter> file_writer_;
  std::unique_ptr<RandomAccessFileReader> file_reader_;
  std::unique_ptr<TableBuilder> table_builder_;
  stl_wrappers::KVMap kv_map_;
  test::StringSink* sink_ = nullptr;

  static uint64_t checksum_file_num_;
};

uint64_t FileChecksumTestHelper::checksum_file_num_ = 1;

INSTANTIATE_TEST_CASE_P(FormatVersions, BlockBasedTableTest,
                        testing::ValuesIn(test::kFooterFormatVersionsToTest));

// This test serves as the living tutorial for the prefix scan of user collected
// properties.
TEST_F(TablePropertyTest, PrefixScanTest) {
  UserCollectedProperties props{
      {"num.111.1", "1"}, {"num.111.2", "2"}, {"num.111.3", "3"},
      {"num.333.1", "1"}, {"num.333.2", "2"}, {"num.333.3", "3"},
      {"num.555.1", "1"}, {"num.555.2", "2"}, {"num.555.3", "3"},
  };

  // prefixes that exist
  for (const std::string prefix : {"num.111", "num.333", "num.555"}) {
    int num = 0;
    for (auto pos = props.lower_bound(prefix);
         pos != props.end() &&
         pos->first.compare(0, prefix.size(), prefix) == 0;
         ++pos) {
      ++num;
      auto key = prefix + "." + std::to_string(num);
      ASSERT_EQ(key, pos->first);
      ASSERT_EQ(std::to_string(num), pos->second);
    }
    ASSERT_EQ(3, num);
  }

  // prefixes that don't exist
  for (const std::string prefix :
       {"num.000", "num.222", "num.444", "num.666"}) {
    auto pos = props.lower_bound(prefix);
    ASSERT_TRUE(pos == props.end() ||
                pos->first.compare(0, prefix.size(), prefix) != 0);
  }
}

namespace {
struct TestIds {
  UniqueId64x3 internal_id;
  UniqueId64x3 external_id;
};

inline bool operator==(const TestIds& lhs, const TestIds& rhs) {
  return lhs.internal_id == rhs.internal_id &&
         lhs.external_id == rhs.external_id;
}

std::ostream& operator<<(std::ostream& os, const TestIds& ids) {
  return os << std::hex << "{{{ 0x" << ids.internal_id[0] << "U, 0x"
            << ids.internal_id[1] << "U, 0x" << ids.internal_id[2]
            << "U }}, {{ 0x" << ids.external_id[0] << "U, 0x"
            << ids.external_id[1] << "U, 0x" << ids.external_id[2] << "U }}}";
}

TestIds GetUniqueId(TableProperties* tp, std::unordered_set<uint64_t>* seen,
                    const std::string& db_id, const std::string& db_session_id,
                    uint64_t file_number) {
  // First test session id logic
  if (db_session_id.size() == 20) {
    uint64_t upper;
    uint64_t lower;
    EXPECT_OK(DecodeSessionId(db_session_id, &upper, &lower));
    EXPECT_EQ(EncodeSessionId(upper, lower), db_session_id);
  }

  // Get external using public API
  tp->db_id = db_id;
  tp->db_session_id = db_session_id;
  tp->orig_file_number = file_number;
  TestIds t;
  {
    std::string euid;
    EXPECT_OK(GetExtendedUniqueIdFromTableProperties(*tp, &euid));
    EXPECT_EQ(euid.size(), 24U);
    t.external_id[0] = DecodeFixed64(&euid[0]);
    t.external_id[1] = DecodeFixed64(&euid[8]);
    t.external_id[2] = DecodeFixed64(&euid[16]);

    std::string uid;
    EXPECT_OK(GetUniqueIdFromTableProperties(*tp, &uid));
    EXPECT_EQ(uid.size(), 16U);
    EXPECT_EQ(uid, euid.substr(0, 16));
    EXPECT_EQ(t.external_id[0], DecodeFixed64(&uid[0]));
    EXPECT_EQ(t.external_id[1], DecodeFixed64(&uid[8]));
  }
  // All these should be effectively random
  EXPECT_TRUE(seen->insert(t.external_id[0]).second);
  EXPECT_TRUE(seen->insert(t.external_id[1]).second);
  EXPECT_TRUE(seen->insert(t.external_id[2]).second);

  // Get internal with internal API
  EXPECT_OK(GetSstInternalUniqueId(db_id, db_session_id, file_number,
                                   &t.internal_id));
  EXPECT_NE(t.internal_id, kNullUniqueId64x3);

  // Verify relationship
  UniqueId64x3 tmp = t.internal_id;
  InternalUniqueIdToExternal(&tmp);
  EXPECT_EQ(tmp, t.external_id);
  ExternalUniqueIdToInternal(&tmp);
  EXPECT_EQ(tmp, t.internal_id);

  // And 128-bit internal version
  UniqueId64x2 tmp2{};
  EXPECT_OK(GetSstInternalUniqueId(db_id, db_session_id, file_number, &tmp2));
  EXPECT_NE(tmp2, kNullUniqueId64x2);

  EXPECT_EQ(tmp2[0], t.internal_id[0]);
  EXPECT_EQ(tmp2[1], t.internal_id[1]);
  InternalUniqueIdToExternal(&tmp2);
  EXPECT_EQ(tmp2[0], t.external_id[0]);
  EXPECT_EQ(tmp2[1], t.external_id[1]);
  ExternalUniqueIdToInternal(&tmp2);
  EXPECT_EQ(tmp2[0], t.internal_id[0]);
  EXPECT_EQ(tmp2[1], t.internal_id[1]);

  return t;
}
}  // namespace

TEST_F(TablePropertyTest, UniqueIdsSchemaAndQuality) {
  // To ensure the computation only depends on the expected entries, we set
  // the rest randomly
  TableProperties tp;
  TEST_SetRandomTableProperties(&tp);

  // DB id is normally RFC-4122
  const std::string db_id1 = "7265b6eb-4e42-4aec-86a4-0dc5e73a228d";
  // Allow other forms of DB id
  const std::string db_id2 = "1728000184588763620";
  const std::string db_id3 = "x";

  // DB session id is normally 20 chars in base-36, but 13 to 24 chars
  // is ok, roughly 64 to 128 bits.
  const std::string ses_id1 = "ABCDEFGHIJ0123456789";
  // Same trailing 13 digits
  const std::string ses_id2 = "HIJ0123456789";
  const std::string ses_id3 = "0123ABCDEFGHIJ0123456789";
  // Different trailing 12 digits
  const std::string ses_id4 = "ABCDEFGH888888888888";
  // And change length
  const std::string ses_id5 = "ABCDEFGHIJ012";
  const std::string ses_id6 = "ABCDEFGHIJ0123456789ABCD";

  using T = TestIds;
  std::unordered_set<uint64_t> seen;
  // Establish a stable schema for the unique IDs. These values must not
  // change for existing table files.
  // (Note: parens needed for macro parsing, extra braces needed for some
  // compilers.)
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id1, 1),
      T({{{0x61d7dcf415d9cf19U, 0x160d77aae90757fdU, 0x907f41dfd90724ffU}},
         {{0xf0bd230365df7464U, 0xca089303f3648eb4U, 0x4b44f7e7324b2817U}}}));
  // Only change internal_id[1] with file number
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id1, 2),
      T({{{0x61d7dcf415d9cf19U, 0x160d77aae90757feU, 0x907f41dfd90724ffU}},
         {{0xf13fdf7adcfebb6dU, 0x97cd2226cc033ea2U, 0x198c438182091f0eU}}}));
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id1, 123456789),
      T({{{0x61d7dcf415d9cf19U, 0x160d77aaee5c9ae9U, 0x907f41dfd90724ffU}},
         {{0x81fbcebe1ac6c4f0U, 0x6b14a64cfdc0f1c4U, 0x7d8fb6eaf18edbb3U}}}));
  // Change internal_id[1] and internal_id[2] with db_id
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id2, ses_id1, 1),
      T({{{0x61d7dcf415d9cf19U, 0xf89c471f572f0d25U, 0x1f0f2a5eb0e6257eU}},
         {{0x7f1d01d453616991U, 0x32ddf2afec804ab2U, 0xd10a1ee2f0c7d9c1U}}}));
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id3, ses_id1, 1),
      T({{{0x61d7dcf415d9cf19U, 0xfed297a8154a57d0U, 0x8b931b9cdebd9e8U}},
         {{0x62b2f43183f6894bU, 0x897ff2b460eefad1U, 0xf4ec189fb2d15e04U}}}));
  // Keeping same last 13 digits of ses_id keeps same internal_id[0]
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id2, 1),
      T({{{0x61d7dcf415d9cf19U, 0x5f6cc4fa2d528c8U, 0x7b70845d5bfb5446U}},
         {{0x96d1c83ffcc94266U, 0x82663eac0ec6e14aU, 0x94a88b49678b77f6U}}}));
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id3, 1),
      T({{{0x61d7dcf415d9cf19U, 0xfc7232879db37ea2U, 0xc0378d74ea4c89cdU}},
         {{0xdf2ef57e98776905U, 0xda5b31c987da833bU, 0x79c1b4bd0a9e760dU}}}));
  // Changing last 12 digits of ses_id only changes internal_id[0]
  // (vs. db_id1, ses_id1, 1)
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id4, 1),
      T({{{0x4f07cc0d003a83a8U, 0x160d77aae90757fdU, 0x907f41dfd90724ffU}},
         {{0xbcf85336a9f71f04U, 0x4f2949e2f3adb60dU, 0x9ca0def976abfa10U}}}));
  // ses_id can change everything.
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id5, 1),
      T({{{0x94b8768e43f87ce6U, 0xc2559653ac4e7c93U, 0xde6dff6bbb1223U}},
         {{0x5a9537af681817fbU, 0x1afcd1fecaead5eaU, 0x767077ad9ebe0008U}}}));
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id6, 1),
      T({{{0x43cfb0ffa3b710edU, 0x263c580426406a1bU, 0xfacc91379a80d29dU}},
         {{0xfa90547d84cb1cdbU, 0x2afe99c641992d4aU, 0x205b7f7b60e51cc2U}}}));

  // Now verify more thoroughly that any small change in inputs completely
  // changes external unique id.
  // (Relying on 'seen' checks etc. in GetUniqueId)
  std::string db_id = "00000000-0000-0000-0000-000000000000";
  std::string ses_id = "000000000000000000000000";
  uint64_t file_num = 1;
  // change db_id
  for (size_t i = 0; i < db_id.size(); ++i) {
    if (db_id[i] == '-') {
      continue;
    }
    for (char alt : std::string("123456789abcdef")) {
      db_id[i] = alt;
      GetUniqueId(&tp, &seen, db_id, ses_id, file_num);
    }
    db_id[i] = '0';
  }
  // change ses_id
  for (size_t i = 0; i < ses_id.size(); ++i) {
    for (char alt : std::string("123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")) {
      ses_id[i] = alt;
      GetUniqueId(&tp, &seen, db_id, ses_id, file_num);
    }
    ses_id[i] = '0';
  }
  // change file_num
  for (int i = 1; i < 64; ++i) {
    GetUniqueId(&tp, &seen, db_id, ses_id, file_num << i);
  }

  // Verify that "all zeros" in first 128 bits is equivalent for internal and
  // external IDs. This way, as long as we avoid "all zeros" in internal IDs,
  // we avoid it in external IDs.
  {
    UniqueId64x3 id1{{0, 0, Random::GetTLSInstance()->Next64()}};
    UniqueId64x3 id2 = id1;
    InternalUniqueIdToExternal(&id1);
    EXPECT_EQ(id1, id2);
    ExternalUniqueIdToInternal(&id2);
    EXPECT_EQ(id1, id2);
  }
}

namespace {
void SetGoodTableProperties(TableProperties* tp) {
  // To ensure the computation only depends on the expected entries, we set
  // the rest randomly
  TEST_SetRandomTableProperties(tp);
  tp->db_id = "7265b6eb-4e42-4aec-86a4-0dc5e73a228d";
  tp->db_session_id = "ABCDEFGHIJ0123456789";
  tp->orig_file_number = 1;
}
}  // namespace

TEST_F(TablePropertyTest, UniqueIdHumanStrings) {
  TableProperties tp;
  SetGoodTableProperties(&tp);

  std::string tmp;
  EXPECT_OK(GetExtendedUniqueIdFromTableProperties(tp, &tmp));
  EXPECT_EQ(tmp,
            (std::string{{'\x64', '\x74', '\xdf', '\x65', '\x03', '\x23',
                          '\xbd', '\xf0', '\xb4', '\x8e', '\x64', '\xf3',
                          '\x03', '\x93', '\x08', '\xca', '\x17', '\x28',
                          '\x4b', '\x32', '\xe7', '\xf7', '\x44', '\x4b'}}));
  EXPECT_EQ(UniqueIdToHumanString(tmp),
            "6474DF650323BDF0-B48E64F3039308CA-17284B32E7F7444B");

  EXPECT_OK(GetUniqueIdFromTableProperties(tp, &tmp));
  EXPECT_EQ(UniqueIdToHumanString(tmp), "6474DF650323BDF0-B48E64F3039308CA");

  // including zero padding
  tmp = std::string(24U, '\0');
  tmp[15] = '\x12';
  tmp[23] = '\xAB';
  EXPECT_EQ(UniqueIdToHumanString(tmp),
            "0000000000000000-0000000000000012-00000000000000AB");

  // And shortened
  tmp = std::string(20U, '\0');
  tmp[5] = '\x12';
  tmp[10] = '\xAB';
  tmp[17] = '\xEF';
  EXPECT_EQ(UniqueIdToHumanString(tmp),
            "0000000000120000-0000AB0000000000-00EF0000");

  tmp.resize(16);
  EXPECT_EQ(UniqueIdToHumanString(tmp), "0000000000120000-0000AB0000000000");

  tmp.resize(11);
  EXPECT_EQ(UniqueIdToHumanString(tmp), "0000000000120000-0000AB");

  tmp.resize(6);
  EXPECT_EQ(UniqueIdToHumanString(tmp), "000000000012");

  // Also internal IDs to human string
  UniqueId64x3 euid = {12345, 678, 9};
  EXPECT_EQ(InternalUniqueIdToHumanString(&euid), "{12345,678,9}");

  UniqueId64x2 uid = {1234, 567890};
  EXPECT_EQ(InternalUniqueIdToHumanString(&uid), "{1234,567890}");
}

TEST_F(TablePropertyTest, UniqueIdsFailure) {
  TableProperties tp;
  std::string tmp;

  // Missing DB id
  SetGoodTableProperties(&tp);
  tp.db_id = "";
  EXPECT_TRUE(GetUniqueIdFromTableProperties(tp, &tmp).IsNotSupported());
  EXPECT_TRUE(
      GetExtendedUniqueIdFromTableProperties(tp, &tmp).IsNotSupported());

  // Missing session id
  SetGoodTableProperties(&tp);
  tp.db_session_id = "";
  EXPECT_TRUE(GetUniqueIdFromTableProperties(tp, &tmp).IsNotSupported());
  EXPECT_TRUE(
      GetExtendedUniqueIdFromTableProperties(tp, &tmp).IsNotSupported());

  // Missing file number
  SetGoodTableProperties(&tp);
  tp.orig_file_number = 0;
  EXPECT_TRUE(GetUniqueIdFromTableProperties(tp, &tmp).IsNotSupported());
  EXPECT_TRUE(
      GetExtendedUniqueIdFromTableProperties(tp, &tmp).IsNotSupported());
}

// This test include all the basic checks except those for index size and block
// size, which will be conducted in separated unit tests.
TEST_P(BlockBasedTableTest, BasicBlockBasedTableProperties) {
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);

  c.Add("a1", "val1");
  c.Add("b2", "val2");
  c.Add("c3", "val3");
  c.Add("d4", "val4");
  c.Add("e5", "val5");
  c.Add("f6", "val6");
  c.Add("g7", "val7");
  c.Add("h8", "val8");
  c.Add("j9", "val9");
  uint64_t diff_internal_user_bytes = 9 * 8;  // 8 is seq size, 9 k-v totally

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.compression = kNoCompression;
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_restart_interval = 1;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  ASSERT_EQ(options.statistics->getTickerCount(NUMBER_BLOCK_NOT_COMPRESSED), 0);

  auto& props = *c.GetTableReader()->GetTableProperties();
  ASSERT_EQ(kvmap.size(), props.num_entries);

  auto raw_key_size = kvmap.size() * 2ul;
  auto raw_value_size = kvmap.size() * 4ul;

  ASSERT_EQ(raw_key_size + diff_internal_user_bytes, props.raw_key_size);
  ASSERT_EQ(raw_value_size, props.raw_value_size);
  ASSERT_EQ(1ul, props.num_data_blocks);
  ASSERT_EQ("", props.filter_policy_name);  // no filter policy is used

  // Verify data size.
  BlockBuilder block_builder(1);
  for (const auto& item : kvmap) {
    block_builder.Add(item.first, item.second);
  }
  Slice content = block_builder.Finish();
  ASSERT_EQ(content.size() + BlockBasedTable::kBlockTrailerSize +
                diff_internal_user_bytes,
            props.data_size);
  c.ResetTableReader();
}

#ifdef SNAPPY
uint64_t BlockBasedTableTest::IndexUncompressedHelper(bool compressed) {
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  constexpr size_t kNumKeys = 10000;

  for (size_t k = 0; k < kNumKeys; ++k) {
    c.Add("key" + std::to_string(k), "val" + std::to_string(k));
  }

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.compression = kSnappyCompression;
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_restart_interval = 1;
  table_options.enable_index_compression = compressed;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  c.ResetTableReader();
  return options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED);
}
TEST_P(BlockBasedTableTest, IndexUncompressed) {
  uint64_t tbl1_compressed_cnt = IndexUncompressedHelper(true);
  uint64_t tbl2_compressed_cnt = IndexUncompressedHelper(false);
  // tbl1_compressed_cnt should include 1 index block
  EXPECT_EQ(tbl2_compressed_cnt + 1, tbl1_compressed_cnt);
}
#endif  // SNAPPY

TEST_P(BlockBasedTableTest, BlockBasedTableProperties2) {
  TableConstructor c(&reverse_key_comparator);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;

  {
    Options options;
    options.compression = CompressionType::kNoCompression;
    BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    const ImmutableOptions ioptions(options);
    const MutableCFOptions moptions(options);
    c.Finish(options, ioptions, moptions, table_options,
             GetPlainInternalComparator(options.comparator), &keys, &kvmap);

    auto& props = *c.GetTableReader()->GetTableProperties();

    // Default comparator
    ASSERT_EQ("leveldb.BytewiseComparator", props.comparator_name);
    // No merge operator
    ASSERT_EQ("nullptr", props.merge_operator_name);
    // No prefix extractor
    ASSERT_EQ("nullptr", props.prefix_extractor_name);
    // No property collectors
    ASSERT_EQ("[]", props.property_collectors_names);
    // No filter policy is used
    ASSERT_EQ("", props.filter_policy_name);
    // Compression type == that set:
    ASSERT_EQ("NoCompression", props.compression_name);
    c.ResetTableReader();
  }

  {
    Options options;
    BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.comparator = &reverse_key_comparator;
    options.merge_operator = MergeOperators::CreateUInt64AddOperator();
    options.prefix_extractor.reset(NewNoopTransform());
    options.table_properties_collector_factories.emplace_back(
        new DummyPropertiesCollectorFactory1());
    options.table_properties_collector_factories.emplace_back(
        new DummyPropertiesCollectorFactory2());

    const ImmutableOptions ioptions(options);
    const MutableCFOptions moptions(options);
    c.Finish(options, ioptions, moptions, table_options,
             GetPlainInternalComparator(options.comparator), &keys, &kvmap);

    auto& props = *c.GetTableReader()->GetTableProperties();

    ASSERT_EQ("rocksdb.ReverseBytewiseComparator", props.comparator_name);
    ASSERT_EQ("UInt64AddOperator", props.merge_operator_name);
    ASSERT_EQ("rocksdb.Noop", props.prefix_extractor_name);
    ASSERT_EQ(
        "[DummyPropertiesCollectorFactory1,DummyPropertiesCollectorFactory2]",
        props.property_collectors_names);
    ASSERT_EQ("", props.filter_policy_name);  // no filter policy is used
    c.ResetTableReader();
  }
}

TEST_P(BlockBasedTableTest, RangeDelBlock) {
  TableConstructor c(BytewiseComparator());
  std::vector<std::string> keys = {"1pika", "2chu"};
  std::vector<std::string> vals = {"p", "c"};

  std::vector<RangeTombstone> expected_tombstones = {
      {"1pika", "2chu", 0},
      {"2chu", "c", 1},
      {"2chu", "c", 0},
      {"c", "p", 0},
  };

  for (int i = 0; i < 2; i++) {
    RangeTombstone t(keys[i], vals[i], i);
    std::pair<InternalKey, Slice> p = t.Serialize();
    c.Add(p.first.Encode().ToString(), p.second);
  }

  std::vector<std::string> sorted_keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_restart_interval = 1;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  std::unique_ptr<InternalKeyComparator> internal_cmp(
      new InternalKeyComparator(options.comparator));
  c.Finish(options, ioptions, moptions, table_options, *internal_cmp,
           &sorted_keys, &kvmap);

  for (int j = 0; j < 2; ++j) {
    std::unique_ptr<InternalIterator> iter(
        c.GetTableReader()->NewRangeTombstoneIterator(ReadOptions()));
    if (j > 0) {
      // For second iteration, delete the table reader object and verify the
      // iterator can still access its metablock's range tombstones.
      c.ResetTableReader();
    }
    ASSERT_FALSE(iter->Valid());
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    for (size_t i = 0; i < expected_tombstones.size(); i++) {
      ASSERT_TRUE(iter->Valid());
      ParsedInternalKey parsed_key;
      ASSERT_OK(
          ParseInternalKey(iter->key(), &parsed_key, true /* log_err_key */));
      RangeTombstone t(parsed_key, iter->value());
      const auto& expected_t = expected_tombstones[i];
      ASSERT_EQ(t.start_key_, expected_t.start_key_);
      ASSERT_EQ(t.end_key_, expected_t.end_key_);
      ASSERT_EQ(t.seq_, expected_t.seq_);
      iter->Next();
    }
    ASSERT_TRUE(!iter->Valid());
  }
}

TEST_P(BlockBasedTableTest, FilterPolicyNameProperties) {
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("a1", "val1");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  Options options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  auto& props = *c.GetTableReader()->GetTableProperties();
  ASSERT_EQ(table_options.filter_policy->Name(), props.filter_policy_name);
  c.ResetTableReader();
}

//
// BlockBasedTableTest::PrefetchTest
//
void AssertKeysInCache(BlockBasedTable* table_reader,
                       const std::vector<std::string>& keys_in_cache,
                       const std::vector<std::string>& keys_not_in_cache,
                       bool convert = false) {
  if (convert) {
    for (auto key : keys_in_cache) {
      InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
      ASSERT_TRUE(table_reader->TEST_KeyInCache(ReadOptions(), ikey.Encode()));
    }
    for (auto key : keys_not_in_cache) {
      InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
      ASSERT_TRUE(!table_reader->TEST_KeyInCache(ReadOptions(), ikey.Encode()));
    }
  } else {
    for (auto key : keys_in_cache) {
      ASSERT_TRUE(table_reader->TEST_KeyInCache(ReadOptions(), key));
    }
    for (auto key : keys_not_in_cache) {
      ASSERT_TRUE(!table_reader->TEST_KeyInCache(ReadOptions(), key));
    }
  }
}

void PrefetchRange(TableConstructor* c, Options* opt,
                   BlockBasedTableOptions* table_options, const char* key_begin,
                   const char* key_end,
                   const std::vector<std::string>& keys_in_cache,
                   const std::vector<std::string>& keys_not_in_cache,
                   const Status expected_status = Status::OK()) {
  // reset the cache and reopen the table
  table_options->block_cache = NewLRUCache(16 * 1024 * 1024, 4);
  opt->table_factory.reset(NewBlockBasedTableFactory(*table_options));
  const ImmutableOptions ioptions2(*opt);
  const MutableCFOptions moptions(*opt);
  ASSERT_OK(c->Reopen(ioptions2, moptions));

  // prefetch
  auto* table_reader = dynamic_cast<BlockBasedTable*>(c->GetTableReader());
  Status s;
  std::unique_ptr<Slice> begin, end;
  std::unique_ptr<InternalKey> i_begin, i_end;
  if (key_begin != nullptr) {
    if (c->ConvertToInternalKey()) {
      i_begin.reset(new InternalKey(key_begin, kMaxSequenceNumber, kTypeValue));
      begin.reset(new Slice(i_begin->Encode()));
    } else {
      begin.reset(new Slice(key_begin));
    }
  }
  if (key_end != nullptr) {
    if (c->ConvertToInternalKey()) {
      i_end.reset(new InternalKey(key_end, kMaxSequenceNumber, kTypeValue));
      end.reset(new Slice(i_end->Encode()));
    } else {
      end.reset(new Slice(key_end));
    }
  }
  s = table_reader->Prefetch(begin.get(), end.get());

  ASSERT_TRUE(s.code() == expected_status.code());

  // assert our expectation in cache warmup
  AssertKeysInCache(table_reader, keys_in_cache, keys_not_in_cache,
                    c->ConvertToInternalKey());
  c->ResetTableReader();
}

TEST_P(BlockBasedTableTest, PrefetchTest) {
  // The purpose of this test is to test the prefetching operation built into
  // BlockBasedTable.
  Options opt;
  std::unique_ptr<InternalKeyComparator> ikc;
  ikc.reset(new test::PlainInternalKeyComparator(opt.comparator));
  opt.compression = kNoCompression;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_size = 1024;
  // big enough so we don't ever lose cached values.
  table_options.block_cache = NewLRUCache(16 * 1024 * 1024, 4);
  opt.table_factory.reset(NewBlockBasedTableFactory(table_options));

  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("k01", "hello");
  c.Add("k02", "hello2");
  c.Add("k03", std::string(10000, 'x'));
  c.Add("k04", std::string(200000, 'x'));
  c.Add("k05", std::string(300000, 'x'));
  c.Add("k06", "hello3");
  c.Add("k07", std::string(100000, 'x'));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(opt);
  const MutableCFOptions moptions(opt);
  c.Finish(opt, ioptions, moptions, table_options, *ikc, &keys, &kvmap);
  c.ResetTableReader();

  // We get the following data spread :
  //
  // Data block         Index
  // ========================
  // [ k01 k02 k03 ]    k03
  // [ k04         ]    k04
  // [ k05         ]    k05
  // [ k06 k07     ]    k07

  // Simple
  PrefetchRange(&c, &opt, &table_options,
                /*key_range=*/"k01", "k05",
                /*keys_in_cache=*/{"k01", "k02", "k03", "k04", "k05"},
                /*keys_not_in_cache=*/{"k06", "k07"});
  PrefetchRange(&c, &opt, &table_options, "k01", "k01", {"k01", "k02", "k03"},
                {"k04", "k05", "k06", "k07"});
  // odd
  PrefetchRange(&c, &opt, &table_options, "a", "z",
                {"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, {});
  PrefetchRange(&c, &opt, &table_options, "k00", "k00", {"k01", "k02", "k03"},
                {"k04", "k05", "k06", "k07"});
  // Edge cases
  PrefetchRange(&c, &opt, &table_options, "k00", "k06",
                {"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, {});
  PrefetchRange(&c, &opt, &table_options, "k00", "zzz",
                {"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, {});
  // null keys
  PrefetchRange(&c, &opt, &table_options, nullptr, nullptr,
                {"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, {});
  PrefetchRange(&c, &opt, &table_options, "k04", nullptr,
                {"k04", "k05", "k06", "k07"}, {"k01", "k02", "k03"});
  PrefetchRange(&c, &opt, &table_options, nullptr, "k05",
                {"k01", "k02", "k03", "k04", "k05"}, {"k06", "k07"});
  // invalid
  PrefetchRange(&c, &opt, &table_options, "k06", "k00", {}, {},
                Status::InvalidArgument(Slice("k06 "), Slice("k07")));
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, TotalOrderSeekOnHashIndex) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  for (int i = 0; i <= 4; ++i) {
    Options options;
    // Make each key/value an individual block
    table_options.block_size = 64;
    switch (i) {
      case 0:
        // Binary search index
        table_options.index_type = BlockBasedTableOptions::kBinarySearch;
        options.table_factory.reset(new BlockBasedTableFactory(table_options));
        break;
      case 1:
        // Hash search index
        table_options.index_type = BlockBasedTableOptions::kHashSearch;
        options.table_factory.reset(new BlockBasedTableFactory(table_options));
        options.prefix_extractor.reset(NewFixedPrefixTransform(4));
        break;
      case 2:
        // Hash search index with filter policy
        table_options.index_type = BlockBasedTableOptions::kHashSearch;
        table_options.filter_policy.reset(NewBloomFilterPolicy(10));
        options.table_factory.reset(new BlockBasedTableFactory(table_options));
        options.prefix_extractor.reset(NewFixedPrefixTransform(4));
        break;
      case 3:
        // Two-level index
        table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
        options.table_factory.reset(new BlockBasedTableFactory(table_options));
        break;
      case 4:
        // Binary search with first key
        table_options.index_type =
            BlockBasedTableOptions::kBinarySearchWithFirstKey;
        options.table_factory.reset(new BlockBasedTableFactory(table_options));
        break;
    }

    TableConstructor c(BytewiseComparator(),
                       true /* convert_to_internal_key_ */);
    c.Add("aaaa1", std::string('a', 56));
    c.Add("bbaa1", std::string('a', 56));
    c.Add("cccc1", std::string('a', 56));
    c.Add("bbbb1", std::string('a', 56));
    c.Add("baaa1", std::string('a', 56));
    c.Add("abbb1", std::string('a', 56));
    c.Add("cccc2", std::string('a', 56));
    std::vector<std::string> keys;
    stl_wrappers::KVMap kvmap;
    const ImmutableOptions ioptions(options);
    const MutableCFOptions moptions(options);
    c.Finish(options, ioptions, moptions, table_options,
             GetPlainInternalComparator(options.comparator), &keys, &kvmap);
    auto props = c.GetTableReader()->GetTableProperties();
    ASSERT_EQ(7u, props->num_data_blocks);
    auto* reader = c.GetTableReader();
    ReadOptions ro;
    ro.total_order_seek = true;
    std::unique_ptr<InternalIterator> iter(reader->NewIterator(
        ro, moptions.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));

    iter->Seek(InternalKey("b", 0, kTypeValue).Encode());
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("baaa1", ExtractUserKey(iter->key()).ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bbaa1", ExtractUserKey(iter->key()).ToString());

    iter->Seek(InternalKey("bb", 0, kTypeValue).Encode());
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bbaa1", ExtractUserKey(iter->key()).ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bbbb1", ExtractUserKey(iter->key()).ToString());

    iter->Seek(InternalKey("bbb", 0, kTypeValue).Encode());
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bbbb1", ExtractUserKey(iter->key()).ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("cccc1", ExtractUserKey(iter->key()).ToString());
  }
}

TEST_P(BlockBasedTableTest, NoopTransformSeek) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));

  Options options;
  options.comparator = BytewiseComparator();
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewNoopTransform());

  TableConstructor c(options.comparator);
  // To tickle the PrefixMayMatch bug it is important that the
  // user-key is a single byte so that the index key exactly matches
  // the user-key.
  InternalKey key("a", 1, kTypeValue);
  c.Add(key.Encode().ToString(), "b");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);
  c.Finish(options, ioptions, moptions, table_options, internal_comparator,
           &keys, &kvmap);

  auto* reader = c.GetTableReader();
  for (int i = 0; i < 2; ++i) {
    ReadOptions ro;
    ro.total_order_seek = (i == 0);
    std::unique_ptr<InternalIterator> iter(reader->NewIterator(
        ro, moptions.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));

    iter->Seek(key.Encode());
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a", ExtractUserKey(iter->key()).ToString());
  }
}

TEST_P(BlockBasedTableTest, SkipPrefixBloomFilter) {
  // if DB is opened with a prefix extractor of a different name,
  // prefix bloom is skipped when read the file
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.filter_policy.reset(NewBloomFilterPolicy(2));
  table_options.whole_key_filtering = false;

  Options options;
  options.comparator = BytewiseComparator();
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));

  TableConstructor c(options.comparator);
  InternalKey key("abcdefghijk", 1, kTypeValue);
  c.Add(key.Encode().ToString(), "test");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);
  c.Finish(options, ioptions, moptions, table_options, internal_comparator,
           &keys, &kvmap);
  // TODO(Zhongyi): update test to use MutableCFOptions
  options.prefix_extractor.reset(NewFixedPrefixTransform(9));
  const ImmutableOptions new_ioptions(options);
  const MutableCFOptions new_moptions(options);
  ASSERT_OK(c.Reopen(new_ioptions, new_moptions));
  auto reader = c.GetTableReader();
  ReadOptions read_options;
  std::unique_ptr<InternalIterator> db_iter(reader->NewIterator(
      read_options, new_moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  // Test point lookup
  // only one kv
  for (auto& kv : kvmap) {
    db_iter->Seek(kv.first);
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_OK(db_iter->status());
    ASSERT_EQ(db_iter->key(), kv.first);
    ASSERT_EQ(db_iter->value(), kv.second);
  }
}

TEST_P(BlockBasedTableTest, BadChecksumType) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();

  Options options;
  options.comparator = BytewiseComparator();
  options.table_factory.reset(new BlockBasedTableFactory(table_options));

  TableConstructor c(options.comparator);
  InternalKey key("abc", 1, kTypeValue);
  c.Add(key.Encode().ToString(), "test");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);
  c.Finish(options, ioptions, moptions, table_options, internal_comparator,
           &keys, &kvmap);

  // Corrupt checksum type (123 is invalid)
  auto& sink = *c.TEST_GetSink();
  size_t len = sink.contents_.size();
  ASSERT_EQ(sink.contents_[len - Footer::kNewVersionsEncodedLength],
            table_options.checksum);
  sink.contents_[len - Footer::kNewVersionsEncodedLength] = char{123};

  // (Re-)Open table file with bad checksum type
  const ImmutableOptions new_ioptions(options);
  const MutableCFOptions new_moptions(options);
  Status s = c.Reopen(new_ioptions, new_moptions);
  ASSERT_NOK(s);
  // "test" is file name
  ASSERT_EQ(s.ToString(),
            "Corruption: Corrupt or unsupported checksum type: 123 in test");
}

namespace {
std::string ChecksumAsString(const std::string& data,
                             ChecksumType checksum_type) {
  uint32_t v = ComputeBuiltinChecksum(checksum_type, data.data(), data.size());

  // Verify consistency with other function
  if (data.size() >= 1) {
    EXPECT_EQ(v, ComputeBuiltinChecksumWithLastByte(
                     checksum_type, data.data(), data.size() - 1, data.back()));
  }
  // Little endian as in file
  std::array<char, 4> raw_bytes;
  EncodeFixed32(raw_bytes.data(), v);
  return Slice(raw_bytes.data(), raw_bytes.size()).ToString(/*hex*/ true);
}

std::string ChecksumAsString(std::string* data, char new_last_byte,
                             ChecksumType checksum_type) {
  data->back() = new_last_byte;
  return ChecksumAsString(*data, checksum_type);
}
}  // namespace

// Make sure that checksum values don't change in later versions, even if
// consistent within current version.
TEST_P(BlockBasedTableTest, ChecksumSchemas) {
  std::string b0 = "x";
  std::string b1 = "This is a short block!x";
  std::string b2;
  for (int i = 0; i < 100; ++i) {
    b2.append("This is a long block!");
  }
  b2.append("x");
  // Trailing 'x' will be replaced by compression type

  std::string empty;

  char ct1 = kNoCompression;
  char ct2 = kSnappyCompression;
  char ct3 = kZSTD;

  // Note: first byte of trailer is compression type, last 4 are checksum

  for (ChecksumType t : GetSupportedChecksums()) {
    switch (t) {
      case kNoChecksum:
        EXPECT_EQ(ChecksumAsString(empty, t), "00000000");
        EXPECT_EQ(ChecksumAsString(&b0, ct1, t), "00000000");
        EXPECT_EQ(ChecksumAsString(&b0, ct2, t), "00000000");
        EXPECT_EQ(ChecksumAsString(&b0, ct3, t), "00000000");
        EXPECT_EQ(ChecksumAsString(&b1, ct1, t), "00000000");
        EXPECT_EQ(ChecksumAsString(&b1, ct2, t), "00000000");
        EXPECT_EQ(ChecksumAsString(&b1, ct3, t), "00000000");
        EXPECT_EQ(ChecksumAsString(&b2, ct1, t), "00000000");
        EXPECT_EQ(ChecksumAsString(&b2, ct2, t), "00000000");
        EXPECT_EQ(ChecksumAsString(&b2, ct3, t), "00000000");
        break;
      case kCRC32c:
        EXPECT_EQ(ChecksumAsString(empty, t), "D8EA82A2");
        EXPECT_EQ(ChecksumAsString(&b0, ct1, t), "D28F2549");
        EXPECT_EQ(ChecksumAsString(&b0, ct2, t), "052B2843");
        EXPECT_EQ(ChecksumAsString(&b0, ct3, t), "46F8F711");
        EXPECT_EQ(ChecksumAsString(&b1, ct1, t), "583F0355");
        EXPECT_EQ(ChecksumAsString(&b1, ct2, t), "2F9B0A57");
        EXPECT_EQ(ChecksumAsString(&b1, ct3, t), "ECE7DA1D");
        EXPECT_EQ(ChecksumAsString(&b2, ct1, t), "943EF0AB");
        EXPECT_EQ(ChecksumAsString(&b2, ct2, t), "43A2EDB1");
        EXPECT_EQ(ChecksumAsString(&b2, ct3, t), "00E53D63");
        break;
      case kxxHash:
        EXPECT_EQ(ChecksumAsString(empty, t), "055DCC02");
        EXPECT_EQ(ChecksumAsString(&b0, ct1, t), "3EB065CF");
        EXPECT_EQ(ChecksumAsString(&b0, ct2, t), "31F79238");
        EXPECT_EQ(ChecksumAsString(&b0, ct3, t), "320D2E00");
        EXPECT_EQ(ChecksumAsString(&b1, ct1, t), "4A2E5FB0");
        EXPECT_EQ(ChecksumAsString(&b1, ct2, t), "0BD9F652");
        EXPECT_EQ(ChecksumAsString(&b1, ct3, t), "B4107E50");
        EXPECT_EQ(ChecksumAsString(&b2, ct1, t), "20F4D4BA");
        EXPECT_EQ(ChecksumAsString(&b2, ct2, t), "8F1A1F99");
        EXPECT_EQ(ChecksumAsString(&b2, ct3, t), "A191A338");
        break;
      case kxxHash64:
        EXPECT_EQ(ChecksumAsString(empty, t), "99E9D851");
        EXPECT_EQ(ChecksumAsString(&b0, ct1, t), "682705DB");
        EXPECT_EQ(ChecksumAsString(&b0, ct2, t), "30E7211B");
        EXPECT_EQ(ChecksumAsString(&b0, ct3, t), "B7BB58E8");
        EXPECT_EQ(ChecksumAsString(&b1, ct1, t), "B74655EF");
        EXPECT_EQ(ChecksumAsString(&b1, ct2, t), "B6C8BBBE");
        EXPECT_EQ(ChecksumAsString(&b1, ct3, t), "AED9E3B4");
        EXPECT_EQ(ChecksumAsString(&b2, ct1, t), "0D4999FE");
        EXPECT_EQ(ChecksumAsString(&b2, ct2, t), "F5932423");
        EXPECT_EQ(ChecksumAsString(&b2, ct3, t), "6B31BAB1");
        break;
      case kXXH3:
        EXPECT_EQ(ChecksumAsString(empty, t), "00000000");
        EXPECT_EQ(ChecksumAsString(&b0, ct1, t), "C294D338");
        EXPECT_EQ(ChecksumAsString(&b0, ct2, t), "1B174353");
        EXPECT_EQ(ChecksumAsString(&b0, ct3, t), "2D0E20C8");
        EXPECT_EQ(ChecksumAsString(&b1, ct1, t), "B37FB5E6");
        EXPECT_EQ(ChecksumAsString(&b1, ct2, t), "6AFC258D");
        EXPECT_EQ(ChecksumAsString(&b1, ct3, t), "5CE54616");
        EXPECT_EQ(ChecksumAsString(&b2, ct1, t), "FA2D482E");
        EXPECT_EQ(ChecksumAsString(&b2, ct2, t), "23AED845");
        EXPECT_EQ(ChecksumAsString(&b2, ct3, t), "15B7BBDE");
        break;
      default:
        // Force this test to be updated on new ChecksumTypes
        assert(false);
        break;
    }
  }
}

void AddInternalKey(TableConstructor* c, const std::string& prefix,
                    std::string value = "v", int /*suffix_len*/ = 800) {
  static Random rnd(1023);
  InternalKey k(prefix + rnd.RandomString(800), 0, kTypeValue);
  c->Add(k.Encode().ToString(), value);
}

void TableTest::IndexTest(BlockBasedTableOptions table_options) {
  TableConstructor c(BytewiseComparator());

  // keys with prefix length 3, make sure the key/value is big enough to fill
  // one block
  AddInternalKey(&c, "0015");
  AddInternalKey(&c, "0035");

  AddInternalKey(&c, "0054");
  AddInternalKey(&c, "0055");

  AddInternalKey(&c, "0056");
  AddInternalKey(&c, "0057");

  AddInternalKey(&c, "0058");
  AddInternalKey(&c, "0075");

  AddInternalKey(&c, "0076");
  AddInternalKey(&c, "0095");

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  table_options.block_size = 1700;
  table_options.block_cache = NewLRUCache(1024, 4);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options, *comparator, &keys,
           &kvmap);
  auto reader = c.GetTableReader();

  auto props = reader->GetTableProperties();
  ASSERT_EQ(5u, props->num_data_blocks);

  // TODO(Zhongyi): update test to use MutableCFOptions
  ReadOptions read_options;
  std::unique_ptr<InternalIterator> index_iter(reader->NewIterator(
      read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  // -- Find keys do not exist, but have common prefix.
  std::vector<std::string> prefixes = {"001", "003", "005", "007", "009"};
  std::vector<std::string> lower_bound = {
      keys[0], keys[1], keys[2], keys[7], keys[9],
  };

  // find the lower bound of the prefix
  for (size_t i = 0; i < prefixes.size(); ++i) {
    index_iter->Seek(InternalKey(prefixes[i], 0, kTypeValue).Encode());
    ASSERT_OK(index_iter->status());
    ASSERT_TRUE(index_iter->Valid());

    // seek the first element in the block
    ASSERT_EQ(lower_bound[i], index_iter->key().ToString());
    ASSERT_EQ("v", index_iter->value().ToString());
  }

  // find the upper bound of prefixes
  std::vector<std::string> upper_bound = {
      keys[1],
      keys[2],
      keys[7],
      keys[9],
  };

  // find existing keys
  for (const auto& item : kvmap) {
    auto ukey = ExtractUserKey(item.first).ToString();
    index_iter->Seek(ukey);

    // ASSERT_OK(regular_iter->status());
    ASSERT_OK(index_iter->status());

    // ASSERT_TRUE(regular_iter->Valid());
    ASSERT_TRUE(index_iter->Valid());

    ASSERT_EQ(item.first, index_iter->key().ToString());
    ASSERT_EQ(item.second, index_iter->value().ToString());
  }

  for (size_t i = 0; i < prefixes.size(); ++i) {
    // the key is greater than any existing keys.
    auto key = prefixes[i] + "9";
    index_iter->Seek(InternalKey(key, 0, kTypeValue).Encode());

    ASSERT_TRUE(index_iter->status().ok() || index_iter->status().IsNotFound());
    ASSERT_TRUE(!index_iter->status().IsNotFound() || !index_iter->Valid());
    if (i == prefixes.size() - 1) {
      // last key
      ASSERT_TRUE(!index_iter->Valid());
    } else {
      ASSERT_TRUE(index_iter->Valid());
      // seek the first element in the block
      ASSERT_EQ(upper_bound[i], index_iter->key().ToString());
      ASSERT_EQ("v", index_iter->value().ToString());
    }
  }

  // find keys with prefix that don't match any of the existing prefixes.
  std::vector<std::string> non_exist_prefixes = {"002", "004", "006", "008"};
  for (const auto& prefix : non_exist_prefixes) {
    index_iter->Seek(InternalKey(prefix, 0, kTypeValue).Encode());
    // regular_iter->Seek(prefix);

    ASSERT_OK(index_iter->status());
    // Seek to non-existing prefixes should yield either invalid, or a
    // key with prefix greater than the target.
    if (index_iter->Valid()) {
      Slice ukey = ExtractUserKey(index_iter->key());
      Slice ukey_prefix = options.prefix_extractor->Transform(ukey);
      ASSERT_TRUE(BytewiseComparator()->Compare(prefix, ukey_prefix) < 0);
    }
  }
  for (const auto& prefix : non_exist_prefixes) {
    index_iter->SeekForPrev(InternalKey(prefix, 0, kTypeValue).Encode());
    // regular_iter->Seek(prefix);

    ASSERT_OK(index_iter->status());
    // Seek to non-existing prefixes should yield either invalid, or a
    // key with prefix greater than the target.
    if (index_iter->Valid()) {
      Slice ukey = ExtractUserKey(index_iter->key());
      Slice ukey_prefix = options.prefix_extractor->Transform(ukey);
      ASSERT_TRUE(BytewiseComparator()->Compare(prefix, ukey_prefix) > 0);
    }
  }

  {
    // Test reseek case. It should impact partitioned index more.
    ReadOptions ro;
    ro.total_order_seek = true;
    std::unique_ptr<InternalIterator> index_iter2(reader->NewIterator(
        ro, moptions.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));

    // Things to cover in partitioned index:
    // 1. Both of Seek() and SeekToLast() has optimization to prevent
    //    rereek leaf index block if it remains to the same one, and
    //    they reuse the same variable.
    // 2. When Next() or Prev() is called, the block moves, so the
    //    optimization should kick in only with the current one.
    index_iter2->Seek(InternalKey("0055", 0, kTypeValue).Encode());
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0055", index_iter2->key().ToString().substr(0, 4));

    index_iter2->SeekToLast();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));

    index_iter2->Seek(InternalKey("0055", 0, kTypeValue).Encode());
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0055", index_iter2->key().ToString().substr(0, 4));

    index_iter2->SeekToLast();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));
    index_iter2->Prev();
    ASSERT_TRUE(index_iter2->Valid());
    index_iter2->Prev();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0075", index_iter2->key().ToString().substr(0, 4));

    index_iter2->Seek(InternalKey("0095", 0, kTypeValue).Encode());
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));
    index_iter2->Prev();
    ASSERT_TRUE(index_iter2->Valid());
    index_iter2->Prev();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0075", index_iter2->key().ToString().substr(0, 4));

    index_iter2->SeekToLast();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));

    index_iter2->Seek(InternalKey("0095", 0, kTypeValue).Encode());
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));

    index_iter2->Prev();
    ASSERT_TRUE(index_iter2->Valid());
    index_iter2->Prev();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0075", index_iter2->key().ToString().substr(0, 4));

    index_iter2->Seek(InternalKey("0075", 0, kTypeValue).Encode());
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0075", index_iter2->key().ToString().substr(0, 4));

    index_iter2->Next();
    ASSERT_TRUE(index_iter2->Valid());
    index_iter2->Next();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));

    index_iter2->SeekToLast();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));
  }

  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, BinaryIndexTest) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.index_type = BlockBasedTableOptions::kBinarySearch;
  IndexTest(table_options);
}

TEST_P(BlockBasedTableTest, HashIndexTest) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  IndexTest(table_options);
}

TEST_P(BlockBasedTableTest, PartitionIndexTest) {
  const int max_index_keys = 5;
  const int est_max_index_key_value_size = 32;
  const int est_max_index_size = max_index_keys * est_max_index_key_value_size;
  for (int i = 1; i <= est_max_index_size + 1; i++) {
    BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
    table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
    table_options.metadata_block_size = i;
    IndexTest(table_options);
  }
}

TEST_P(BlockBasedTableTest, IndexSeekOptimizationIncomplete) {
  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  Options options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);

  TableConstructor c(BytewiseComparator());
  AddInternalKey(&c, "pika");

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  c.Finish(options, ioptions, moptions, table_options, *comparator, &keys,
           &kvmap);
  ASSERT_EQ(1, keys.size());

  auto reader = c.GetTableReader();
  ReadOptions ropt;
  ropt.read_tier = ReadTier::kBlockCacheTier;
  std::unique_ptr<InternalIterator> iter(reader->NewIterator(
      ropt, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  auto ikey = [](Slice user_key) {
    return InternalKey(user_key, 0, kTypeValue).Encode().ToString();
  };

  iter->Seek(ikey("pika"));
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsIncomplete());

  // This used to crash at some point.
  iter->Seek(ikey("pika"));
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsIncomplete());
}

TEST_P(BlockBasedTableTest, BinaryIndexWithFirstKey1) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.index_type = BlockBasedTableOptions::kBinarySearchWithFirstKey;
  IndexTest(table_options);
}

class CustomFlushBlockPolicy : public FlushBlockPolicyFactory,
                               public FlushBlockPolicy {
 public:
  explicit CustomFlushBlockPolicy(std::vector<int> keys_per_block)
      : keys_per_block_(keys_per_block) {}

  const char* Name() const override { return "CustomFlushBlockPolicy"; }

  FlushBlockPolicy* NewFlushBlockPolicy(const BlockBasedTableOptions&,
                                        const BlockBuilder&) const override {
    return new CustomFlushBlockPolicy(keys_per_block_);
  }

  bool Update(const Slice&, const Slice&) override {
    if (keys_in_current_block_ >= keys_per_block_.at(current_block_idx_)) {
      ++current_block_idx_;
      keys_in_current_block_ = 1;
      return true;
    }

    ++keys_in_current_block_;
    return false;
  }

  std::vector<int> keys_per_block_;

  int current_block_idx_ = 0;
  int keys_in_current_block_ = 0;
};

TEST_P(BlockBasedTableTest, BinaryIndexWithFirstKey2) {
  for (int use_first_key = 0; use_first_key < 2; ++use_first_key) {
    SCOPED_TRACE("use_first_key = " + std::to_string(use_first_key));
    BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
    table_options.index_type =
        use_first_key ? BlockBasedTableOptions::kBinarySearchWithFirstKey
                      : BlockBasedTableOptions::kBinarySearch;
    table_options.block_cache = NewLRUCache(10000);  // fits all blocks
    table_options.index_shortening =
        BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
    table_options.flush_block_policy_factory =
        std::make_shared<CustomFlushBlockPolicy>(std::vector<int>{2, 1, 3, 2});
    Options options;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.statistics = CreateDBStatistics();
    Statistics* stats = options.statistics.get();
    std::unique_ptr<InternalKeyComparator> comparator(
        new InternalKeyComparator(BytewiseComparator()));
    const ImmutableOptions ioptions(options);
    const MutableCFOptions moptions(options);

    TableConstructor c(BytewiseComparator());

    // Block 0.
    AddInternalKey(&c, "aaaa", "v0");
    AddInternalKey(&c, "aaac", "v1");

    // Block 1.
    AddInternalKey(&c, "aaca", "v2");

    // Block 2.
    AddInternalKey(&c, "caaa", "v3");
    AddInternalKey(&c, "caac", "v4");
    AddInternalKey(&c, "caae", "v5");

    // Block 3.
    AddInternalKey(&c, "ccaa", "v6");
    AddInternalKey(&c, "ccac", "v7");

    // Write the file.
    std::vector<std::string> keys;
    stl_wrappers::KVMap kvmap;
    c.Finish(options, ioptions, moptions, table_options, *comparator, &keys,
             &kvmap);
    ASSERT_EQ(8, keys.size());

    auto reader = c.GetTableReader();
    auto props = reader->GetTableProperties();
    ASSERT_EQ(4u, props->num_data_blocks);
    ReadOptions read_options;
    std::unique_ptr<InternalIterator> iter(reader->NewIterator(
        read_options, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized,
        /*compaction_readahead_size=*/0, /*allow_unprepared_value=*/true));

    // Shouldn't have read data blocks before iterator is seeked.
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    auto ikey = [](Slice user_key) {
      return InternalKey(user_key, 0, kTypeValue).Encode().ToString();
    };

    // Seek to a key between blocks. If index contains first key, we shouldn't
    // read any data blocks until value is requested.
    iter->Seek(ikey("aaba"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[2], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 0 : 1,
              stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v2", iter->value().ToString());
    EXPECT_EQ(1, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Seek to the middle of a block. The block should be read right away.
    iter->Seek(ikey("caab"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[4], iter->key().ToString());
    EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v4", iter->value().ToString());
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Seek to just before the same block and don't access value.
    // The iterator should keep pinning the block contents.
    iter->Seek(ikey("baaa"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[3], iter->key().ToString());
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Seek to the same block again to check that the block is still pinned.
    iter->Seek(ikey("caae"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[5], iter->key().ToString());
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v5", iter->value().ToString());
    EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Step forward and fall through to the next block. Don't access value.
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[6], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 2 : 3,
              stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Step forward again. Block should be read.
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[7], iter->key().ToString());
    EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v7", iter->value().ToString());
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Step forward and reach the end.
    iter->Next();
    EXPECT_FALSE(iter->Valid());
    EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Seek to a single-key block and step forward without accessing value.
    iter->Seek(ikey("aaca"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[2], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 0 : 1,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[3], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 1 : 2,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v3", iter->value().ToString());
    EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    // Seek between blocks and step back without accessing value.
    iter->Seek(ikey("aaca"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[2], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 2 : 3,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[1], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 2 : 3,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    // All blocks are in cache now, there'll be no more misses ever.
    EXPECT_EQ(4, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v1", iter->value().ToString());

    // Next into the next block again.
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[2], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 2 : 4,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Seek to first and step back without accessing value.
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[0], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 2 : 5,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    iter->Prev();
    EXPECT_FALSE(iter->Valid());
    EXPECT_EQ(use_first_key ? 2 : 5,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Do some SeekForPrev() and SeekToLast() just to cover all methods.
    iter->SeekForPrev(ikey("caad"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[4], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 3 : 6,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v4", iter->value().ToString());
    EXPECT_EQ(use_first_key ? 3 : 6,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[7], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 4 : 7,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v7", iter->value().ToString());
    EXPECT_EQ(use_first_key ? 4 : 7,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    EXPECT_EQ(4, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    c.ResetTableReader();
  }
}

TEST_P(BlockBasedTableTest, BinaryIndexWithFirstKeyGlobalSeqno) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.index_type = BlockBasedTableOptions::kBinarySearchWithFirstKey;
  table_options.block_cache = NewLRUCache(10000);
  Options options;
  options.statistics = CreateDBStatistics();
  Statistics* stats = options.statistics.get();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);

  TableConstructor c(BytewiseComparator(), /* convert_to_internal_key */ false,
                     /* level */ -1, /* largest_seqno */ 42);

  c.Add(InternalKey("b", 0, kTypeValue).Encode().ToString(), "x");
  c.Add(InternalKey("c", 0, kTypeValue).Encode().ToString(), "y");

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  c.Finish(options, ioptions, moptions, table_options, *comparator, &keys,
           &kvmap);
  ASSERT_EQ(2, keys.size());

  auto reader = c.GetTableReader();
  auto props = reader->GetTableProperties();
  ASSERT_EQ(1u, props->num_data_blocks);
  ReadOptions read_options;
  std::unique_ptr<InternalIterator> iter(reader->NewIterator(
      read_options, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized,
      /*compaction_readahead_size=*/0, /*allow_unprepared_value=*/true));

  iter->Seek(InternalKey("a", 0, kTypeValue).Encode().ToString());
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(InternalKey("b", 42, kTypeValue).Encode().ToString(),
            iter->key().ToString());
  EXPECT_NE(keys[0], iter->key().ToString());
  // Key should have been served from index, without reading data blocks.
  EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

  ASSERT_TRUE(iter->PrepareValue());
  EXPECT_EQ("x", iter->value().ToString());
  EXPECT_EQ(1, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
  EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
  EXPECT_EQ(InternalKey("b", 42, kTypeValue).Encode().ToString(),
            iter->key().ToString());

  c.ResetTableReader();
}

// It's very hard to figure out the index block size of a block accurately.
// To make sure we get the index size, we just make sure as key number
// grows, the filter block size also grows.
TEST_P(BlockBasedTableTest, IndexSizeStat) {
  uint64_t last_index_size = 0;

  // we need to use random keys since the pure human readable texts
  // may be well compressed, resulting insignifcant change of index
  // block size.
  Random rnd(test::RandomSeed());
  std::vector<std::string> keys;

  for (int i = 0; i < 100; ++i) {
    keys.push_back(rnd.RandomString(10000));
  }

  // Each time we load one more key to the table. the table index block
  // size is expected to be larger than last time's.
  for (size_t i = 1; i < keys.size(); ++i) {
    TableConstructor c(BytewiseComparator(),
                       true /* convert_to_internal_key_ */);
    for (size_t j = 0; j < i; ++j) {
      c.Add(keys[j], "val");
    }

    std::vector<std::string> ks;
    stl_wrappers::KVMap kvmap;
    Options options;
    options.compression = kNoCompression;
    BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
    table_options.block_restart_interval = 1;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    const ImmutableOptions ioptions(options);
    const MutableCFOptions moptions(options);
    c.Finish(options, ioptions, moptions, table_options,
             GetPlainInternalComparator(options.comparator), &ks, &kvmap);
    auto index_size = c.GetTableReader()->GetTableProperties()->index_size;
    ASSERT_GT(index_size, last_index_size);
    last_index_size = index_size;
    c.ResetTableReader();
  }
}

TEST_P(BlockBasedTableTest, NumBlockStat) {
  Random rnd(test::RandomSeed());
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  Options options;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_restart_interval = 1;
  table_options.block_size = 1000;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  for (int i = 0; i < 10; ++i) {
    // the key/val are slightly smaller than block size, so that each block
    // holds roughly one key/value pair.
    c.Add(rnd.RandomString(900), "val");
  }

  std::vector<std::string> ks;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &ks, &kvmap);
  ASSERT_EQ(kvmap.size(),
            c.GetTableReader()->GetTableProperties()->num_data_blocks);
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, TracingGetTest) {
  TableConstructor c(BytewiseComparator());
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  options.create_if_missing = true;
  table_options.block_cache = NewLRUCache(1024 * 1024, 0);
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  SetupTracingTest(&c);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  std::string user_key = "k01";
  InternalKey internal_key(user_key, 0, kTypeValue);
  std::string encoded_key = internal_key.Encode().ToString();
  for (uint32_t i = 1; i <= 2; i++) {
    PinnableSlice value;
    GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, user_key, &value, nullptr,
                           nullptr, nullptr, true, nullptr, nullptr, nullptr,
                           nullptr, nullptr, nullptr, /*tracing_get_id=*/i);
    get_perf_context()->Reset();
    ASSERT_OK(c.GetTableReader()->Get(ReadOptions(), encoded_key, &get_context,
                                      moptions.prefix_extractor.get()));
    ASSERT_EQ(get_context.State(), GetContext::kFound);
    ASSERT_EQ(value.ToString(), kDummyValue);
  }

  // Verify traces.
  std::vector<BlockCacheTraceRecord> expected_records;
  // The first two records should be prefetching index and filter blocks.
  BlockCacheTraceRecord record;
  record.block_type = TraceType::kBlockTraceIndexBlock;
  record.caller = TableReaderCaller::kPrefetch;
  record.is_cache_hit = false;
  record.no_insert = false;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceFilterBlock;
  expected_records.push_back(record);
  // Then we should have three records for one index, one filter, and one data
  // block access.
  record.get_id = 1;
  record.block_type = TraceType::kBlockTraceFilterBlock;
  record.caller = TableReaderCaller::kUserGet;
  record.get_from_user_specified_snapshot = false;
  record.referenced_key = encoded_key;
  record.referenced_key_exist_in_block = true;
  record.is_cache_hit = true;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceIndexBlock;
  expected_records.push_back(record);
  record.is_cache_hit = false;
  record.block_type = TraceType::kBlockTraceDataBlock;
  expected_records.push_back(record);
  // The second get should all observe cache hits.
  record.is_cache_hit = true;
  record.get_id = 2;
  record.block_type = TraceType::kBlockTraceFilterBlock;
  record.caller = TableReaderCaller::kUserGet;
  record.get_from_user_specified_snapshot = false;
  record.referenced_key = encoded_key;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceIndexBlock;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceDataBlock;
  expected_records.push_back(record);
  VerifyBlockAccessTrace(&c, expected_records);
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, TracingApproximateOffsetOfTest) {
  TableConstructor c(BytewiseComparator());
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  options.create_if_missing = true;
  table_options.block_cache = NewLRUCache(1024 * 1024, 0);
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  SetupTracingTest(&c);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  for (uint32_t i = 1; i <= 2; i++) {
    std::string user_key = "k01";
    InternalKey internal_key(user_key, 0, kTypeValue);
    std::string encoded_key = internal_key.Encode().ToString();
    c.GetTableReader()->ApproximateOffsetOf(
        encoded_key, TableReaderCaller::kUserApproximateSize);
  }
  // Verify traces.
  std::vector<BlockCacheTraceRecord> expected_records;
  // The first two records should be prefetching index and filter blocks.
  BlockCacheTraceRecord record;
  record.block_type = TraceType::kBlockTraceIndexBlock;
  record.caller = TableReaderCaller::kPrefetch;
  record.is_cache_hit = false;
  record.no_insert = false;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceFilterBlock;
  expected_records.push_back(record);
  // Then we should have two records for only index blocks.
  record.block_type = TraceType::kBlockTraceIndexBlock;
  record.caller = TableReaderCaller::kUserApproximateSize;
  record.is_cache_hit = true;
  expected_records.push_back(record);
  expected_records.push_back(record);
  VerifyBlockAccessTrace(&c, expected_records);
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, TracingIterator) {
  TableConstructor c(BytewiseComparator());
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  options.create_if_missing = true;
  table_options.block_cache = NewLRUCache(1024 * 1024, 0);
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  SetupTracingTest(&c);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  for (uint32_t i = 1; i <= 2; i++) {
    ReadOptions read_options;
    std::unique_ptr<InternalIterator> iter(c.GetTableReader()->NewIterator(
        read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUserIterator));
    iter->SeekToFirst();
    while (iter->Valid()) {
      iter->key();
      iter->value();
      iter->Next();
    }
    ASSERT_OK(iter->status());
    iter.reset();
  }

  // Verify traces.
  std::vector<BlockCacheTraceRecord> expected_records;
  // The first two records should be prefetching index and filter blocks.
  BlockCacheTraceRecord record;
  record.block_type = TraceType::kBlockTraceIndexBlock;
  record.caller = TableReaderCaller::kPrefetch;
  record.is_cache_hit = false;
  record.no_insert = false;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceFilterBlock;
  expected_records.push_back(record);
  // Then we should have three records for index and two data block access.
  record.block_type = TraceType::kBlockTraceIndexBlock;
  record.caller = TableReaderCaller::kUserIterator;
  record.is_cache_hit = true;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceDataBlock;
  record.is_cache_hit = false;
  expected_records.push_back(record);
  expected_records.push_back(record);
  // When we iterate this file for the second time, we should observe all cache
  // hits.
  record.block_type = TraceType::kBlockTraceIndexBlock;
  record.is_cache_hit = true;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceDataBlock;
  expected_records.push_back(record);
  expected_records.push_back(record);
  VerifyBlockAccessTrace(&c, expected_records);
  c.ResetTableReader();
}

// A simple tool that takes the snapshot of block cache statistics.
class BlockCachePropertiesSnapshot {
 public:
  explicit BlockCachePropertiesSnapshot(Statistics* statistics) {
    block_cache_miss = statistics->getTickerCount(BLOCK_CACHE_MISS);
    block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_HIT);
    index_block_cache_miss = statistics->getTickerCount(BLOCK_CACHE_INDEX_MISS);
    index_block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_INDEX_HIT);
    data_block_cache_miss = statistics->getTickerCount(BLOCK_CACHE_DATA_MISS);
    data_block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_DATA_HIT);
    filter_block_cache_miss =
        statistics->getTickerCount(BLOCK_CACHE_FILTER_MISS);
    filter_block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_FILTER_HIT);
    block_cache_bytes_read = statistics->getTickerCount(BLOCK_CACHE_BYTES_READ);
    block_cache_bytes_write =
        statistics->getTickerCount(BLOCK_CACHE_BYTES_WRITE);
  }

  void AssertIndexBlockStat(int64_t expected_index_block_cache_miss,
                            int64_t expected_index_block_cache_hit) {
    ASSERT_EQ(expected_index_block_cache_miss, index_block_cache_miss);
    ASSERT_EQ(expected_index_block_cache_hit, index_block_cache_hit);
  }

  void AssertFilterBlockStat(int64_t expected_filter_block_cache_miss,
                             int64_t expected_filter_block_cache_hit) {
    ASSERT_EQ(expected_filter_block_cache_miss, filter_block_cache_miss);
    ASSERT_EQ(expected_filter_block_cache_hit, filter_block_cache_hit);
  }

  // Check if the fetched props matches the expected ones.
  // TODO(kailiu) Use this only when you disabled filter policy!
  void AssertEqual(int64_t expected_index_block_cache_miss,
                   int64_t expected_index_block_cache_hit,
                   int64_t expected_data_block_cache_miss,
                   int64_t expected_data_block_cache_hit) const {
    ASSERT_EQ(expected_index_block_cache_miss, index_block_cache_miss);
    ASSERT_EQ(expected_index_block_cache_hit, index_block_cache_hit);
    ASSERT_EQ(expected_data_block_cache_miss, data_block_cache_miss);
    ASSERT_EQ(expected_data_block_cache_hit, data_block_cache_hit);
    ASSERT_EQ(expected_index_block_cache_miss + expected_data_block_cache_miss,
              block_cache_miss);
    ASSERT_EQ(expected_index_block_cache_hit + expected_data_block_cache_hit,
              block_cache_hit);
  }

  int64_t GetCacheBytesRead() { return block_cache_bytes_read; }

  int64_t GetCacheBytesWrite() { return block_cache_bytes_write; }

 private:
  int64_t block_cache_miss = 0;
  int64_t block_cache_hit = 0;
  int64_t index_block_cache_miss = 0;
  int64_t index_block_cache_hit = 0;
  int64_t data_block_cache_miss = 0;
  int64_t data_block_cache_hit = 0;
  int64_t filter_block_cache_miss = 0;
  int64_t filter_block_cache_hit = 0;
  int64_t block_cache_bytes_read = 0;
  int64_t block_cache_bytes_write = 0;
};

// Make sure, by default, index/filter blocks were pre-loaded (meaning we won't
// use block cache to store them).
TEST_P(BlockBasedTableTest, BlockCacheDisabledTest) {
  Options options;
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_cache = NewLRUCache(1024, 4);
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;

  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("key", "value");
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  // preloading filter/index blocks is enabled.
  auto reader = dynamic_cast<BlockBasedTable*>(c.GetTableReader());
  ASSERT_FALSE(reader->TEST_FilterBlockInCache());
  ASSERT_FALSE(reader->TEST_IndexBlockInCache());

  {
    // nothing happens in the beginning
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertIndexBlockStat(0, 0);
    props.AssertFilterBlockStat(0, 0);
  }

  {
    GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, Slice(), nullptr, nullptr,
                           nullptr, nullptr, true, nullptr, nullptr);
    // a hack that just to trigger BlockBasedTable::GetFilter.
    ASSERT_OK(reader->Get(ReadOptions(), "non-exist-key", &get_context,
                          moptions.prefix_extractor.get()));
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertIndexBlockStat(0, 0);
    props.AssertFilterBlockStat(0, 0);
  }
}

// Due to the difficulities of the intersaction between statistics, this test
// only tests the case when "index block is put to block cache"
TEST_P(BlockBasedTableTest, FilterBlockInBlockCache) {
  // -- Table construction
  Options options;
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();

  // Enable the cache for index/filter blocks
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  LRUCacheOptions co;
  co.capacity = 2048;
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  table_options.block_cache = NewLRUCache(co);
  table_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;

  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("key", "value");
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  // preloading filter/index blocks is prohibited.
  auto* reader = dynamic_cast<BlockBasedTable*>(c.GetTableReader());
  ASSERT_FALSE(reader->TEST_FilterBlockInCache());
  ASSERT_TRUE(reader->TEST_IndexBlockInCache());

  // -- PART 1: Open with regular block cache.
  // Since block_cache is disabled, no cache activities will be involved.
  std::unique_ptr<InternalIterator> iter;

  int64_t last_cache_bytes_read = 0;
  // At first, no block will be accessed.
  {
    BlockCachePropertiesSnapshot props(options.statistics.get());
    // index will be added to block cache.
    props.AssertEqual(1,  // index block miss
                      0, 0, 0);
    ASSERT_EQ(props.GetCacheBytesRead(), 0);
    ASSERT_EQ(props.GetCacheBytesWrite(),
              static_cast<int64_t>(table_options.block_cache->GetUsage()));
    last_cache_bytes_read = props.GetCacheBytesRead();
  }

  // Only index block will be accessed
  {
    iter.reset(c.NewIterator(moptions.prefix_extractor.get()));
    BlockCachePropertiesSnapshot props(options.statistics.get());
    // NOTE: to help better highlight the "detla" of each ticker, I use
    // <last_value> + <added_value> to indicate the increment of changed
    // value; other numbers remain the same.
    props.AssertEqual(1, 0 + 1,  // index block hit
                      0, 0);
    // Cache hit, bytes read from cache should increase
    ASSERT_GT(props.GetCacheBytesRead(), last_cache_bytes_read);
    ASSERT_EQ(props.GetCacheBytesWrite(),
              static_cast<int64_t>(table_options.block_cache->GetUsage()));
    last_cache_bytes_read = props.GetCacheBytesRead();
  }

  // Only data block will be accessed
  {
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertEqual(1, 1, 0 + 1,  // data block miss
                      0);
    // Cache miss, Bytes read from cache should not change
    ASSERT_EQ(props.GetCacheBytesRead(), last_cache_bytes_read);
    ASSERT_EQ(props.GetCacheBytesWrite(),
              static_cast<int64_t>(table_options.block_cache->GetUsage()));
    last_cache_bytes_read = props.GetCacheBytesRead();
  }

  // Data block will be in cache
  {
    iter.reset(c.NewIterator(moptions.prefix_extractor.get()));
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertEqual(1, 1 + 1, /* index block hit */
                      1, 0 + 1 /* data block hit */);
    // Cache hit, bytes read from cache should increase
    ASSERT_GT(props.GetCacheBytesRead(), last_cache_bytes_read);
    ASSERT_EQ(props.GetCacheBytesWrite(),
              static_cast<int64_t>(table_options.block_cache->GetUsage()));
  }
  // release the iterator so that the block cache can reset correctly.
  iter.reset();

  c.ResetTableReader();

  // -- PART 2: Open with very small block cache
  // In this test, no block will ever get hit since the block cache is
  // too small to fit even one entry.
  table_options.block_cache = NewLRUCache(1, 4);
  options.statistics = CreateDBStatistics();
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  const ImmutableOptions ioptions2(options);
  const MutableCFOptions moptions2(options);
  ASSERT_OK(c.Reopen(ioptions2, moptions2));
  {
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertEqual(1,  // index block miss
                      0, 0, 0);
    // Cache miss, Bytes read from cache should not change
    ASSERT_EQ(props.GetCacheBytesRead(), 0);
  }

  {
    // Both index and data block get accessed.
    // It first cache index block then data block. But since the cache size
    // is only 1, index block will be purged after data block is inserted.
    iter.reset(c.NewIterator(moptions2.prefix_extractor.get()));
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertEqual(1 + 1,  // index block miss
                      0, 0,   // data block miss
                      0);
    // Cache hit, bytes read from cache should increase
    ASSERT_EQ(props.GetCacheBytesRead(), 0);
  }

  {
    // SeekToFirst() accesses data block. With similar reason, we expect data
    // block's cache miss.
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertEqual(2, 0, 0 + 1,  // data block miss
                      0);
    // Cache miss, Bytes read from cache should not change
    ASSERT_EQ(props.GetCacheBytesRead(), 0);
  }
  iter.reset();
  c.ResetTableReader();

  // -- PART 3: Open table with bloom filter enabled but not in SST file
  table_options.block_cache = NewLRUCache(4096, 4);
  table_options.cache_index_and_filter_blocks = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  TableConstructor c3(BytewiseComparator());
  std::string user_key = "k01";
  InternalKey internal_key(user_key, 0, kTypeValue);
  c3.Add(internal_key.Encode().ToString(), "hello");
  ImmutableOptions ioptions3(options);
  MutableCFOptions moptions3(options);
  // Generate table without filter policy
  c3.Finish(options, ioptions3, moptions3, table_options,
            GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  c3.ResetTableReader();

  // Open table with filter policy
  table_options.filter_policy.reset(NewBloomFilterPolicy(1));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  options.statistics = CreateDBStatistics();
  ImmutableOptions ioptions4(options);
  MutableCFOptions moptions4(options);
  ASSERT_OK(c3.Reopen(ioptions4, moptions4));
  reader = dynamic_cast<BlockBasedTable*>(c3.GetTableReader());
  ASSERT_FALSE(reader->TEST_FilterBlockInCache());
  PinnableSlice value;
  GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, user_key, &value, nullptr,
                         nullptr, nullptr, true, nullptr, nullptr);
  ASSERT_OK(reader->Get(ReadOptions(), internal_key.Encode(), &get_context,
                        moptions4.prefix_extractor.get()));
  ASSERT_STREQ(value.data(), "hello");
  BlockCachePropertiesSnapshot props(options.statistics.get());
  props.AssertFilterBlockStat(0, 0);
  c3.ResetTableReader();
}

void ValidateBlockSizeDeviation(int value, int expected) {
  BlockBasedTableOptions table_options;
  table_options.block_size_deviation = value;
  BlockBasedTableFactory* factory = new BlockBasedTableFactory(table_options);

  const BlockBasedTableOptions* normalized_table_options =
      factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_EQ(normalized_table_options->block_size_deviation, expected);

  delete factory;
}

void ValidateBlockRestartInterval(int value, int expected) {
  BlockBasedTableOptions table_options;
  table_options.block_restart_interval = value;
  BlockBasedTableFactory* factory = new BlockBasedTableFactory(table_options);

  const BlockBasedTableOptions* normalized_table_options =
      factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_EQ(normalized_table_options->block_restart_interval, expected);

  delete factory;
}

TEST_P(BlockBasedTableTest, InvalidOptions) {
  // invalid values for block_size_deviation (<0 or >100) are silently set to 0
  ValidateBlockSizeDeviation(-10, 0);
  ValidateBlockSizeDeviation(-1, 0);
  ValidateBlockSizeDeviation(0, 0);
  ValidateBlockSizeDeviation(1, 1);
  ValidateBlockSizeDeviation(99, 99);
  ValidateBlockSizeDeviation(100, 100);
  ValidateBlockSizeDeviation(101, 0);
  ValidateBlockSizeDeviation(1000, 0);

  // invalid values for block_restart_interval (<1) are silently set to 1
  ValidateBlockRestartInterval(-10, 1);
  ValidateBlockRestartInterval(-1, 1);
  ValidateBlockRestartInterval(0, 1);
  ValidateBlockRestartInterval(1, 1);
  ValidateBlockRestartInterval(2, 2);
  ValidateBlockRestartInterval(1000, 1000);
}

TEST_P(BlockBasedTableTest, BlockReadCountTest) {
  // bloom_filter_type = 1 -- full filter using use_block_based_builder=false
  // bloom_filter_type = 2 -- full filter using use_block_based_builder=true
  //                          because of API change to hide block-based filter
  for (int bloom_filter_type = 1; bloom_filter_type <= 2; ++bloom_filter_type) {
    for (int index_and_filter_in_cache = 0; index_and_filter_in_cache < 2;
         ++index_and_filter_in_cache) {
      Options options;
      options.create_if_missing = true;

      BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
      table_options.block_cache = NewLRUCache(1, 0);
      table_options.cache_index_and_filter_blocks = index_and_filter_in_cache;
      table_options.filter_policy.reset(
          NewBloomFilterPolicy(10, bloom_filter_type == 2));
      options.table_factory.reset(new BlockBasedTableFactory(table_options));
      std::vector<std::string> keys;
      stl_wrappers::KVMap kvmap;

      TableConstructor c(BytewiseComparator());
      std::string user_key = "k04";
      InternalKey internal_key(user_key, 0, kTypeValue);
      std::string encoded_key = internal_key.Encode().ToString();
      c.Add(encoded_key, "hello");
      ImmutableOptions ioptions(options);
      MutableCFOptions moptions(options);
      // Generate table with filter policy
      c.Finish(options, ioptions, moptions, table_options,
               GetPlainInternalComparator(options.comparator), &keys, &kvmap);
      auto reader = c.GetTableReader();
      PinnableSlice value;
      {
        GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                               GetContext::kNotFound, user_key, &value, nullptr,
                               nullptr, nullptr, true, nullptr, nullptr);
        get_perf_context()->Reset();
        ASSERT_OK(reader->Get(ReadOptions(), encoded_key, &get_context,
                              moptions.prefix_extractor.get()));
        if (index_and_filter_in_cache) {
          // data, index and filter block
          ASSERT_EQ(get_perf_context()->block_read_count, 3);
          ASSERT_EQ(get_perf_context()->index_block_read_count, 1);
          ASSERT_EQ(get_perf_context()->filter_block_read_count, 1);
        } else {
          // just the data block
          ASSERT_EQ(get_perf_context()->block_read_count, 1);
        }
        ASSERT_EQ(get_context.State(), GetContext::kFound);
        ASSERT_STREQ(value.data(), "hello");
      }

      // Get non-existing key
      user_key = "does-not-exist";
      internal_key = InternalKey(user_key, 0, kTypeValue);
      encoded_key = internal_key.Encode().ToString();

      value.Reset();
      {
        GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                               GetContext::kNotFound, user_key, &value, nullptr,
                               nullptr, nullptr, true, nullptr, nullptr);
        get_perf_context()->Reset();
        ASSERT_OK(reader->Get(ReadOptions(), encoded_key, &get_context,
                              moptions.prefix_extractor.get()));
        ASSERT_EQ(get_context.State(), GetContext::kNotFound);
      }

      if (index_and_filter_in_cache) {
        if (bloom_filter_type == 0) {
          // with block-based, we read index and then the filter
          ASSERT_EQ(get_perf_context()->block_read_count, 2);
          ASSERT_EQ(get_perf_context()->index_block_read_count, 1);
          ASSERT_EQ(get_perf_context()->filter_block_read_count, 1);
        } else {
          // with full-filter, we read filter first and then we stop
          ASSERT_EQ(get_perf_context()->block_read_count, 1);
          ASSERT_EQ(get_perf_context()->filter_block_read_count, 1);
        }
      } else {
        // filter is already in memory and it figures out that the key doesn't
        // exist
        ASSERT_EQ(get_perf_context()->block_read_count, 0);
      }
    }
  }
}

TEST_P(BlockBasedTableTest, BlockCacheLeak) {
  // Check that when we reopen a table we don't lose access to blocks already
  // in the cache. This test checks whether the Table actually makes use of the
  // unique ID from the file.

  Options opt;
  std::unique_ptr<InternalKeyComparator> ikc;
  ikc.reset(new test::PlainInternalKeyComparator(opt.comparator));
  opt.compression = kNoCompression;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_size = 1024;
  // big enough so we don't ever lose cached values.
  table_options.block_cache = NewLRUCache(16 * 1024 * 1024, 4);
  opt.table_factory.reset(NewBlockBasedTableFactory(table_options));

  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("k01", "hello");
  c.Add("k02", "hello2");
  c.Add("k03", std::string(10000, 'x'));
  c.Add("k04", std::string(200000, 'x'));
  c.Add("k05", std::string(300000, 'x'));
  c.Add("k06", "hello3");
  c.Add("k07", std::string(100000, 'x'));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(opt);
  const MutableCFOptions moptions(opt);
  c.Finish(opt, ioptions, moptions, table_options, *ikc, &keys, &kvmap);

  std::unique_ptr<InternalIterator> iter(
      c.NewIterator(moptions.prefix_extractor.get()));
  iter->SeekToFirst();
  while (iter->Valid()) {
    iter->key();
    iter->value();
    iter->Next();
  }
  ASSERT_OK(iter->status());
  iter.reset();

  const ImmutableOptions ioptions1(opt);
  const MutableCFOptions moptions1(opt);
  ASSERT_OK(c.Reopen(ioptions1, moptions1));
  auto table_reader = dynamic_cast<BlockBasedTable*>(c.GetTableReader());
  for (const std::string& key : keys) {
    InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
    ASSERT_TRUE(table_reader->TEST_KeyInCache(ReadOptions(), ikey.Encode()));
  }
  c.ResetTableReader();

  // rerun with different block cache
  table_options.block_cache = NewLRUCache(16 * 1024 * 1024, 4);
  opt.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions ioptions2(opt);
  const MutableCFOptions moptions2(opt);
  ASSERT_OK(c.Reopen(ioptions2, moptions2));
  table_reader = dynamic_cast<BlockBasedTable*>(c.GetTableReader());
  for (const std::string& key : keys) {
    InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
    ASSERT_TRUE(!table_reader->TEST_KeyInCache(ReadOptions(), ikey.Encode()));
  }
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, MemoryAllocator) {
  auto default_memory_allocator = std::make_shared<DefaultMemoryAllocator>();
  auto custom_memory_allocator =
      std::make_shared<CountedMemoryAllocator>(default_memory_allocator);
  {
    Options opt;
    std::unique_ptr<InternalKeyComparator> ikc;
    ikc.reset(new test::PlainInternalKeyComparator(opt.comparator));
    opt.compression = kNoCompression;
    BlockBasedTableOptions table_options;
    table_options.block_size = 1024;
    LRUCacheOptions lruOptions;
    lruOptions.memory_allocator = custom_memory_allocator;
    lruOptions.capacity = 16 * 1024 * 1024;
    lruOptions.num_shard_bits = 4;
    table_options.block_cache = NewLRUCache(std::move(lruOptions));
    opt.table_factory.reset(NewBlockBasedTableFactory(table_options));

    TableConstructor c(BytewiseComparator(),
                       true /* convert_to_internal_key_ */);
    c.Add("k01", "hello");
    c.Add("k02", "hello2");
    c.Add("k03", std::string(10000, 'x'));
    c.Add("k04", std::string(200000, 'x'));
    c.Add("k05", std::string(300000, 'x'));
    c.Add("k06", "hello3");
    c.Add("k07", std::string(100000, 'x'));
    std::vector<std::string> keys;
    stl_wrappers::KVMap kvmap;
    const ImmutableOptions ioptions(opt);
    const MutableCFOptions moptions(opt);
    c.Finish(opt, ioptions, moptions, table_options, *ikc, &keys, &kvmap);

    std::unique_ptr<InternalIterator> iter(
        c.NewIterator(moptions.prefix_extractor.get()));
    iter->SeekToFirst();
    while (iter->Valid()) {
      iter->key();
      iter->value();
      iter->Next();
    }
    ASSERT_OK(iter->status());
  }

  // out of scope, block cache should have been deleted, all allocations
  // deallocated
  EXPECT_EQ(custom_memory_allocator->GetNumAllocations(),
            custom_memory_allocator->GetNumDeallocations());
  // make sure that allocations actually happened through the cache allocator
  EXPECT_GT(custom_memory_allocator->GetNumAllocations(), 0);
}

// Test the file checksum of block based table
TEST_P(BlockBasedTableTest, NoFileChecksum) {
  Options options;
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  int level = 0;
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;
  std::string column_family_name;

  FileChecksumTestHelper f(true);
  f.CreateWritableFile();
  std::unique_ptr<TableBuilder> builder;
  builder.reset(ioptions.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, *comparator,
                          &int_tbl_prop_collector_factories,
                          options.compression, options.compression_opts,
                          kUnknownColumnFamily, column_family_name, level),
      f.GetFileWriter()));
  ASSERT_OK(f.ResetTableBuilder(std::move(builder)));
  f.AddKVtoKVMap(1000);
  ASSERT_OK(f.WriteKVAndFlushTable());
  ASSERT_STREQ(f.GetFileChecksumFuncName(), kUnknownFileChecksumFuncName);
  ASSERT_STREQ(f.GetFileChecksum().c_str(), kUnknownFileChecksum);
}

TEST_P(BlockBasedTableTest, Crc32cFileChecksum) {
  FileChecksumGenCrc32cFactory* file_checksum_gen_factory =
      new FileChecksumGenCrc32cFactory();
  Options options;
  options.file_checksum_gen_factory.reset(file_checksum_gen_factory);
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  int level = 0;
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;
  std::string column_family_name;

  FileChecksumGenContext gen_context;
  gen_context.file_name = "db/tmp";
  std::unique_ptr<FileChecksumGenerator> checksum_crc32c_gen1 =
      options.file_checksum_gen_factory->CreateFileChecksumGenerator(
          gen_context);
  FileChecksumTestHelper f(true);
  f.CreateWritableFile();
  f.SetFileChecksumGenerator(checksum_crc32c_gen1.release());
  std::unique_ptr<TableBuilder> builder;
  builder.reset(ioptions.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, *comparator,
                          &int_tbl_prop_collector_factories,
                          options.compression, options.compression_opts,
                          kUnknownColumnFamily, column_family_name, level),
      f.GetFileWriter()));
  ASSERT_OK(f.ResetTableBuilder(std::move(builder)));
  f.AddKVtoKVMap(1000);
  ASSERT_OK(f.WriteKVAndFlushTable());
  ASSERT_STREQ(f.GetFileChecksumFuncName(), "FileChecksumCrc32c");

  std::unique_ptr<FileChecksumGenerator> checksum_crc32c_gen2 =
      options.file_checksum_gen_factory->CreateFileChecksumGenerator(
          gen_context);
  std::string checksum;
  ASSERT_OK(f.CalculateFileChecksum(checksum_crc32c_gen2.get(), &checksum));
  ASSERT_STREQ(f.GetFileChecksum().c_str(), checksum.c_str());

  // Unit test the generator itself for schema stability
  std::unique_ptr<FileChecksumGenerator> checksum_crc32c_gen3 =
      options.file_checksum_gen_factory->CreateFileChecksumGenerator(
          gen_context);
  const char data[] = "here is some data";
  checksum_crc32c_gen3->Update(data, sizeof(data));
  checksum_crc32c_gen3->Finalize();
  checksum = checksum_crc32c_gen3->GetChecksum();
  ASSERT_STREQ(checksum.c_str(), "\345\245\277\110");
}

// Plain table is not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
TEST_F(PlainTableTest, BasicPlainTableProperties) {
  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = 8;
  plain_table_options.bloom_bits_per_key = 8;
  plain_table_options.hash_table_ratio = 0;

  PlainTableFactory factory(plain_table_options);
  std::unique_ptr<FSWritableFile> sink(new test::StringSink());
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(sink), "" /* don't care */, FileOptions()));
  Options options;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;
  std::string column_family_name;
  int unknown_level = -1;
  std::unique_ptr<TableBuilder> builder(factory.NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, ikc,
                          &int_tbl_prop_collector_factories, kNoCompression,
                          CompressionOptions(), kUnknownColumnFamily,
                          column_family_name, unknown_level),
      file_writer.get()));

  for (char c = 'a'; c <= 'z'; ++c) {
    std::string key(8, c);
    key.append("\1       ");  // PlainTable expects internal key structure
    std::string value(28, c + 42);
    builder->Add(key, value);
  }
  ASSERT_OK(builder->Finish());
  ASSERT_OK(file_writer->Flush());

  test::StringSink* ss =
      static_cast<test::StringSink*>(file_writer->writable_file());
  std::unique_ptr<FSRandomAccessFile> source(
      new test::StringSource(ss->contents(), 72242, true));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(source), "test"));

  std::unique_ptr<TableProperties> props;
  auto s = ReadTableProperties(file_reader.get(), ss->contents().size(),
                               kPlainTableMagicNumber, ioptions, &props);
  ASSERT_OK(s);

  ASSERT_EQ(0ul, props->index_size);
  ASSERT_EQ(0ul, props->filter_size);
  ASSERT_EQ(16ul * 26, props->raw_key_size);
  ASSERT_EQ(28ul * 26, props->raw_value_size);
  ASSERT_EQ(26ul, props->num_entries);
  ASSERT_EQ(1ul, props->num_data_blocks);
}

TEST_F(PlainTableTest, NoFileChecksum) {
  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = 20;
  plain_table_options.bloom_bits_per_key = 8;
  plain_table_options.hash_table_ratio = 0;
  PlainTableFactory factory(plain_table_options);

  Options options;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;
  std::string column_family_name;
  int unknown_level = -1;
  FileChecksumTestHelper f(true);
  f.CreateWritableFile();

  std::unique_ptr<TableBuilder> builder(factory.NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, ikc,
                          &int_tbl_prop_collector_factories, kNoCompression,
                          CompressionOptions(), kUnknownColumnFamily,
                          column_family_name, unknown_level),
      f.GetFileWriter()));
  ASSERT_OK(f.ResetTableBuilder(std::move(builder)));
  f.AddKVtoKVMap(1000);
  ASSERT_OK(f.WriteKVAndFlushTable());
  ASSERT_STREQ(f.GetFileChecksumFuncName(), kUnknownFileChecksumFuncName);
  EXPECT_EQ(f.GetFileChecksum(), kUnknownFileChecksum);
}

TEST_F(PlainTableTest, Crc32cFileChecksum) {
  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = 20;
  plain_table_options.bloom_bits_per_key = 8;
  plain_table_options.hash_table_ratio = 0;
  PlainTableFactory factory(plain_table_options);

  FileChecksumGenCrc32cFactory* file_checksum_gen_factory =
      new FileChecksumGenCrc32cFactory();
  Options options;
  options.file_checksum_gen_factory.reset(file_checksum_gen_factory);
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;
  std::string column_family_name;
  int unknown_level = -1;

  FileChecksumGenContext gen_context;
  gen_context.file_name = "db/tmp";
  std::unique_ptr<FileChecksumGenerator> checksum_crc32c_gen1 =
      options.file_checksum_gen_factory->CreateFileChecksumGenerator(
          gen_context);
  FileChecksumTestHelper f(true);
  f.CreateWritableFile();
  f.SetFileChecksumGenerator(checksum_crc32c_gen1.release());

  std::unique_ptr<TableBuilder> builder(factory.NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, ikc,
                          &int_tbl_prop_collector_factories, kNoCompression,
                          CompressionOptions(), kUnknownColumnFamily,
                          column_family_name, unknown_level),
      f.GetFileWriter()));
  ASSERT_OK(f.ResetTableBuilder(std::move(builder)));
  f.AddKVtoKVMap(1000);
  ASSERT_OK(f.WriteKVAndFlushTable());
  ASSERT_STREQ(f.GetFileChecksumFuncName(), "FileChecksumCrc32c");

  std::unique_ptr<FileChecksumGenerator> checksum_crc32c_gen2 =
      options.file_checksum_gen_factory->CreateFileChecksumGenerator(
          gen_context);
  std::string checksum;
  ASSERT_OK(f.CalculateFileChecksum(checksum_crc32c_gen2.get(), &checksum));
  EXPECT_STREQ(f.GetFileChecksum().c_str(), checksum.c_str());
}

#endif  // !ROCKSDB_LITE

TEST_F(GeneralTableTest, ApproximateOffsetOfPlain) {
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("k01", "hello");
  c.Add("k02", "hello2");
  c.Add("k03", std::string(10000, 'x'));
  c.Add("k04", std::string(200000, 'x'));
  c.Add("k05", std::string(300000, 'x'));
  c.Add("k06", "hello3");
  c.Add("k07", std::string(100000, 'x'));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.db_host_id = "";
  test::PlainInternalKeyComparator internal_comparator(options.comparator);
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options, internal_comparator,
           &keys, &kvmap);

  ASSERT_TRUE(Between(c.ApproximateOffsetOf("abc"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01a"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k02"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k03"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04"), 10000, 11000));
  // k04 and k05 will be in two consecutive blocks, the index is
  // an arbitrary slice between k04 and k05, either before or after k04a
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04a"), 10000, 211000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k05"), 210000, 211000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k06"), 510000, 511000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k07"), 510000, 511000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("xyz"), 610000, 612000));
  c.ResetTableReader();
}

static void DoCompressionTest(CompressionType comp) {
  Random rnd(301);
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  std::string tmp;
  c.Add("k01", "hello");
  c.Add("k02", test::CompressibleString(&rnd, 0.25, 10000, &tmp));
  c.Add("k03", "hello3");
  c.Add("k04", test::CompressibleString(&rnd, 0.25, 10000, &tmp));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  test::PlainInternalKeyComparator ikc(options.comparator);
  options.compression = comp;
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options, ikc, &keys, &kvmap);

  ASSERT_TRUE(Between(c.ApproximateOffsetOf("abc"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k02"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k03"), 2000, 3525));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04"), 2000, 3525));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("xyz"), 4000, 7075));
  c.ResetTableReader();
}

TEST_F(GeneralTableTest, ApproximateOffsetOfCompressed) {
  std::vector<CompressionType> compression_state;
  if (!Snappy_Supported()) {
    fprintf(stderr, "skipping snappy compression tests\n");
  } else {
    compression_state.push_back(kSnappyCompression);
  }

  if (!Zlib_Supported()) {
    fprintf(stderr, "skipping zlib compression tests\n");
  } else {
    compression_state.push_back(kZlibCompression);
  }

  // TODO(kailiu) DoCompressionTest() doesn't work with BZip2.
  /*
  if (!BZip2_Supported()) {
    fprintf(stderr, "skipping bzip2 compression tests\n");
  } else {
    compression_state.push_back(kBZip2Compression);
  }
  */

  if (!LZ4_Supported()) {
    fprintf(stderr, "skipping lz4 and lz4hc compression tests\n");
  } else {
    compression_state.push_back(kLZ4Compression);
    compression_state.push_back(kLZ4HCCompression);
  }

  if (!XPRESS_Supported()) {
    fprintf(stderr, "skipping xpress and xpress compression tests\n");
  } else {
    compression_state.push_back(kXpressCompression);
  }

  for (auto state : compression_state) {
    DoCompressionTest(state);
  }
}

TEST_F(GeneralTableTest, ApproximateKeyAnchors) {
  Random rnd(301);
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  std::string tmp;
  for (int i = 1000; i < 9000; i++) {
    c.Add(std::to_string(i), rnd.RandomString(2000));
  }
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  InternalKeyComparator ikc(options.comparator);
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 4096;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options, ikc, &keys, &kvmap);

  std::vector<TableReader::Anchor> anchors;
  ASSERT_OK(c.GetTableReader()->ApproximateKeyAnchors(ReadOptions(), anchors));
  // The target is 128 anchors. But in reality it can be slightly more or fewer.
  ASSERT_GT(anchors.size(), 120);
  ASSERT_LT(anchors.size(), 140);

  // We have around 8000 keys. With 128 anchors, in average 62.5 keys per
  // anchor. Here we take a rough range and estimate the distance between
  // anchors is between 50 and 100.
  // Total data size is about 18,000,000, so each anchor range is about
  // 140,625. We also take a rough range.
  int prev_num = 1000;
  // Non-last anchor
  for (size_t i = 0; i + 1 < anchors.size(); i++) {
    auto& anchor = anchors[i];
    ASSERT_GT(anchor.range_size, 100000);
    ASSERT_LT(anchor.range_size, 200000);

    // Key might be shortened, so fill 0 in the end if it is the case.
    std::string key_cpy = anchor.user_key;
    key_cpy.append(4 - key_cpy.size(), '0');
    int num = std::stoi(key_cpy);
    ASSERT_GT(num - prev_num, 50);
    ASSERT_LT(num - prev_num, 100);
    prev_num = num;
  }

  ASSERT_EQ("8999", anchors.back().user_key);
  ASSERT_LT(anchors.back().range_size, 200000);

  c.ResetTableReader();
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
TEST_P(ParameterizedHarnessTest, RandomizedHarnessTest) {
  Random rnd(test::RandomSeed() + 5);
  for (int num_entries = 0; num_entries < 2000;
       num_entries += (num_entries < 50 ? 1 : 200)) {
    for (int e = 0; e < num_entries; e++) {
      Add(test::RandomKey(&rnd, rnd.Skewed(4)),
          rnd.RandomString(rnd.Skewed(5)));
    }
    Test(&rnd);
  }
}

#ifndef ROCKSDB_LITE
TEST_F(DBHarnessTest, RandomizedLongDB) {
  Random rnd(test::RandomSeed());
  int num_entries = 100000;
  for (int e = 0; e < num_entries; e++) {
    std::string v;
    Add(test::RandomKey(&rnd, rnd.Skewed(4)), rnd.RandomString(rnd.Skewed(5)));
  }
  Test(&rnd);

  // We must have created enough data to force merging
  int files = 0;
  for (int level = 0; level < db()->NumberLevels(); level++) {
    std::string value;
    char name[100];
    snprintf(name, sizeof(name), "rocksdb.num-files-at-level%d", level);
    ASSERT_TRUE(db()->GetProperty(name, &value));
    files += atoi(value.c_str());
  }
  ASSERT_GT(files, 0);
}
#endif  // ROCKSDB_LITE
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

class MemTableTest : public testing::Test {
 public:
  MemTableTest() {
    InternalKeyComparator cmp(BytewiseComparator());
    auto table_factory = std::make_shared<SkipListFactory>();
    options_.memtable_factory = table_factory;
    ImmutableOptions ioptions(options_);
    wb_ = new WriteBufferManager(options_.db_write_buffer_size);
    memtable_ = new MemTable(cmp, ioptions, MutableCFOptions(options_), wb_,
                             kMaxSequenceNumber, 0 /* column_family_id */);
    memtable_->Ref();
  }

  ~MemTableTest() {
    delete memtable_->Unref();
    delete wb_;
  }

  MemTable* GetMemTable() { return memtable_; }

 private:
  MemTable* memtable_;
  Options options_;
  WriteBufferManager* wb_;
};

TEST_F(MemTableTest, Simple) {
  WriteBatch batch;
  WriteBatchInternal::SetSequence(&batch, 100);
  ASSERT_OK(batch.Put(std::string("k1"), std::string("v1")));
  ASSERT_OK(batch.Put(std::string("k2"), std::string("v2")));
  ASSERT_OK(batch.Put(std::string("k3"), std::string("v3")));
  ASSERT_OK(batch.Put(std::string("largekey"), std::string("vlarge")));
  ASSERT_OK(batch.DeleteRange(std::string("chi"), std::string("xigua")));
  ASSERT_OK(batch.DeleteRange(std::string("begin"), std::string("end")));
  ColumnFamilyMemTablesDefault cf_mems_default(GetMemTable());
  ASSERT_TRUE(
      WriteBatchInternal::InsertInto(&batch, &cf_mems_default, nullptr, nullptr)
          .ok());

  for (int i = 0; i < 2; ++i) {
    Arena arena;
    ScopedArenaIterator arena_iter_guard;
    std::unique_ptr<InternalIterator> iter_guard;
    InternalIterator* iter;
    if (i == 0) {
      iter = GetMemTable()->NewIterator(ReadOptions(), &arena);
      arena_iter_guard.set(iter);
    } else {
      iter = GetMemTable()->NewRangeTombstoneIterator(
          ReadOptions(), kMaxSequenceNumber /* read_seq */,
          false /* immutable_memtable */);
      iter_guard.reset(iter);
    }
    if (iter == nullptr) {
      continue;
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
      fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
              iter->value().ToString().c_str());
      iter->Next();
    }
  }
}

// Test the empty key
TEST_P(ParameterizedHarnessTest, SimpleEmptyKey) {
  Random rnd(test::RandomSeed() + 1);
  Add("", "v");
  Test(&rnd);
}

TEST_P(ParameterizedHarnessTest, SimpleSingle) {
  Random rnd(test::RandomSeed() + 2);
  Add("abc", "v");
  Test(&rnd);
}

TEST_P(ParameterizedHarnessTest, SimpleMulti) {
  Random rnd(test::RandomSeed() + 3);
  Add("abc", "v");
  Add("abcd", "v");
  Add("ac", "v2");
  Test(&rnd);
}

TEST_P(ParameterizedHarnessTest, SimpleSpecialKey) {
  Random rnd(test::RandomSeed() + 4);
  Add("\xff\xff", "v3");
  Test(&rnd);
}

TEST(TableTest, FooterTests) {
  Random* r = Random::GetTLSInstance();
  uint64_t data_size = (uint64_t{1} << r->Uniform(40)) + r->Uniform(100);
  uint64_t index_size = r->Uniform(1000000000);
  uint64_t metaindex_size = r->Uniform(1000000);
  // 5 == block trailer size
  BlockHandle index(data_size + 5, index_size);
  BlockHandle meta_index(data_size + index_size + 2 * 5, metaindex_size);
  uint64_t footer_offset = data_size + metaindex_size + index_size + 3 * 5;
  {
    // legacy block based
    FooterBuilder footer;
    footer.Build(kBlockBasedTableMagicNumber, /* format_version */ 0,
                 footer_offset, kCRC32c, meta_index, index);
    Footer decoded_footer;
    ASSERT_OK(decoded_footer.DecodeFrom(footer.GetSlice(), footer_offset));
    ASSERT_EQ(decoded_footer.table_magic_number(), kBlockBasedTableMagicNumber);
    ASSERT_EQ(decoded_footer.checksum_type(), kCRC32c);
    ASSERT_EQ(decoded_footer.metaindex_handle().offset(), meta_index.offset());
    ASSERT_EQ(decoded_footer.metaindex_handle().size(), meta_index.size());
    ASSERT_EQ(decoded_footer.index_handle().offset(), index.offset());
    ASSERT_EQ(decoded_footer.index_handle().size(), index.size());
    ASSERT_EQ(decoded_footer.format_version(), 0U);
    ASSERT_EQ(decoded_footer.GetBlockTrailerSize(), 5U);
    // Ensure serialized with legacy magic
    ASSERT_EQ(
        DecodeFixed64(footer.GetSlice().data() + footer.GetSlice().size() - 8),
        kLegacyBlockBasedTableMagicNumber);
  }
  // block based, various checksums, various versions
  for (auto t : GetSupportedChecksums()) {
    for (uint32_t fv = 1; IsSupportedFormatVersion(fv); ++fv) {
      FooterBuilder footer;
      footer.Build(kBlockBasedTableMagicNumber, fv, footer_offset, t,
                   meta_index, index);
      Footer decoded_footer;
      ASSERT_OK(decoded_footer.DecodeFrom(footer.GetSlice(), footer_offset));
      ASSERT_EQ(decoded_footer.table_magic_number(),
                kBlockBasedTableMagicNumber);
      ASSERT_EQ(decoded_footer.checksum_type(), t);
      ASSERT_EQ(decoded_footer.metaindex_handle().offset(),
                meta_index.offset());
      ASSERT_EQ(decoded_footer.metaindex_handle().size(), meta_index.size());
      ASSERT_EQ(decoded_footer.index_handle().offset(), index.offset());
      ASSERT_EQ(decoded_footer.index_handle().size(), index.size());
      ASSERT_EQ(decoded_footer.format_version(), fv);
      ASSERT_EQ(decoded_footer.GetBlockTrailerSize(), 5U);
    }
  }
// Plain table is not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
  {
    // legacy plain table
    FooterBuilder footer;
    footer.Build(kPlainTableMagicNumber, /* format_version */ 0, footer_offset,
                 kNoChecksum, meta_index);
    Footer decoded_footer;
    ASSERT_OK(decoded_footer.DecodeFrom(footer.GetSlice(), footer_offset));
    ASSERT_EQ(decoded_footer.table_magic_number(), kPlainTableMagicNumber);
    ASSERT_EQ(decoded_footer.checksum_type(), kCRC32c);
    ASSERT_EQ(decoded_footer.metaindex_handle().offset(), meta_index.offset());
    ASSERT_EQ(decoded_footer.metaindex_handle().size(), meta_index.size());
    ASSERT_EQ(decoded_footer.index_handle().offset(), 0U);
    ASSERT_EQ(decoded_footer.index_handle().size(), 0U);
    ASSERT_EQ(decoded_footer.format_version(), 0U);
    ASSERT_EQ(decoded_footer.GetBlockTrailerSize(), 0U);
    // Ensure serialized with legacy magic
    ASSERT_EQ(
        DecodeFixed64(footer.GetSlice().data() + footer.GetSlice().size() - 8),
        kLegacyPlainTableMagicNumber);
  }
  {
    // xxhash plain table (not currently used)
    FooterBuilder footer;
    footer.Build(kPlainTableMagicNumber, /* format_version */ 1, footer_offset,
                 kxxHash, meta_index);
    Footer decoded_footer;
    ASSERT_OK(decoded_footer.DecodeFrom(footer.GetSlice(), footer_offset));
    ASSERT_EQ(decoded_footer.table_magic_number(), kPlainTableMagicNumber);
    ASSERT_EQ(decoded_footer.checksum_type(), kxxHash);
    ASSERT_EQ(decoded_footer.metaindex_handle().offset(), meta_index.offset());
    ASSERT_EQ(decoded_footer.metaindex_handle().size(), meta_index.size());
    ASSERT_EQ(decoded_footer.index_handle().offset(), 0U);
    ASSERT_EQ(decoded_footer.index_handle().size(), 0U);
    ASSERT_EQ(decoded_footer.format_version(), 1U);
    ASSERT_EQ(decoded_footer.GetBlockTrailerSize(), 0U);
  }
#endif  // !ROCKSDB_LITE
}

class IndexBlockRestartIntervalTest
    : public TableTest,
      public ::testing::WithParamInterface<std::pair<int, bool>> {
 public:
  static std::vector<std::pair<int, bool>> GetRestartValues() {
    return {{-1, false}, {0, false},  {1, false}, {8, false},
            {16, false}, {32, false}, {-1, true}, {0, true},
            {1, true},   {8, true},   {16, true}, {32, true}};
  }
};

INSTANTIATE_TEST_CASE_P(
    IndexBlockRestartIntervalTest, IndexBlockRestartIntervalTest,
    ::testing::ValuesIn(IndexBlockRestartIntervalTest::GetRestartValues()));

TEST_P(IndexBlockRestartIntervalTest, IndexBlockRestartInterval) {
  const int kKeysInTable = 10000;
  const int kKeySize = 100;
  const int kValSize = 500;

  const int index_block_restart_interval = std::get<0>(GetParam());
  const bool value_delta_encoding = std::get<1>(GetParam());

  Options options;
  BlockBasedTableOptions table_options;
  table_options.block_size = 64;  // small block size to get big index block
  table_options.index_block_restart_interval = index_block_restart_interval;
  if (value_delta_encoding) {
    table_options.format_version = 4;
  } else {
    table_options.format_version = 3;
  }
  options.table_factory.reset(new BlockBasedTableFactory(table_options));

  TableConstructor c(BytewiseComparator());
  static Random rnd(301);
  for (int i = 0; i < kKeysInTable; i++) {
    InternalKey k(rnd.RandomString(kKeySize), 0, kTypeValue);
    c.Add(k.Encode().ToString(), rnd.RandomString(kValSize));
  }

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options, *comparator, &keys,
           &kvmap);
  auto reader = c.GetTableReader();

  ReadOptions read_options;
  std::unique_ptr<InternalIterator> db_iter(reader->NewIterator(
      read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  // Test point lookup
  for (auto& kv : kvmap) {
    db_iter->Seek(kv.first);

    ASSERT_TRUE(db_iter->Valid());
    ASSERT_OK(db_iter->status());
    ASSERT_EQ(db_iter->key(), kv.first);
    ASSERT_EQ(db_iter->value(), kv.second);
  }

  // Test iterating
  auto kv_iter = kvmap.begin();
  for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
    ASSERT_EQ(db_iter->key(), kv_iter->first);
    ASSERT_EQ(db_iter->value(), kv_iter->second);
    kv_iter++;
  }
  ASSERT_EQ(kv_iter, kvmap.end());
  c.ResetTableReader();
}

class PrefixTest : public testing::Test {
 public:
  PrefixTest() : testing::Test() {}
  ~PrefixTest() override {}
};

namespace {
// A simple PrefixExtractor that only works for test PrefixAndWholeKeyTest
class TestPrefixExtractor : public ROCKSDB_NAMESPACE::SliceTransform {
 public:
  ~TestPrefixExtractor() override{};
  const char* Name() const override { return "TestPrefixExtractor"; }

  ROCKSDB_NAMESPACE::Slice Transform(
      const ROCKSDB_NAMESPACE::Slice& src) const override {
    assert(IsValid(src));
    return ROCKSDB_NAMESPACE::Slice(src.data(), 3);
  }

  bool InDomain(const ROCKSDB_NAMESPACE::Slice& src) const override {
    return IsValid(src);
  }

  bool InRange(const ROCKSDB_NAMESPACE::Slice& /*dst*/) const override {
    return true;
  }

  bool IsValid(const ROCKSDB_NAMESPACE::Slice& src) const {
    if (src.size() != 4) {
      return false;
    }
    if (src[0] != '[') {
      return false;
    }
    if (src[1] < '0' || src[1] > '9') {
      return false;
    }
    if (src[2] != ']') {
      return false;
    }
    if (src[3] < '0' || src[3] > '9') {
      return false;
    }
    return true;
  }
};
}  // namespace

TEST_F(PrefixTest, PrefixAndWholeKeyTest) {
  ROCKSDB_NAMESPACE::Options options;
  options.compaction_style = ROCKSDB_NAMESPACE::kCompactionStyleUniversal;
  options.num_levels = 20;
  options.create_if_missing = true;
  options.optimize_filters_for_hits = false;
  options.target_file_size_base = 268435456;
  options.prefix_extractor = std::make_shared<TestPrefixExtractor>();
  ROCKSDB_NAMESPACE::BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10));
  bbto.block_size = 262144;
  bbto.whole_key_filtering = true;

  const std::string kDBPath = test::PerThreadDBPath("table_prefix_test");
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  ASSERT_OK(DestroyDB(kDBPath, options));
  ROCKSDB_NAMESPACE::DB* db;
  ASSERT_OK(ROCKSDB_NAMESPACE::DB::Open(options, kDBPath, &db));

  // Create a bunch of keys with 10 filters.
  for (int i = 0; i < 10; i++) {
    std::string prefix = "[" + std::to_string(i) + "]";
    for (int j = 0; j < 10; j++) {
      std::string key = prefix + std::to_string(j);
      ASSERT_OK(db->Put(ROCKSDB_NAMESPACE::WriteOptions(), key, "1"));
    }
  }

  // Trigger compaction.
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  delete db;
  // In the second round, turn whole_key_filtering off and expect
  // rocksdb still works.
}

/*
 * Disable TableWithGlobalSeqno since RocksDB does not store global_seqno in
 * the SST file any more. Instead, RocksDB deduces global_seqno from the
 * MANIFEST while reading from an SST. Therefore, it's not possible to test the
 * functionality of global_seqno in a single, isolated unit test without the
 * involvement of Version, VersionSet, etc.
 */
TEST_P(BlockBasedTableTest, DISABLED_TableWithGlobalSeqno) {
  BlockBasedTableOptions bbto = GetBlockBasedTableOptions();
  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(holder), "" /* don't care */, FileOptions()));
  Options options;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;
  int_tbl_prop_collector_factories.emplace_back(
      new SstFileWriterPropertiesCollectorFactory(2 /* version */,
                                                  0 /* global_seqno*/));
  std::string column_family_name;
  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, ikc,
                          &int_tbl_prop_collector_factories, kNoCompression,
                          CompressionOptions(), kUnknownColumnFamily,
                          column_family_name, -1),
      file_writer.get()));

  for (char c = 'a'; c <= 'z'; ++c) {
    std::string key(8, c);
    std::string value = key;
    InternalKey ik(key, 0, kTypeValue);

    builder->Add(ik.Encode(), value);
  }
  ASSERT_OK(builder->Finish());
  ASSERT_OK(file_writer->Flush());

  test::RandomRWStringSink ss_rw(sink);
  uint32_t version;
  uint64_t global_seqno;
  uint64_t global_seqno_offset;

  // Helper function to get version, global_seqno, global_seqno_offset
  std::function<void()> GetVersionAndGlobalSeqno = [&]() {
    std::unique_ptr<FSRandomAccessFile> source(
        new test::StringSource(ss_rw.contents(), 73342, true));
    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(source), ""));

    std::unique_ptr<TableProperties> props;
    ASSERT_OK(ReadTableProperties(file_reader.get(), ss_rw.contents().size(),
                                  kBlockBasedTableMagicNumber, ioptions,
                                  &props));

    UserCollectedProperties user_props = props->user_collected_properties;
    version = DecodeFixed32(
        user_props[ExternalSstFilePropertyNames::kVersion].c_str());
    global_seqno = DecodeFixed64(
        user_props[ExternalSstFilePropertyNames::kGlobalSeqno].c_str());
    global_seqno_offset = props->external_sst_file_global_seqno_offset;
  };

  // Helper function to update the value of the global seqno in the file
  std::function<void(uint64_t)> SetGlobalSeqno = [&](uint64_t val) {
    std::string new_global_seqno;
    PutFixed64(&new_global_seqno, val);

    ASSERT_OK(ss_rw.Write(global_seqno_offset, new_global_seqno, IOOptions(),
                          nullptr));
  };

  // Helper function to get the contents of the table InternalIterator
  std::unique_ptr<TableReader> table_reader;
  const ReadOptions read_options;
  std::function<InternalIterator*()> GetTableInternalIter = [&]() {
    std::unique_ptr<FSRandomAccessFile> source(
        new test::StringSource(ss_rw.contents(), 73342, true));
    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(source), ""));

    options.table_factory->NewTableReader(
        TableReaderOptions(ioptions, moptions.prefix_extractor, EnvOptions(),
                           ikc),
        std::move(file_reader), ss_rw.contents().size(), &table_reader);

    return table_reader->NewIterator(
        read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized);
  };

  GetVersionAndGlobalSeqno();
  ASSERT_EQ(2u, version);
  ASSERT_EQ(0u, global_seqno);

  InternalIterator* iter = GetTableInternalIter();
  char current_c = 'a';
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey pik;
    ASSERT_OK(ParseInternalKey(iter->key(), &pik, true /* log_err_key */));

    ASSERT_EQ(pik.type, ValueType::kTypeValue);
    ASSERT_EQ(pik.sequence, 0);
    ASSERT_EQ(pik.user_key, iter->value());
    ASSERT_EQ(pik.user_key.ToString(), std::string(8, current_c));
    current_c++;
  }
  ASSERT_EQ(current_c, 'z' + 1);
  delete iter;

  // Update global sequence number to 10
  SetGlobalSeqno(10);
  GetVersionAndGlobalSeqno();
  ASSERT_EQ(2u, version);
  ASSERT_EQ(10u, global_seqno);

  iter = GetTableInternalIter();
  current_c = 'a';
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey pik;
    ASSERT_OK(ParseInternalKey(iter->key(), &pik, true /* log_err_key */));

    ASSERT_EQ(pik.type, ValueType::kTypeValue);
    ASSERT_EQ(pik.sequence, 10);
    ASSERT_EQ(pik.user_key, iter->value());
    ASSERT_EQ(pik.user_key.ToString(), std::string(8, current_c));
    current_c++;
  }
  ASSERT_EQ(current_c, 'z' + 1);

  // Verify Seek
  for (char c = 'a'; c <= 'z'; c++) {
    std::string k = std::string(8, c);
    InternalKey ik(k, 10, kValueTypeForSeek);
    iter->Seek(ik.Encode());
    ASSERT_TRUE(iter->Valid());

    ParsedInternalKey pik;
    ASSERT_OK(ParseInternalKey(iter->key(), &pik, true /* log_err_key */));

    ASSERT_EQ(pik.type, ValueType::kTypeValue);
    ASSERT_EQ(pik.sequence, 10);
    ASSERT_EQ(pik.user_key.ToString(), k);
    ASSERT_EQ(iter->value().ToString(), k);
  }
  delete iter;

  // Update global sequence number to 3
  SetGlobalSeqno(3);
  GetVersionAndGlobalSeqno();
  ASSERT_EQ(2u, version);
  ASSERT_EQ(3u, global_seqno);

  iter = GetTableInternalIter();
  current_c = 'a';
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey pik;
    ASSERT_OK(ParseInternalKey(iter->key(), &pik, true /* log_err_key */));

    ASSERT_EQ(pik.type, ValueType::kTypeValue);
    ASSERT_EQ(pik.sequence, 3);
    ASSERT_EQ(pik.user_key, iter->value());
    ASSERT_EQ(pik.user_key.ToString(), std::string(8, current_c));
    current_c++;
  }
  ASSERT_EQ(current_c, 'z' + 1);

  // Verify Seek
  for (char c = 'a'; c <= 'z'; c++) {
    std::string k = std::string(8, c);
    // seqno=4 is less than 3 so we still should get our key
    InternalKey ik(k, 4, kValueTypeForSeek);
    iter->Seek(ik.Encode());
    ASSERT_TRUE(iter->Valid());

    ParsedInternalKey pik;
    ASSERT_OK(ParseInternalKey(iter->key(), &pik, true /* log_err_key */));

    ASSERT_EQ(pik.type, ValueType::kTypeValue);
    ASSERT_EQ(pik.sequence, 3);
    ASSERT_EQ(pik.user_key.ToString(), k);
    ASSERT_EQ(iter->value().ToString(), k);
  }

  delete iter;
}

TEST_P(BlockBasedTableTest, BlockAlignTest) {
  BlockBasedTableOptions bbto = GetBlockBasedTableOptions();
  bbto.block_align = true;
  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(holder), "" /* don't care */, FileOptions()));
  Options options;
  options.compression = kNoCompression;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;
  std::string column_family_name;
  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, ikc,
                          &int_tbl_prop_collector_factories, kNoCompression,
                          CompressionOptions(), kUnknownColumnFamily,
                          column_family_name, -1),
      file_writer.get()));

  for (int i = 1; i <= 10000; ++i) {
    std::ostringstream ostr;
    ostr << std::setfill('0') << std::setw(5) << i;
    std::string key = ostr.str();
    std::string value = "val";
    InternalKey ik(key, 0, kTypeValue);

    builder->Add(ik.Encode(), value);
  }
  ASSERT_OK(builder->Finish());
  ASSERT_OK(file_writer->Flush());

  std::unique_ptr<FSRandomAccessFile> source(
      new test::StringSource(sink->contents(), 73342, false));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(source), "test"));
  // Helper function to get version, global_seqno, global_seqno_offset
  std::function<void()> VerifyBlockAlignment = [&]() {
    std::unique_ptr<TableProperties> props;
    ASSERT_OK(ReadTableProperties(file_reader.get(), sink->contents().size(),
                                  kBlockBasedTableMagicNumber, ioptions,
                                  &props));

    uint64_t data_block_size = props->data_size / props->num_data_blocks;
    ASSERT_EQ(data_block_size, 4096);
    ASSERT_EQ(props->data_size, data_block_size * props->num_data_blocks);
  };

  VerifyBlockAlignment();

  // The below block of code verifies that we can read back the keys. Set
  // block_align to false when creating the reader to ensure we can flip between
  // the two modes without any issues
  std::unique_ptr<TableReader> table_reader;
  bbto.block_align = false;
  Options options2;
  options2.table_factory.reset(NewBlockBasedTableFactory(bbto));
  ImmutableOptions ioptions2(options2);
  const MutableCFOptions moptions2(options2);

  ASSERT_OK(ioptions.table_factory->NewTableReader(
      TableReaderOptions(ioptions2, moptions2.prefix_extractor, EnvOptions(),
                         GetPlainInternalComparator(options2.comparator)),
      std::move(file_reader), sink->contents().size(), &table_reader));

  ReadOptions read_options;
  std::unique_ptr<InternalIterator> db_iter(table_reader->NewIterator(
      read_options, moptions2.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  int expected_key = 1;
  for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
    std::ostringstream ostr;
    ostr << std::setfill('0') << std::setw(5) << expected_key++;
    std::string key = ostr.str();
    std::string value = "val";

    ASSERT_OK(db_iter->status());
    ASSERT_EQ(ExtractUserKey(db_iter->key()).ToString(), key);
    ASSERT_EQ(db_iter->value().ToString(), value);
  }
  expected_key--;
  ASSERT_EQ(expected_key, 10000);
  table_reader.reset();
}

TEST_P(BlockBasedTableTest, PropertiesBlockRestartPointTest) {
  BlockBasedTableOptions bbto = GetBlockBasedTableOptions();
  bbto.block_align = true;
  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(holder), "" /* don't care */, FileOptions()));

  Options options;
  options.compression = kNoCompression;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;
  std::string column_family_name;

  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, ikc,
                          &int_tbl_prop_collector_factories, kNoCompression,
                          CompressionOptions(), kUnknownColumnFamily,
                          column_family_name, -1),
      file_writer.get()));

  for (int i = 1; i <= 10000; ++i) {
    std::ostringstream ostr;
    ostr << std::setfill('0') << std::setw(5) << i;
    std::string key = ostr.str();
    std::string value = "val";
    InternalKey ik(key, 0, kTypeValue);

    builder->Add(ik.Encode(), value);
  }
  ASSERT_OK(builder->Finish());
  ASSERT_OK(file_writer->Flush());

  std::unique_ptr<FSRandomAccessFile> source(
      new test::StringSource(sink->contents(), 73342, true));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(source), "test"));

  {
    RandomAccessFileReader* file = file_reader.get();
    uint64_t file_size = sink->contents().size();

    Footer footer;
    IOOptions opts;
    ASSERT_OK(ReadFooterFromFile(opts, file, *FileSystem::Default(),
                                 nullptr /* prefetch_buffer */, file_size,
                                 &footer, kBlockBasedTableMagicNumber));

    auto BlockFetchHelper = [&](const BlockHandle& handle, BlockType block_type,
                                BlockContents* contents) {
      ReadOptions read_options;
      read_options.verify_checksums = false;
      PersistentCacheOptions cache_options;

      BlockFetcher block_fetcher(
          file, nullptr /* prefetch_buffer */, footer, read_options, handle,
          contents, ioptions, false /* decompress */,
          false /*maybe_compressed*/, block_type,
          UncompressionDict::GetEmptyDict(), cache_options);

      ASSERT_OK(block_fetcher.ReadBlockContents());
    };

    // -- Read metaindex block
    auto metaindex_handle = footer.metaindex_handle();
    BlockContents metaindex_contents;

    BlockFetchHelper(metaindex_handle, BlockType::kMetaIndex,
                     &metaindex_contents);
    Block metaindex_block(std::move(metaindex_contents));

    std::unique_ptr<InternalIterator> meta_iter(metaindex_block.NewDataIterator(
        BytewiseComparator(), kDisableGlobalSequenceNumber));

    // -- Read properties block
    BlockHandle properties_handle;
    ASSERT_OK(FindOptionalMetaBlock(meta_iter.get(), kPropertiesBlockName,
                                    &properties_handle));
    ASSERT_FALSE(properties_handle.IsNull());
    BlockContents properties_contents;
    BlockFetchHelper(properties_handle, BlockType::kProperties,
                     &properties_contents);
    Block properties_block(std::move(properties_contents));

    ASSERT_EQ(properties_block.NumRestarts(), 1u);
  }
}

TEST_P(BlockBasedTableTest, PropertiesMetaBlockLast) {
  // The properties meta-block should come at the end since we always need to
  // read it when opening a file, unlike index/filter/other meta-blocks, which
  // are sometimes read depending on the user's configuration. This ordering
  // allows us to do a small readahead on the end of the file to read properties
  // and meta-index blocks with one I/O.
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("a1", "val1");
  c.Add("b2", "val2");
  c.Add("c3", "val3");
  c.Add("d4", "val4");
  c.Add("e5", "val5");
  c.Add("f6", "val6");
  c.Add("g7", "val7");
  c.Add("h8", "val8");
  c.Add("j9", "val9");

  // write an SST file
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.filter_policy.reset(NewBloomFilterPolicy(
      8 /* bits_per_key */, false /* use_block_based_filter */));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  // get file reader
  test::StringSink* table_sink = c.TEST_GetSink();
  std::unique_ptr<FSRandomAccessFile> source(new test::StringSource(
      table_sink->contents(), 0 /* unique_id */, false /* allow_mmap_reads */));

  std::unique_ptr<RandomAccessFileReader> table_reader(
      new RandomAccessFileReader(std::move(source), "test"));
  size_t table_size = table_sink->contents().size();

  // read footer
  Footer footer;
  IOOptions opts;
  ASSERT_OK(ReadFooterFromFile(opts, table_reader.get(), *FileSystem::Default(),
                               nullptr /* prefetch_buffer */, table_size,
                               &footer, kBlockBasedTableMagicNumber));

  // read metaindex
  auto metaindex_handle = footer.metaindex_handle();
  BlockContents metaindex_contents;
  PersistentCacheOptions pcache_opts;
  BlockFetcher block_fetcher(
      table_reader.get(), nullptr /* prefetch_buffer */, footer, ReadOptions(),
      metaindex_handle, &metaindex_contents, ioptions, false /* decompress */,
      false /*maybe_compressed*/, BlockType::kMetaIndex,
      UncompressionDict::GetEmptyDict(), pcache_opts,
      nullptr /*memory_allocator*/);
  ASSERT_OK(block_fetcher.ReadBlockContents());
  Block metaindex_block(std::move(metaindex_contents));

  // verify properties block comes last
  std::unique_ptr<InternalIterator> metaindex_iter{
      metaindex_block.NewMetaIterator()};
  uint64_t max_offset = 0;
  std::string key_at_max_offset;
  for (metaindex_iter->SeekToFirst(); metaindex_iter->Valid();
       metaindex_iter->Next()) {
    BlockHandle handle;
    Slice value = metaindex_iter->value();
    ASSERT_OK(handle.DecodeFrom(&value));
    if (handle.offset() > max_offset) {
      max_offset = handle.offset();
      key_at_max_offset = metaindex_iter->key().ToString();
    }
  }
  ASSERT_EQ(kPropertiesBlockName, key_at_max_offset);
  // index handle is stored in footer rather than metaindex block, so need
  // separate logic to verify it comes before properties block.
  ASSERT_GT(max_offset, footer.index_handle().offset());
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, SeekMetaBlocks) {
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("foo_a1", "val1");
  c.Add("foo_b2", "val2");
  c.Add("foo_c3", "val3");
  c.Add("foo_d4", "val4");
  c.Add("foo_e5", "val5");
  c.Add("foo_f6", "val6");
  c.Add("foo_g7", "val7");
  c.Add("foo_h8", "val8");
  c.Add("foo_j9", "val9");

  // write an SST file
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  table_options.filter_policy.reset(NewBloomFilterPolicy(
      8 /* bits_per_key */, false /* use_block_based_filter */));
  options.prefix_extractor.reset(NewFixedPrefixTransform(4));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  // get file reader
  test::StringSink* table_sink = c.TEST_GetSink();
  std::unique_ptr<FSRandomAccessFile> source(new test::StringSource(
      table_sink->contents(), 0 /* unique_id */, false /* allow_mmap_reads */));

  std::unique_ptr<RandomAccessFileReader> table_reader(
      new RandomAccessFileReader(std::move(source), "test"));
  size_t table_size = table_sink->contents().size();

  // read footer
  Footer footer;
  IOOptions opts;
  ASSERT_OK(ReadFooterFromFile(opts, table_reader.get(), *FileSystem::Default(),
                               nullptr /* prefetch_buffer */, table_size,
                               &footer, kBlockBasedTableMagicNumber));

  // read metaindex
  auto metaindex_handle = footer.metaindex_handle();
  BlockContents metaindex_contents;
  PersistentCacheOptions pcache_opts;
  BlockFetcher block_fetcher(
      table_reader.get(), nullptr /* prefetch_buffer */, footer, ReadOptions(),
      metaindex_handle, &metaindex_contents, ioptions, false /* decompress */,
      false /*maybe_compressed*/, BlockType::kMetaIndex,
      UncompressionDict::GetEmptyDict(), pcache_opts,
      nullptr /*memory_allocator*/);
  ASSERT_OK(block_fetcher.ReadBlockContents());
  Block metaindex_block(std::move(metaindex_contents));

  // verify properties block comes last
  std::unique_ptr<MetaBlockIter> metaindex_iter(
      metaindex_block.NewMetaIterator());
  bool has_hash_prefixes = false;
  bool has_hash_metadata = false;
  for (metaindex_iter->SeekToFirst(); metaindex_iter->Valid();
       metaindex_iter->Next()) {
    if (metaindex_iter->key().ToString() == kHashIndexPrefixesBlock) {
      has_hash_prefixes = true;
    } else if (metaindex_iter->key().ToString() ==
               kHashIndexPrefixesMetadataBlock) {
      has_hash_metadata = true;
    }
  }
  if (has_hash_metadata) {
    metaindex_iter->Seek(kHashIndexPrefixesMetadataBlock);
    ASSERT_TRUE(metaindex_iter->Valid());
    ASSERT_EQ(kHashIndexPrefixesMetadataBlock,
              metaindex_iter->key().ToString());
  }
  if (has_hash_prefixes) {
    metaindex_iter->Seek(kHashIndexPrefixesBlock);
    ASSERT_TRUE(metaindex_iter->Valid());
    ASSERT_EQ(kHashIndexPrefixesBlock, metaindex_iter->key().ToString());
  }
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, BadOptions) {
  ROCKSDB_NAMESPACE::Options options;
  options.compression = kNoCompression;
  BlockBasedTableOptions bbto = GetBlockBasedTableOptions();
  bbto.block_size = 4000;
  bbto.block_align = true;

  const std::string kDBPath =
      test::PerThreadDBPath("block_based_table_bad_options_test");
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  ASSERT_OK(DestroyDB(kDBPath, options));
  ROCKSDB_NAMESPACE::DB* db;
  ASSERT_NOK(ROCKSDB_NAMESPACE::DB::Open(options, kDBPath, &db));

  bbto.block_size = 4096;
  options.compression = kSnappyCompression;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  ASSERT_NOK(ROCKSDB_NAMESPACE::DB::Open(options, kDBPath, &db));
}

TEST_F(BBTTailPrefetchTest, TestTailPrefetchStats) {
  TailPrefetchStats tpstats;
  ASSERT_EQ(0, tpstats.GetSuggestedPrefetchSize());
  tpstats.RecordEffectiveSize(size_t{1000});
  tpstats.RecordEffectiveSize(size_t{1005});
  tpstats.RecordEffectiveSize(size_t{1002});
  ASSERT_EQ(1005, tpstats.GetSuggestedPrefetchSize());

  // One single super large value shouldn't influence much
  tpstats.RecordEffectiveSize(size_t{1002000});
  tpstats.RecordEffectiveSize(size_t{999});
  ASSERT_LE(1005, tpstats.GetSuggestedPrefetchSize());
  ASSERT_GT(1200, tpstats.GetSuggestedPrefetchSize());

  // Only history of 32 is kept
  for (int i = 0; i < 32; i++) {
    tpstats.RecordEffectiveSize(size_t{100});
  }
  ASSERT_EQ(100, tpstats.GetSuggestedPrefetchSize());

  // 16 large values and 16 small values. The result should be closer
  // to the small value as the algorithm.
  for (int i = 0; i < 16; i++) {
    tpstats.RecordEffectiveSize(size_t{1000});
  }
  tpstats.RecordEffectiveSize(size_t{10});
  tpstats.RecordEffectiveSize(size_t{20});
  for (int i = 0; i < 6; i++) {
    tpstats.RecordEffectiveSize(size_t{100});
  }
  ASSERT_LE(80, tpstats.GetSuggestedPrefetchSize());
  ASSERT_GT(200, tpstats.GetSuggestedPrefetchSize());
}

TEST_F(BBTTailPrefetchTest, FilePrefetchBufferMinOffset) {
  TailPrefetchStats tpstats;
  FilePrefetchBuffer buffer(0 /* readahead_size */, 0 /* max_readahead_size */,
                            false /* enable */, true /* track_min_offset */);
  IOOptions opts;
  buffer.TryReadFromCache(opts, nullptr /* reader */, 500 /* offset */,
                          10 /* n */, nullptr /* result */,
                          nullptr /* status */,
                          Env::IO_TOTAL /* rate_limiter_priority */);
  buffer.TryReadFromCache(opts, nullptr /* reader */, 480 /* offset */,
                          10 /* n */, nullptr /* result */,
                          nullptr /* status */,
                          Env::IO_TOTAL /* rate_limiter_priority */);
  buffer.TryReadFromCache(opts, nullptr /* reader */, 490 /* offset */,
                          10 /* n */, nullptr /* result */,
                          nullptr /* status */,
                          Env::IO_TOTAL /* rate_limiter_priority */);
  ASSERT_EQ(480, buffer.min_offset_read());
}

TEST_P(BlockBasedTableTest, DataBlockHashIndex) {
  const int kNumKeys = 500;
  const int kKeySize = 8;
  const int kValSize = 40;

  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.data_block_index_type =
      BlockBasedTableOptions::kDataBlockBinaryAndHash;

  Options options;
  options.comparator = BytewiseComparator();

  options.table_factory.reset(new BlockBasedTableFactory(table_options));

  TableConstructor c(options.comparator);

  static Random rnd(1048);
  for (int i = 0; i < kNumKeys; i++) {
    // padding one "0" to mark existent keys.
    std::string random_key(rnd.RandomString(kKeySize - 1) + "1");
    InternalKey k(random_key, 0, kTypeValue);
    c.Add(k.Encode().ToString(), rnd.RandomString(kValSize));
  }

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);
  c.Finish(options, ioptions, moptions, table_options, internal_comparator,
           &keys, &kvmap);

  auto reader = c.GetTableReader();

  std::unique_ptr<InternalIterator> seek_iter;
  ReadOptions read_options;
  seek_iter.reset(reader->NewIterator(
      read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));
  for (int i = 0; i < 2; ++i) {
    ReadOptions ro;
    // for every kv, we seek using two method: Get() and Seek()
    // Get() will use the SuffixIndexHash in Block. For non-existent key it
    //      will invalidate the iterator
    // Seek() will use the default BinarySeek() in Block. So for non-existent
    //      key it will land at the closest key that is large than target.

    // Search for existent keys
    for (auto& kv : kvmap) {
      if (i == 0) {
        // Search using Seek()
        seek_iter->Seek(kv.first);
        ASSERT_OK(seek_iter->status());
        ASSERT_TRUE(seek_iter->Valid());
        ASSERT_EQ(seek_iter->key(), kv.first);
        ASSERT_EQ(seek_iter->value(), kv.second);
      } else {
        // Search using Get()
        PinnableSlice value;
        std::string user_key = ExtractUserKey(kv.first).ToString();
        GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                               GetContext::kNotFound, user_key, &value, nullptr,
                               nullptr, nullptr, true, nullptr, nullptr);
        ASSERT_OK(reader->Get(ro, kv.first, &get_context,
                              moptions.prefix_extractor.get()));
        ASSERT_EQ(get_context.State(), GetContext::kFound);
        ASSERT_EQ(value, Slice(kv.second));
        value.Reset();
      }
    }

    // Search for non-existent keys
    for (auto& kv : kvmap) {
      std::string user_key = ExtractUserKey(kv.first).ToString();
      user_key.back() = '0';  // make it non-existent key
      InternalKey internal_key(user_key, 0, kTypeValue);
      std::string encoded_key = internal_key.Encode().ToString();
      if (i == 0) {  // Search using Seek()
        seek_iter->Seek(encoded_key);
        ASSERT_OK(seek_iter->status());
        if (seek_iter->Valid()) {
          ASSERT_TRUE(BytewiseComparator()->Compare(
                          user_key, ExtractUserKey(seek_iter->key())) < 0);
        }
      } else {  // Search using Get()
        PinnableSlice value;
        GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                               GetContext::kNotFound, user_key, &value, nullptr,
                               nullptr, nullptr, true, nullptr, nullptr);
        ASSERT_OK(reader->Get(ro, encoded_key, &get_context,
                              moptions.prefix_extractor.get()));
        ASSERT_EQ(get_context.State(), GetContext::kNotFound);
        value.Reset();
      }
    }
  }
}

// BlockBasedTableIterator should invalidate itself and return
// OutOfBound()=true immediately after Seek(), to allow LevelIterator
// filter out corresponding level.
TEST_P(BlockBasedTableTest, OutOfBoundOnSeek) {
  TableConstructor c(BytewiseComparator(), true /*convert_to_internal_key*/);
  c.Add("foo", "v1");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  BlockBasedTableOptions table_opt(GetBlockBasedTableOptions());
  options.table_factory.reset(NewBlockBasedTableFactory(table_opt));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_opt,
           GetPlainInternalComparator(BytewiseComparator()), &keys, &kvmap);
  auto* reader = c.GetTableReader();
  ReadOptions read_opt;
  std::string upper_bound = "bar";
  Slice upper_bound_slice(upper_bound);
  read_opt.iterate_upper_bound = &upper_bound_slice;
  std::unique_ptr<InternalIterator> iter;
  iter.reset(new KeyConvertingIterator(reader->NewIterator(
      read_opt, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized)));
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->UpperBoundCheckResult() == IterBoundCheck::kOutOfBound);
  iter.reset(new KeyConvertingIterator(reader->NewIterator(
      read_opt, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized)));
  iter->Seek("foo");
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->UpperBoundCheckResult() == IterBoundCheck::kOutOfBound);
}

// BlockBasedTableIterator should invalidate itself and return
// OutOfBound()=true after Next(), if it finds current index key is no smaller
// than upper bound, unless it is pointing to the last data block.
TEST_P(BlockBasedTableTest, OutOfBoundOnNext) {
  TableConstructor c(BytewiseComparator(), true /*convert_to_internal_key*/);
  c.Add("bar", "v");
  c.Add("foo", "v");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  BlockBasedTableOptions table_opt(GetBlockBasedTableOptions());
  table_opt.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  options.table_factory.reset(NewBlockBasedTableFactory(table_opt));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_opt,
           GetPlainInternalComparator(BytewiseComparator()), &keys, &kvmap);
  auto* reader = c.GetTableReader();
  ReadOptions read_opt;
  std::string ub1 = "bar_after";
  Slice ub_slice1(ub1);
  read_opt.iterate_upper_bound = &ub_slice1;
  std::unique_ptr<InternalIterator> iter;
  iter.reset(new KeyConvertingIterator(reader->NewIterator(
      read_opt, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized)));
  iter->Seek("bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar", iter->key());
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->UpperBoundCheckResult() == IterBoundCheck::kOutOfBound);
  std::string ub2 = "foo_after";
  Slice ub_slice2(ub2);
  read_opt.iterate_upper_bound = &ub_slice2;
  iter.reset(new KeyConvertingIterator(reader->NewIterator(
      read_opt, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized)));
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key());
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_FALSE(iter->UpperBoundCheckResult() == IterBoundCheck::kOutOfBound);
}

class ChargeCompressionDictionaryBuildingBufferTest
    : public BlockBasedTableTestBase {};
TEST_F(ChargeCompressionDictionaryBuildingBufferTest, Basic) {
  constexpr std::size_t kSizeDummyEntry = 256 * 1024;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;
  constexpr std::size_t kCacheCapacity = 8 * 1024 * 1024;
  constexpr std::size_t kMaxDictBytes = 1024;
  constexpr std::size_t kMaxDictBufferBytes = 1024;

  for (CacheEntryRoleOptions::Decision
           charge_compression_dictionary_building_buffer :
       {CacheEntryRoleOptions::Decision::kEnabled,
        CacheEntryRoleOptions::Decision::kDisabled}) {
    BlockBasedTableOptions table_options;
    LRUCacheOptions lo;
    lo.capacity = kCacheCapacity;
    lo.num_shard_bits = 0;  // 2^0 shard
    lo.strict_capacity_limit = true;
    std::shared_ptr<Cache> cache(NewLRUCache(lo));
    table_options.block_cache = cache;
    table_options.flush_block_policy_factory =
        std::make_shared<FlushBlockEveryKeyPolicyFactory>();
    table_options.cache_usage_options.options_overrides.insert(
        {CacheEntryRole::kCompressionDictionaryBuildingBuffer,
         {/*.charged = */ charge_compression_dictionary_building_buffer}});
    Options options;
    options.compression = kSnappyCompression;
    options.compression_opts.max_dict_bytes = kMaxDictBytes;
    options.compression_opts.max_dict_buffer_bytes = kMaxDictBufferBytes;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    test::StringSink* sink = new test::StringSink();
    std::unique_ptr<FSWritableFile> holder(sink);
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(holder), "test_file_name", FileOptions()));

    ImmutableOptions ioptions(options);
    MutableCFOptions moptions(options);
    InternalKeyComparator ikc(options.comparator);
    IntTblPropCollectorFactories int_tbl_prop_collector_factories;

    std::unique_ptr<TableBuilder> builder(
        options.table_factory->NewTableBuilder(
            TableBuilderOptions(
                ioptions, moptions, ikc, &int_tbl_prop_collector_factories,
                kSnappyCompression, options.compression_opts,
                kUnknownColumnFamily, "test_cf", -1 /* level */),
            file_writer.get()));

    std::string key1 = "key1";
    std::string value1 = "val1";
    InternalKey ik1(key1, 0 /* sequnce number */, kTypeValue);
    // Adding the first key won't trigger a flush by FlushBlockEveryKeyPolicy
    // therefore won't trigger any data block's buffering
    builder->Add(ik1.Encode(), value1);
    ASSERT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);

    std::string key2 = "key2";
    std::string value2 = "val2";
    InternalKey ik2(key2, 1 /* sequnce number */, kTypeValue);
    // Adding the second key will trigger a flush of the last data block (the
    // one containing key1 and value1) by FlushBlockEveryKeyPolicy and hence
    // trigger buffering of that data block.
    builder->Add(ik2.Encode(), value2);
    // Cache charging will increase for last buffered data block (the one
    // containing key1 and value1) since the buffer limit is not exceeded after
    // that buffering and the cache will not be full after this reservation
    if (charge_compression_dictionary_building_buffer ==
        CacheEntryRoleOptions::Decision::kEnabled) {
      EXPECT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry);
      EXPECT_LT(cache->GetPinnedUsage(),
                1 * kSizeDummyEntry + kMetaDataChargeOverhead);
    } else {
      EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);
    }

    ASSERT_OK(builder->Finish());
    EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);
  }
}

TEST_F(ChargeCompressionDictionaryBuildingBufferTest,
       BasicWithBufferLimitExceed) {
  constexpr std::size_t kSizeDummyEntry = 256 * 1024;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;
  constexpr std::size_t kCacheCapacity = 8 * 1024 * 1024;
  constexpr std::size_t kMaxDictBytes = 1024;
  constexpr std::size_t kMaxDictBufferBytes = 2 * kSizeDummyEntry;

  // `CacheEntryRoleOptions::charged` is enabled by default for
  // CacheEntryRole::kCompressionDictionaryBuildingBuffer
  BlockBasedTableOptions table_options;
  LRUCacheOptions lo;
  lo.capacity = kCacheCapacity;
  lo.num_shard_bits = 0;  // 2^0 shard
  lo.strict_capacity_limit = true;
  std::shared_ptr<Cache> cache(NewLRUCache(lo));
  table_options.block_cache = cache;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();

  Options options;
  options.compression = kSnappyCompression;
  options.compression_opts.max_dict_bytes = kMaxDictBytes;
  options.compression_opts.max_dict_buffer_bytes = kMaxDictBufferBytes;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(holder), "test_file_name", FileOptions()));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;

  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, ikc,
                          &int_tbl_prop_collector_factories, kSnappyCompression,
                          options.compression_opts, kUnknownColumnFamily,
                          "test_cf", -1 /* level */),
      file_writer.get()));

  std::string key1 = "key1";
  std::string value1(kSizeDummyEntry, '0');
  InternalKey ik1(key1, 0 /* sequnce number */, kTypeValue);
  // Adding the first key won't trigger a flush by FlushBlockEveryKeyPolicy
  // therefore won't trigger any data block's buffering
  builder->Add(ik1.Encode(), value1);
  ASSERT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);

  std::string key2 = "key2";
  std::string value2(kSizeDummyEntry, '0');
  InternalKey ik2(key2, 1 /* sequnce number */, kTypeValue);
  // Adding the second key will trigger a flush of the last data block (the one
  // containing key1 and value1) by FlushBlockEveryKeyPolicy and hence trigger
  // buffering of the last data block.
  builder->Add(ik2.Encode(), value2);
  // Cache charging will increase for last buffered data block (the one
  // containing key1 and value1) since the buffer limit is not exceeded after
  // the buffering and the cache will not be full after this reservation
  EXPECT_GE(cache->GetPinnedUsage(), 2 * kSizeDummyEntry);
  EXPECT_LT(cache->GetPinnedUsage(),
            2 * kSizeDummyEntry + kMetaDataChargeOverhead);

  std::string key3 = "key3";
  std::string value3 = "val3";
  InternalKey ik3(key3, 2 /* sequnce number */, kTypeValue);
  // Adding the third key will trigger a flush of the last data block (the one
  // containing key2 and value2) by FlushBlockEveryKeyPolicy and hence trigger
  // buffering of the last data block.
  builder->Add(ik3.Encode(), value3);
  // Cache charging will decrease since the buffer limit is now exceeded
  // after the last buffering and EnterUnbuffered() is triggered
  EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);

  ASSERT_OK(builder->Finish());
  EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);
}

TEST_F(ChargeCompressionDictionaryBuildingBufferTest, BasicWithCacheFull) {
  constexpr std::size_t kSizeDummyEntry = 256 * 1024;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;
  // A small kCacheCapacity is chosen so that increase cache charging for
  // buffering two data blocks, each containing key1/value1, key2/a big
  // value2, will cause cache full
  constexpr std::size_t kCacheCapacity =
      1 * kSizeDummyEntry + kSizeDummyEntry / 2;
  constexpr std::size_t kMaxDictBytes = 1024;
  // A big kMaxDictBufferBytes is chosen so that adding a big key value pair
  // (key2, value2) won't exceed the buffer limit
  constexpr std::size_t kMaxDictBufferBytes = 1024 * 1024 * 1024;

  // `CacheEntryRoleOptions::charged` is enabled by default for
  // CacheEntryRole::kCompressionDictionaryBuildingBuffer
  BlockBasedTableOptions table_options;
  LRUCacheOptions lo;
  lo.capacity = kCacheCapacity;
  lo.num_shard_bits = 0;  // 2^0 shard
  lo.strict_capacity_limit = true;
  std::shared_ptr<Cache> cache(NewLRUCache(lo));
  table_options.block_cache = cache;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();

  Options options;
  options.compression = kSnappyCompression;
  options.compression_opts.max_dict_bytes = kMaxDictBytes;
  options.compression_opts.max_dict_buffer_bytes = kMaxDictBufferBytes;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(holder), "test_file_name", FileOptions()));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;

  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, ikc,
                          &int_tbl_prop_collector_factories, kSnappyCompression,
                          options.compression_opts, kUnknownColumnFamily,
                          "test_cf", -1 /* level */),
      file_writer.get()));

  std::string key1 = "key1";
  std::string value1 = "val1";
  InternalKey ik1(key1, 0 /* sequnce number */, kTypeValue);
  // Adding the first key won't trigger a flush by FlushBlockEveryKeyPolicy
  // therefore won't trigger any data block's buffering
  builder->Add(ik1.Encode(), value1);
  ASSERT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);

  std::string key2 = "key2";
  std::string value2(kSizeDummyEntry, '0');
  InternalKey ik2(key2, 1 /* sequnce number */, kTypeValue);
  // Adding the second key will trigger a flush of the last data block (the one
  // containing key1 and value1) by FlushBlockEveryKeyPolicy and hence trigger
  // buffering of the last data block.
  builder->Add(ik2.Encode(), value2);
  // Cache charging will increase for the last buffered data block (the one
  // containing key1 and value1) since the buffer limit is not exceeded after
  // the buffering and the cache will not be full after this reservation
  EXPECT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry);
  EXPECT_LT(cache->GetPinnedUsage(),
            1 * kSizeDummyEntry + kMetaDataChargeOverhead);

  std::string key3 = "key3";
  std::string value3 = "value3";
  InternalKey ik3(key3, 2 /* sequnce number */, kTypeValue);
  // Adding the third key will trigger a flush of the last data block (the one
  // containing key2 and value2) by FlushBlockEveryKeyPolicy and hence trigger
  // buffering of the last data block.
  builder->Add(ik3.Encode(), value3);
  // Cache charging will decrease since the cache is now full after
  // increasing reservation for the last buffered block and EnterUnbuffered() is
  // triggered
  EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);

  ASSERT_OK(builder->Finish());
  EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);
}

class CacheUsageOptionsOverridesTest : public DBTestBase {
 public:
  CacheUsageOptionsOverridesTest()
      : DBTestBase("cache_usage_options_overrides_test",
                   /*env_do_fsync=*/false) {}
};

TEST_F(CacheUsageOptionsOverridesTest, SanitizeAndValidateOptions) {
  // To test `cache_usage_options.options_overrides` is sanitized
  // where `cache_usage_options.options` is used when there is no entry in
  // `cache_usage_options.options_overrides`
  Options options;
  options.create_if_missing = true;
  BlockBasedTableOptions table_options = BlockBasedTableOptions();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Destroy(options);
  Status s = TryReopen(options);
  EXPECT_TRUE(s.ok());
  const auto* sanitized_table_options =
      options.table_factory->GetOptions<BlockBasedTableOptions>();
  const auto sanitized_options_overrides =
      sanitized_table_options->cache_usage_options.options_overrides;
  EXPECT_EQ(sanitized_options_overrides.size(), kNumCacheEntryRoles);
  for (auto options_overrides_iter = sanitized_options_overrides.cbegin();
       options_overrides_iter != sanitized_options_overrides.cend();
       ++options_overrides_iter) {
    CacheEntryRoleOptions role_options = options_overrides_iter->second;
    CacheEntryRoleOptions default_options =
        sanitized_table_options->cache_usage_options.options;
    EXPECT_TRUE(role_options == default_options);
  }
  Destroy(options);

  // To test option validation on unsupported CacheEntryRole
  table_options = BlockBasedTableOptions();
  table_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kDataBlock,
       {/*.charged = */ CacheEntryRoleOptions::Decision::kDisabled}});
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Destroy(options);
  s = TryReopen(options);
  EXPECT_TRUE(s.IsNotSupported());
  EXPECT_TRUE(
      s.ToString().find("Enable/Disable CacheEntryRoleOptions::charged") !=
      std::string::npos);
  EXPECT_TRUE(
      s.ToString().find(kCacheEntryRoleToCamelString[static_cast<uint32_t>(
          CacheEntryRole::kDataBlock)]) != std::string::npos);
  Destroy(options);

  // To test option validation on existence of block cache
  table_options = BlockBasedTableOptions();
  table_options.no_block_cache = true;
  table_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kFilterConstruction,
       {/*.charged = */ CacheEntryRoleOptions::Decision::kEnabled}});
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Destroy(options);
  s = TryReopen(options);
  EXPECT_TRUE(s.IsInvalidArgument());
  EXPECT_TRUE(s.ToString().find("Enable CacheEntryRoleOptions::charged") !=
              std::string::npos);
  EXPECT_TRUE(
      s.ToString().find(kCacheEntryRoleToCamelString[static_cast<std::size_t>(
          CacheEntryRole::kFilterConstruction)]) != std::string::npos);
  EXPECT_TRUE(s.ToString().find("block cache is disabled") !=
              std::string::npos);
  Destroy(options);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
