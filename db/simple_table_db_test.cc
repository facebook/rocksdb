// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <algorithm>
#include <set>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "db/db_statistics.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "rocksdb/table.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

using std::unique_ptr;

namespace rocksdb {

// SimpleTable is a simple table format for UNIT TEST ONLY. It is not built
// as production quality.
// SimpleTable requires the input key size to be fixed 16 bytes, value cannot
// be longer than 150000 bytes and stored data on disk in this format:
// +--------------------------------------------+  <= key1 offset
// | key1            | value_size (4 bytes) |   |
// +----------------------------------------+   |
// | value1                                     |
// |                                            |
// +----------------------------------------+---+  <= key2 offset
// | key2            | value_size (4 bytes) |   |
// +----------------------------------------+   |
// | value2                                     |
// |                                            |
// |        ......                              |
// +-----------------+--------------------------+   <= index_block_offset
// | key1            | key1 offset (8 bytes)    |
// +-----------------+--------------------------+
// | key2            | key2 offset (8 bytes)    |
// +-----------------+--------------------------+
// | key3            | key3 offset (8 bytes)    |
// +-----------------+--------------------------+
// |        ......                              |
// +-----------------+------------+-------------+
// | index_block_offset (8 bytes) |
// +------------------------------+

// SimpleTable is a simple table format for UNIT TEST ONLY. It is not built
// as production quality.
class SimpleTableReader: public TableReader {
public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to nullptr and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options& options, const EnvOptions& soptions,
                     unique_ptr<RandomAccessFile> && file, uint64_t file_size,
                     unique_ptr<TableReader>* table_reader);

  bool PrefixMayMatch(const Slice& internal_prefix) override;

  Iterator* NewIterator(const ReadOptions&) override;

  Status Get(
      const ReadOptions&, const Slice& key, void* arg,
      bool (*handle_result)(void* arg, const Slice& k, const Slice& v, bool),
      void (*mark_key_may_exist)(void*) = nullptr) override;

  uint64_t ApproximateOffsetOf(const Slice& key) override;

  bool TEST_KeyInCache(const ReadOptions& options, const Slice& key) override;

  void SetupForCompaction() override;

  TableProperties& GetTableProperties() override;

  ~SimpleTableReader();

private:
  struct Rep;
  Rep* rep_;

  explicit SimpleTableReader(Rep* rep) {
    rep_ = rep;
  }
  friend class TableCache;
  friend class SimpleTableIterator;

  Status GetOffset(const Slice& target, uint64_t* offset);

  // No copying allowed
  explicit SimpleTableReader(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;
};

// Iterator to iterate SimpleTable
class SimpleTableIterator: public Iterator {
public:
  explicit SimpleTableIterator(SimpleTableReader* table);
  ~SimpleTableIterator();

  bool Valid() const;

  void SeekToFirst();

  void SeekToLast();

  void Seek(const Slice& target);

  void Next();

  void Prev();

  Slice key() const;

  Slice value() const;

  Status status() const;

private:
  SimpleTableReader* table_;
  uint64_t offset_;
  uint64_t next_offset_;
  Slice key_;
  Slice value_;
  char tmp_str_[4];
  char* key_str_;
  char* value_str_;
  int value_str_len_;
  Status status_;
  // No copying allowed
  SimpleTableIterator(const SimpleTableIterator&) = delete;
  void operator=(const Iterator&) = delete;
};

struct SimpleTableReader::Rep {
  ~Rep() {
  }
  Rep(const EnvOptions& storage_options, uint64_t index_start_offset,
      int num_entries) :
      soptions(storage_options), index_start_offset(index_start_offset),
      num_entries(num_entries) {
  }

  Options options;
  const EnvOptions& soptions;
  Status status;
  unique_ptr<RandomAccessFile> file;
  uint64_t index_start_offset;
  int num_entries;
  TableProperties table_properties;

  const static int user_key_size = 16;
  const static int offset_length = 8;
  const static int key_footer_len = 8;

  static int GetInternalKeyLength() {
    return user_key_size + key_footer_len;
  }
};

SimpleTableReader::~SimpleTableReader() {
  delete rep_;
}

Status SimpleTableReader::Open(const Options& options,
                               const EnvOptions& soptions,
                               unique_ptr<RandomAccessFile> && file,
                               uint64_t size,
                               unique_ptr<TableReader>* table_reader) {
  char footer_space[Rep::offset_length];
  Slice footer_input;
  Status s = file->Read(size - Rep::offset_length, Rep::offset_length,
                        &footer_input, footer_space);
  if (s.ok()) {
    uint64_t index_start_offset = DecodeFixed64(footer_space);

    int num_entries = (size - Rep::offset_length - index_start_offset)
        / (Rep::GetInternalKeyLength() + Rep::offset_length);
    SimpleTableReader::Rep* rep = new SimpleTableReader::Rep(soptions,
                                                             index_start_offset,
                                                             num_entries);

    rep->file = std::move(file);
    rep->options = options;
    table_reader->reset(new SimpleTableReader(rep));
  }
  return s;
}

void SimpleTableReader::SetupForCompaction() {
}

TableProperties& SimpleTableReader::GetTableProperties() {
  return rep_->table_properties;
}

bool SimpleTableReader::PrefixMayMatch(const Slice& internal_prefix) {
  return true;
}

Iterator* SimpleTableReader::NewIterator(const ReadOptions& options) {
  return new SimpleTableIterator(this);
}

Status SimpleTableReader::GetOffset(const Slice& target, uint64_t* offset) {
  uint32_t left = 0;
  uint32_t right = rep_->num_entries - 1;
  char key_chars[Rep::GetInternalKeyLength()];
  Slice tmp_slice;

  uint32_t target_offset = 0;
  while (left <= right) {
    uint32_t mid = (left + right + 1) / 2;

    uint64_t offset_to_read = rep_->index_start_offset
        + (Rep::GetInternalKeyLength() + Rep::offset_length) * mid;
    Status s = rep_->file->Read(offset_to_read, Rep::GetInternalKeyLength(),
                                &tmp_slice, key_chars);
    if (!s.ok()) {
      return s;
    }

    int compare_result = rep_->options.comparator->Compare(tmp_slice, target);

    if (compare_result < 0) {
      if (left == right) {
        target_offset = right + 1;
        break;
      }
      left = mid;
    } else {
      if (left == right) {
        target_offset = left;
        break;
      }
      right = mid - 1;
    }
  }

  if (target_offset >= (uint32_t) rep_->num_entries) {
    *offset = rep_->index_start_offset;
    return Status::OK();
  }

  char value_offset_chars[Rep::offset_length];

  int64_t offset_for_value_offset = rep_->index_start_offset
      + (Rep::GetInternalKeyLength() + Rep::offset_length) * target_offset
      + Rep::GetInternalKeyLength();
  Status s = rep_->file->Read(offset_for_value_offset, Rep::offset_length,
                              &tmp_slice, value_offset_chars);
  if (s.ok()) {
    *offset = DecodeFixed64(value_offset_chars);
  }
  return s;
}

Status SimpleTableReader::Get(
    const ReadOptions& options, const Slice& k, void* arg,
    bool (*saver)(void*, const Slice&, const Slice&, bool),
    void (*mark_key_may_exist)(void*)) {
  Status s;
  SimpleTableIterator* iter = new SimpleTableIterator(this);
  for (iter->Seek(k); iter->Valid(); iter->Next()) {
    if (!(*saver)(arg, iter->key(), iter->value(), true)) {
      break;
    }
  }
  s = iter->status();
  delete iter;
  return s;
}

bool SimpleTableReader::TEST_KeyInCache(const ReadOptions& options,
                                        const Slice& key) {
  return false;
}

uint64_t SimpleTableReader::ApproximateOffsetOf(const Slice& key) {
  return 0;
}

SimpleTableIterator::SimpleTableIterator(SimpleTableReader* table) :
    table_(table) {
  key_str_ = new char[SimpleTableReader::Rep::GetInternalKeyLength()];
  value_str_len_ = -1;
  SeekToFirst();
}

SimpleTableIterator::~SimpleTableIterator() {
 delete[] key_str_;
 if (value_str_len_ >= 0) {
   delete[] value_str_;
 }
}

bool SimpleTableIterator::Valid() const {
  return offset_ < table_->rep_->index_start_offset;
}

void SimpleTableIterator::SeekToFirst() {
  next_offset_ = 0;
  Next();
}

void SimpleTableIterator::SeekToLast() {
  assert(false);
}

void SimpleTableIterator::Seek(const Slice& target) {
  Status s = table_->GetOffset(target, &next_offset_);
  if (!s.ok()) {
    status_ = s;
  }
  Next();
}

void SimpleTableIterator::Next() {
  offset_ = next_offset_;
  if (offset_ >= table_->rep_->index_start_offset) {
    return;
  }
  Slice result;
  int internal_key_size = SimpleTableReader::Rep::GetInternalKeyLength();

  Status s = table_->rep_->file->Read(next_offset_, internal_key_size, &result,
                                      key_str_);
  next_offset_ += internal_key_size;
  key_ = result;

  Slice value_size_slice;
  s = table_->rep_->file->Read(next_offset_, 4, &value_size_slice, tmp_str_);
  next_offset_ += 4;
  uint32_t value_size = DecodeFixed32(tmp_str_);

  Slice value_slice;
  if ((int) value_size > value_str_len_) {
    if (value_str_len_ >= 0) {
      delete[] value_str_;
    }
    value_str_ = new char[value_size];
    value_str_len_ = value_size;
  }
  s = table_->rep_->file->Read(next_offset_, value_size, &value_slice,
                               value_str_);
  next_offset_ += value_size;
  value_ = value_slice;
}

void SimpleTableIterator::Prev() {
  assert(false);
}

Slice SimpleTableIterator::key() const {
  Log(table_->rep_->options.info_log, "key!!!!");
  return key_;
}

Slice SimpleTableIterator::value() const {
  return value_;
}

Status SimpleTableIterator::status() const {
  return status_;
}

class SimpleTableBuilder: public TableBuilder {
public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish(). The output file
  // will be part of level specified by 'level'.  A value of -1 means
  // that the caller does not know which level the output file will reside.
  SimpleTableBuilder(const Options& options, WritableFile* file,
                     CompressionType compression_type);

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~SimpleTableBuilder();

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  // Return non-ok iff some error has been detected.
  Status status() const override;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

private:
  struct Rep;
  Rep* rep_;

  // No copying allowed
  SimpleTableBuilder(const SimpleTableBuilder&) = delete;
  void operator=(const SimpleTableBuilder&) = delete;
};

struct SimpleTableBuilder::Rep {
  Options options;
  WritableFile* file;
  uint64_t offset = 0;
  Status status;

  uint64_t num_entries = 0;

  bool closed = false;  // Either Finish() or Abandon() has been called.

  const static int user_key_size = 16;
  const static int offset_length = 8;
  const static int key_footer_len = 8;

  static int GetInternalKeyLength() {
    return user_key_size + key_footer_len;
  }

  std::string index;

  Rep(const Options& opt, WritableFile* f) :
      options(opt), file(f) {
  }
  ~Rep() {
  }
};

SimpleTableBuilder::SimpleTableBuilder(const Options& options,
                                       WritableFile* file,
                                       CompressionType compression_type) :
    rep_(new SimpleTableBuilder::Rep(options, file)) {
}

SimpleTableBuilder::~SimpleTableBuilder() {
  delete (rep_);
}

void SimpleTableBuilder::Add(const Slice& key, const Slice& value) {
  assert((int ) key.size() == Rep::GetInternalKeyLength());

  // Update index
  rep_->index.append(key.data(), key.size());
  PutFixed64(&(rep_->index), rep_->offset);

  // Write key-value pair
  rep_->file->Append(key);
  rep_->offset += Rep::GetInternalKeyLength();

  std::string size;
  int value_size = value.size();
  PutFixed32(&size, value_size);
  Slice sizeSlice(size);
  rep_->file->Append(sizeSlice);
  rep_->file->Append(value);
  rep_->offset += value_size + 4;

  rep_->num_entries++;
}

Status SimpleTableBuilder::status() const {
  return Status::OK();
}

Status SimpleTableBuilder::Finish() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;

  uint64_t index_offset = rep_->offset;
  Slice index_slice(rep_->index);
  rep_->file->Append(index_slice);
  rep_->offset += index_slice.size();

  std::string index_offset_str;
  PutFixed64(&index_offset_str, index_offset);
  Slice foot_slice(index_offset_str);
  rep_->file->Append(foot_slice);
  rep_->offset += foot_slice.size();

  return Status::OK();
}

void SimpleTableBuilder::Abandon() {
  rep_->closed = true;
}

uint64_t SimpleTableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t SimpleTableBuilder::FileSize() const {
  return rep_->offset;
}

class SimpleTableFactory: public TableFactory {
public:
  ~SimpleTableFactory() {
  }
  SimpleTableFactory() {
  }
  const char* Name() const override {
    return "SimpleTable";
  }
  Status GetTableReader(const Options& options, const EnvOptions& soptions,
                        unique_ptr<RandomAccessFile> && file,
                        uint64_t file_size,
                        unique_ptr<TableReader>* table_reader) const;

  TableBuilder* GetTableBuilder(const Options& options, WritableFile* file,
                                CompressionType compression_type) const;
};

Status SimpleTableFactory::GetTableReader(
    const Options& options, const EnvOptions& soptions,
    unique_ptr<RandomAccessFile> && file, uint64_t file_size,
    unique_ptr<TableReader>* table_reader) const {

  return SimpleTableReader::Open(options, soptions, std::move(file), file_size,
                                 table_reader);
}

TableBuilder* SimpleTableFactory::GetTableBuilder(
    const Options& options, WritableFile* file,
    CompressionType compression_type) const {
  return new SimpleTableBuilder(options, file, compression_type);
}

class SimpleTableDBTest {
protected:
public:
  std::string dbname_;
  Env* env_;
  DB* db_;

  Options last_options_;

  SimpleTableDBTest() :
      env_(Env::Default()) {
    dbname_ = test::TmpDir() + "/simple_table_db_test";
    ASSERT_OK(DestroyDB(dbname_, Options()));
    db_ = nullptr;
    Reopen();
  }

  ~SimpleTableDBTest() {
    delete db_;
    ASSERT_OK(DestroyDB(dbname_, Options()));
  }

  // Return the current option configuration.
  Options CurrentOptions() {
    Options options;
    options.table_factory.reset(new SimpleTableFactory());
    return options;
  }

  DBImpl* dbfull() {
    return reinterpret_cast<DBImpl*>(db_);
  }

  void Reopen(Options* options = nullptr) {
    ASSERT_OK(TryReopen(options));
  }

  void Close() {
    delete db_;
    db_ = nullptr;
  }

  void DestroyAndReopen(Options* options = nullptr) {
    //Destroy using last options
    Destroy(&last_options_);
    ASSERT_OK(TryReopen(options));
  }

  void Destroy(Options* options) {
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, *options));
  }

  Status PureReopen(Options* options, DB** db) {
    return DB::Open(*options, dbname_, db);
  }

  Status TryReopen(Options* options = nullptr) {
    delete db_;
    db_ = nullptr;
    Options opts;
    if (options != nullptr) {
      opts = *options;
    } else {
      opts = CurrentOptions();
      opts.create_if_missing = true;
    }
    last_options_ = opts;

    return DB::Open(opts, dbname_, &db_);
  }

  Status Put(const Slice& k, const Slice& v) {
    return db_->Put(WriteOptions(), k, v);
  }

  Status Delete(const std::string& k) {
    return db_->Delete(WriteOptions(), k);
  }

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }


  int NumTableFilesAtLevel(int level) {
    std::string property;
    ASSERT_TRUE(
        db_->GetProperty("rocksdb.num-files-at-level" + NumberToString(level),
                         &property));
    return atoi(property.c_str());
  }

  // Return spread of files per level
  std::string FilesPerLevel() {
    std::string result;
    int last_non_zero_offset = 0;
    for (int level = 0; level < db_->NumberLevels(); level++) {
      int f = NumTableFilesAtLevel(level);
      char buf[100];
      snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
      result += buf;
      if (f > 0) {
        last_non_zero_offset = result.size();
      }
    }
    result.resize(last_non_zero_offset);
    return result;
  }

  std::string IterStatus(Iterator* iter) {
    std::string result;
    if (iter->Valid()) {
      result = iter->key().ToString() + "->" + iter->value().ToString();
    } else {
      result = "(invalid)";
    }
    return result;
  }
};

TEST(SimpleTableDBTest, Empty) {
  ASSERT_TRUE(db_ != nullptr);
  ASSERT_EQ("NOT_FOUND", Get("0000000000000foo"));
}

TEST(SimpleTableDBTest, ReadWrite) {
  ASSERT_OK(Put("0000000000000foo", "v1"));
  ASSERT_EQ("v1", Get("0000000000000foo"));
  ASSERT_OK(Put("0000000000000bar", "v2"));
  ASSERT_OK(Put("0000000000000foo", "v3"));
  ASSERT_EQ("v3", Get("0000000000000foo"));
  ASSERT_EQ("v2", Get("0000000000000bar"));
}

TEST(SimpleTableDBTest, Flush) {
  ASSERT_OK(Put("0000000000000foo", "v1"));
  ASSERT_OK(Put("0000000000000bar", "v2"));
  ASSERT_OK(Put("0000000000000foo", "v3"));
  dbfull()->TEST_FlushMemTable();
  ASSERT_EQ("v3", Get("0000000000000foo"));
  ASSERT_EQ("v2", Get("0000000000000bar"));
}

TEST(SimpleTableDBTest, Flush2) {
  ASSERT_OK(Put("0000000000000bar", "b"));
  ASSERT_OK(Put("0000000000000foo", "v1"));
  dbfull()->TEST_FlushMemTable();

  ASSERT_OK(Put("0000000000000foo", "v2"));
  dbfull()->TEST_FlushMemTable();
  ASSERT_EQ("v2", Get("0000000000000foo"));

  ASSERT_OK(Put("0000000000000eee", "v3"));
  dbfull()->TEST_FlushMemTable();
  ASSERT_EQ("v3", Get("0000000000000eee"));

  ASSERT_OK(Delete("0000000000000bar"));
  dbfull()->TEST_FlushMemTable();
  ASSERT_EQ("NOT_FOUND", Get("0000000000000bar"));

  ASSERT_OK(Put("0000000000000eee", "v5"));
  dbfull()->TEST_FlushMemTable();
  ASSERT_EQ("v5", Get("0000000000000eee"));
}

static std::string Key(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "key_______%06d", i);
  return std::string(buf);
}

static std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}

TEST(SimpleTableDBTest, CompactionTrigger) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 << 10; //100KB
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  options.level0_file_num_compaction_trigger = 3;
  Reopen(&options);

  Random rnd(301);

  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
      num++) {
    std::vector<std::string> values;
    // Write 120KB (12 values, each 10K)
    for (int i = 0; i < 12; i++) {
      values.push_back(RandomString(&rnd, 10000));
      ASSERT_OK(Put(Key(i), values[i]));
    }
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(NumTableFilesAtLevel(0), num + 1);
  }

  //generate one more file in level-0, and should trigger level-0 compaction
  std::vector<std::string> values;
  for (int i = 0; i < 12; i++) {
    values.push_back(RandomString(&rnd, 10000));
    ASSERT_OK(Put(Key(i), values[i]));
  }
  dbfull()->TEST_WaitForCompact();

  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}
