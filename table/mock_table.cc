//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/mock_table.h"

#include "db/dbformat.h"
#include "env/composite_env_wrapper.h"
#include "file/random_access_file_reader.h"
#include "port/port.h"
#include "rocksdb/table_properties.h"
#include "table/get_context.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
namespace mock {

namespace {

const InternalKeyComparator icmp_(BytewiseComparator());

}  // namespace

stl_wrappers::KVMap MakeMockFile(
    std::initializer_list<std::pair<const std::string, std::string>> l) {
  return stl_wrappers::KVMap(l, stl_wrappers::LessOfComparator(&icmp_));
}

class MockTableReader : public TableReader {
 public:
  explicit MockTableReader(const stl_wrappers::KVMap& table) : table_(table) {}

  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* arena, bool skip_filters,
                                TableReaderCaller caller,
                                size_t compaction_readahead_size = 0,
                                bool allow_unprepared_value = false) override;

  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters = false) override;

  uint64_t ApproximateOffsetOf(const Slice& /*key*/,
                               TableReaderCaller /*caller*/) override {
    return 0;
  }

  uint64_t ApproximateSize(const Slice& /*start*/, const Slice& /*end*/,
                           TableReaderCaller /*caller*/) override {
    return 0;
  }

  size_t ApproximateMemoryUsage() const override { return 0; }

  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties> GetTableProperties() const override;

  ~MockTableReader() {}

 private:
  const stl_wrappers::KVMap& table_;
};

class MockTableIterator : public InternalIterator {
 public:
  explicit MockTableIterator(const stl_wrappers::KVMap& table) : table_(table) {
    itr_ = table_.end();
  }

  bool Valid() const override { return itr_ != table_.end(); }

  void SeekToFirst() override { itr_ = table_.begin(); }

  void SeekToLast() override {
    itr_ = table_.end();
    --itr_;
  }

  void Seek(const Slice& target) override {
    std::string str_target(target.data(), target.size());
    itr_ = table_.lower_bound(str_target);
  }

  void SeekForPrev(const Slice& target) override {
    std::string str_target(target.data(), target.size());
    itr_ = table_.upper_bound(str_target);
    Prev();
  }

  void Next() override { ++itr_; }

  void Prev() override {
    if (itr_ == table_.begin()) {
      itr_ = table_.end();
    } else {
      --itr_;
    }
  }

  Slice key() const override { return Slice(itr_->first); }

  Slice value() const override { return Slice(itr_->second); }

  Status status() const override { return Status::OK(); }

 private:
  const stl_wrappers::KVMap& table_;
  stl_wrappers::KVMap::const_iterator itr_;
};

class MockTableBuilder : public TableBuilder {
 public:
  MockTableBuilder(uint32_t id, MockTableFileSystem* file_system,
                   MockTableFactory::MockCorruptionMode corrupt_mode =
                       MockTableFactory::kCorruptNone)
      : id_(id), file_system_(file_system), corrupt_mode_(corrupt_mode) {
    table_ = MakeMockFile({});
  }

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~MockTableBuilder() {}

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override {
    if (corrupt_mode_ == MockTableFactory::kCorruptValue) {
      // Corrupt the value
      table_.insert({key.ToString(), value.ToString() + " "});
      corrupt_mode_ = MockTableFactory::kCorruptNone;
    } else if (corrupt_mode_ == MockTableFactory::kCorruptKey) {
      table_.insert({key.ToString() + " ", value.ToString()});
      corrupt_mode_ = MockTableFactory::kCorruptNone;
    } else {
      table_.insert({key.ToString(), value.ToString()});
    }
  }

  // Return non-ok iff some error has been detected.
  Status status() const override { return Status::OK(); }

  // Return non-ok iff some error happens during IO.
  IOStatus io_status() const override { return IOStatus::OK(); }

  Status Finish() override {
    MutexLock lock_guard(&file_system_->mutex);
    file_system_->files.insert({id_, table_});
    return Status::OK();
  }

  void Abandon() override {}

  uint64_t NumEntries() const override { return table_.size(); }

  uint64_t FileSize() const override { return table_.size(); }

  TableProperties GetTableProperties() const override {
    return TableProperties();
  }

  // Get file checksum
  std::string GetFileChecksum() const override { return kUnknownFileChecksum; }
  // Get file checksum function name
  const char* GetFileChecksumFuncName() const override {
    return kUnknownFileChecksumFuncName;
  }

 private:
  uint32_t id_;
  MockTableFileSystem* file_system_;
  int corrupt_mode_;
  stl_wrappers::KVMap table_;
};

InternalIterator* MockTableReader::NewIterator(
    const ReadOptions&, const SliceTransform* /* prefix_extractor */,
    Arena* /*arena*/, bool /*skip_filters*/, TableReaderCaller /*caller*/,
    size_t /*compaction_readahead_size*/, bool /* allow_unprepared_value */) {
  return new MockTableIterator(table_);
}

Status MockTableReader::Get(const ReadOptions&, const Slice& key,
                            GetContext* get_context,
                            const SliceTransform* /*prefix_extractor*/,
                            bool /*skip_filters*/) {
  std::unique_ptr<MockTableIterator> iter(new MockTableIterator(table_));
  for (iter->Seek(key); iter->Valid(); iter->Next()) {
    ParsedInternalKey parsed_key;
    if (!ParseInternalKey(iter->key(), &parsed_key)) {
      return Status::Corruption(Slice());
    }

    bool dont_care __attribute__((__unused__));
    if (!get_context->SaveValue(parsed_key, iter->value(), &dont_care)) {
      break;
    }
  }
  return Status::OK();
}

std::shared_ptr<const TableProperties> MockTableReader::GetTableProperties()
    const {
  return std::shared_ptr<const TableProperties>(new TableProperties());
}

MockTableFactory::MockTableFactory()
    : next_id_(1), corrupt_mode_(MockTableFactory::kCorruptNone) {}

Status MockTableFactory::NewTableReader(
    const ReadOptions& /*ro*/,
    const TableReaderOptions& /*table_reader_options*/,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t /*file_size*/,
    std::unique_ptr<TableReader>* table_reader,
    bool /*prefetch_index_and_filter_in_cache*/) const {
  uint32_t id = GetIDFromFile(file.get());

  MutexLock lock_guard(&file_system_.mutex);

  auto it = file_system_.files.find(id);
  if (it == file_system_.files.end()) {
    return Status::IOError("Mock file not found");
  }

  table_reader->reset(new MockTableReader(it->second));

  return Status::OK();
}

TableBuilder* MockTableFactory::NewTableBuilder(
    const TableBuilderOptions& /*table_builder_options*/,
    uint32_t /*column_family_id*/, WritableFileWriter* file) const {
  uint32_t id = GetAndWriteNextID(file);

  return new MockTableBuilder(id, &file_system_, corrupt_mode_);
}

Status MockTableFactory::CreateMockTable(Env* env, const std::string& fname,
                                         stl_wrappers::KVMap file_contents) {
  std::unique_ptr<WritableFile> file;
  auto s = env->NewWritableFile(fname, &file, EnvOptions());
  if (!s.ok()) {
    return s;
  }

  WritableFileWriter file_writer(NewLegacyWritableFileWrapper(std::move(file)),
                                 fname, EnvOptions());

  uint32_t id = GetAndWriteNextID(&file_writer);
  file_system_.files.insert({id, std::move(file_contents)});
  return Status::OK();
}

uint32_t MockTableFactory::GetAndWriteNextID(WritableFileWriter* file) const {
  uint32_t next_id = next_id_.fetch_add(1);
  char buf[4];
  EncodeFixed32(buf, next_id);
  file->Append(Slice(buf, 4));
  return next_id;
}

uint32_t MockTableFactory::GetIDFromFile(RandomAccessFileReader* file) const {
  char buf[4];
  Slice result;
  file->Read(IOOptions(), 0, 4, &result, buf, nullptr);
  assert(result.size() == 4);
  return DecodeFixed32(buf);
}

void MockTableFactory::AssertSingleFile(
    const stl_wrappers::KVMap& file_contents) {
  ASSERT_EQ(file_system_.files.size(), 1U);
  ASSERT_EQ(file_contents, file_system_.files.begin()->second);
}

void MockTableFactory::AssertLatestFile(
    const stl_wrappers::KVMap& file_contents) {
  ASSERT_GE(file_system_.files.size(), 1U);
  auto latest = file_system_.files.end();
  --latest;

  if (file_contents != latest->second) {
    std::cout << "Wrong content! Content of latest file:" << std::endl;
    for (const auto& kv : latest->second) {
      ParsedInternalKey ikey;
      std::string key, value;
      std::tie(key, value) = kv;
      ParseInternalKey(Slice(key), &ikey);
      std::cout << ikey.DebugString(false) << " -> " << value << std::endl;
    }
    FAIL();
  }
}

}  // namespace mock
}  // namespace ROCKSDB_NAMESPACE
