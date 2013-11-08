//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <map>
#include <memory>
#include <string>

#include "db/dbformat.h"
#include "db/db_impl.h"
#include "db/table_stats_collector.h"
#include "rocksdb/table_stats.h"
#include "rocksdb/table.h"
#include "util/coding.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class TableStatsTest {
 private:
  unique_ptr<TableReader> table_reader_;
};

// TODO(kailiu) the following classes should be moved to some more general
// places, so that other tests can also make use of them.
// `FakeWritableFile` and `FakeRandomeAccessFile` bypass the real file system
// and therefore enable us to quickly setup the tests.
class FakeWritableFile : public WritableFile {
 public:
  ~FakeWritableFile() { }

  const std::string& contents() const { return contents_; }

  virtual Status Close() { return Status::OK(); }
  virtual Status Flush() { return Status::OK(); }
  virtual Status Sync() { return Status::OK(); }

  virtual Status Append(const Slice& data) {
    contents_.append(data.data(), data.size());
    return Status::OK();
  }

 private:
  std::string contents_;
};


class FakeRandomeAccessFile : public RandomAccessFile {
 public:
  explicit FakeRandomeAccessFile(const Slice& contents)
      : contents_(contents.data(), contents.size()) {
  }

  virtual ~FakeRandomeAccessFile() { }

  uint64_t Size() const { return contents_.size(); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                       char* scratch) const {
    if (offset > contents_.size()) {
      return Status::InvalidArgument("invalid Read offset");
    }
    if (offset + n > contents_.size()) {
      n = contents_.size() - offset;
    }
    memcpy(scratch, &contents_[offset], n);
    *result = Slice(scratch, n);
    return Status::OK();
  }

 private:
  std::string contents_;
};


class DumbLogger : public Logger {
 public:
  virtual void Logv(const char* format, va_list ap) { }
  virtual size_t GetLogFileSize() const { return 0; }
};

// Utilities test functions
void MakeBuilder(
    Options options,
    std::unique_ptr<FakeWritableFile>* writable,
    std::unique_ptr<TableBuilder>* builder) {
  writable->reset(new FakeWritableFile);
  if (!options.flush_block_policy_factory) {
    options.SetUpDefaultFlushBlockPolicyFactory();
  }
  builder->reset(
      options.table_factory->GetTableBuilder(options, writable->get(),
                                             options.compression));
}

void OpenTable(
    const Options& options,
    const std::string& contents,
    std::unique_ptr<TableReader>* table_reader) {
  std::unique_ptr<RandomAccessFile> file(new FakeRandomeAccessFile(contents));
  auto s = options.table_factory->GetTableReader(
      options,
      EnvOptions(),
      std::move(file),
      contents.size(),
      table_reader
  );
  ASSERT_OK(s);
}

// Collects keys that starts with "A" in a table.
class RegularKeysStartWithA: public TableStatsCollector {
 public:
   const char* Name() const { return "RegularKeysStartWithA"; }

   Status Finish(TableStats::UserCollectedStats* stats) {
     std::string encoded;
     PutVarint32(&encoded, count_);
     *stats = TableStats::UserCollectedStats {
       { "TableStatsTest", "Rocksdb" },
       { "Count", encoded }
     };
     return Status::OK();
   }

   Status Add(const Slice& user_key, const Slice& value) {
     // simply asssume all user keys are not empty.
     if (user_key.data()[0] == 'A') {
       ++count_;
     }
     return Status::OK();
   }

 private:
  uint32_t count_ = 0;
};

TEST(TableStatsTest, CustomizedTableStatsCollector) {
  Options options;

  // make sure the entries will be inserted with order.
  std::map<std::string, std::string> kvs = {
    {"About",     "val5"},  // starts with 'A'
    {"Abstract",  "val2"},  // starts with 'A'
    {"Around",    "val7"},  // starts with 'A'
    {"Beyond",    "val3"},
    {"Builder",   "val1"},
    {"Cancel",    "val4"},
    {"Find",      "val6"},
  };

  // Test stats collectors with internal keys or regular keys
  for (bool encode_as_internal : { true, false }) {
    // -- Step 1: build table
    auto collector = new RegularKeysStartWithA();
    if (encode_as_internal) {
      options.table_stats_collectors = {
        std::make_shared<UserKeyTableStatsCollector>(collector)
      };
    } else {
      options.table_stats_collectors.resize(1);
      options.table_stats_collectors[0].reset(collector);
    }
    std::unique_ptr<TableBuilder> builder;
    std::unique_ptr<FakeWritableFile> writable;
    MakeBuilder(options, &writable, &builder);

    for (const auto& kv : kvs) {
      if (encode_as_internal) {
        InternalKey ikey(kv.first, 0, ValueType::kTypeValue);
        builder->Add(ikey.Encode(), kv.second);
      } else {
        builder->Add(kv.first, kv.second);
      }
    }
    ASSERT_OK(builder->Finish());

    // -- Step 2: Open table
    std::unique_ptr<TableReader> table_reader;
    OpenTable(options, writable->contents(), &table_reader);
    const auto& stats = table_reader->GetTableStats().user_collected_stats;

    ASSERT_EQ("Rocksdb", stats.at("TableStatsTest"));

    uint32_t starts_with_A = 0;
    Slice key(stats.at("Count"));
    ASSERT_TRUE(GetVarint32(&key, &starts_with_A));
    ASSERT_EQ(3u, starts_with_A);
  }
}

TEST(TableStatsTest, InternalKeyStatsCollector) {
  InternalKey keys[] = {
    InternalKey("A", 0, ValueType::kTypeValue),
    InternalKey("B", 0, ValueType::kTypeValue),
    InternalKey("C", 0, ValueType::kTypeValue),
    InternalKey("W", 0, ValueType::kTypeDeletion),
    InternalKey("X", 0, ValueType::kTypeDeletion),
    InternalKey("Y", 0, ValueType::kTypeDeletion),
    InternalKey("Z", 0, ValueType::kTypeDeletion),
  };

  for (bool sanitized : { false, true }) {
    std::unique_ptr<TableBuilder> builder;
    std::unique_ptr<FakeWritableFile> writable;
    Options options;
    if (sanitized) {
      options.table_stats_collectors = {
        std::make_shared<RegularKeysStartWithA>()
      };
      // with sanitization, even regular stats collector will be able to
      // handle internal keys.
      auto comparator = options.comparator;
      // HACK: Set options.info_log to avoid writing log in
      // SanitizeOptions().
      options.info_log = std::make_shared<DumbLogger>();
      options = SanitizeOptions(
          "db",  // just a place holder
          nullptr,  // with skip internal key comparator
          nullptr,  // don't care filter policy
          options
      );
      options.comparator = comparator;
    } else {
      options.table_stats_collectors = {
        std::make_shared<InternalKeyStatsCollector>()
      };
    }

    MakeBuilder(options, &writable, &builder);
    for (const auto& k : keys) {
      builder->Add(k.Encode(), "val");
    }

    ASSERT_OK(builder->Finish());

    std::unique_ptr<TableReader> table_reader;
    OpenTable(options, writable->contents(), &table_reader);
    const auto& stats = table_reader->GetTableStats().user_collected_stats;

    uint64_t deleted = 0;
    Slice key(stats.at(InternalKeyTableStatsNames::kDeletedKeys));
    ASSERT_TRUE(GetVarint64(&key, &deleted));
    ASSERT_EQ(4u, deleted);

    if (sanitized) {
      uint32_t starts_with_A = 0;
      Slice key(stats.at("Count"));
      ASSERT_TRUE(GetVarint32(&key, &starts_with_A));
      ASSERT_EQ(1u, starts_with_A);
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}
