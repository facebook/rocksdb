//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <map>
#include <memory>
#include <string>

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/table_properties_collector.h"
#include "rocksdb/table.h"
#include "table/block_based_table_factory.h"
#include "table/meta_blocks.h"
#include "table/plain_table_factory.h"
#include "table/table_builder.h"
#include "util/coding.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class TablePropertiesTest {
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
namespace {
void MakeBuilder(const Options& options,
                 const InternalKeyComparator& internal_comparator,
                 std::unique_ptr<FakeWritableFile>* writable,
                 std::unique_ptr<TableBuilder>* builder) {
  writable->reset(new FakeWritableFile);
  builder->reset(options.table_factory->NewTableBuilder(
      options, internal_comparator, writable->get(), options.compression));
}
}  // namespace

// Collects keys that starts with "A" in a table.
class RegularKeysStartWithA: public TablePropertiesCollector {
 public:
   const char* Name() const { return "RegularKeysStartWithA"; }

   Status Finish(UserCollectedProperties* properties) {
     std::string encoded;
     PutVarint32(&encoded, count_);
     *properties = UserCollectedProperties {
       { "TablePropertiesTest", "Rocksdb" },
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

  virtual UserCollectedProperties GetReadableProperties() const {
    return UserCollectedProperties{};
  }

 private:
  uint32_t count_ = 0;
};

class RegularKeysStartWithAFactory : public TablePropertiesCollectorFactory {
 public:
  virtual TablePropertiesCollector* CreateTablePropertiesCollector() {
    return new RegularKeysStartWithA();
  }
  const char* Name() const { return "RegularKeysStartWithA"; }
};

extern uint64_t kBlockBasedTableMagicNumber;
extern uint64_t kPlainTableMagicNumber;
namespace {
void TestCustomizedTablePropertiesCollector(
    uint64_t magic_number, bool encode_as_internal, const Options& options,
    const InternalKeyComparator& internal_comparator) {
  // make sure the entries will be inserted with order.
  std::map<std::string, std::string> kvs = {
    {"About   ", "val5"},  // starts with 'A'
    {"Abstract", "val2"},  // starts with 'A'
    {"Around  ", "val7"},  // starts with 'A'
    {"Beyond  ", "val3"},
    {"Builder ", "val1"},
    {"Cancel  ", "val4"},
    {"Find    ", "val6"},
  };

  // -- Step 1: build table
  std::unique_ptr<TableBuilder> builder;
  std::unique_ptr<FakeWritableFile> writable;
  MakeBuilder(options, internal_comparator, &writable, &builder);

  for (const auto& kv : kvs) {
    if (encode_as_internal) {
      InternalKey ikey(kv.first, 0, ValueType::kTypeValue);
      builder->Add(ikey.Encode(), kv.second);
    } else {
      builder->Add(kv.first, kv.second);
    }
  }
  ASSERT_OK(builder->Finish());

  // -- Step 2: Read properties
  FakeRandomeAccessFile readable(writable->contents());
  TableProperties* props;
  Status s = ReadTableProperties(
      &readable,
      writable->contents().size(),
      magic_number,
      Env::Default(),
      nullptr,
      &props
  );
  std::unique_ptr<TableProperties> props_guard(props);
  ASSERT_OK(s);

  auto user_collected = props->user_collected_properties;

  ASSERT_EQ("Rocksdb", user_collected.at("TablePropertiesTest"));

  uint32_t starts_with_A = 0;
  Slice key(user_collected.at("Count"));
  ASSERT_TRUE(GetVarint32(&key, &starts_with_A));
  ASSERT_EQ(3u, starts_with_A);
}
}  // namespace

TEST(TablePropertiesTest, CustomizedTablePropertiesCollector) {
  // Test properties collectors with internal keys or regular keys
  // for block based table
  for (bool encode_as_internal : { true, false }) {
    Options options;
    std::shared_ptr<TablePropertiesCollectorFactory> collector_factory(
        new RegularKeysStartWithAFactory());
    if (encode_as_internal) {
      options.table_properties_collector_factories.emplace_back(
          new UserKeyTablePropertiesCollectorFactory(collector_factory));
    } else {
      options.table_properties_collector_factories.resize(1);
      options.table_properties_collector_factories[0] = collector_factory;
    }
    test::PlainInternalKeyComparator ikc(options.comparator);
    TestCustomizedTablePropertiesCollector(kBlockBasedTableMagicNumber,
                                           encode_as_internal, options, ikc);
  }

  // test plain table
  Options options;
  options.table_properties_collector_factories.emplace_back(
      new RegularKeysStartWithAFactory());

  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = 8;
  plain_table_options.bloom_bits_per_key = 8;
  plain_table_options.hash_table_ratio = 0;

  options.table_factory =
      std::make_shared<PlainTableFactory>(plain_table_options);
  test::PlainInternalKeyComparator ikc(options.comparator);
  TestCustomizedTablePropertiesCollector(kPlainTableMagicNumber, true, options,
                                         ikc);
}

namespace {
void TestInternalKeyPropertiesCollector(
    uint64_t magic_number,
    bool sanitized,
    std::shared_ptr<TableFactory> table_factory) {
  InternalKey keys[] = {
    InternalKey("A       ", 0, ValueType::kTypeValue),
    InternalKey("B       ", 0, ValueType::kTypeValue),
    InternalKey("C       ", 0, ValueType::kTypeValue),
    InternalKey("W       ", 0, ValueType::kTypeDeletion),
    InternalKey("X       ", 0, ValueType::kTypeDeletion),
    InternalKey("Y       ", 0, ValueType::kTypeDeletion),
    InternalKey("Z       ", 0, ValueType::kTypeDeletion),
  };

  std::unique_ptr<TableBuilder> builder;
  std::unique_ptr<FakeWritableFile> writable;
  Options options;
  test::PlainInternalKeyComparator pikc(options.comparator);

  options.table_factory = table_factory;
  if (sanitized) {
    options.table_properties_collector_factories.emplace_back(
        new RegularKeysStartWithAFactory());
    // with sanitization, even regular properties collector will be able to
    // handle internal keys.
    auto comparator = options.comparator;
    // HACK: Set options.info_log to avoid writing log in
    // SanitizeOptions().
    options.info_log = std::make_shared<DumbLogger>();
    options = SanitizeOptions("db",            // just a place holder
                              &pikc,
                              options);
    options.comparator = comparator;
  } else {
    options.table_properties_collector_factories = {
        std::make_shared<InternalKeyPropertiesCollectorFactory>()};
  }

  for (int iter = 0; iter < 2; ++iter) {
    MakeBuilder(options, pikc, &writable, &builder);
    for (const auto& k : keys) {
      builder->Add(k.Encode(), "val");
    }

    ASSERT_OK(builder->Finish());

    FakeRandomeAccessFile readable(writable->contents());
    TableProperties* props;
    Status s =
        ReadTableProperties(&readable, writable->contents().size(),
                            magic_number, Env::Default(), nullptr, &props);
    ASSERT_OK(s);

    std::unique_ptr<TableProperties> props_guard(props);
    auto user_collected = props->user_collected_properties;
    uint64_t deleted = GetDeletedKeys(user_collected);
    ASSERT_EQ(4u, deleted);

    if (sanitized) {
      uint32_t starts_with_A = 0;
      Slice key(user_collected.at("Count"));
      ASSERT_TRUE(GetVarint32(&key, &starts_with_A));
      ASSERT_EQ(1u, starts_with_A);
    }
  }
}
}  // namespace

TEST(TablePropertiesTest, InternalKeyPropertiesCollector) {
  TestInternalKeyPropertiesCollector(
      kBlockBasedTableMagicNumber,
      true /* sanitize */,
      std::make_shared<BlockBasedTableFactory>()
  );
  TestInternalKeyPropertiesCollector(
      kBlockBasedTableMagicNumber,
      true /* not sanitize */,
      std::make_shared<BlockBasedTableFactory>()
  );

  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = 8;
  plain_table_options.bloom_bits_per_key = 8;
  plain_table_options.hash_table_ratio = 0;

  TestInternalKeyPropertiesCollector(
      kPlainTableMagicNumber, false /* not sanitize */,
      std::make_shared<PlainTableFactory>(plain_table_options));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}
