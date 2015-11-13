//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <cctype>
#include <unordered_map>

#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_util.h"
#include "util/options_parser.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include <gflags/gflags.h>
using GFLAGS::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace rocksdb {
class OptionsUtilTest : public testing::Test {
 public:
  OptionsUtilTest() : rnd_(0xFB) {
    env_.reset(new test::StringEnv(Env::Default()));
    dbname_ = test::TmpDir() + "/options_util_test";
  }

 protected:
  std::unique_ptr<test::StringEnv> env_;
  std::string dbname_;
  Random rnd_;
};

bool IsBlockBasedTableFactory(TableFactory* tf) {
  return tf->Name() == BlockBasedTableFactory().Name();
}

TEST_F(OptionsUtilTest, SaveAndLoad) {
  const size_t kCFCount = 5;

  DBOptions db_opt;
  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;
  test::RandomInitDBOptions(&db_opt, &rnd_);
  for (size_t i = 0; i < kCFCount; ++i) {
    cf_names.push_back(i == 0 ? kDefaultColumnFamilyName
                              : test::RandomName(&rnd_, 10));
    cf_opts.emplace_back();
    test::RandomInitCFOptions(&cf_opts.back(), &rnd_);
  }

  const std::string kFileName = "OPTIONS-123456";
  PersistRocksDBOptions(db_opt, cf_names, cf_opts, kFileName, env_.get());

  DBOptions loaded_db_opt;
  std::vector<ColumnFamilyDescriptor> loaded_cf_descs;
  ASSERT_OK(LoadOptionsFromFile(kFileName, env_.get(), &loaded_db_opt,
                                &loaded_cf_descs));

  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(db_opt, loaded_db_opt));
  test::RandomInitDBOptions(&db_opt, &rnd_);
  ASSERT_NOK(RocksDBOptionsParser::VerifyDBOptions(db_opt, loaded_db_opt));

  for (size_t i = 0; i < kCFCount; ++i) {
    ASSERT_EQ(cf_names[i], loaded_cf_descs[i].name);
    ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
        cf_opts[i], loaded_cf_descs[i].options));
    if (IsBlockBasedTableFactory(cf_opts[i].table_factory.get())) {
      ASSERT_OK(RocksDBOptionsParser::VerifyTableFactory(
          cf_opts[i].table_factory.get(),
          loaded_cf_descs[i].options.table_factory.get()));
    }
    test::RandomInitCFOptions(&cf_opts[i], &rnd_);
    ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
        cf_opts[i], loaded_cf_descs[i].options));
  }

  for (size_t i = 0; i < kCFCount; ++i) {
    if (cf_opts[i].compaction_filter) {
      delete cf_opts[i].compaction_filter;
    }
  }
}

namespace {
class DummyTableFactory : public TableFactory {
 public:
  DummyTableFactory() {}
  virtual ~DummyTableFactory() {}

  virtual const char* Name() const { return "DummyTableFactory"; }

  virtual Status NewTableReader(const TableReaderOptions& table_reader_options,
                                unique_ptr<RandomAccessFileReader>&& file,
                                uint64_t file_size,
                                unique_ptr<TableReader>* table_reader) const {
    return Status::NotSupported();
  }

  virtual TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, WritableFileWriter* file) const {
    return nullptr;
  }

  virtual Status SanitizeOptions(const DBOptions& db_opts,
                                 const ColumnFamilyOptions& cf_opts) const {
    return Status::NotSupported();
  }

  virtual std::string GetPrintableTableOptions() const { return ""; }
};

class DummyMergeOperator : public MergeOperator {
 public:
  DummyMergeOperator() {}
  virtual ~DummyMergeOperator() {}

  virtual bool FullMerge(const Slice& key, const Slice* existing_value,
                         const std::deque<std::string>& operand_list,
                         std::string* new_value, Logger* logger) const {
    return false;
  }

  virtual bool PartialMergeMulti(const Slice& key,
                                 const std::deque<Slice>& operand_list,
                                 std::string* new_value, Logger* logger) const {
    return false;
  }

  virtual const char* Name() const { return "DummyMergeOperator"; }
};

class DummySliceTransform : public SliceTransform {
 public:
  DummySliceTransform() {}
  virtual ~DummySliceTransform() {}

  // Return the name of this transformation.
  virtual const char* Name() const { return "DummySliceTransform"; }

  // transform a src in domain to a dst in the range
  virtual Slice Transform(const Slice& src) const { return src; }

  // determine whether this is a valid src upon the function applies
  virtual bool InDomain(const Slice& src) const { return false; }

  // determine whether dst=Transform(src) for some src
  virtual bool InRange(const Slice& dst) const { return false; }
};

}  // namespace

TEST_F(OptionsUtilTest, SanityCheck) {
  DBOptions db_opt;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  const size_t kCFCount = 5;
  for (size_t i = 0; i < kCFCount; ++i) {
    cf_descs.emplace_back();
    cf_descs.back().name =
        (i == 0) ? kDefaultColumnFamilyName : test::RandomName(&rnd_, 10);

    cf_descs.back().options.table_factory.reset(NewBlockBasedTableFactory());
    cf_descs.back().options.prefix_extractor.reset(
        test::RandomSliceTransform(&rnd_));
    cf_descs.back().options.merge_operator.reset(
        test::RandomMergeOperator(&rnd_));
  }

  db_opt.create_missing_column_families = true;
  db_opt.create_if_missing = true;

  DestroyDB(dbname_, Options(db_opt, cf_descs[0].options));
  DB* db;
  std::vector<ColumnFamilyHandle*> handles;
  // open and persist the options
  ASSERT_OK(DB::Open(db_opt, dbname_, cf_descs, &handles, &db));

  // close the db
  for (auto* handle : handles) {
    delete handle;
  }
  delete db;

  // perform sanity check
  ASSERT_OK(
      CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

  ASSERT_GE(kCFCount, 5);
  // merge operator
  {
    std::shared_ptr<MergeOperator> merge_op =
        cf_descs[0].options.merge_operator;

    ASSERT_NE(merge_op.get(), nullptr);
    cf_descs[0].options.merge_operator.reset();
    ASSERT_NOK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[0].options.merge_operator.reset(new DummyMergeOperator());
    ASSERT_NOK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[0].options.merge_operator = merge_op;
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));
  }

  // prefix extractor
  {
    std::shared_ptr<const SliceTransform> prefix_extractor =
        cf_descs[1].options.prefix_extractor;

    ASSERT_NE(prefix_extractor, nullptr);
    cf_descs[1].options.prefix_extractor.reset();
    ASSERT_NOK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[1].options.prefix_extractor.reset(new DummySliceTransform());
    ASSERT_NOK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[1].options.prefix_extractor = prefix_extractor;
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));
  }

  // comparator
  {
    test::SimpleSuffixReverseComparator comparator;

    auto* prev_comparator = cf_descs[2].options.comparator;
    cf_descs[2].options.comparator = &comparator;
    ASSERT_NOK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[2].options.comparator = prev_comparator;
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));
  }

  // table factory
  {
    std::shared_ptr<TableFactory> table_factory =
        cf_descs[3].options.table_factory;

    ASSERT_NE(table_factory, nullptr);
    cf_descs[3].options.table_factory.reset(new DummyTableFactory());
    ASSERT_NOK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));

    cf_descs[3].options.table_factory = table_factory;
    ASSERT_OK(
        CheckOptionsCompatibility(dbname_, Env::Default(), db_opt, cf_descs));
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}

#else
#include <cstdio>

int main(int argc, char** argv) {
  printf("Skipped in RocksDBLite as utilities are not supported.\n");
  return 0;
}
#endif  // !ROCKSDB_LITE
