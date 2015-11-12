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
  OptionsUtilTest() { env_.reset(new test::StringEnv(Env::Default())); }

 protected:
  std::unique_ptr<test::StringEnv> env_;
};

bool IsBlockBasedTableFactory(TableFactory* tf) {
  return tf->Name() == BlockBasedTableFactory().Name();
}

TEST_F(OptionsUtilTest, SaveAndLoad) {
  const size_t kCFCount = 5;
  Random rnd(0xFB);

  DBOptions db_opt;
  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;
  test::RandomInitDBOptions(&db_opt, &rnd);
  for (size_t i = 0; i < kCFCount; ++i) {
    cf_names.push_back(i == 0 ? kDefaultColumnFamilyName
                              : test::RandomName(&rnd, 10));
    cf_opts.emplace_back();
    test::RandomInitCFOptions(&cf_opts.back(), &rnd);
  }

  const std::string kFileName = "OPTIONS-123456";
  PersistRocksDBOptions(db_opt, cf_names, cf_opts, kFileName, env_.get());

  DBOptions loaded_db_opt;
  std::vector<ColumnFamilyDescriptor> loaded_cf_descs;
  ASSERT_OK(LoadOptionsFromFile(kFileName, env_.get(), &loaded_db_opt,
                                &loaded_cf_descs));

  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(db_opt, loaded_db_opt));
  test::RandomInitDBOptions(&db_opt, &rnd);
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
    test::RandomInitCFOptions(&cf_opts[i], &rnd);
    ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
        cf_opts[i], loaded_cf_descs[i].options));
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
