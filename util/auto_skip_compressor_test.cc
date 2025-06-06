//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Testing the features of auto skip compression manager
//
// ***********************************************************************
// EXPERIMENTAL - subject to change while under development
// ***********************************************************************

#include "util/auto_skip_compressor.h"

#include <atomic>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>

#include "db/db_test_util.h"
#include "db/read_callback.h"
#include "db/version_edit.h"
#include "env/fs_readonly.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/experimental.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/trace_record_result.h"
#include "rocksdb/utilities/replayer.h"
#include "rocksdb/wal_filter.h"
#include "test_util/testutil.h"
#include "util/defer.h"
#include "util/random.h"
#include "util/simple_mixed_compressor.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class DBAutoSkip : public DBTestBase {
 public:
  DBAutoSkip() : DBTestBase("db_auto_skip", /*env_do_fsync=*/true) {}
};
TEST_F(DBAutoSkip, AutoSkipCompressionManager) {
  Options options = CurrentOptions();
  options.compression = kZSTD;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
  BlockBasedTableOptions bbto;
  bbto.enable_index_compression = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  auto mgr = std::make_shared<AutoSkipCompressorManager>(
      GetDefaultBuiltinCompressionManager());
  options.compression_manager = mgr;
  DestroyAndReopen(options);
  Random rnd(301);
  std::vector<std::string> values;
  constexpr int kCount = 13;
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
}
}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
