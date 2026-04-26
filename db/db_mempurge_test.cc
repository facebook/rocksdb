//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/compaction_filter.h"

namespace ROCKSDB_NAMESPACE {

class DBMempurgeTest : public DBTestBase {
 public:
  DBMempurgeTest() : DBTestBase("db_mempurge_test", /*env_do_fsync=*/true) {}
};

class SnapshotCompactionFilter : public CompactionFilter {
 public:
  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*existing_value*/,
              std::string* /*new_value*/, bool* /*value_changed*/) const override {
    return false;
  }
  const char* Name() const override { return "SnapshotCompactionFilter"; }
  bool IgnoreSnapshots() const override { return false; }
};

class SnapshotCompactionFilterFactory : public CompactionFilterFactory {
 public:
  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::unique_ptr<CompactionFilter>(new SnapshotCompactionFilter());
  }
  const char* Name() const override { return "SnapshotCompactionFilterFactory"; }
  bool ShouldFilterTableFileCreation(TableFileCreationReason reason) const override {
      return reason == TableFileCreationReason::kFlush;
  }
};

TEST_F(DBMempurgeTest, MemPurgeCompactionFilterDeadlock) {
  Options options = CurrentOptions();
  options.write_buffer_size = 1024; // Small buffer
  options.experimental_mempurge_threshold = 1.0;
  options.compaction_filter_factory = std::make_shared<SnapshotCompactionFilterFactory>();
  
  Reopen(options);
  
  // Large value to trigger automatic flush (kWriteBufferFull)
  std::string value(2048, 'v');
  ASSERT_OK(Put("key", value));

  // Wait for background flush to finish
  dbfull()->TEST_WaitForFlushMemTable();

  // Verify data persisted correctly
  ASSERT_EQ(value, Get("key"));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
