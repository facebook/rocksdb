//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "rocksdb/db_plugin.h"

#include <limits>
#include <string>
#include <unordered_map>

#include "db/db_test_util.h"
#include "options/options_helper.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db_plugin.h"
#include "rocksdb/utilities/stackable_db.h"

namespace ROCKSDB_NAMESPACE {

class DBPluginTest : public DBTestBase {
 public:
  DBPluginTest() : DBTestBase("/db_plugin_test") {}
};

class DefaultPlugin : public DBPlugin {
 public:
  const char* Name() const override { return "DefaultPlugin"; }
};

class CountingPlugin : public DBPlugin {
 public:
  CountingPlugin(const std::string& name = "CountingPlugin") : name_(name) {
    reset();
  }
  const char* Name() const override { return name_.c_str(); }
  Status SanitizeCB(OpenMode mode, const std::string& db_name,
                    DBOptions* db_opts,
                    std::vector<ColumnFamilyDescriptor>* cfds) override {
    if (sanitized < 0) {
      return Status::InvalidArgument("Cannot sanitize");
    } else {
      sanitized++;
      return DBPlugin::SanitizeCB(mode, db_name, db_opts, cfds);
    }
  }

  Status ValidateCB(
      OpenMode mode, const std::string& db_name, const DBOptions& db_opts,
      const std::vector<ColumnFamilyDescriptor>& cfds) const override {
    if (validated < 0) {
      return Status::InvalidArgument("Cannot validatee");
    } else {
      validated++;
      return DBPlugin::ValidateCB(mode, db_name, db_opts, cfds);
    }
  }

  Status OpenCB(OpenMode mode, DB* db,
                const std::vector<ColumnFamilyHandle*>& handles,
                DB** wrapped) override {
    if (mode == OpenMode::ReadOnly) {
      if (readonly < 0) {
        return Status::InvalidArgument("Cannot open readonly");
      } else {
        readonly++;
      }
    } else if (mode == OpenMode::Normal) {
      if (opened < 0) {
        return Status::InvalidArgument("Cannot open");
      } else {
        opened++;
      }
    }
    return DBPlugin::OpenCB(mode, new StackableDB(db), handles, wrapped);
  }

  bool SupportsOpenMode(OpenMode mode) const override {
    if (mode == OpenMode::ReadOnly) {
      return supports_ro;
    } else {
      return mode == OpenMode::Normal;
    }
  }

  void reset() {
    supports_ro = true;
    sanitized = 0;
    validated = 0;
    opened = 0;
    readonly = 0;
  }
  const std::string name_;
  bool supports_ro;
  int sanitized;
  mutable int validated;
  int opened;
  int readonly;
};

TEST_F(DBPluginTest, TestDefaultPlugin) {
  // GetOptions should be able to get latest option changed by SetOptions.
  Options options;
  std::shared_ptr<CountingPlugin> counter = std::make_shared<CountingPlugin>();
  options.plugins.push_back(counter);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(counter->opened, 1);
  ASSERT_EQ(counter->readonly, 0);
  ASSERT_TRUE(counter->sanitized > 0);
  ASSERT_TRUE(counter->validated > 0);
  Close();
  counter->reset();
#ifndef ROCKSDB_LITE
  ReadOnlyReopen(options);
  ASSERT_EQ(counter->readonly, 1);
  ASSERT_EQ(counter->opened, 0);
  ASSERT_TRUE(counter->sanitized > 0);
  //**TODO: RO open is not validated  ASSERT_TRUE(counter->validated > 0);
#endif  // ROCKSDB_LITE
}

TEST_F(DBPluginTest, TestNoSanitize) {
  Options options;
  std::shared_ptr<CountingPlugin> counter = std::make_shared<CountingPlugin>();
  options.plugins.push_back(counter);
  counter->sanitized = -1;
  ASSERT_NOK(TryReopen(options));
  ASSERT_EQ(counter->opened, 0);
  ASSERT_EQ(db_, nullptr);
  ASSERT_NOK(ReadOnlyReopen(options));
  ASSERT_EQ(counter->readonly, 0);
  ASSERT_EQ(db_, nullptr);
  counter->sanitized = 0;
  counter->validated = -1;
  ASSERT_NOK(TryReopen(options));
  ASSERT_EQ(counter->opened, 0);
  ASSERT_TRUE(counter->sanitized > 0);
  ASSERT_EQ(db_, nullptr);
}

TEST_F(DBPluginTest, TestOpenFailed) {
  Options options;
  std::shared_ptr<CountingPlugin> counter = std::make_shared<CountingPlugin>();
  options.plugins.push_back(counter);
  counter->opened = -1;
  counter->supports_ro = false;
  ASSERT_NOK(TryReopen(options));
  ASSERT_EQ(db_, nullptr);
  ASSERT_NOK(ReadOnlyReopen(options));
  ASSERT_EQ(db_, nullptr);
  counter->supports_ro = true;
  counter->readonly = -1;
  ASSERT_NOK(ReadOnlyReopen(options));
  ASSERT_EQ(db_, nullptr);
}

TEST_F(DBPluginTest, TestTwoPlugins) {
  Options options;
  std::shared_ptr<CountingPlugin> counter1 =
      std::make_shared<CountingPlugin>("Counter1");
  std::shared_ptr<CountingPlugin> counter2 =
      std::make_shared<CountingPlugin>("Counter2");
  options.plugins.push_back(counter1);
  options.plugins.push_back(counter2);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(counter1->opened, 1);
  ASSERT_EQ(counter2->opened, 1);
  ASSERT_NE(db_, nullptr);
  Close();
#ifndef ROCKSDB_LITE
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ(counter1->readonly, 1);
  ASSERT_EQ(counter2->readonly, 1);
  ASSERT_NE(db_, nullptr);
  Close();
#endif  // ROCKSDB_LITE

  counter1->reset();
  counter2->reset();
  counter1->sanitized = -1;
  ASSERT_NOK(TryReopen(options));
  ASSERT_EQ(counter2->sanitized, 0);
  ASSERT_EQ(db_, nullptr);
#ifndef ROCKSDB_LITE
  ASSERT_NOK(ReadOnlyReopen(options));
  ASSERT_EQ(counter2->sanitized, 0);
  ASSERT_EQ(db_, nullptr);
#endif  // ROCKSDB_LITE

  counter1->reset();
  counter2->reset();
  counter2->sanitized = -1;
  ASSERT_NOK(TryReopen(options));
  ASSERT_GE(counter1->sanitized, 1);
  ASSERT_EQ(db_, nullptr);
#ifndef ROCKSDB_LITE
  ASSERT_NOK(ReadOnlyReopen(options));
  ASSERT_GE(counter1->sanitized, 1);
  ASSERT_EQ(db_, nullptr);
#endif  // ROCKSDB_LITE

  counter1->reset();
  counter2->reset();
  counter1->opened = -1;
  counter1->readonly = -1;
  ASSERT_NOK(TryReopen(options));
  ASSERT_EQ(db_, nullptr);
#ifndef ROCKSDB_LITE
  ASSERT_NOK(ReadOnlyReopen(options));
  ASSERT_EQ(db_, nullptr);
#endif  // ROCKSDB_LITE

  counter1->reset();
  counter2->reset();
  counter2->opened = -1;
  counter2->readonly = -1;
  ASSERT_NOK(TryReopen(options));
  ASSERT_EQ(db_, nullptr);
#ifndef ROCKSDB_LITE
  ASSERT_NOK(ReadOnlyReopen(options));
  ASSERT_EQ(db_, nullptr);
#endif  // ROCKSDB_LITE
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
