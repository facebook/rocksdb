//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <cctype>
#include <cstring>
#include <unordered_map>
#include <inttypes.h>

#include "cache/lru_cache.h"
#include "cache/sharded_cache.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "options/options_sanity_check.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/leveldb_options.h"
#include "util/random.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "extensions/extension_test.h"

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace rocksdb {

class ExtensionTest : public testing::Test {};

#ifndef ROCKSDB_LITE  // GetOptionsFromMap is not supported in ROCKSDB_LITE

class TestListener : public EventListener {
private:
  bool        sanitize;
  std::string listenerName;
public:
  TestListener(const std::string & n) : listenerName(n) { sanitize=true;}
  const char * Name() const override { return listenerName.c_str(); }
  virtual Status SetOption(const std::string & name,
			   const std::string & value,
			   bool ignore_unknown_options,
			   bool input_strings_escaped) override {
    if (name == "test.listener.sanitize") {
      sanitize = ParseBoolean(name, value);
      return Status::OK();
    } else {
      return EventListener::SetOption(name, value,
				      ignore_unknown_options,
				      input_strings_escaped);
    }
  }

  virtual Status SanitizeOptions(const DBOptions & dbOpts) const override {
    if (sanitize) {
      return EventListener::SanitizeOptions(dbOpts);
    } else {
      return Status::InvalidArgument("Sanitized=false");
    }
  }
};

  Extension *CreateTestListener(const std::string & name,
				const DBOptions &,
				const ColumnFamilyOptions *,
				std::unique_ptr<Extension>* guard) {
    guard->reset(new TestListener(name));
    return guard->get();
  }
  

extern "C" {
  void testListenerFactory(ExtensionLoader & factory, const std::string & name) {
    factory.RegisterFactory(EventListener::Type(), name,
			    CreateTestListener);
  }
}

static EventListener *AssertNewListener(DBOptions & dbOpts, const char *name,
					bool isValid,
					std::unique_ptr<EventListener> *guard) {
  EventListener *listener;
  AssertNewExtension(dbOpts, name, isValid, &listener, true, guard);
  return listener;
}

static void AssertNewListener(DBOptions & dbOpts, const char *name,
			      bool isValid) {
  std::unique_ptr<EventListener> guard;
  AssertNewListener(dbOpts, name, isValid, &guard);
}

  
TEST_F(ExtensionTest, RegisterLocalExtensions) {
  DBOptions dbOpt1, dbOpt2;
  const char *name1 = "listener1";
  const char *name2= "listener2";
  dbOpt1.extensions->RegisterFactory(EventListener::Type(), name1,
				     CreateTestListener);
  dbOpt2.extensions->RegisterFactory(EventListener::Type(), name2,
				     CreateTestListener);

  AssertNewListener(dbOpt1, name1, true);
  AssertNewListener(dbOpt2, name1, false);
  AssertNewListener(dbOpt1, name2, false);
  AssertNewListener(dbOpt2, name2, true);
}
  
TEST_F(ExtensionTest, RegisterDefaultExtensions) {
  DBOptions dbOpt1;
  const char *name = "DefaultListener";
  AssertNewListener(dbOpt1, name, false);

  ExtensionLoader::Default()->RegisterFactory(EventListener::Type(),
					      name, CreateTestListener);
  
  AssertNewListener(dbOpt1, name, true);
  DBOptions dbOpt2;
  AssertNewListener(dbOpt2, name, true);
}

TEST_F(ExtensionTest, RegisterFactories) {
  DBOptions dbOptions;
  const char *name = "factoryListener";
  dbOptions.extensions->RegisterFactories(testListenerFactory, name);
  AssertNewListener(dbOptions, name, true);
}

TEST_F(ExtensionTest, LoadUnknownLibrary) {
  DBOptions dbOptions;
  Status s = dbOptions.AddExtensionLibrary("", "fred", "fred");
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(ExtensionTest, LoadExtensionLibrary) {
  DBOptions dbOptions;
  const char *name = "testListener";
  ASSERT_OK(dbOptions.AddExtensionLibrary("", "testListenerFactory", name));
  AssertNewListener(dbOptions, "Not found", false);
  AssertNewListener(dbOptions, name, true);
}

TEST_F(ExtensionTest, SetOptions) {
  DBOptions dbOptions;
  std::unique_ptr<EventListener> guard;
  const char *name = "setOptions";
  ExtensionLoader::Default()->RegisterFactory(EventListener::Type(),
					      name, CreateTestListener);
  AssertNewListener(dbOptions, name, true, &guard);
  ASSERT_OK(guard->SetOption("unknown", "bad", true, true));
  ASSERT_EQ(Status::InvalidArgument(), guard->SetOption("unknown", "bad", false, false));
  ASSERT_EQ(Status::InvalidArgument(), guard->SetOption("unknown", "bad"));  
  ASSERT_OK(guard->SetOption("test.listener.sanitize", "true"));
}

  TEST_F(ExtensionTest, SanitizeOptions) {
  DBOptions dbOptions;
  std::unique_ptr<EventListener> guard;
  const char *name = "setOptions";
  ExtensionLoader::Default()->RegisterFactory(EventListener::Type(),
					      name, CreateTestListener);
  AssertNewListener(dbOptions, name, true, &guard);
  ASSERT_OK(guard->SetOption("test.listener.sanitize", "false"));
  ASSERT_EQ(Status::InvalidArgument(), guard->SanitizeOptions(dbOptions));
  ASSERT_OK(guard->SetOption("test.listener.sanitize", "true"));
  ASSERT_OK(guard->SanitizeOptions(dbOptions));
}

TEST_F(ExtensionTest, ConfigureOptionsFromString) {
  DBOptions dbOptions;
  std::unique_ptr<EventListener> guard;
  const char *name = "fromString";
  ExtensionLoader::Default()->RegisterFactory(EventListener::Type(),
					      name, CreateTestListener);
  AssertNewListener(dbOptions, name, true, &guard);
  ASSERT_OK(guard->ConfigureFromString("test.listener.sanitize=true",
				       dbOptions));
  ASSERT_OK(guard->SanitizeOptions(dbOptions));
  ASSERT_OK(guard->ConfigureFromString("test.listener.sanitize=false",
				       dbOptions));
  ASSERT_EQ(Status::InvalidArgument(), guard->SanitizeOptions(dbOptions));
  ASSERT_OK(guard->ConfigureFromString("test.listener.sanitize=true;unknown.options=x",
				       dbOptions, nullptr, true, false));
  ASSERT_OK(guard->SanitizeOptions(dbOptions));
  ASSERT_EQ(Status::InvalidArgument(), 
	    guard->ConfigureFromString("test.listener.sanitize=true;unknown.options=x",
				       dbOptions));
  ASSERT_OK(guard->SanitizeOptions(dbOptions));
}
  
TEST_F(ExtensionTest, ConfigureOptionsFromMap) {
  DBOptions dbOptions;
  std::unique_ptr<EventListener> guard;
  const char *name = "fromString";
  std::unordered_map<std::string, std::string> opt_map;
  opt_map["test.listener.sanitize"]="false";
  ExtensionLoader::Default()->RegisterFactory(EventListener::Type(),
					      name, CreateTestListener);
  AssertNewListener(dbOptions, name, true, &guard);

  ASSERT_OK(guard->ConfigureFromMap(opt_map, dbOptions));
  ASSERT_EQ(Status::InvalidArgument(), guard->SanitizeOptions(dbOptions));

  opt_map["test.listener.sanitize"]="true";
  ASSERT_OK(guard->ConfigureFromMap(opt_map, dbOptions));
  ASSERT_OK(guard->SanitizeOptions(dbOptions));
  
  opt_map["unknown.options"]="true";
  ASSERT_OK(guard->ConfigureFromMap(opt_map, dbOptions, nullptr, true, false));
  ASSERT_OK(guard->SanitizeOptions(dbOptions));

  ASSERT_EQ(Status::InvalidArgument(),
	    guard->ConfigureFromMap(opt_map, dbOptions));
}
  
#endif  // !ROCKSDB_LITE
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
