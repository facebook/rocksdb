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

#include "rocksdb/extensions.h"
#include "rocksdb/extension_loader.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "util/testharness.h"
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

class MockExtension : public Extension {
private:
  const std::string name_;
  std::string prefix_;
  bool        sanitize;
  std::shared_ptr<MockExtension> child;
  
public:
  using Extension::SetOption;
  static const std::string kType;
  static const std::string & Type() { return kType; }
public:
  MockExtension(const std::string & name) : name_(name) {
    if (name.compare(0, 5, "mock.") == 0) {
      prefix_ = "test.";
    } else {
      prefix_ = "test.mock.";
    }
    prefix_.append(name);
  }
  
  virtual const char *Name() const override { return name_.c_str(); }

  virtual Status SetOption(const std::string & name,
			   const std::string & value,
			   bool input_strings_escaped) override {
    if (name == (prefix_ + ".sanitize")) {
      sanitize = ParseBoolean(name, value);
      if (child) {
	return child->SetOption(name, value, input_strings_escaped);
      } else {
	return Status::OK();
      }
    } else {
      return Extension::SetOption(name, value, input_strings_escaped);
    }
  }
  virtual Status SetOption(const DBOptions & dbOpts,
			   const ColumnFamilyOptions *cfOpts,
			   const std::string & name,
			   const std::string & value,
			   bool input_strings_escaped) override {
    Status s = SetSharedOption(dbOpts, cfOpts, name, value,
			       input_strings_escaped, 
			       prefix_, &child);
    if (s.IsNotFound()) {
      return Extension::SetOption(dbOpts, cfOpts, name, value, input_strings_escaped);
    } else {
      return s;
    }
  }

  virtual Status SanitizeOptions(const DBOptions & dbOpts) const override {
    if (child) {
      Status s = child->SanitizeOptions(dbOpts);
      if (! s.ok()) {
	return s;
      }
    }
    if (sanitize) {
      return Extension::SanitizeOptions(dbOpts);
    } else {
      return Status::InvalidArgument("Sanitized=false");
    }
  }
};

const std::string MockExtension::kType = "test-extension";

#ifndef ROCKSDB_LITE
Extension *CreateMockExtension(const std::string & name,
				const DBOptions &,
				const ColumnFamilyOptions *,
				std::unique_ptr<Extension>* guard) {
  guard->reset(new MockExtension(name));
  return guard->get();
}
  

extern "C" {
  void testMockExtensionFactory(ExtensionLoader & factory, const std::string & name) {
    factory.RegisterFactory(MockExtension::Type(), name,
			    CreateMockExtension);
  }
}

TEST_F(ExtensionTest, RegisterLocalExtensions) {
  std::shared_ptr<MockExtension> extension;
  DBOptions dbOpt1, dbOpt2;
  const char *name1 = "test1";
  const char *name2= "test2";
  dbOpt1.extensions->RegisterFactory(MockExtension::Type(), name1,
				     CreateMockExtension);
  dbOpt2.extensions->RegisterFactory(MockExtension::Type(), name2,
				     CreateMockExtension);
  AssertNewSharedExtension(dbOpt1, name1, true, &extension);
  AssertNewSharedExtension(dbOpt1, name2, false, &extension);
  AssertNewSharedExtension(dbOpt2, name1, false, &extension);
  AssertNewSharedExtension(dbOpt2, name2, true, &extension);
}
  
TEST_F(ExtensionTest, RegisterDefaultExtensions) {
  DBOptions dbOpt1;
  std::shared_ptr<MockExtension> extension;
  const char *name = "Default";
  AssertNewSharedExtension(dbOpt1, name, false, &extension);

  ExtensionLoader::Default()->RegisterFactory(MockExtension::Type(),
					      name, CreateMockExtension);
  
  AssertNewSharedExtension(dbOpt1, name, true, &extension);
  DBOptions dbOpt2;
  AssertNewSharedExtension(dbOpt2, name, true, &extension);
}

TEST_F(ExtensionTest, RegisterFactories) {
  DBOptions dbOptions;
  const char *name = "factory";
  std::shared_ptr<MockExtension> extension;

  dbOptions.extensions->RegisterFactories(testMockExtensionFactory, name);
  AssertNewSharedExtension(dbOptions, name, true, &extension);
}

TEST_F(ExtensionTest, LoadUnknownLibrary) {
  DBOptions dbOptions;
  Status s = dbOptions.AddExtensionLibrary("", "fred", "fred");
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(ExtensionTest, LoadExtensionLibrary) {
  DBOptions dbOptions;
  const char *name = "test";
  std::shared_ptr<MockExtension> extension;
  ASSERT_OK(dbOptions.AddExtensionLibrary("", "testMockExtensionFactory", name));
  AssertNewSharedExtension(dbOptions, "Not found", false, &extension);
  AssertNewSharedExtension(dbOptions, name, true, &extension);
}

TEST_F(ExtensionTest, SetOptions) {
  DBOptions dbOptions;
  const char *name = "setOptions";
  std::shared_ptr<MockExtension> extension;
  
  ExtensionLoader::Default()->RegisterFactory(MockExtension::Type(),
					      name, CreateMockExtension);
  AssertNewSharedExtension(dbOptions, name, true, &extension);
  ASSERT_EQ(Status::NotFound(), extension->SetOption("unknown", "bad"));  
  ASSERT_OK(extension->SetOption("test.mock.setOptions.sanitize", "true"));
}

TEST_F(ExtensionTest, SanitizeOptions) {
  DBOptions dbOptions;
  std::shared_ptr<MockExtension> ext;
  const char *name = "sanitize";
  ExtensionLoader::Default()->RegisterFactory(MockExtension::Type(),
					      name, CreateMockExtension);
  AssertNewSharedExtension(dbOptions, name, true, &ext);
  ASSERT_OK(ext->SetOption("test.mock.sanitize.sanitize", "false"));
  ASSERT_EQ(Status::InvalidArgument(), ext->SanitizeOptions(dbOptions));
  ASSERT_OK(ext->SetOption("test.mock.sanitize.sanitize", "true"));
  ASSERT_OK(ext->SanitizeOptions(dbOptions));
}

TEST_F(ExtensionTest, ConfigureOptionsFromString) {
  DBOptions dbOptions;
  std::shared_ptr<MockExtension> ext;
  const char *name = "fromString";
  ExtensionLoader::Default()->RegisterFactory(MockExtension::Type(),
					      name, CreateMockExtension);
  AssertNewSharedExtension(dbOptions, name, true, &ext);
  ASSERT_OK(ext->ConfigureFromString(dbOptions, "test.mock.fromString.sanitize=true"));
  ASSERT_OK(ext->SanitizeOptions(dbOptions));
  ASSERT_OK(ext->ConfigureFromString(dbOptions, "test.mock.fromString.sanitize=false"));
  ASSERT_EQ(Status::InvalidArgument(), ext->SanitizeOptions(dbOptions));
  ASSERT_OK(ext->ConfigureFromString(dbOptions, nullptr,
				     "test.mock.fromString.sanitize=true;"
				     "unknown.options=x",
				     false, true));
  ASSERT_OK(ext->SanitizeOptions(dbOptions));
  ASSERT_EQ(Status::NotFound(), 
	    ext->ConfigureFromString(dbOptions,
				     "test.mock.fromString.sanitize=true;"
				     "unknown.options=x"));
  ASSERT_OK(ext->SanitizeOptions(dbOptions));
}
  
TEST_F(ExtensionTest, ConfigureOptionsFromMap) {
  DBOptions dbOptions;
  std::shared_ptr<MockExtension> guard;
  const char *name = "map";
  std::unordered_map<std::string, std::string> opt_map;
  opt_map["test.mock.map.sanitize"]="false";
  ExtensionLoader::Default()->RegisterFactory(MockExtension::Type(),
					      name, CreateMockExtension);
  AssertNewSharedExtension(dbOptions, name, true, &guard);

  ASSERT_OK(guard->ConfigureFromMap(dbOptions, opt_map));
  ASSERT_EQ(Status::InvalidArgument(), guard->SanitizeOptions(dbOptions));

  opt_map["test.mock.map.sanitize"]="true";
  ASSERT_OK(guard->ConfigureFromMap(dbOptions, opt_map));
  ASSERT_OK(guard->SanitizeOptions(dbOptions));
  
  opt_map["unknown.options"]="true";
  ASSERT_OK(guard->ConfigureFromMap(dbOptions, nullptr, opt_map, false, true));
  ASSERT_OK(guard->SanitizeOptions(dbOptions));

  ASSERT_EQ(Status::NotFound(),
	    guard->ConfigureFromMap(dbOptions, opt_map));
}

TEST_F(ExtensionTest, NewGuardedExtension) {
  DBOptions dbOpts;
  MockExtension *ext;
  std::unique_ptr<MockExtension> guard;
  std::shared_ptr<MockExtension> shared;
  
  AssertNewUniqueExtension(dbOpts, "guarded", false, &ext, &guard, true);
  dbOpts.extensions->RegisterFactory(
				 MockExtension::kType,
				 "guarded",
				 [](const std::string & name,
				    const DBOptions &,
				    const ColumnFamilyOptions *,
				    std::unique_ptr<Extension> * guard) {
				   guard->reset(new MockExtension(name));
				   return guard->get();
				 });
  AssertNewUniqueExtension(dbOpts, "guarded", true, &ext, &guard, true);
  AssertNewSharedExtension(dbOpts, "guarded", true, &shared);
}

TEST_F(ExtensionTest, NewUnguardedExtension) {
  DBOptions dbOpts;
  MockExtension *ext;
  std::shared_ptr<MockExtension> shared;
  std::unique_ptr<MockExtension> guard;
  
  AssertNewUniqueExtension(dbOpts, "unguarded", false, &ext, &guard, false);
  dbOpts.extensions->RegisterFactory(
				 MockExtension::kType,
				 "unguarded",
				 [](const std::string & name,
				    const DBOptions &,
				    const ColumnFamilyOptions *,
				    std::unique_ptr<Extension> * guard) {
				   guard->reset();
				   return new MockExtension(name);
				 });
  AssertNewUniqueExtension(dbOpts, "unguarded", true, &ext, &guard, false);
  Status status = NewSharedExtension("unguarded", dbOpts, nullptr, &shared);
  ASSERT_TRUE(status.IsNotSupported());
  ASSERT_EQ(shared.get(), nullptr);
  delete ext;
}
  
TEST_F(ExtensionTest, CreateFromPattern) {
  std::shared_ptr<MockExtension> ext;
  DBOptions dbOptions;
  dbOptions.extensions->RegisterFactory(
				 MockExtension::kType,
				 "good.*",
				 [](const std::string & name,
				    const DBOptions &,
				    const ColumnFamilyOptions *,
				    std::unique_ptr<Extension> * guard) {
				   guard->reset(new MockExtension(name));
				   return guard->get();
				 });
  AssertNewSharedExtension(dbOptions, "good.1", true, &ext);
  AssertNewSharedExtension(dbOptions, "good.2", true, &ext);
  AssertNewSharedExtension(dbOptions, "bad.2", false, &ext);
}

TEST_F(ExtensionTest, ConfigureFromProperties) {
  std::shared_ptr<MockExtension> ext;
  DBOptions dbOptions;
  dbOptions.extensions->RegisterFactory(
				 MockExtension::kType,
				 "mock.*",
				 [](const std::string & name,
				    const DBOptions &,
				    const ColumnFamilyOptions *,
				    std::unique_ptr<Extension> * guard) {
				   guard->reset(new MockExtension(name));
				   return guard->get();
				 });
  AssertNewSharedExtension(dbOptions, "mock.parent", true, &ext);
  ASSERT_OK(ext->SetOption(dbOptions, "test.mock.parent",
			   "name=mock.child;"
			   "options={test.mock.child={name=mock.grand}}"));
  ASSERT_OK(ext->ConfigureFromString(dbOptions,
				     "test.mock.parent={"
				     "name=mock.1;"
				     "options={test.mock.1={name=mock.2;"
				     "options={test.mock.2.sanitize=true}}}}"));
  ASSERT_EQ(Status::InvalidArgument(),
	    ext->SetOption(dbOptions, "test.mock.parent",
			   "name=mock.child;"
			   "options={test.mock.child.unknown=unknown}"
			   "unknown=unknown"));
  ASSERT_EQ(Status::InvalidArgument(),
	    ext->SetOption(dbOptions, "test.mock.parent",
			   "name=bad.child;"));
  ASSERT_EQ(Status::NotFound(),
	    ext->SetOption(dbOptions, "test.mock.parent",
			   "name=mock.child;"
			   "options={test.mock.child.unknown=unknown}"));
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
