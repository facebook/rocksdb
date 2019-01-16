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
class MockExtension;
struct MockOptions {
  bool        sanitize;
  std::shared_ptr<MockExtension> inner;
};

static OptionTypeMap mockOptionsMap =
{
 {"sanitize",
  {offsetof(struct MockOptions, sanitize),
   OptionType::kBoolean, OptionVerificationType::kByName,
   false, 0}
 }
};


class MockExtension : public Extension {
private:
  const std::string name_;
protected:
  std::string prefix_;
  MockOptions options_;
public:
  using Extension::SetOption;
  static const std::string kType;
  static const std::string & Type() { return kType; }
public:
  MockExtension(const std::string & name) : name_(name) {
    if (name.compare(0, 9, "test.mock") == 0) {
      prefix_ = "mock.";
    } else {
      prefix_ = name + ".";
    }
  }
  
  virtual const std::string & GetOptionPrefix() const override {
    return prefix_;
  }
  
  virtual const char *Name() const override { return name_.c_str(); }

  virtual Status SanitizeOptions(const DBOptions & dbOpts) const override {
    if (options_.inner) {
      Status s = options_.inner->SanitizeOptions(dbOpts);
      if (! s.ok()) {
	return s;
      }
    }
    if (options_.sanitize) {
      return Extension::SanitizeOptions(dbOpts);
    } else {
      return Status::InvalidArgument("Sanitized=false");
    }
  }  
};

class MockExtensionWithOptions : public MockExtension {
public:
  MockExtensionWithOptions(const std::string & name) : MockExtension(name) { }
  virtual Status SetOption(const std::string & name,
			   const std::string & value,
			   bool input_strings_escaped) override {
    Status status = SetExtensionOption(options_.inner.get(),
				       prefix_ + "inner",
				       name, value, input_strings_escaped);
    if (status.IsNotFound()) {
      if (name == (prefix_ + "sanitize")) {
	options_.sanitize = ParseBoolean(name, value);
	status = Status::OK();
      } else {
	status = Extension::SetOption(name, value, input_strings_escaped);
      }
    }
    return status;
  }
  
  virtual Status SetOption(const DBOptions & dbOpts,
			   const ColumnFamilyOptions *cfOpts,
			   const std::string & name,
			   const std::string & value,
			   bool input_strings_escaped) override {
    Status s = SetSharedOption(dbOpts, cfOpts, name, value,
			       input_strings_escaped, 
			       prefix_ + "inner", &options_.inner);
    if (s.IsNotFound()) {
      return Extension::SetOption(dbOpts, cfOpts, name, value, input_strings_escaped);
    } else {
      return s;
    }
  }
};

class MockExtensionWithMap : public MockExtension {
public:
  MockExtensionWithMap(const std::string & name) : MockExtension(name) {
  }
  virtual void *GetOptions() override { return &options_; }
  virtual const OptionTypeMap *GetOptionTypeMap() const override {
    return &mockOptionsMap;
  }
};
  
const std::string MockExtension::kType = "test-extension";

#ifndef ROCKSDB_LITE
Extension *MockExtensionOptionsFactory(const std::string & name,
				       const DBOptions &,
				       const ColumnFamilyOptions *,
				       std::unique_ptr<Extension>* guard) {
  guard->reset(new MockExtensionWithOptions(name));
  return guard->get();
}
  
Extension *MockExtensionMapFactory(const std::string & name,
				   const DBOptions &,
				   const ColumnFamilyOptions *,
				   std::unique_ptr<Extension>* guard) {
  guard->reset(new MockExtensionWithMap(name));
  return guard->get();
}
  
extern "C" {
    void testMockRegistrar(ExtensionLoader & factory, const std::string & name) {
    factory.RegisterFactory(MockExtension::Type(), name,
			    MockExtensionOptionsFactory);
  }
}

class ExtensionTest : public testing::Test {
public:
  DBOptions dbOptions_;
  std::string pattern_;
  ExtensionTest() {
    pattern_ = "test.mock";
    dbOptions_.extensions->RegisterFactory(MockExtension::Type(), pattern_,
					   MockExtensionOptionsFactory);
  }
};
  
class ExtensionTestWithParam :
    public ExtensionTest,
    public ::testing::WithParamInterface<std::pair<bool, std::string> > {
public:
  bool useMap_;
  ExtensionTestWithParam() {
    std::pair<bool, std::string> param_pair = GetParam();
    useMap_  = param_pair.first;
    pattern_ = param_pair.second;
    RegisterFactory(pattern_);
  }    

  void RegisterFactory(const std::string & pattern) {
    if (useMap_) {
      dbOptions_.extensions->RegisterFactory(MockExtension::Type(), pattern,
					     MockExtensionMapFactory);
    } else {
      dbOptions_.extensions->RegisterFactory(MockExtension::Type(), pattern,
					     MockExtensionOptionsFactory);
    }
  }
};


TEST_F(ExtensionTest, RegisterLocalExtensions) {
  std::shared_ptr<MockExtension> extension;
  DBOptions dbOpt1, dbOpt2;
  const char *name1 = "test1";
  const char *name2= "test2";
  dbOpt1.extensions->RegisterFactory(MockExtension::Type(), name1,
				     MockExtensionOptionsFactory);
  dbOpt2.extensions->RegisterFactory(MockExtension::Type(), name2,
				     MockExtensionOptionsFactory);
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

  ExtensionLoader::Default()->RegisterFactory(MockExtension::Type(), name,
					      MockExtensionOptionsFactory);
  
  AssertNewSharedExtension(dbOpt1, name, true, &extension);
  DBOptions dbOpt2;
  AssertNewSharedExtension(dbOpt2, name, true, &extension);
}

TEST_F(ExtensionTest, RegisterFactories) {
  const char *name = "factory";
  std::shared_ptr<MockExtension> extension;

  dbOptions_.extensions->RegisterFactories(testMockRegistrar, name);
  AssertNewSharedExtension(dbOptions_, name, true, &extension);
}

TEST_F(ExtensionTest, LoadUnknownLibrary) {
  Status s = dbOptions_.AddExtensionLibrary("", "fred", "fred");
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(ExtensionTest, LoadExtensionLibrary) {
  const char *name = "test";
  std::shared_ptr<MockExtension> extension;
  ASSERT_OK(dbOptions_.AddExtensionLibrary("", "testMockRegistrar", name));
  AssertNewSharedExtension(dbOptions_, "Not found", false, &extension);
  AssertNewSharedExtension(dbOptions_, name, true, &extension);
}

TEST_F(ExtensionTest, NewGuardedExtension) {
  MockExtension *ext;
  std::unique_ptr<MockExtension> guard;
  std::shared_ptr<MockExtension> shared;
  
  AssertNewUniqueExtension(dbOptions_, "guarded", false, &ext, &guard, true);
  dbOptions_.extensions->RegisterFactory(
					 MockExtension::kType,
					 "guarded",
					 [](const std::string & name,
					    const DBOptions &,
					    const ColumnFamilyOptions *,
					    std::unique_ptr<Extension> * guard) {
					   guard->reset(new MockExtension(name));
					   return guard->get();
					 });
  AssertNewUniqueExtension(dbOptions_, "guarded", true, &ext, &guard, true);
  AssertNewSharedExtension(dbOptions_, "guarded", true, &shared);
}

TEST_F(ExtensionTest, NewUnguardedExtension) {
  MockExtension *ext;
  std::shared_ptr<MockExtension> shared;
  std::unique_ptr<MockExtension> guard;
  
  AssertNewUniqueExtension(dbOptions_, "unguarded", false, &ext, &guard, false);
  dbOptions_.extensions->RegisterFactory(MockExtension::kType,
					 "unguarded",
					 [](const std::string & name,
					    const DBOptions &,
					    const ColumnFamilyOptions *,
					    std::unique_ptr<Extension> * guard) {
					   guard->reset();
					   return new MockExtension(name);
					 });
  AssertNewUniqueExtension(dbOptions_, "unguarded", true, &ext, &guard, false);
  Status status = NewSharedExtension("unguarded", dbOptions_, nullptr, &shared);
  ASSERT_TRUE(status.IsNotSupported());
  ASSERT_EQ(shared.get(), nullptr);
  delete ext;
}
  
TEST_F(ExtensionTest, CreateFromPattern) {
  std::shared_ptr<MockExtension> ext;
  dbOptions_.extensions->RegisterFactory(MockExtension::kType, "good.*",
					 MockExtensionOptionsFactory);
  AssertNewSharedExtension(dbOptions_, "good.1", true, &ext);
  AssertNewSharedExtension(dbOptions_, "good.2", true, &ext);
  AssertNewSharedExtension(dbOptions_, "bad.2", false, &ext);
}
  
TEST_P(ExtensionTestWithParam, SetOptions) {
  std::shared_ptr<MockExtension> extension;
  AssertNewSharedExtension(dbOptions_, pattern_, true, &extension);
  ASSERT_EQ(Status::NotFound(), extension->SetOption("unknown", "bad"));  
  ASSERT_OK(extension->SetOption("mock.sanitize", "true"));
}

TEST_P(ExtensionTestWithParam, SanitizeOptions) {
  std::shared_ptr<MockExtension> ext;
  AssertNewSharedExtension(dbOptions_, pattern_, true, &ext);
  ASSERT_OK(ext->SetOption("mock.sanitize", "false"));
  ASSERT_EQ(Status::InvalidArgument(), ext->SanitizeOptions(dbOptions_));
  ASSERT_OK(ext->SetOption("mock.sanitize", "true"));
  ASSERT_OK(ext->SanitizeOptions(dbOptions_));
}

TEST_P(ExtensionTestWithParam, ConfigureOptionsFromString) {
  std::shared_ptr<MockExtension> ext;
  AssertNewSharedExtension(dbOptions_, pattern_, true, &ext);
  ASSERT_OK(ext->ConfigureFromString(dbOptions_, "mock.sanitize=true"));
  ASSERT_OK(ext->SanitizeOptions(dbOptions_));
  ASSERT_OK(ext->ConfigureFromString(dbOptions_, "mock.sanitize=false"));
  ASSERT_EQ(Status::InvalidArgument(), ext->SanitizeOptions(dbOptions_));
  ASSERT_OK(ext->ConfigureFromString(dbOptions_, nullptr,
				     "mock.sanitize=true;"
				     "unknown.options=x",
				     false, true));
  ASSERT_OK(ext->SanitizeOptions(dbOptions_));
  ASSERT_EQ(Status::NotFound(), 
	    ext->ConfigureFromString(dbOptions_,
				     "mock.sanitize=true;"
				     "unknown.options=x"));
  ASSERT_OK(ext->SanitizeOptions(dbOptions_));
}
  
TEST_P(ExtensionTestWithParam, ConfigureOptionsFromMap) {
  std::shared_ptr<MockExtension> guard;
  std::unordered_map<std::string, std::string> opt_map;
  opt_map["mock.sanitize"]="false";
  AssertNewSharedExtension(dbOptions_, pattern_, true, &guard);

  ASSERT_OK(guard->ConfigureFromMap(dbOptions_, opt_map));
  ASSERT_EQ(Status::InvalidArgument(), guard->SanitizeOptions(dbOptions_));

  opt_map["mock.sanitize"]="true";
  ASSERT_OK(guard->ConfigureFromMap(dbOptions_, opt_map));
  ASSERT_OK(guard->SanitizeOptions(dbOptions_));
  
  opt_map["unknown.options"]="true";
  ASSERT_OK(guard->ConfigureFromMap(dbOptions_, nullptr, opt_map, false, true));
  ASSERT_OK(guard->SanitizeOptions(dbOptions_));

  ASSERT_EQ(Status::NotFound(),
	    guard->ConfigureFromMap(dbOptions_, opt_map));
}

TEST_P(ExtensionTestWithParam, ConfigureFromProperties) {
  std::shared_ptr<MockExtension> ext;
  RegisterFactory("mock.*");
  AssertNewSharedExtension(dbOptions_, "mock.root", true, &ext);
  ASSERT_OK(ext->SetOption(dbOptions_, "mock.root.inner",
			   "name=mock.child;"
			   "options={mock.child.inner={name=mock.grand}}"));
  ASSERT_OK(ext->ConfigureFromString(dbOptions_,
				     "mock.root.inner={"
				     "name=mock.1;"
				     "options={mock.1.inner={name=mock.2;"
				     "options={mock.2.sanitize=true}}}}"));
  ASSERT_EQ(Status::InvalidArgument(),
	    ext->SetOption(dbOptions_, "mock.root.inner",
			   "name=bad.child;"));
  ASSERT_EQ(Status::NotFound(),
	    ext->SetOption(dbOptions_, "mock.root.inner",
			   "name=mock.child;"
			   "options={mock.child.unknown=unknown}"));
  ASSERT_EQ(Status::NotFound(),
	    ext->SetOption(dbOptions_, "mock.root.inner",
			   "name=mock.child;"
			   "options={mock.child.unknown=unknown}"));
}
  
INSTANTIATE_TEST_CASE_P(ExtensionFromOptionTest, ExtensionTestWithParam,
			::testing::Values(std::pair<bool, std::string>(false, "test.mock.opt")));
  //INSTANTIATE_TEST_CASE_P(ExtensionFromMapTest,  ExtensionTestWithParam, ::testing::Values(true));

#endif  // !ROCKSDB_LITE
}  // namespace rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
