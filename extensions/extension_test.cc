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
  int         intOpt = -1;
  bool        boolOpt = false;
  std::string strOpt = "unknown";
  std::shared_ptr<MockExtension> inner;
};

static OptionTypeMap mockOptionsMap =
{
 {"bool", {offsetof(struct MockOptions, boolOpt),
	       OptionType::kBoolean, OptionVerificationType::kByName,
	       false, 0}},
 {"int", {offsetof(struct MockOptions, intOpt),
	       OptionType::kInt, OptionVerificationType::kByName,
	       false, 0}},
 {"string", {offsetof(struct MockOptions, strOpt),
	       OptionType::kString, OptionVerificationType::kByName,
	       false, 0}},
 { "inner",   {offsetof(struct MockOptions, inner),
	       OptionType::kExtension, OptionVerificationType::kByName,
	       false, 0}}
};

static const std::string MockPrefix = "test.mock";

class MockExtension : public Extension {
private:
  const std::string name_;
  const std::string prefix_;
protected:
  using Extension::ParseExtension;
  using Extension::SanitizeOptions;
public:
  using Extension::ConfigureOption;
  MockOptions options_;
    static const char * Type() { return "test-extension"; };
public:
  MockExtension(const std::string & name,
		const std::string & prefix) : name_(name), prefix_(prefix) {
    reset();

  }
  virtual const std::string & GetOptionsPrefix() const override {
    return prefix_;
  }
  
  void reset() {
    if (options_.inner) {
      options_.inner->reset();
      options_.inner.reset();
    }
    options_.intOpt = -1;
    options_.boolOpt = false;
    options_.strOpt.clear();
  }
  
  virtual const char *Name() const override { return name_.c_str(); }

  virtual Status ParseExtension(const std::string & name, const std::string & value) override {
    Status status = Status::OK();
    if (! ConfigureExtension("inner", options_.inner.get(), name, value, &status)) {
      status = Extension::ParseExtension(name, value);
    }
    return status;
  }
  
  virtual Status ParseExtension(const DBOptions & dbOpts, const ColumnFamilyOptions *cfOpts,
				const std::string & name, const std::string & value) override {
    Status status = Status::OK();
    if (! ConfigureSharedExtension("inner", dbOpts, cfOpts, name, value, &options_.inner, &status)) {
      status = Extension::ParseExtension(dbOpts, cfOpts, name, value);
    }
    return status;
  }
  
  virtual Status ParseUnknown(const DBOptions & dbOpts, const ColumnFamilyOptions *cfOpts,
			      const std::string & name, const std::string & value) override {
    Status status = ParseUnknown(name, value);
    if (! status.ok() && options_.inner) {
      Status inner = options_.inner->ConfigureOption(dbOpts, cfOpts, name, value, false);
      if (inner.ok()) {
	return inner;
      }
    }
    return status;
  }
  
  virtual Status ParseUnknown(const std::string & name, const std::string & value) override {
    if (options_.inner) {
      Status status = options_.inner->ConfigureOption(name, value, false);
      if (! status.IsNotFound()) {
	return status;
      }
    }
    return Extension::ParseUnknown(name, value);
  }
  
  virtual Status SanitizeOptions(const DBOptions & dbOpts) const override {
    if (options_.inner) {
      Status s = options_.inner->SanitizeOptions(dbOpts);
      if (! s.ok()) {
	return s;
      }
    }
    return Extension::SanitizeOptions(dbOpts);
  }

  virtual Status SanitizeOptions() const override {
    if (options_.intOpt < 0) {
      return Status::InvalidArgument("Int option < 0");
    } else if (! options_.boolOpt) {
      return Status::InvalidArgument("Bool opt is false");
    } else if (options_.strOpt.empty()) {
      return Status::InvalidArgument("String option is not set");
    } else {
      return Extension::SanitizeOptions();
    }
  }  
};

class MockExtensionWithOptions : public MockExtension {
protected:
  using Extension::ParseExtension;
public:
  MockExtensionWithOptions(const std::string & name,
			   const std::string & prefix) :
    MockExtension(name, prefix) { }
  
  virtual Status ParseUnknown(const DBOptions & dbOpts, const ColumnFamilyOptions *cfOpts,
			      const std::string & name, const std::string & value) override {
    Status status = Status::OK();
    if (! ConfigureSharedExtension("inner", dbOpts, cfOpts, name, value, &options_.inner, &status)) {
      status = MockExtension::ParseUnknown(dbOpts, cfOpts, name, value);
    }
    return status;
  }
  

  virtual Status ParseUnknown(const std::string & name, const std::string & value) override {
    Status status = Status::OK();
    if (name == "bool") {
	options_.boolOpt = ParseBoolean(name, value);
    } else if (name == "int") {
	options_.intOpt = ParseInt(value);
    } else if (name == "string") {
	options_.strOpt = value;
    } else if (! ConfigureExtension("inner", options_.inner.get(), name, value, &status)) {
      status = MockExtension::ParseUnknown(name, value);
    }
    return status;
  }
};

class MockExtensionWithMap : public MockExtension {
public:
  MockExtensionWithMap(const std::string & name,
		       const std::string & prefix) :
    MockExtension(name, prefix) {
  }
  virtual const OptionTypeMap *GetOptionsMap() const override {
    return &mockOptionsMap;
  }
  
protected:
  virtual void *GetOptionsPtr() override { return &options_; }
};
  

#ifndef ROCKSDB_LITE
Extension *MockExtensionOptionsFactory(const std::string & name,
				       const DBOptions &,
				       const ColumnFamilyOptions *,
				       std::unique_ptr<Extension>* guard) {
  auto period = name.find_last_of('.');
  if (period != std::string::npos) {
    guard->reset(new MockExtensionWithOptions(name.substr(period + 1), name.substr(0, period + 1)));
  } else {
    guard->reset(new MockExtensionWithOptions(name, MockPrefix));
  }
  return guard->get();
}
  
Extension *MockExtensionMapFactory(const std::string & name,
				   const DBOptions &,
				   const ColumnFamilyOptions *,
				   std::unique_ptr<Extension>* guard) {
  auto period = name.find_last_of('.');
  if (period != std::string::npos) {
    guard->reset(new MockExtensionWithMap(name.substr(period + 1), name.substr(0, period + 1)));
  } else {
    guard->reset(new MockExtensionWithMap(name, MockPrefix));
  }
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
  ExtensionTest() {
  }
};

class ExtensionTestWithParam :
    public ExtensionTest, public ::testing::WithParamInterface<std::pair<std::string, std::string> > {
public:
  bool useMap_;
  std::string name_;
  std::string prefix_;
  ExtensionTestWithParam() {
    std::pair<std::string, std::string> param_pair = GetParam();
    name_    = param_pair.first;
    prefix_  = param_pair.second;
    useMap_  = name_.find("map") != std::string::npos;
    RegisterFactory(MockPrefix + "." + name_);
  }    

  void RegisterFactory(const std::string & pattern) {
    const bool useMap = useMap_;
    dbOptions_.extensions->RegisterFactory(
					   MockExtension::Type(),
					   pattern,
					   [useMap, pattern](const std::string & name,
					      const DBOptions &,
					      const ColumnFamilyOptions *,
					      std::unique_ptr<Extension> * guard) {
					     auto wildcard = pattern.find_last_of('*');
					     std::string prefix = MockPrefix;
					     if (wildcard != std::string::npos) {
					       auto period = name.find_last_of('.');
					       prefix = name.substr(period+1) + "." + MockPrefix;
					     }
					     if (useMap) { 
					       guard->reset(new MockExtensionWithMap(name, prefix + "."));
					     } else {
					       guard->reset(new MockExtensionWithOptions(name, prefix + "."));
					     }
					     return guard->get();
					   });
  }
  
  void AssertNewMockExtension(bool isValid, std::shared_ptr<MockExtension> * mock) {
    AssertNewSharedExtension(dbOptions_, MockPrefix + "." + name_, isValid, mock);
  }
};


static const std::string kUnknownType="unknown";
class UnknownExtension : public Extension {
public:
  static const std::string & Type() { return kUnknownType; }
  const char *Name() const override { return "unknown"; }
public:
};
  
TEST_F(ExtensionTest, UnknownExtensionType) {
  std::shared_ptr<UnknownExtension> unknown;
  unknown.reset(new UnknownExtension());
  AssertNewSharedExtension(dbOptions_, "bad.2", false, &unknown);
}
  
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
					 MockExtension::Type(),
					 "guarded",
					 [](const std::string & name,
					    const DBOptions &,
					    const ColumnFamilyOptions *,
					    std::unique_ptr<Extension> * guard) {
					   guard->reset(new MockExtension(name, name + "."));
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
  dbOptions_.extensions->RegisterFactory(MockExtension::Type(),
					 "unguarded",
					 [](const std::string & name,
					    const DBOptions &,
					    const ColumnFamilyOptions *,
					    std::unique_ptr<Extension> * guard) {
					   guard->reset();
					   return new MockExtension(name, name + ".");
					 });
  AssertNewUniqueExtension(dbOptions_, "unguarded", true, &ext, &guard, false);
  Status status = NewSharedExtension("unguarded", dbOptions_, nullptr, &shared);
  ASSERT_TRUE(status.IsNotSupported());
  ASSERT_EQ(shared.get(), nullptr);
  delete ext;
}
  
TEST_F(ExtensionTest, CreateFromPattern) {
  std::shared_ptr<MockExtension> ext;
  dbOptions_.extensions->RegisterFactory(MockExtension::Type(), "good.*",
					 MockExtensionOptionsFactory);
  AssertNewSharedExtension(dbOptions_, "good.1", true, &ext, "1");
  AssertNewSharedExtension(dbOptions_, "good.2", true, &ext, "2");
  AssertNewSharedExtension(dbOptions_, "bad.2", false, &ext);
}

#define AssertInnerExtension(expected, result, inner, name)	\
  { if (expected.ok()) {	      \
      ASSERT_OK(result);              \
      ASSERT_NE(inner, nullptr);      \
      ASSERT_EQ(inner->Name(), name); \
    } else {                          \
      ASSERT_EQ(expected, result);    \
      ASSERT_EQ(inner, nullptr);      \
    }                                 \
  }

static Status OK = Status::OK();
static Status NotFound = Status::NotFound();
static Status Invalid  = Status::InvalidArgument();
  
#define AssertConfigureProperty(expected, result, actual, value)	\
  { if (expected.ok()) {	      \
      ASSERT_OK(result); ASSERT_EQ(actual, value); \
    } else {                          \
      ASSERT_EQ(expected, result);    \
    }                                 \
  }

TEST_P(ExtensionTestWithParam, BadConversions) {
  std::shared_ptr<MockExtension> extension;
  AssertNewMockExtension(true, &extension);
  //AssertConfigureProperty(Invalid, extension->ConfigureOption(prefix_ + "bool",   "1"),      true, extension->options_.boolOpt);
  AssertConfigureProperty(Invalid, extension->ConfigureOption(prefix_ + "bool",   "string"), true, extension->options_.boolOpt);
  AssertConfigureProperty(Invalid, extension->ConfigureOption(prefix_ + "int",    "string"),     1, extension->options_.intOpt);
  AssertNewMockExtension(true, &extension->options_.inner);
  AssertConfigureProperty(Invalid, extension->ConfigureOption(prefix_ + "inner.options", "bool=string"), true, extension->options_.inner->options_.boolOpt);
  AssertConfigureProperty(Invalid, extension->ConfigureOption(prefix_ + "inner.options", "int=string"),     1, extension->options_.inner->options_.intOpt);
  extension->options_.inner->reset();
  AssertConfigureProperty(Invalid, extension->ConfigureOption(prefix_ + "inner", "options=bool=string;name="+MockPrefix + "." + name_), true, extension->options_.inner->options_.boolOpt);
  extension->options_.inner->reset();
  AssertConfigureProperty(Invalid, extension->ConfigureOption(prefix_ + "inner", "options=int=string;name="+MockPrefix + "." + name_),     1, extension->options_.inner->options_.intOpt);

}
  
TEST_P(ExtensionTestWithParam, ConfigureOptions) {
  std::shared_ptr<MockExtension> extension;
  AssertNewMockExtension(true, &extension);
  ASSERT_EQ(Invalid, extension->SanitizeOptions(dbOptions_));
  AssertConfigureProperty(NotFound, extension->ConfigureOption("unknown", "bad"), true, true);
  AssertConfigureProperty(OK,       extension->ConfigureOption(prefix_ + "bool",   "true"), true, extension->options_.boolOpt);
  AssertConfigureProperty(OK,       extension->ConfigureOption(prefix_ + "int",    "1"),       1, extension->options_.intOpt);
  AssertConfigureProperty(OK,       extension->ConfigureOption(prefix_ + "string", "hi"),   "hi", extension->options_.strOpt);
  ASSERT_OK(extension->SanitizeOptions(dbOptions_));
}

TEST_P(ExtensionTestWithParam, ConfigureOptionsFromString) {
  std::shared_ptr<MockExtension> ext;
  AssertNewMockExtension(true, &ext);
  AssertConfigureProperty(OK, ext->ConfigureFromString(dbOptions_,
						       prefix_+ "int=1"), 1, ext->options_.intOpt);
  AssertConfigureProperty(OK, ext->ConfigureFromString(dbOptions_,
						       prefix_ + "bool=true;" +
						       prefix_ + "int=2;" +
						       prefix_ + "string=string"), 2, ext->options_.intOpt);
  ASSERT_EQ(true, ext->options_.boolOpt);
  ASSERT_EQ("string", ext->options_.strOpt);
  ASSERT_OK(ext->ConfigureFromString(dbOptions_, nullptr, prefix_ + "unknown=x",false, true));
  AssertConfigureProperty(NotFound, ext->ConfigureFromString(dbOptions_, prefix_ + "unknown.options=x"), true, true);
}

TEST_P(ExtensionTestWithParam, ConfigureOptionsFromMap) {
  std::shared_ptr<MockExtension> ext;
  std::unordered_map<std::string, std::string> opt_map;
  AssertNewMockExtension(true, &ext);
  opt_map[prefix_ + "bool"]="true";
  opt_map[prefix_ + "int"]="1";
  opt_map[prefix_ + "string"]="string";

  AssertConfigureProperty(OK, ext->ConfigureFromMap(dbOptions_, opt_map), 1, ext->options_.intOpt);
  ASSERT_EQ(true, ext->options_.boolOpt);
  ASSERT_EQ("string", ext->options_.strOpt);

  opt_map[prefix_ + "unknown.options"]="true";
  AssertConfigureProperty(OK, ext->ConfigureFromMap(dbOptions_, nullptr, opt_map, false, true), true, ext->options_.boolOpt);
  AssertConfigureProperty(NotFound, ext->ConfigureFromMap(dbOptions_, opt_map), "string", ext->options_.strOpt);
}

TEST_P(ExtensionTestWithParam, ParseExtensionOptions) {
  std::string onePrefix = std::string("one.") + MockPrefix + ".";
  std::string oneName   = MockPrefix + "." + name_ + ".one";
  std::string twoPrefix = std::string("two.") + MockPrefix + ".";
  std::string twoName   = MockPrefix + "." + name_ + ".two";
  
  RegisterFactory(MockPrefix + ".*");
  std::shared_ptr<MockExtension> ext;
  AssertNewMockExtension(true, &ext);

  // Cannot find inner options until an inner exists
  AssertConfigureProperty(NotFound, ext->ConfigureOption(dbOptions_, onePrefix + "int", "1"), true, true);
  AssertConfigureProperty(NotFound, ext->ConfigureOption(dbOptions_, twoPrefix + "int", "1"), true, true);
  
  // Cannot create inner extension without the dbOptions
  AssertInnerExtension(NotFound, ext->ConfigureOption(prefix_ + "inner.name", oneName), ext->options_.inner, oneName);
  AssertInnerExtension(NotFound, ext->ConfigureOption(prefix_ + "inner", std::string("name=") + oneName), ext->options_.inner, oneName);

  // Cannot configure inner extension without an one...
  AssertInnerExtension(NotFound, ext->ConfigureOption(prefix_ + "inner.options", "int=1"), ext->options_.inner, oneName);
  AssertInnerExtension(NotFound, ext->ConfigureOption(prefix_ + "inner", "int=1"), ext->options_.inner, oneName);
  // ... Even if dbOptions are specified
  AssertInnerExtension(NotFound, ext->ConfigureOption(dbOptions_, prefix_ + "inner.options", "int=1"), ext->options_.inner, oneName);
  AssertInnerExtension(NotFound, ext->ConfigureOption(dbOptions_, prefix_ + "inner", "int=1"), ext->options_.inner, oneName);

  // Cannot create an inner extension with a bad name
  AssertInnerExtension(Invalid, ext->ConfigureOption(dbOptions_, prefix_ + "inner.name", "bad"), ext->options_.inner, "bad");

  // Create a valid inner extension via "inner.name"
  AssertInnerExtension(OK, ext->ConfigureOption(dbOptions_, prefix_ + "inner.name", oneName), ext->options_.inner, oneName);
  // No we can set a simple property with or without the dbOptions argument
  AssertConfigureProperty(OK, ext->ConfigureOption(            prefix_ + "inner.options", "int=1"), 1, ext->options_.inner->options_.intOpt);
  AssertConfigureProperty(OK, ext->ConfigureOption(dbOptions_, prefix_ + "inner.options", "int=2"), 2, ext->options_.inner->options_.intOpt);
  AssertConfigureProperty(OK, ext->ConfigureOption(dbOptions_, onePrefix + "int",        "3"),    3, ext->options_.inner->options_.intOpt);
  // Even though "one" exists, two does not and its properties cannot be set
  AssertConfigureProperty(NotFound, ext->ConfigureOption(dbOptions_, twoPrefix + "int", "1"), true, true);
  ext->options_.inner.reset(); ASSERT_EQ(ext->options_.inner, nullptr);

  // Create a valid inner extension via "inner" and no options
  AssertInnerExtension(OK, ext->ConfigureOption(dbOptions_, prefix_ + "inner", std::string("name=") + oneName), ext->options_.inner, oneName);
  AssertConfigureProperty(OK, ext->ConfigureOption(prefix_ + "inner.options", "int=1"), 1, ext->options_.inner->options_.intOpt);
  ext->options_.inner.reset(); ASSERT_EQ(ext->options_.inner, nullptr);

  // Create a valid inner extension via "inner" and options
  AssertInnerExtension(OK, ext->ConfigureOption(dbOptions_, prefix_ + "inner", std::string("int=4;name=") + oneName), ext->options_.inner, oneName);
  ASSERT_EQ(4, ext->options_.inner->options_.intOpt);

  // Still cannot create an extension with a bad name...
  AssertConfigureProperty(Invalid, ext->ConfigureOption(dbOptions_, prefix_ + "inner.name", "bad"), true, true);
  // ... but it doesn't change the one that already existed
  ASSERT_NE(ext->options_.inner, nullptr);
  ASSERT_EQ(ext->options_.inner->Name(), oneName);
  ASSERT_EQ(4, ext->options_.inner->options_.intOpt);
  // ... but a second name can replace the first instance
  AssertInnerExtension(OK, ext->ConfigureOption(dbOptions_, prefix_ + "inner", std::string("name=") + twoName), ext->options_.inner, twoName);
  
  // Now attempt to set up an "inner inner"
  AssertInnerExtension(OK, ext->ConfigureOption(dbOptions_, twoPrefix + "inner.name", oneName), ext->options_.inner->options_.inner, oneName);
  // And we can set both one and two prefix options
  AssertConfigureProperty(OK, ext->ConfigureOption(dbOptions_, onePrefix + "int", "1"), 1, ext->options_.inner->options_.inner->options_.intOpt);
  AssertConfigureProperty(OK, ext->ConfigureOption(dbOptions_, twoPrefix + "int", "2"), 2, ext->options_.inner->options_.intOpt);
  ASSERT_EQ(1,  ext->options_.inner->options_.inner->options_.intOpt);
  ext->options_.inner.reset(); ASSERT_EQ(ext->options_.inner, nullptr);
  // Now set both of "one" and "two" in one command
  AssertInnerExtension(OK, ext->ConfigureOption(dbOptions_, prefix_ + "inner",
						std::string("name=") + oneName + ";" +
						"int=10;" + 
						"inner={name=" + twoName + "; int=20}"), ext->options_.inner, oneName);
  ASSERT_NE(ext->options_.inner->options_.inner, nullptr);
  ASSERT_EQ(ext->options_.inner->options_.inner->Name(), twoName);
  ASSERT_EQ(10, ext->options_.inner->options_.intOpt);
  ASSERT_EQ(20, ext->options_.inner->options_.inner->options_.intOpt);
}
  
  

TEST_P(ExtensionTestWithParam, ParseExtensionFromString) {
  std::string onePrefix = std::string("one.") + MockPrefix + ".";
  std::string oneName   = MockPrefix + "." + name_ + ".one";
  std::string twoPrefix = std::string("two.") + MockPrefix + ".";
  std::string twoName   = MockPrefix + "." + name_ + ".two";
  
  RegisterFactory(MockPrefix + ".*");
  std::shared_ptr<MockExtension> ext;
  AssertNewMockExtension(true, &ext);

  // Cannot find inner options until an inner exists
  AssertConfigureProperty(NotFound, ext->ConfigureFromString(dbOptions_, onePrefix + "int=1"), true, true);
  AssertConfigureProperty(NotFound, ext->ConfigureFromString(dbOptions_, twoPrefix + "int=1"), true, true);
  
  // Cannot create inner extension without the dbOptions
  AssertInnerExtension(NotFound, ext->ConfigureFromString(prefix_ + "inner.name=" + oneName), ext->options_.inner, oneName);
  AssertInnerExtension(NotFound, ext->ConfigureFromString(prefix_ + "inner=name=" + oneName), ext->options_.inner, oneName);

  // Cannot configure inner extension without an one...
  AssertInnerExtension(NotFound, ext->ConfigureFromString(prefix_ + "inner.options=int=1"), ext->options_.inner, oneName);
  AssertInnerExtension(NotFound, ext->ConfigureFromString(prefix_ + "inner=int=1"), ext->options_.inner, oneName);
  // ... Even if dbOptions are specified
  AssertInnerExtension(NotFound, ext->ConfigureFromString(dbOptions_, prefix_ + "inner.options=int=1"), ext->options_.inner, oneName);
  AssertInnerExtension(NotFound, ext->ConfigureFromString(dbOptions_, prefix_ + "inner=int=1"), ext->options_.inner, oneName);

  // Cannot create an inner extension with a bad name
  AssertInnerExtension(Invalid, ext->ConfigureFromString(dbOptions_, prefix_ + "inner.name=bad"), ext->options_.inner, "bad");

  // Create a valid inner extension via "inner.name"
  AssertInnerExtension(OK, ext->ConfigureFromString(dbOptions_, prefix_ + "inner.name=" + oneName), ext->options_.inner, oneName);
  // No we can set a simple property with or without the dbOptions argument
  AssertConfigureProperty(OK, ext->ConfigureFromString(            prefix_ + "inner.options=int=1"), 1, ext->options_.inner->options_.intOpt);
  AssertConfigureProperty(OK, ext->ConfigureFromString(dbOptions_, prefix_ + "inner.options=int=2"), 2, ext->options_.inner->options_.intOpt);
  AssertConfigureProperty(OK, ext->ConfigureFromString(dbOptions_, onePrefix + "int=3"),    3, ext->options_.inner->options_.intOpt);
  // Even though "one" exists, two does not and its properties cannot be set
  AssertConfigureProperty(NotFound, ext->ConfigureFromString(dbOptions_, twoPrefix + "int=4"), 3, ext->options_.inner->options_.intOpt);
  ext->options_.inner.reset(); ASSERT_EQ(ext->options_.inner, nullptr);

  // Create a valid inner extension via "inner" via name and options
  AssertInnerExtension(OK, ext->ConfigureFromString(dbOptions_,
						    prefix_ + "inner.name=" + oneName + ";" + 
						    prefix_ + "inner.options=int=1"), ext->options_.inner, oneName);
  ASSERT_EQ(ext->options_.inner->options_.intOpt, 1);
  ext->options_.inner.reset(); ASSERT_EQ(ext->options_.inner, nullptr);
  
  // Create a valid inner extension via "inner" and options
  AssertInnerExtension(OK, ext->ConfigureFromString(dbOptions_, prefix_ + "inner={int=4;name=" + oneName + "}"), ext->options_.inner, oneName);
  ASSERT_EQ(4, ext->options_.inner->options_.intOpt);

  // Still cannot create an extension with a bad name...
  AssertConfigureProperty(Invalid, ext->ConfigureFromString(dbOptions_, prefix_ + "inner.name=bad"), true, true);
  // ... but it doesn't change the one that already existed
  ASSERT_NE(ext->options_.inner, nullptr);
  ASSERT_EQ(ext->options_.inner->Name(), oneName);
  ASSERT_EQ(4, ext->options_.inner->options_.intOpt);
  // ... but a second name can replace the first instance
  AssertInnerExtension(OK, ext->ConfigureFromString(dbOptions_, prefix_ + "inner={name=" + twoName + "}"), ext->options_.inner, twoName);
  
  // Now attempt to set up an "inner inner"
  AssertInnerExtension(OK, ext->ConfigureFromString(dbOptions_,
						    twoPrefix + "inner.name=" + oneName + ";" + 
						    onePrefix + "int=1;" + twoPrefix + "int=2"),
		       ext->options_.inner->options_.inner, oneName);
  ASSERT_EQ(1, ext->options_.inner->options_.inner->options_.intOpt);
  ASSERT_EQ(2, ext->options_.inner->options_.intOpt);
  ext->options_.inner.reset(); ASSERT_EQ(ext->options_.inner, nullptr);
  // Now set both of "one" and "two" in one command
  AssertInnerExtension(OK, ext->ConfigureFromString(dbOptions_,
						    prefix_ + "inner={name=" + oneName + ";" +
						    "int=20;" + "inner={name=" + twoName + "; int=30}};" +
						    prefix_ + "int=10"), ext->options_.inner, oneName);
  ASSERT_NE(ext->options_.inner->options_.inner, nullptr);
  ASSERT_EQ(ext->options_.inner->options_.inner->Name(), twoName);
  ASSERT_EQ(10, ext->options_.intOpt);
  ASSERT_EQ(20, ext->options_.inner->options_.intOpt);
  ASSERT_EQ(30, ext->options_.inner->options_.inner->options_.intOpt);
  // Now set both of "one" and "two" in one command over multiple options
  ext->options_.inner.reset(); ASSERT_EQ(ext->options_.inner, nullptr);
  AssertInnerExtension(OK, ext->ConfigureFromString(dbOptions_,
						    prefix_ + "int=100;" +
						    prefix_ + "inner={name=" + oneName + "}; " +
						    onePrefix + "inner.name=" + twoName + "; " +
						    onePrefix + "inner.options=int=300;" +
						    onePrefix + "int=200"),
		       ext->options_.inner, oneName);
  ASSERT_NE(ext->options_.inner->options_.inner, nullptr);
  ASSERT_EQ(ext->options_.inner->options_.inner->Name(), twoName);
  ASSERT_EQ(100, ext->options_.intOpt);
  ASSERT_EQ(200, ext->options_.inner->options_.intOpt);
  ASSERT_EQ(300, ext->options_.inner->options_.inner->options_.intOpt);

}

TEST_P(ExtensionTestWithParam, ParseExtensionFromMap) {
  std::string onePrefix = std::string("one.") + MockPrefix + ".";
  std::string oneName   = MockPrefix + "." + name_ + ".one";
  std::string twoPrefix = std::string("two.") + MockPrefix + ".";
  std::string twoName   = MockPrefix + "." + name_ + ".two";
  
  RegisterFactory(MockPrefix + ".*");
  std::shared_ptr<MockExtension> ext;
  std::unordered_map<std::string, std::string> options;
  AssertNewMockExtension(true, &ext);

  // Cannot find inner options until an inner exists
  options.clear(); options[onePrefix + "int"] = "1"; AssertConfigureProperty(NotFound, ext->ConfigureFromMap(dbOptions_, options), true, true);
  options.clear(); options[twoPrefix + "int"] = "2"; AssertConfigureProperty(NotFound, ext->ConfigureFromMap(dbOptions_, options), true, true);
  
  // Cannot create inner extension without the dbOptions
  options.clear(); options[prefix_ + "inner.name"] = oneName;      AssertInnerExtension(NotFound, ext->ConfigureFromMap(options), ext->options_.inner, oneName);
  options.clear(); options[prefix_ + "inner"] = "name=" + oneName; AssertInnerExtension(NotFound, ext->ConfigureFromMap(options), ext->options_.inner, oneName);

  // Cannot configure inner extension without an one...
  options.clear(); options[prefix_ + "inner.options"] = "int=1";
  AssertInnerExtension(NotFound, ext->ConfigureFromMap(options), ext->options_.inner, oneName);
  AssertInnerExtension(NotFound, ext->ConfigureFromMap(dbOptions_, options), ext->options_.inner, oneName);

  options.clear(); options[prefix_ + "inner"] = "int=1";
  AssertInnerExtension(NotFound, ext->ConfigureFromMap(options), ext->options_.inner, oneName);
  AssertInnerExtension(NotFound, ext->ConfigureFromMap(dbOptions_, options), ext->options_.inner, oneName);

  // Cannot create an inner extension with a bad name
  options.clear(); options[prefix_ + "inner.name"] = "bad"; AssertInnerExtension(Invalid, ext->ConfigureFromMap(dbOptions_, options), ext->options_.inner, "bad");

  // Create a valid inner extension via "inner.name"
  options[prefix_ + "inner.name"] = oneName;
  AssertInnerExtension(OK, ext->ConfigureFromMap(dbOptions_, options), ext->options_.inner, oneName);
  // No we can set a simple property with or without the dbOptions argument
  options.clear();
  options[prefix_ + "inner.options"]="int=1"; AssertConfigureProperty(OK, ext->ConfigureFromMap(            options), 1, ext->options_.inner->options_.intOpt);
  options[prefix_ + "inner.options"]="int=2"; AssertConfigureProperty(OK, ext->ConfigureFromMap(dbOptions_, options), 2, ext->options_.inner->options_.intOpt);
  options.clear();
  options[onePrefix + "int"]="3"; AssertConfigureProperty(OK,       ext->ConfigureFromMap(dbOptions_, options), 3, ext->options_.inner->options_.intOpt);
  // Even though "one" exists, two does not and its properties cannot be set
  options[twoPrefix + "int"]="4"; AssertConfigureProperty(NotFound, ext->ConfigureFromMap(dbOptions_, options), 3, ext->options_.inner->options_.intOpt);
  ext->options_.inner.reset(); ASSERT_EQ(ext->options_.inner, nullptr);

  // Create a valid inner extension via "inner" via name and options
  options.clear();
  options[prefix_ + "inner.name"]    = oneName;
  options[prefix_ + "inner.options"] = "int=1";
  AssertInnerExtension(OK, ext->ConfigureFromMap(dbOptions_, options), ext->options_.inner, oneName);
  ASSERT_EQ(ext->options_.inner->options_.intOpt, 1);

  // Still cannot create an extension with a bad name...
  options[prefix_ + "inner.name"]    = "bad";
  AssertConfigureProperty(Invalid, ext->ConfigureFromMap(dbOptions_, options), true, true);
  // ... but it doesn't change the one that already existed
  ASSERT_NE(ext->options_.inner, nullptr);
  ASSERT_EQ(ext->options_.inner->Name(), oneName);
  ASSERT_EQ(1, ext->options_.inner->options_.intOpt);

  // ... but a second name can replace the first instance
  options.clear();
  options[prefix_ + "inner"] = "int=4;name=" + twoName;
  AssertInnerExtension(OK, ext->ConfigureFromMap(dbOptions_, options), ext->options_.inner, twoName);
  
  // Now attempt to set up an "inner inner"
  options.clear();
  options[twoPrefix + "inner.name"] = oneName;
  options[onePrefix + "int"]="1";
  options[twoPrefix + "int"]="2";
  
  AssertInnerExtension(OK, ext->ConfigureFromMap(dbOptions_, options), ext->options_.inner->options_.inner, oneName);
  ASSERT_EQ(1, ext->options_.inner->options_.inner->options_.intOpt);
  ASSERT_EQ(2, ext->options_.inner->options_.intOpt);
  ext->options_.inner.reset(); ASSERT_EQ(ext->options_.inner, nullptr);
  // Now set both of "one" and "two" in one command
  options.clear();
  ext->options_.inner.reset(); ASSERT_EQ(ext->options_.inner, nullptr);
  options[prefix_ + "int"]= "10";
  options[prefix_ + "inner"]="name=" + oneName + ";int=20; inner={int=30;name=" + twoName + "}";
  AssertInnerExtension(OK, ext->ConfigureFromMap(dbOptions_, options), ext->options_.inner, oneName);
  ASSERT_NE(ext->options_.inner->options_.inner, nullptr);
  ASSERT_EQ(ext->options_.inner->options_.inner->Name(), twoName);
  ASSERT_EQ(10, ext->options_.intOpt);
  ASSERT_EQ(20, ext->options_.inner->options_.intOpt);
  ASSERT_EQ(30, ext->options_.inner->options_.inner->options_.intOpt);
  // Now set both of "one" and "two" in one command over multiple options
  options.clear();
  ext->options_.inner.reset(); ASSERT_EQ(ext->options_.inner, nullptr);
  options[prefix_   + "int"]= "100";
  options[prefix_   + "inner.name"]=oneName;
  options[onePrefix + "inner.name"] = twoName;
  options[onePrefix + "int"]= "200";
  options[twoPrefix + "int"]= "300";
  AssertInnerExtension(OK, ext->ConfigureFromMap(dbOptions_, options), ext->options_.inner, oneName);
  ASSERT_NE(ext->options_.inner->options_.inner, nullptr);
  ASSERT_EQ(ext->options_.inner->options_.inner->Name(), twoName);
  ASSERT_EQ(100, ext->options_.intOpt);
  ASSERT_EQ(200, ext->options_.inner->options_.intOpt);
  ASSERT_EQ(300, ext->options_.inner->options_.inner->options_.intOpt);
}

INSTANTIATE_TEST_CASE_P(ExtensionFromOptionTest, ExtensionTestWithParam, 
			::testing::Values(std::pair<std::string, std::string>("opt", MockPrefix + "."), 
					  std::pair<std::string, std::string>("opt", "")
					  ));
INSTANTIATE_TEST_CASE_P(ExtensionFromMapTest,  ExtensionTestWithParam, 
			::testing::Values(std::pair<std::string, std::string>("map", MockPrefix + "."), 
					  std::pair<std::string, std::string>("map", "")
					  ));

#endif  // !ROCKSDB_LITE
}  // namespace rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
