//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/customizable.h"

#include <cctype>
#include <cinttypes>
#include <cstring>
#include <unordered_map>

#include "cache/lru_cache.h"
#include "options/customizable_helper.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "rocksdb/convenience.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/memory_allocator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "table/mock_table.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace ROCKSDB_NAMESPACE {
class StringLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    char buffer[1000];
    vsnprintf(buffer, sizeof(buffer), format, ap);
    string_.append(buffer);
  }
  const std::string& str() const { return string_; }
  void clear() { string_.clear(); }

 private:
  std::string string_;
};

class TestCustomizable : public Customizable {
 public:
  TestCustomizable(const std::string& name) : name_(name) {}
  const char* Name() const override { return name_.c_str(); }
  static const char* Type() { return "test.custom"; }
  static Status CreateFromString(const ConfigOptions& opts,
                                 const std::string& value,
                                 std::unique_ptr<TestCustomizable>* result);
  static Status CreateFromString(const ConfigOptions& opts,
                                 const std::string& value,
                                 std::shared_ptr<TestCustomizable>* result);
  static Status CreateFromString(const ConfigOptions& opts,
                                 const std::string& value,
                                 TestCustomizable** result);
  const Customizable* FindInstance(const std::string& name) const override {
    if (name == "TestCustomizable") {
      return this;
    } else {
      return Customizable::FindInstance(name);
    }
  }

 protected:
  const std::string name_;
};

struct AOptions {
  int i = 0;
  bool b = false;
};

static std::unordered_map<std::string, OptionTypeInfo> a_option_info = {
#ifndef ROCKSDB_LITE
    {"int",
     {offsetof(struct AOptions, i), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"bool",
     {offsetof(struct AOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
#endif  // ROCKSDB_LITE
};
class ACustomizable : public TestCustomizable {
 public:
  ACustomizable(const std::string& id) : TestCustomizable("A"), id_(id) {
    RegisterOptions("A", &opts_, &a_option_info);
  }
  std::string GetId() const override { return id_; }

 private:
  AOptions opts_;
  const std::string id_;
};

#ifndef ROCKSDB_LITE
static int A_count = 0;
const FactoryFunc<TestCustomizable>& a_func =
    ObjectLibrary::Default()->Register<TestCustomizable>(
        "A.*",
        [](const std::string& name, std::unique_ptr<TestCustomizable>* guard,
           std::string* /* msg */) {
          guard->reset(new ACustomizable(name));
          A_count++;
          return guard->get();
        });
#endif  // ROCKSDB_LITE

struct BOptions {
  std::string s;
  bool b = false;
};

static std::unordered_map<std::string, OptionTypeInfo> b_option_info = {
#ifndef ROCKSDB_LITE
    {"string",
     {offsetof(struct BOptions, s), OptionType::kString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"bool",
     {offsetof(struct BOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
#endif  // ROCKSDB_LITE
};

class BCustomizable : public TestCustomizable {
 private:
 public:
  BCustomizable(const std::string& name) : TestCustomizable(name) {
    RegisterOptions(name, &opts_, &b_option_info);
  }

 private:
  BOptions opts_;
};

static bool LoadSharedB(const std::string& id,
                        std::shared_ptr<TestCustomizable>* result) {
  if (id == "B") {
    result->reset(new BCustomizable(id));
    return true;
  } else if (id.empty()) {
    result->reset();
    return true;
  } else {
    return false;
  }
}
Status TestCustomizable::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<TestCustomizable>* result) {
  return LoadSharedObject<TestCustomizable>(config_options, value, LoadSharedB,
                                            result);
}

Status TestCustomizable::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::unique_ptr<TestCustomizable>* result) {
  return LoadUniqueObject<TestCustomizable>(
      config_options, value,
      [](const std::string& id, std::unique_ptr<TestCustomizable>* u) {
        if (id == "B") {
          u->reset(new BCustomizable(id));
          return true;
        } else if (id.empty()) {
          u->reset();
          return true;
        } else {
          return false;
        }
      },
      result);
}

Status TestCustomizable::CreateFromString(const ConfigOptions& config_options,
                                          const std::string& value,
                                          TestCustomizable** result) {
  return LoadStaticObject<TestCustomizable>(
      config_options, value,
      [](const std::string& id, TestCustomizable** ptr) {
        if (id == "B") {
          *ptr = new BCustomizable(id);
          return true;
        } else if (id.empty()) {
          *ptr = nullptr;
          return true;
        } else {
          return false;
        }
      },
      result);
}

#ifndef ROCKSDB_LITE
const FactoryFunc<TestCustomizable>& s_func =
    ObjectLibrary::Default()->Register<TestCustomizable>(
        "S", [](const std::string& name,
                std::unique_ptr<TestCustomizable>* /* guard */,
                std::string* /* msg */) { return new BCustomizable(name); });
#endif  // ROCKSDB_LITE

struct SimpleOptions {
  bool b = true;
  std::unique_ptr<TestCustomizable> cu;
  std::shared_ptr<TestCustomizable> cs;
  TestCustomizable* cp = nullptr;
};

static std::unordered_map<std::string, OptionTypeInfo> simple_option_info = {
#ifndef ROCKSDB_LITE
    {"bool",
     {offsetof(struct SimpleOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"unique", OptionTypeInfo::AsCustomU<TestCustomizable>(
                   offsetof(struct SimpleOptions, cu),
                   OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
    {"shared", OptionTypeInfo::AsCustomS<TestCustomizable>(
                   offsetof(struct SimpleOptions, cs),
                   OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
    {"pointer", OptionTypeInfo::AsCustomP<TestCustomizable>(
                    offsetof(struct SimpleOptions, cp),
                    OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
#endif  // ROCKSDB_LITE
};

class SimpleConfigurable : public Configurable {
 private:
  SimpleOptions simple_;

 public:
  SimpleConfigurable() {
    RegisterOptions("simple", &simple_, &simple_option_info);
  }

  SimpleConfigurable(
      const std::unordered_map<std::string, OptionTypeInfo>* map) {
    RegisterOptions("simple", &simple_, map);
  }
};

class CustomizableTest : public testing::Test {
 public:
  ConfigOptions config_options_;
};

#ifndef ROCKSDB_LITE  // GetOptionsFromMap is not supported in ROCKSDB_LITE
// Tests that a Customizable can be created by:
//    - a simple name
//    - a XXX.id option
//    - a property with a name
TEST_F(CustomizableTest, CreateByNameTest) {
  ObjectLibrary::Default()->Register<TestCustomizable>(
      "TEST.*",
      [](const std::string& name, std::unique_ptr<TestCustomizable>* guard,
         std::string* /* msg */) {
        guard->reset(new TestCustomizable(name));
        return guard->get();
      });
  std::unique_ptr<Configurable> configurable(new SimpleConfigurable());
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_OK(
      configurable->ConfigureFromString(config_options_, "unique={id=TEST_1}"));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "TEST_1");
  ASSERT_OK(
      configurable->ConfigureFromString(config_options_, "unique.id=TEST_2"));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "TEST_2");
  ASSERT_OK(
      configurable->ConfigureFromString(config_options_, "unique=TEST_3"));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "TEST_3");
}

TEST_F(CustomizableTest, ToStringTest) {
  std::unique_ptr<TestCustomizable> custom(new TestCustomizable("test"));
  ASSERT_EQ(custom->ToString(config_options_), "test");
}

TEST_F(CustomizableTest, SimpleConfigureTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique", "id=A;int=1;bool=true"},
      {"shared", "id=B;string=s"},
  };
  std::unique_ptr<Configurable> configurable(new SimpleConfigurable());
  ASSERT_OK(configurable->ConfigureFromMap(config_options_, opt_map));
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "A");
  std::string opt_str;
  ASSERT_OK(configurable->GetOptionString(config_options_, &opt_str));
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromString(config_options_, opt_str));
  ASSERT_TRUE(configurable->Matches(config_options_, copy.get()));
}

static void GetMapFromProperties(
    const std::string& props,
    std::unordered_map<std::string, std::string>* map) {
  std::istringstream iss(props);
  std::unordered_map<std::string, std::string> copy_map;
  std::string line;
  map->clear();
  for (int line_num = 0; std::getline(iss, line); line_num++) {
    std::string name;
    std::string value;
    ASSERT_OK(
        RocksDBOptionsParser::ParseStatement(&name, &value, line, line_num));
    (*map)[name] = value;
  }
}

TEST_F(CustomizableTest, ConfigureFromPropsTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique.id", "A"}, {"unique.A.int", "1"},    {"unique.A.bool", "true"},
      {"shared.id", "B"}, {"shared.B.string", "s"},
  };
  std::unique_ptr<Configurable> configurable(new SimpleConfigurable());
  ASSERT_OK(configurable->ConfigureFromMap(config_options_, opt_map));
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "A");
  std::string opt_str;
  config_options_.delimiter = "\n";
  std::unordered_map<std::string, std::string> props;
  ASSERT_OK(configurable->GetOptionString(config_options_, &opt_str));
  GetMapFromProperties(opt_str, &props);
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromMap(config_options_, props));
  ASSERT_TRUE(configurable->Matches(config_options_, copy.get()));
}

TEST_F(CustomizableTest, ConfigureFromShortTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique.id", "A"}, {"unique.A.int", "1"},    {"unique.A.bool", "true"},
      {"shared.id", "B"}, {"shared.B.string", "s"},
  };
  std::unique_ptr<Configurable> configurable(new SimpleConfigurable());
  ASSERT_OK(configurable->ConfigureFromMap(config_options_, opt_map));
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "A");
}

TEST_F(CustomizableTest, MatchesOptionsTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique", "id=A;int=1;bool=true"},
      {"shared", "id=A;int=1;bool=true"},
  };
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  std::unique_ptr<Configurable> c2(new SimpleConfigurable());
  ASSERT_OK(c1->ConfigureFromMap(config_options_, opt_map));
  ASSERT_OK(c2->ConfigureFromMap(config_options_, opt_map));
  ASSERT_TRUE(c1->Matches(config_options_, c2.get()));
  SimpleOptions* simple = c1->GetOptions<SimpleOptions>("simple");
  ASSERT_TRUE(simple->cu->Matches(config_options_, simple->cs.get()));
  ASSERT_OK(simple->cu->ConfigureOption(config_options_, "int", "2"));
  ASSERT_FALSE(simple->cu->Matches(config_options_, simple->cs.get()));
  ASSERT_FALSE(c1->Matches(config_options_, c2.get()));
  ConfigOptions loosely = config_options_;
  loosely.sanity_level = ConfigOptions::kSanityLevelLooselyCompatible;
  ASSERT_TRUE(c1->Matches(loosely, c2.get()));
  ASSERT_TRUE(simple->cu->Matches(loosely, simple->cs.get()));

  ASSERT_OK(c1->ConfigureOption(config_options_, "shared", "id=B;string=3"));
  ASSERT_TRUE(c1->Matches(loosely, c2.get()));
  ASSERT_FALSE(c1->Matches(config_options_, c2.get()));
  ASSERT_FALSE(simple->cs->Matches(loosely, simple->cu.get()));
  simple->cs.reset();
  ASSERT_TRUE(c1->Matches(loosely, c2.get()));
  ASSERT_FALSE(c1->Matches(config_options_, c2.get()));
}

// Tests that we can initialize a customizable from its options
TEST_F(CustomizableTest, ConfigureStandaloneCustomTest) {
  std::unique_ptr<TestCustomizable> base, copy;
  auto registry = ObjectRegistry::NewInstance();
  ASSERT_OK(registry->NewUniqueObject<TestCustomizable>("A", &base));
  ASSERT_OK(registry->NewUniqueObject<TestCustomizable>("A", &copy));
  ASSERT_OK(base->ConfigureFromString(config_options_, "int=33;bool=true"));
  std::string opt_str;
  ASSERT_OK(base->GetOptionString(config_options_, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(config_options_, opt_str));
  ASSERT_TRUE(base->Matches(config_options_, copy.get()));
}

// Tests that we fail appropriately if the pattern is not registered
TEST_F(CustomizableTest, BadNameTest) {
  config_options_.ignore_unknown_objects = false;
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  Status s =
      c1->ConfigureFromString(config_options_, "unique.shared.id=bad name");
  ASSERT_NOK(
      c1->ConfigureFromString(config_options_, "unique.shared.id=bad name"));
  config_options_.ignore_unknown_objects = true;
  ASSERT_OK(
      c1->ConfigureFromString(config_options_, "unique.shared.id=bad name"));
}

// Tests that we fail appropriately if a bad option is passed to the underlying
// configurable
TEST_F(CustomizableTest, BadOptionTest) {
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  ConfigOptions ignore = config_options_;
  ignore.ignore_unknown_options = true;

  ASSERT_NOK(c1->ConfigureFromString(config_options_, "A.int=11"));
  ASSERT_NOK(c1->ConfigureFromString(config_options_, "shared={id=B;int=1}"));
  ASSERT_OK(c1->ConfigureFromString(ignore, "shared={id=A;string=s}"));
  ASSERT_NOK(c1->ConfigureFromString(config_options_, "B.int=11"));
  ASSERT_OK(c1->ConfigureFromString(ignore, "B.int=11"));
  ASSERT_NOK(c1->ConfigureFromString(config_options_, "A.string=s"));
  ASSERT_OK(c1->ConfigureFromString(ignore, "A.string=s"));
  // Test as detached
  ASSERT_NOK(
      c1->ConfigureFromString(config_options_, "shared.id=A;A.string=b}"));
  ASSERT_OK(c1->ConfigureFromString(ignore, "shared.id=A;A.string=s}"));
}

// Tests that different IDs lead to different objects
TEST_F(CustomizableTest, UniqueIdTest) {
  std::unique_ptr<Configurable> base(new SimpleConfigurable());
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=A_1;int=1;bool=true}"));
  SimpleOptions* simple = base->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), std::string("A_1"));
  std::string opt_str;
  ASSERT_OK(base->GetOptionString(config_options_, &opt_str));
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromString(config_options_, opt_str));
  ASSERT_TRUE(base->Matches(config_options_, copy.get()));
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=A_2;int=1;bool=true}"));
  ASSERT_FALSE(base->Matches(config_options_, copy.get()));
  ASSERT_EQ(simple->cu->GetId(), std::string("A_2"));
}

class WrappedCustomizable : public Customizable {
 public:
  WrappedCustomizable(const std::shared_ptr<Customizable>& w) : inner(w) {}
  const char* Name() const override { return "Wrapped"; }

 protected:
  Configurable* Inner() const override { return inner.get(); }

 private:
  std::shared_ptr<Customizable> inner;
};

TEST_F(CustomizableTest, FindInstanceTest) {
  std::shared_ptr<TestCustomizable> ac =
      std::make_shared<TestCustomizable>("A");

  ASSERT_EQ(ac->FindInstance("A"), ac.get());
  ASSERT_EQ(ac->FindInstance("TestCustomizable"), ac.get());
  ASSERT_EQ(ac->FindInstance("B"), nullptr);
  WrappedCustomizable wc(ac);
  ASSERT_EQ(wc.FindInstance("Wrapped"), &wc);
  ASSERT_EQ(wc.FindInstance("A"), ac.get());
  ASSERT_EQ(wc.FindInstance("TestCustomizable"), ac.get());
  ASSERT_EQ(wc.FindInstance("B"), nullptr);
}

static std::unordered_map<std::string, OptionTypeInfo> inner_option_info = {
#ifndef ROCKSDB_LITE
    {"inner",
     OptionTypeInfo::AsCustomS<TestCustomizable>(
         0, OptionVerificationType::kNormal, OptionTypeFlags::kStringNameOnly)}
#endif  // ROCKSDB_LITE
};

class ShallowCustomizable : public Customizable {
 public:
  ShallowCustomizable() {
    inner_ = std::make_shared<ACustomizable>("a");
    RegisterOptions("inner", &inner_, &inner_option_info);
  };
  const char* Name() const override { return "shallow"; }

 private:
  std::shared_ptr<TestCustomizable> inner_;
};

TEST_F(CustomizableTest, TestStringDepth) {
  ConfigOptions shallow = config_options_;
  std::unique_ptr<Configurable> c(new ShallowCustomizable());
  std::string opt_str;
  shallow.depth = ConfigOptions::Depth::kDepthShallow;
  ASSERT_OK(c->GetOptionString(shallow, &opt_str));
  ASSERT_EQ(opt_str, "inner=a;");
  shallow.depth = ConfigOptions::Depth::kDepthDetailed;
  ASSERT_OK(c->GetOptionString(shallow, &opt_str));
  ASSERT_NE(opt_str, "inner=a;");
}

// Tests that we only get a new customizable when it changes
TEST_F(CustomizableTest, NewCustomizableTest) {
  std::unique_ptr<Configurable> base(new SimpleConfigurable());
  A_count = 0;
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=A_1;int=1;bool=true}"));
  SimpleOptions* simple = base->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(A_count, 1);  // Created one A
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=A_1;int=1;bool=false}"));
  ASSERT_EQ(A_count, 2);  // Create another A_1
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=A_2;int=1;bool=false}"));
  ASSERT_EQ(A_count, 3);  // Created another A
  ASSERT_OK(base->ConfigureFromString(config_options_, "unique="));
  ASSERT_EQ(simple->cu, nullptr);
  ASSERT_EQ(A_count, 3);
}

TEST_F(CustomizableTest, IgnoreUnknownObjects) {
  ConfigOptions ignore = config_options_;
  std::shared_ptr<TestCustomizable> shared;
  std::unique_ptr<TestCustomizable> unique;
  TestCustomizable* pointer = nullptr;
  ignore.ignore_unknown_objects = false;
  ASSERT_NOK(
      LoadSharedObject<TestCustomizable>(ignore, "Unknown", nullptr, &shared));
  ASSERT_NOK(
      LoadUniqueObject<TestCustomizable>(ignore, "Unknown", nullptr, &unique));
  ASSERT_NOK(
      LoadStaticObject<TestCustomizable>(ignore, "Unknown", nullptr, &pointer));
  ASSERT_EQ(shared.get(), nullptr);
  ASSERT_EQ(unique.get(), nullptr);
  ASSERT_EQ(pointer, nullptr);
  ignore.ignore_unknown_objects = true;
  ASSERT_OK(
      LoadSharedObject<TestCustomizable>(ignore, "Unknown", nullptr, &shared));
  ASSERT_OK(
      LoadUniqueObject<TestCustomizable>(ignore, "Unknown", nullptr, &unique));
  ASSERT_OK(
      LoadStaticObject<TestCustomizable>(ignore, "Unknown", nullptr, &pointer));
  ASSERT_EQ(shared.get(), nullptr);
  ASSERT_EQ(unique.get(), nullptr);
  ASSERT_EQ(pointer, nullptr);
  ASSERT_OK(LoadSharedObject<TestCustomizable>(ignore, "id=Unknown", nullptr,
                                               &shared));
  ASSERT_OK(LoadUniqueObject<TestCustomizable>(ignore, "id=Unknown", nullptr,
                                               &unique));
  ASSERT_OK(LoadStaticObject<TestCustomizable>(ignore, "id=Unknown", nullptr,
                                               &pointer));
  ASSERT_EQ(shared.get(), nullptr);
  ASSERT_EQ(unique.get(), nullptr);
  ASSERT_EQ(pointer, nullptr);
  ASSERT_OK(LoadSharedObject<TestCustomizable>(ignore, "id=Unknown;option=bad",
                                               nullptr, &shared));
  ASSERT_OK(LoadUniqueObject<TestCustomizable>(ignore, "id=Unknown;option=bad",
                                               nullptr, &unique));
  ASSERT_OK(LoadStaticObject<TestCustomizable>(ignore, "id=Unknown;option=bad",
                                               nullptr, &pointer));
  ASSERT_EQ(shared.get(), nullptr);
  ASSERT_EQ(unique.get(), nullptr);
  ASSERT_EQ(pointer, nullptr);
}

TEST_F(CustomizableTest, FactoryFunctionTest) {
  std::shared_ptr<TestCustomizable> shared;
  std::unique_ptr<TestCustomizable> unique;
  TestCustomizable* pointer = nullptr;
  ConfigOptions ignore = config_options_;
  ignore.ignore_unknown_objects = false;
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "B", &shared));
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "B", &unique));
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "B", &pointer));
  ASSERT_NE(shared.get(), nullptr);
  ASSERT_NE(unique.get(), nullptr);
  ASSERT_NE(pointer, nullptr);
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "", &shared));
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "", &unique));
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "", &pointer));
  ASSERT_EQ(shared.get(), nullptr);
  ASSERT_EQ(unique.get(), nullptr);
  ASSERT_EQ(pointer, nullptr);
  ASSERT_NOK(TestCustomizable::CreateFromString(ignore, "option=bad", &shared));
  ASSERT_NOK(TestCustomizable::CreateFromString(ignore, "option=bad", &unique));
  ASSERT_NOK(
      TestCustomizable::CreateFromString(ignore, "option=bad", &pointer));
}
#endif  // ROCKSDB_LITE

#ifndef ROCKSDB_LITE
// This method loads existing test classes into the ObjectRegistry
static void RegisterTestObjects(ObjectLibrary& library,
                                const std::string& /*arg*/) {
  library.Register<TableFactory>(
      "MockTable",
      [](const std::string& /*uri*/, std::unique_ptr<TableFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new mock::MockTableFactory());
        return guard->get();
      });
  library.Register<const Comparator>(
      "SimpleSuffixReverseComparator",
      [](const std::string& /*uri*/,
         std::unique_ptr<const Comparator>* /*guard*/,
         std::string* /* errmsg */) {
        return new test::SimpleSuffixReverseComparator();
      });
  library.Register<MergeOperator>(
      "ChanglingMergeOperator",
      [](const std::string& /*uri*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /* errmsg */) {
        guard->reset(new test::ChanglingMergeOperator("Changling"));
        return guard->get();
      });
  library.Register<CompactionFilterFactory>(
      "ChanglingCompactionFilterFactory",
      [](const std::string& /*uri*/,
         std::unique_ptr<CompactionFilterFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new test::ChanglingCompactionFilterFactory("Changling"));
        return guard->get();
      });
  library.Register<const CompactionFilter>(
      "ChanglingCompactionFilter",
      [](const std::string& /*uri*/,
         std::unique_ptr<const CompactionFilter>* /*guard*/,
         std::string* /* errmsg */) {
        return new test::ChanglingCompactionFilter("Changling");
      });
  library.Register<Env>(
      "ErrorEnv",
      [](const std::string& /*uri*/, std::unique_ptr<Env>* /*guard*/,
         std::string* /* errmsg */) { return new test::ErrorEnv(); });
  library.Register<Env>(
      "StringEnv", [](const std::string& /*uri*/, std::unique_ptr<Env>* guard,
                      std::string* /* errmsg */) {
        guard->reset(new test::StringEnv(nullptr));
        return guard->get();
      });
  library.Register<Env>(
      "StringEnv", [](const std::string& /*uri*/, std::unique_ptr<Env>* guard,
                      std::string* /* errmsg */) {
        guard->reset(new test::StringEnv(nullptr));
        return guard->get();
      });
}

class MockFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
 public:
  const char* Name() const override { return "Test"; }
  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions&, const BlockBuilder&) const override {
    return nullptr;
  }
};

class TestStatistics : public StatisticsImpl {
 public:
  TestStatistics() : StatisticsImpl(nullptr) {}
  const char* Name() const override { return "Test"; }
};

class MockSkipListFactory : public SkipListFactory {
 public:
  const char* Name() const override { return "Test"; }
};

class MockCache : public LRUCache {
 public:
  MockCache() : LRUCache(0) {}
  const char* Name() const override { return "Test"; }
};

class MockFilterPolicy : public FilterPolicy {
 public:
  const char* Name() const override { return "Test"; }
  void CreateFilter(const Slice*, int, std::string*) const override {}

  bool KeyMayMatch(const Slice&, const Slice&) const override { return false; }
};

class MockMemoryAllocator : public MemoryAllocator {
 public:
  const char* Name() const override { return "Test"; }
  void* Allocate(size_t size) override { return malloc(size); }
  void Deallocate(void* p) override { free(p); }
};

static void RegisterLocalObjects(ObjectLibrary& library,
                                 const std::string& /*arg*/) {
  // Load any locally defined objects here
  library.Register<FlushBlockPolicyFactory>(
      "Test", [](const std::string& /*uri*/,
                 std::unique_ptr<FlushBlockPolicyFactory>* guard,
                 std::string* /* errmsg */) {
        guard->reset(new MockFlushBlockPolicyFactory());
        return guard->get();
      });
  library.Register<Statistics>(
      "Test", [](const std::string& /*uri*/, std::unique_ptr<Statistics>* guard,
                 std::string* /* errmsg */) {
        guard->reset(new TestStatistics());
        return guard->get();
      });
  library.Register<MemTableRepFactory>(
      "Test",
      [](const std::string& /*uri*/, std::unique_ptr<MemTableRepFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockSkipListFactory());
        return guard->get();
      });
  library.Register<Cache>(
      "Test", [](const std::string& /*uri*/, std::unique_ptr<Cache>* guard,
                 std::string* /* errmsg */) {
        guard->reset(new MockCache());
        return guard->get();
      });
  library.Register<FilterPolicy>(
      "Test",
      [](const std::string& /*uri*/, std::unique_ptr<FilterPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockFilterPolicy());
        return guard->get();
      });
  library.Register<MemoryAllocator>(
      "Test",
      [](const std::string& /*uri*/, std::unique_ptr<MemoryAllocator>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockMemoryAllocator());
        return guard->get();
      });
}
#endif  // !ROCKSDB_LITE

class LoadCustomizableTest : public testing::Test {
 public:
  LoadCustomizableTest() {
    config_options_.ignore_unknown_options = false;
    config_options_.ignore_unknown_objects = false;
    config_options_.input_strings_escaped = false;
#ifndef ROCKSDB_LITE
    config_options_.registry = db_opts_.object_registry;
#endif  // !ROCKSDB_LITE
  }
  bool RegisterTests(const std::string& arg) {
#ifndef ROCKSDB_LITE
    config_options_.registry->AddLocalLibrary(RegisterTestObjects,
                                              "RegisterTestObjects", arg);
    config_options_.registry->AddLocalLibrary(RegisterLocalObjects,
                                              "RegisterLocalObjects", arg);
    return true;
#else
    (void)arg;
    return false;
#endif  // !ROCKSDB_LITE
  }

 protected:
  DBOptions db_opts_;
  ColumnFamilyOptions cf_opts_;
  ConfigOptions config_options_;
};

TEST_F(LoadCustomizableTest, LoadTableFactoryTest) {
  std::shared_ptr<TableFactory> factory;
  ASSERT_NOK(
      TableFactory::CreateFromString(config_options_, "MockTable", &factory));
  ASSERT_OK(TableFactory::CreateFromString(
      config_options_, TableFactory::kBlockBasedTableName, &factory));
  ASSERT_NE(factory, nullptr);
  ASSERT_EQ(factory->Name(), TableFactory::kBlockBasedTableName);

  if (RegisterTests("Test")) {
    ASSERT_OK(
        TableFactory::CreateFromString(config_options_, "MockTable", &factory));
    ASSERT_NE(factory, nullptr);
    ASSERT_EQ(factory->Name(), std::string("MockTable"));
  }
}

TEST_F(LoadCustomizableTest, LoadFlushBlockPolicyTest) {
  std::shared_ptr<FlushBlockPolicyFactory> factory;
  ASSERT_NOK(FlushBlockPolicyFactory::CreateFromString(config_options_, "Test",
                                                       &factory));
  ASSERT_OK(FlushBlockPolicyFactory::CreateFromString(
      config_options_, FlushBlockPolicyFactory::kSizePolicyFactory, &factory));
  ASSERT_NE(factory, nullptr);
  ASSERT_EQ(factory->GetId(), FlushBlockPolicyFactory::kSizePolicyFactory);
#ifndef ROCKSDB_LITE
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      "block_based_table_factory={flush_block_policy_factory={id=Test}}",
      &cf_opts_));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      "block_based_table_factory={flush_block_policy_factory={id="
      "FlushBlockBySizePolicyFactory}}",
      &cf_opts_));
  auto const* options =
      cf_opts_.table_factory->GetOptions<BlockBasedTableOptions>(
          TableFactory::kBlockBasedTableOpts);
  ASSERT_NE(options, nullptr);
  ASSERT_NE(options->flush_block_policy_factory, nullptr);
  ASSERT_EQ(options->flush_block_policy_factory->Name(),
            std::string("FlushBlockBySizePolicyFactory"));
  if (RegisterTests("Test")) {
    ASSERT_OK(FlushBlockPolicyFactory::CreateFromString(config_options_, "Test",
                                                        &factory));
    ASSERT_NE(factory, nullptr);
    ASSERT_EQ(factory->Name(), std::string("Test"));

    ASSERT_OK(GetColumnFamilyOptionsFromString(
        config_options_, cf_opts_,
        "block_based_table_factory={flush_block_policy_factory={id=Test}}",
        &cf_opts_));
    options = cf_opts_.table_factory->GetOptions<BlockBasedTableOptions>(
        TableFactory::kBlockBasedTableOpts);
    ASSERT_NE(options, nullptr);
    ASSERT_NE(options->flush_block_policy_factory, nullptr);
    ASSERT_EQ(options->flush_block_policy_factory->Name(), std::string("Test"));
  }
#endif  // ROCKSDB_LITE
}

TEST_F(LoadCustomizableTest, LoadStatisticsTest) {
  std::shared_ptr<Statistics> stats;
  ASSERT_NOK(Statistics::CreateFromString(config_options_, "Test", &stats));
  ASSERT_OK(
      Statistics::CreateFromString(config_options_, "BasicStatistics", &stats));
  ASSERT_NE(stats, nullptr);
  ASSERT_EQ(stats->Name(), std::string("BasicStatistics"));
#ifndef ROCKSDB_LITE
  ASSERT_NOK(GetDBOptionsFromString(config_options_, db_opts_,
                                    "statistics=Test", &db_opts_));
  ASSERT_OK(GetDBOptionsFromString(config_options_, db_opts_,
                                   "statistics=BasicStatistics", &db_opts_));
  ASSERT_NE(db_opts_.statistics, nullptr);
  ASSERT_EQ(db_opts_.statistics->Name(), std::string("BasicStatistics"));

  RegisterTests("test");

  ASSERT_OK(Statistics::CreateFromString(config_options_, "Test", &stats));
  ASSERT_NE(stats, nullptr);
  ASSERT_EQ(stats->Name(), std::string("Test"));

  ASSERT_OK(GetDBOptionsFromString(config_options_, db_opts_, "statistics=Test",
                                   &db_opts_));
  ASSERT_NE(db_opts_.statistics, nullptr);
  ASSERT_EQ(db_opts_.statistics->Name(), std::string("Test"));

  ASSERT_OK(GetDBOptionsFromString(config_options_, db_opts_,
                                   "statistics={id=Test;inner=BasicStatistics}",
                                   &db_opts_));
  ASSERT_NE(db_opts_.statistics, nullptr);
  ASSERT_EQ(db_opts_.statistics->Name(), std::string("Test"));
  auto* inner = db_opts_.statistics->GetOptions<std::shared_ptr<Statistics>>(
      "StatisticsOptions");
  ASSERT_NE(inner, nullptr);
  ASSERT_NE(inner->get(), nullptr);
  ASSERT_EQ(inner->get()->Name(), std::string("BasicStatistics"));
#endif
}

TEST_F(LoadCustomizableTest, LoadComparatorTest) {
  const Comparator* comparator = nullptr;
  ASSERT_NOK(Comparator::CreateFromString(
      config_options_, "SimpleSuffixReverseComparator", &comparator));
  ASSERT_OK(Comparator::CreateFromString(
      config_options_, "leveldb.BytewiseComparator", &comparator));
  ASSERT_NE(comparator, nullptr);
  ASSERT_EQ(comparator->Name(), std::string("leveldb.BytewiseComparator"));
#ifndef ROCKSDB_LITE
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      "comparator={id=SimpleSuffixReverseComparator}", &cf_opts_));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_, "comparator={id=leveldb.BytewiseComparator}",
      &cf_opts_));
  RegisterTests("test");
  ASSERT_OK(Comparator::CreateFromString(
      config_options_, "SimpleSuffixReverseComparator", &comparator));
  ASSERT_NE(comparator, nullptr);
  ASSERT_EQ(comparator->Name(), std::string("SimpleSuffixReverseComparator"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      "comparator={id=SimpleSuffixReverseComparator}", &cf_opts_));
  ASSERT_NE(cf_opts_.comparator, nullptr);
  ASSERT_EQ(cf_opts_.comparator->Name(),
            std::string("SimpleSuffixReverseComparator"));
#endif  // ROCKSDB_LITE
}

TEST_F(LoadCustomizableTest, LoadMemTableRepFactoryTest) {
  std::shared_ptr<MemTableRepFactory> mtrf;
  ASSERT_NOK(
      MemTableRepFactory::CreateFromString(config_options_, "Test", &mtrf));
#ifndef ROCKSDB_LITE
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_, "memtable_factory={id=Test}", &cf_opts_));
  RegisterTests("Test");
  ASSERT_OK(
      MemTableRepFactory::CreateFromString(config_options_, "Test", &mtrf));
  ASSERT_NE(mtrf, nullptr);
  ASSERT_EQ(mtrf->Name(), std::string("Test"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_, "memtable_factory={id=Test}", &cf_opts_));
  ASSERT_NE(cf_opts_.memtable_factory, nullptr);
  ASSERT_EQ(cf_opts_.memtable_factory->Name(), std::string("Test"));
#endif
}

TEST_F(LoadCustomizableTest, LoadCacheTest) {
  std::shared_ptr<Cache> cache;
  std::shared_ptr<TableFactory> tf;
  ASSERT_NOK(Cache::CreateFromString(config_options_, "Test", &cache));
  ASSERT_NOK(TableFactory::CreateFromString(
      config_options_, "id=BlockBasedTable;block_cache=Test;", &tf));
  ASSERT_OK(Cache::CreateFromString(config_options_, "1024", &cache));
  ASSERT_EQ(cache->Name(), std::string("LRUCache"));
  ASSERT_EQ(cache->GetCapacity(), 1024);

#ifndef ROCKSDB_LITE
  ASSERT_OK(Cache::CreateFromString(config_options_, "capacity=1024", &cache));
  ASSERT_EQ(cache->Name(), std::string("LRUCache"));
  ASSERT_EQ(cache->GetCapacity(), 1024);

  RegisterTests("Test");
  ASSERT_OK(Cache::CreateFromString(config_options_, "Test", &cache));
  ASSERT_NE(cache, nullptr);
  ASSERT_EQ(cache->Name(), std::string("Test"));
  ASSERT_OK(TableFactory::CreateFromString(
      config_options_, "id=BlockBasedTable;block_cache=Test;", &tf));
  auto bbto = tf->GetOptions<BlockBasedTableOptions>(
      TableFactory::kBlockBasedTableOpts);
  ASSERT_NE(bbto, nullptr);
  ASSERT_NE(bbto->block_cache.get(), nullptr);
  ASSERT_EQ(bbto->block_cache->Name(), std::string("Test"));
#endif
}

TEST_F(LoadCustomizableTest, LoadFilterPolicyTest) {
  std::shared_ptr<const FilterPolicy> policy;
  ASSERT_NOK(FilterPolicy::CreateFromString(config_options_, "Test", &policy));
  ASSERT_OK(FilterPolicy::CreateFromString(
      config_options_, "rocksdb.BuiltinBloomFilter", &policy));
  ASSERT_NE(policy, nullptr);
  ASSERT_EQ(policy->Name(), std::string("rocksdb.BuiltinBloomFilter"));
#ifndef ROCKSDB_LITE
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      "block_based_table_factory={filter_policy=Test}", &cf_opts_));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      "block_based_table_factory={filter_policy=rocksdb.BuiltinBloomFilter}",
      &cf_opts_));
  auto const* options =
      cf_opts_.table_factory->GetOptions<BlockBasedTableOptions>(
          TableFactory::kBlockBasedTableOpts);
  ASSERT_NE(options, nullptr);
  ASSERT_NE(options->filter_policy, nullptr);
  ASSERT_EQ(options->filter_policy->Name(),
            std::string("rocksdb.BuiltinBloomFilter"));
  RegisterTests("test");
  ASSERT_OK(FilterPolicy::CreateFromString(config_options_, "Test", &policy));
  ASSERT_NE(policy, nullptr);
  ASSERT_EQ(policy->Name(), std::string("Test"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      "block_based_table_factory={filter_policy=Test}", &cf_opts_));
  options = cf_opts_.table_factory->GetOptions<BlockBasedTableOptions>(
      TableFactory::kBlockBasedTableOpts);
  ASSERT_NE(options, nullptr);
  ASSERT_NE(options->filter_policy, nullptr);
  ASSERT_EQ(options->filter_policy->Name(), std::string("Test"));
#endif  // ROCKSDB_LITE
}

TEST_F(LoadCustomizableTest, LoadMergeOperatorTest) {
  std::shared_ptr<MergeOperator> op;
  ASSERT_NOK(MergeOperator::CreateFromString(config_options_,
                                             "ChanglingMergeOperator", &op));
  ASSERT_OK(MergeOperator::CreateFromString(config_options_, "max", &op));
  ASSERT_NE(op, nullptr);
  ASSERT_EQ(op->Name(), std::string("MaxOperator"));
#ifndef ROCKSDB_LITE
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_, "merge_operator={id=ChanglingMergeOperator}",
      &cf_opts_));
  ASSERT_EQ(cf_opts_.merge_operator, nullptr);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_, "merge_operator={id=max}", &cf_opts_));

  RegisterTests("test");

  ASSERT_OK(MergeOperator::CreateFromString(config_options_,
                                            "ChanglingMergeOperator", &op));
  ASSERT_NE(op, nullptr);
  ASSERT_EQ(op->Name(), std::string("ChanglingMergeOperator"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_, "merge_operator={id=ChanglingMergeOperator}",
      &cf_opts_));
  ASSERT_NE(cf_opts_.merge_operator, nullptr);
  ASSERT_EQ(cf_opts_.merge_operator->Name(),
            std::string("ChanglingMergeOperator"));
#endif  // ROCKSDB_LITE
}

TEST_F(LoadCustomizableTest, LoadCompactionFilterTest) {
  const CompactionFilter* cf = nullptr;
  ASSERT_NOK(CompactionFilter::CreateFromString(
      config_options_, "ChanglingCompactionFilter", &cf));
#ifndef ROCKSDB_LITE
  ASSERT_OK(CompactionFilter::CreateFromString(
      config_options_, "RemoveEmptyValueCompactionFilter", &cf));
  ASSERT_NE(cf, nullptr);
  ASSERT_EQ(cf->Name(), std::string("RemoveEmptyValueCompactionFilter"));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      "compaction_filter={id=ChanglingCompactionFilter}", &cf_opts_));
  ASSERT_EQ(cf_opts_.compaction_filter, nullptr);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      "compaction_filter={id=RemoveEmptyValueCompactionFilter}", &cf_opts_));
  RegisterTests("test");

  ASSERT_OK(CompactionFilter::CreateFromString(
      config_options_, "ChanglingCompactionFilter", &cf));
  ASSERT_NE(cf, nullptr);
  ASSERT_EQ(cf->Name(), std::string("ChanglingCompactionFilter"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      "compaction_filter={id=ChanglingCompactionFilter}", &cf_opts_));
  ASSERT_NE(cf_opts_.compaction_filter, nullptr);
  ASSERT_EQ(cf_opts_.compaction_filter->Name(),
            std::string("ChanglingCompactionFilter"));
#endif  // ROCKSDB_LITE
}

TEST_F(LoadCustomizableTest, LoadCompactionFilterFactoryTest) {
  std::shared_ptr<CompactionFilterFactory> cff;
  ASSERT_NOK(CompactionFilterFactory::CreateFromString(
      config_options_, "ChanglingCompactionFilterFactory", &cff));
#ifndef ROCKSDB_LITE
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      "compaction_filter_factory={id=ChanglingCompactionFilterFactory}",
      &cf_opts_));
  ASSERT_EQ(cf_opts_.compaction_filter_factory, nullptr);
  RegisterTests("test");

  ASSERT_OK(CompactionFilterFactory::CreateFromString(
      config_options_, "ChanglingCompactionFilterFactory", &cff));
  ASSERT_NE(cff, nullptr);
  ASSERT_EQ(cff->Name(), std::string("ChanglingCompactionFilterFactory"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, cf_opts_,
      "compaction_filter_factory={id=ChanglingCompactionFilterFactory}",
      &cf_opts_));
  ASSERT_NE(cf_opts_.compaction_filter_factory, nullptr);
  ASSERT_EQ(cf_opts_.compaction_filter_factory->Name(),
            std::string("ChanglingCompactionFilterFactory"));
#endif  // ROCKSDB_LITE
}

TEST_F(LoadCustomizableTest, LoadEnvTest) {
  // The ErrorEnv is registered as static, the StringEnv as shared
  std::shared_ptr<Env> shared;
  Env* env = nullptr;

  ASSERT_NOK(
      Env::CreateFromString(config_options_, "StringEnv", &env, &shared));
  ASSERT_NOK(Env::CreateFromString(config_options_, "ErrorEnv", &env));
  ASSERT_OK(Env::CreateFromString(config_options_, "Posix", &env));
#ifndef ROCKSDB_LITE
  ASSERT_NOK(GetDBOptionsFromString(config_options_, db_opts_, "env=StringEnv",
                                    &db_opts_));
  ASSERT_NOK(GetDBOptionsFromString(config_options_, db_opts_, "env=ErrorEnv",
                                    &db_opts_));
  ASSERT_OK(GetDBOptionsFromString(config_options_, db_opts_, "env=Posix",
                                   &db_opts_));
  RegisterTests("test");
  ASSERT_NOK(Env::CreateFromString(config_options_, "StringEnv", &env));
  ASSERT_OK(Env::CreateFromString(config_options_, "StringEnv", &env, &shared));
  ASSERT_OK(Env::CreateFromString(config_options_, "ErrorEnv", &env));
  delete env;  // Not really static
  ASSERT_NOK(GetDBOptionsFromString(config_options_, db_opts_, "env=StringEnv",
                                    &db_opts_));
  ASSERT_OK(GetDBOptionsFromString(config_options_, db_opts_, "env=ErrorEnv",
                                   &db_opts_));
  ASSERT_NE(db_opts_.env, nullptr);
  ASSERT_EQ(db_opts_.env->GetId(), "ErrorEnv");
  delete db_opts_.env;
#endif
}

TEST_F(LoadCustomizableTest, LoadMemoryAllocatorTest) {
  std::shared_ptr<MemoryAllocator> allocator;
  std::shared_ptr<Cache> cache;
  ASSERT_NOK(
      MemoryAllocator::CreateFromString(config_options_, "Test", &allocator));
  ASSERT_NOK(Cache::CreateFromString(config_options_,
                                     "id=LRUCache; allocator=Test", &cache));
#ifndef ROCKSDB_LITE
  RegisterTests("test");
  ASSERT_OK(
      MemoryAllocator::CreateFromString(config_options_, "Test", &allocator));
  ASSERT_EQ(allocator->GetId(), "Test");
  ASSERT_OK(Cache::CreateFromString(config_options_,
                                    "id=LRUCache; allocator=Test", &cache));
  ASSERT_EQ(cache->memory_allocator(), allocator.get());
#endif  // ROCKSDB_LITE
#ifdef MEMKIND
  ASSERT_NOK(MemoryAllocator::CreateFromString(
      config_options_, "MemkindkMemAllocator", &allocator));
#endif
}
}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
