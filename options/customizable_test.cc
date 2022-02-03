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

#include "options/configurable_helper.h"
#include "options/customizable_helper.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "rocksdb/convenience.h"
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
  // Method to allow CheckedCast to work for this class
  static const char* kClassName() {
    return "TestCustomizable";
    ;
  }

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
  bool IsInstanceOf(const std::string& name) const override {
    if (name == kClassName()) {
      return true;
    } else {
      return Customizable::IsInstanceOf(name);
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
      OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
    {"bool",
     {offsetof(struct AOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
#endif  // ROCKSDB_LITE
};
class ACustomizable : public TestCustomizable {
 public:
  ACustomizable(const std::string& id) : TestCustomizable("A"), id_(id) {
    ConfigurableHelper::RegisterOptions(*this, "A", &opts_, &a_option_info);
  }
  std::string GetId() const override { return id_; }
  static const char* kClassName() { return "A"; }

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
      OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
    {"bool",
     {offsetof(struct BOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
#endif  // ROCKSDB_LITE
};

class BCustomizable : public TestCustomizable {
 private:
 public:
  BCustomizable(const std::string& name) : TestCustomizable(name) {
    ConfigurableHelper::RegisterOptions(*this, name, &opts_, &b_option_info);
  }
  static const char* kClassName() { return "B"; }

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
  bool is_mutable = true;
  std::unique_ptr<TestCustomizable> cu;
  std::shared_ptr<TestCustomizable> cs;
  TestCustomizable* cp = nullptr;
};

static std::unordered_map<std::string, OptionTypeInfo> simple_option_info = {
#ifndef ROCKSDB_LITE
    {"bool",
     {offsetof(struct SimpleOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"unique", OptionTypeInfo::AsCustomUniquePtr<TestCustomizable>(
                   offsetof(struct SimpleOptions, cu),
                   OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
    {"shared", OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
                   offsetof(struct SimpleOptions, cs),
                   OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
    {"pointer", OptionTypeInfo::AsCustomRawPtr<TestCustomizable>(
                    offsetof(struct SimpleOptions, cp),
                    OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
#endif  // ROCKSDB_LITE
};

class SimpleConfigurable : public Configurable {
 private:
  SimpleOptions simple_;

 public:
  SimpleConfigurable() {
    ConfigurableHelper::RegisterOptions(*this, "simple", &simple_,
                                        &simple_option_info);
  }

  SimpleConfigurable(
      const std::unordered_map<std::string, OptionTypeInfo>* map) {
    ConfigurableHelper::RegisterOptions(*this, "simple", &simple_, map);
  }

  bool IsPrepared() const override {
    if (simple_.is_mutable) {
      return false;
    } else {
      return Configurable::IsPrepared();
    }
  }

 private:
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
  std::string mismatch;
  ASSERT_OK(configurable->GetOptionString(config_options_, &opt_str));
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromString(config_options_, opt_str));
  ASSERT_TRUE(
      configurable->AreEquivalent(config_options_, copy.get(), &mismatch));
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
  std::string mismatch;
  config_options_.delimiter = "\n";
  std::unordered_map<std::string, std::string> props;
  ASSERT_OK(configurable->GetOptionString(config_options_, &opt_str));
  GetMapFromProperties(opt_str, &props);
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromMap(config_options_, props));
  ASSERT_TRUE(
      configurable->AreEquivalent(config_options_, copy.get(), &mismatch));
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

TEST_F(CustomizableTest, AreEquivalentOptionsTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique", "id=A;int=1;bool=true"},
      {"shared", "id=A;int=1;bool=true"},
  };
  std::string mismatch;
  ConfigOptions config_options = config_options_;
  config_options.invoke_prepare_options = false;
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  std::unique_ptr<Configurable> c2(new SimpleConfigurable());
  ASSERT_OK(c1->ConfigureFromMap(config_options, opt_map));
  ASSERT_OK(c2->ConfigureFromMap(config_options, opt_map));
  ASSERT_TRUE(c1->AreEquivalent(config_options, c2.get(), &mismatch));
  SimpleOptions* simple = c1->GetOptions<SimpleOptions>("simple");
  ASSERT_TRUE(
      simple->cu->AreEquivalent(config_options, simple->cs.get(), &mismatch));
  ASSERT_OK(simple->cu->ConfigureOption(config_options, "int", "2"));
  ASSERT_FALSE(
      simple->cu->AreEquivalent(config_options, simple->cs.get(), &mismatch));
  ASSERT_FALSE(c1->AreEquivalent(config_options, c2.get(), &mismatch));
  ConfigOptions loosely = config_options;
  loosely.sanity_level = ConfigOptions::kSanityLevelLooselyCompatible;
  ASSERT_TRUE(c1->AreEquivalent(loosely, c2.get(), &mismatch));
  ASSERT_TRUE(simple->cu->AreEquivalent(loosely, simple->cs.get(), &mismatch));

  ASSERT_OK(c1->ConfigureOption(config_options, "shared", "id=B;string=3"));
  ASSERT_TRUE(c1->AreEquivalent(loosely, c2.get(), &mismatch));
  ASSERT_FALSE(c1->AreEquivalent(config_options, c2.get(), &mismatch));
  ASSERT_FALSE(simple->cs->AreEquivalent(loosely, simple->cu.get(), &mismatch));
  simple->cs.reset();
  ASSERT_TRUE(c1->AreEquivalent(loosely, c2.get(), &mismatch));
  ASSERT_FALSE(c1->AreEquivalent(config_options, c2.get(), &mismatch));
}

// Tests that we can initialize a customizable from its options
TEST_F(CustomizableTest, ConfigureStandaloneCustomTest) {
  std::unique_ptr<TestCustomizable> base, copy;
  auto registry = ObjectRegistry::NewInstance();
  ASSERT_OK(registry->NewUniqueObject<TestCustomizable>("A", &base));
  ASSERT_OK(registry->NewUniqueObject<TestCustomizable>("A", &copy));
  ASSERT_OK(base->ConfigureFromString(config_options_, "int=33;bool=true"));
  std::string opt_str;
  std::string mismatch;
  ASSERT_OK(base->GetOptionString(config_options_, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(config_options_, opt_str));
  ASSERT_TRUE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
}

// Tests that we fail appropriately if the pattern is not registered
TEST_F(CustomizableTest, BadNameTest) {
  config_options_.ignore_unsupported_options = false;
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  ASSERT_NOK(
      c1->ConfigureFromString(config_options_, "unique.shared.id=bad name"));
  config_options_.ignore_unsupported_options = true;
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
  std::string mismatch;
  ASSERT_OK(base->GetOptionString(config_options_, &opt_str));
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromString(config_options_, opt_str));
  ASSERT_TRUE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=A_2;int=1;bool=true}"));
  ASSERT_FALSE(base->AreEquivalent(config_options_, copy.get(), &mismatch));
  ASSERT_EQ(simple->cu->GetId(), std::string("A_2"));
}

TEST_F(CustomizableTest, IsInstanceOfTest) {
  std::shared_ptr<TestCustomizable> tc = std::make_shared<ACustomizable>("A");

  ASSERT_TRUE(tc->IsInstanceOf("A"));
  ASSERT_TRUE(tc->IsInstanceOf("TestCustomizable"));
  ASSERT_FALSE(tc->IsInstanceOf("B"));
  ASSERT_EQ(tc->CheckedCast<ACustomizable>(), tc.get());
  ASSERT_EQ(tc->CheckedCast<TestCustomizable>(), tc.get());
  ASSERT_EQ(tc->CheckedCast<BCustomizable>(), nullptr);

  tc.reset(new BCustomizable("B"));
  ASSERT_TRUE(tc->IsInstanceOf("B"));
  ASSERT_TRUE(tc->IsInstanceOf("TestCustomizable"));
  ASSERT_FALSE(tc->IsInstanceOf("A"));
  ASSERT_EQ(tc->CheckedCast<BCustomizable>(), tc.get());
  ASSERT_EQ(tc->CheckedCast<TestCustomizable>(), tc.get());
  ASSERT_EQ(tc->CheckedCast<ACustomizable>(), nullptr);
}

static std::unordered_map<std::string, OptionTypeInfo> inner_option_info = {
#ifndef ROCKSDB_LITE
    {"inner",
     OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
         0, OptionVerificationType::kNormal, OptionTypeFlags::kStringNameOnly)}
#endif  // ROCKSDB_LITE
};

class ShallowCustomizable : public Customizable {
 public:
  ShallowCustomizable() {
    inner_ = std::make_shared<ACustomizable>("a");
    ConfigurableHelper::RegisterOptions(*this, "inner", &inner_,
                                        &inner_option_info);
  };
  static const char* kClassName() { return "shallow"; }
  const char* Name() const override { return kClassName(); }

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
  ASSERT_OK(base->ConfigureFromString(config_options_, "unique.id="));
  ASSERT_EQ(simple->cu, nullptr);
  ASSERT_EQ(A_count, 3);
}

TEST_F(CustomizableTest, IgnoreUnknownObjects) {
  ConfigOptions ignore = config_options_;
  std::shared_ptr<TestCustomizable> shared;
  std::unique_ptr<TestCustomizable> unique;
  TestCustomizable* pointer = nullptr;
  ignore.ignore_unsupported_options = false;
  ASSERT_NOK(
      LoadSharedObject<TestCustomizable>(ignore, "Unknown", nullptr, &shared));
  ASSERT_NOK(
      LoadUniqueObject<TestCustomizable>(ignore, "Unknown", nullptr, &unique));
  ASSERT_NOK(
      LoadStaticObject<TestCustomizable>(ignore, "Unknown", nullptr, &pointer));
  ASSERT_EQ(shared.get(), nullptr);
  ASSERT_EQ(unique.get(), nullptr);
  ASSERT_EQ(pointer, nullptr);
  ignore.ignore_unsupported_options = true;
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
  ignore.ignore_unsupported_options = false;
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "B", &shared));
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "B", &unique));
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "B", &pointer));
  ASSERT_NE(shared.get(), nullptr);
  ASSERT_NE(unique.get(), nullptr);
  ASSERT_NE(pointer, nullptr);
  delete pointer;
  pointer = nullptr;
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "id=", &shared));
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "id=", &unique));
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "id=", &pointer));
  ASSERT_EQ(shared.get(), nullptr);
  ASSERT_EQ(unique.get(), nullptr);
  ASSERT_EQ(pointer, nullptr);
  ASSERT_NOK(TestCustomizable::CreateFromString(ignore, "option=bad", &shared));
  ASSERT_NOK(TestCustomizable::CreateFromString(ignore, "option=bad", &unique));
  ASSERT_NOK(
      TestCustomizable::CreateFromString(ignore, "option=bad", &pointer));
  ASSERT_EQ(pointer, nullptr);
}

TEST_F(CustomizableTest, MutableOptionsTest) {
  static std::unordered_map<std::string, OptionTypeInfo> mutable_option_info = {
      {"mutable",
       OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
           0, OptionVerificationType::kNormal, OptionTypeFlags::kMutable)}};
  static std::unordered_map<std::string, OptionTypeInfo> immutable_option_info =
      {{"immutable",
        OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
            0, OptionVerificationType::kNormal, OptionTypeFlags::kNone)}};

  class MutableCustomizable : public Customizable {
   private:
    std::shared_ptr<TestCustomizable> mutable_;
    std::shared_ptr<TestCustomizable> immutable_;

   public:
    MutableCustomizable() {
      ConfigurableHelper::RegisterOptions(*this, "mutable", &mutable_,
                                          &mutable_option_info);
      ConfigurableHelper::RegisterOptions(*this, "immutable", &immutable_,
                                          &immutable_option_info);
    }
    const char* Name() const override { return "MutableCustomizable"; }
  };
  MutableCustomizable mc;

  ConfigOptions options = config_options_;
  ASSERT_FALSE(mc.IsPrepared());
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "{id=B;}"));
  ASSERT_OK(mc.ConfigureOption(options, "immutable", "{id=A; int=10}"));
  auto* mm = mc.GetOptions<std::shared_ptr<TestCustomizable>>("mutable");
  auto* im = mc.GetOptions<std::shared_ptr<TestCustomizable>>("immutable");
  ASSERT_NE(mm, nullptr);
  ASSERT_NE(mm->get(), nullptr);
  ASSERT_NE(im, nullptr);
  ASSERT_NE(im->get(), nullptr);

  // Now only deal with mutable options
  options.mutable_options_only = true;

  // Setting nested immutable customizable options fails
  ASSERT_NOK(mc.ConfigureOption(options, "immutable", "{id=B;}"));
  ASSERT_NOK(mc.ConfigureOption(options, "immutable.id", "B"));
  ASSERT_NOK(mc.ConfigureOption(options, "immutable.bool", "true"));
  ASSERT_NOK(mc.ConfigureOption(options, "immutable", "bool=true"));
  ASSERT_NOK(mc.ConfigureOption(options, "immutable", "{int=11;bool=true}"));
  auto* im_a = im->get()->GetOptions<AOptions>("A");
  ASSERT_NE(im_a, nullptr);
  ASSERT_EQ(im_a->i, 10);
  ASSERT_EQ(im_a->b, false);

  // Setting nested mutable customizable options succeeds but the object did not
  // change
  ASSERT_OK(mc.ConfigureOption(options, "immutable.int", "11"));
  ASSERT_EQ(im_a->i, 11);
  ASSERT_EQ(im_a, im->get()->GetOptions<AOptions>("A"));

  // The mutable configurable itself can be changed
  ASSERT_OK(mc.ConfigureOption(options, "mutable.id", "A"));
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "A"));
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "{id=A}"));
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "{bool=true}"));

  // The Nested options in the mutable object can be changed
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "{bool=true}"));
  auto* mm_a = mm->get()->GetOptions<AOptions>("A");
  ASSERT_EQ(mm_a->b, true);
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "{int=11;bool=false}"));
  mm_a = mm->get()->GetOptions<AOptions>("A");
  ASSERT_EQ(mm_a->i, 11);
  ASSERT_EQ(mm_a->b, false);
}
#endif  // !ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
