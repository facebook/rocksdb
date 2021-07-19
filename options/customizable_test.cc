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
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env_encryption.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "table/block_based/flush_block_policy.h"
#include "table/mock_table.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

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
  explicit ACustomizable(const std::string& id)
      : TestCustomizable("A"), id_(id) {
    RegisterOptions("A", &opts_, &a_option_info);
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
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"bool",
     {offsetof(struct BOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
#endif  // ROCKSDB_LITE
};

class BCustomizable : public TestCustomizable {
 private:
 public:
  explicit BCustomizable(const std::string& name) : TestCustomizable(name) {
    RegisterOptions(name, &opts_, &b_option_info);
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
  static const char* kName() { return "simple"; }
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
    {"unique",
     OptionTypeInfo::AsCustomUniquePtr<TestCustomizable>(
         offsetof(struct SimpleOptions, cu), OptionVerificationType::kNormal,
         OptionTypeFlags::kAllowNull)},
    {"shared",
     OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
         offsetof(struct SimpleOptions, cs), OptionVerificationType::kNormal,
         OptionTypeFlags::kAllowNull)},
    {"pointer",
     OptionTypeInfo::AsCustomRawPtr<TestCustomizable>(
         offsetof(struct SimpleOptions, cp), OptionVerificationType::kNormal,
         OptionTypeFlags::kAllowNull)},
#endif  // ROCKSDB_LITE
};

class SimpleConfigurable : public Configurable {
 private:
  SimpleOptions simple_;

 public:
  SimpleConfigurable() { RegisterOptions(&simple_, &simple_option_info); }

  explicit SimpleConfigurable(
      const std::unordered_map<std::string, OptionTypeInfo>* map) {
    RegisterOptions(&simple_, map);
  }
};

class CustomizableTest : public testing::Test {
 public:
  CustomizableTest() { config_options_.invoke_prepare_options = false; }

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
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>();
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
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>();
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
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>();
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
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>();
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
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  std::unique_ptr<Configurable> c2(new SimpleConfigurable());
  ASSERT_OK(c1->ConfigureFromMap(config_options, opt_map));
  ASSERT_OK(c2->ConfigureFromMap(config_options, opt_map));
  ASSERT_TRUE(c1->AreEquivalent(config_options, c2.get(), &mismatch));
  SimpleOptions* simple = c1->GetOptions<SimpleOptions>();
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
  SimpleOptions* simple = base->GetOptions<SimpleOptions>();
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

TEST_F(CustomizableTest, PrepareOptionsTest) {
  static std::unordered_map<std::string, OptionTypeInfo> p_option_info = {
#ifndef ROCKSDB_LITE
      {"can_prepare",
       {0, OptionType::kBoolean, OptionVerificationType::kNormal,
        OptionTypeFlags::kNone}},
#endif  // ROCKSDB_LITE
  };

  class PrepareCustomizable : public TestCustomizable {
   public:
    bool can_prepare_ = true;

    PrepareCustomizable() : TestCustomizable("P") {
      RegisterOptions("Prepare", &can_prepare_, &p_option_info);
    }

    Status PrepareOptions(const ConfigOptions& opts) override {
      if (!can_prepare_) {
        return Status::InvalidArgument("Cannot Prepare");
      } else {
        return TestCustomizable::PrepareOptions(opts);
      }
    }
  };

  ObjectLibrary::Default()->Register<TestCustomizable>(
      "P",
      [](const std::string& /*name*/, std::unique_ptr<TestCustomizable>* guard,
         std::string* /* msg */) {
        guard->reset(new PrepareCustomizable());
        return guard->get();
      });

  std::unique_ptr<Configurable> base(new SimpleConfigurable());
  ConfigOptions prepared(config_options_);
  prepared.invoke_prepare_options = true;

  ASSERT_FALSE(base->IsPrepared());
  ASSERT_OK(base->ConfigureFromString(
      prepared, "unique=A_1; shared={id=B;string=s}; pointer.id=S"));
  SimpleOptions* simple = base->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_NE(simple->cs, nullptr);
  ASSERT_NE(simple->cp, nullptr);
  ASSERT_TRUE(base->IsPrepared());
  ASSERT_TRUE(simple->cu->IsPrepared());
  ASSERT_TRUE(simple->cs->IsPrepared());
  ASSERT_TRUE(simple->cp->IsPrepared());
  delete simple->cp;
  base.reset(new SimpleConfigurable());
  ASSERT_OK(base->ConfigureFromString(
      config_options_, "unique=A_1; shared={id=B;string=s}; pointer.id=S"));

  simple = base->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_NE(simple->cs, nullptr);
  ASSERT_NE(simple->cp, nullptr);
  ASSERT_FALSE(base->IsPrepared());
  ASSERT_FALSE(simple->cu->IsPrepared());
  ASSERT_FALSE(simple->cs->IsPrepared());
  ASSERT_FALSE(simple->cp->IsPrepared());

  ASSERT_OK(base->PrepareOptions(config_options_));
  ASSERT_TRUE(base->IsPrepared());
  ASSERT_TRUE(simple->cu->IsPrepared());
  ASSERT_TRUE(simple->cs->IsPrepared());
  ASSERT_TRUE(simple->cp->IsPrepared());
  delete simple->cp;
  base.reset(new SimpleConfigurable());

  ASSERT_NOK(
      base->ConfigureFromString(prepared, "unique={id=P; can_prepare=false}"));
  simple = base->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_FALSE(simple->cu->IsPrepared());

  ASSERT_OK(
      base->ConfigureFromString(prepared, "unique={id=P; can_prepare=true}"));
  ASSERT_TRUE(simple->cu->IsPrepared());

  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=P; can_prepare=true}"));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_FALSE(simple->cu->IsPrepared());
  ASSERT_OK(simple->cu->PrepareOptions(prepared));
  ASSERT_TRUE(simple->cu->IsPrepared());

  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=P; can_prepare=false}"));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_FALSE(simple->cu->IsPrepared());
  ASSERT_NOK(simple->cu->PrepareOptions(prepared));
  ASSERT_FALSE(simple->cu->IsPrepared());
}

static std::unordered_map<std::string, OptionTypeInfo> inner_option_info = {
#ifndef ROCKSDB_LITE
    {"inner",
     OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
         0, OptionVerificationType::kNormal, OptionTypeFlags::kStringNameOnly)}
#endif  // ROCKSDB_LITE
};

class InnerCustomizable : public Customizable {
 public:
  explicit InnerCustomizable(const std::shared_ptr<Customizable>& w)
      : inner_(w) {}
  static const char* kClassName() { return "Inner"; }
  bool IsInstanceOf(const std::string& name) const override {
    if (name == kClassName()) {
      return true;
    } else {
      return Customizable::IsInstanceOf(name);
    }
  }

 protected:
  const Customizable* Inner() const override { return inner_.get(); }

 private:
  std::shared_ptr<Customizable> inner_;
};

class WrappedCustomizable1 : public InnerCustomizable {
 public:
  explicit WrappedCustomizable1(const std::shared_ptr<Customizable>& w)
      : InnerCustomizable(w) {}
  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "Wrapped1"; }
};

class WrappedCustomizable2 : public InnerCustomizable {
 public:
  explicit WrappedCustomizable2(const std::shared_ptr<Customizable>& w)
      : InnerCustomizable(w) {}
  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "Wrapped2"; }
};

TEST_F(CustomizableTest, WrappedInnerTest) {
  std::shared_ptr<TestCustomizable> ac =
      std::make_shared<TestCustomizable>("A");

  ASSERT_TRUE(ac->IsInstanceOf("A"));
  ASSERT_TRUE(ac->IsInstanceOf("TestCustomizable"));
  ASSERT_EQ(ac->CheckedCast<TestCustomizable>(), ac.get());
  ASSERT_EQ(ac->CheckedCast<InnerCustomizable>(), nullptr);
  ASSERT_EQ(ac->CheckedCast<WrappedCustomizable1>(), nullptr);
  ASSERT_EQ(ac->CheckedCast<WrappedCustomizable2>(), nullptr);
  std::shared_ptr<Customizable> wc1 =
      std::make_shared<WrappedCustomizable1>(ac);

  ASSERT_TRUE(wc1->IsInstanceOf(WrappedCustomizable1::kClassName()));
  ASSERT_EQ(wc1->CheckedCast<WrappedCustomizable1>(), wc1.get());
  ASSERT_EQ(wc1->CheckedCast<WrappedCustomizable2>(), nullptr);
  ASSERT_EQ(wc1->CheckedCast<InnerCustomizable>(), wc1.get());
  ASSERT_EQ(wc1->CheckedCast<TestCustomizable>(), ac.get());

  std::shared_ptr<Customizable> wc2 =
      std::make_shared<WrappedCustomizable2>(wc1);
  ASSERT_TRUE(wc2->IsInstanceOf(WrappedCustomizable2::kClassName()));
  ASSERT_EQ(wc2->CheckedCast<WrappedCustomizable2>(), wc2.get());
  ASSERT_EQ(wc2->CheckedCast<WrappedCustomizable1>(), wc1.get());
  ASSERT_EQ(wc2->CheckedCast<InnerCustomizable>(), wc2.get());
  ASSERT_EQ(wc2->CheckedCast<TestCustomizable>(), ac.get());
}

class ShallowCustomizable : public Customizable {
 public:
  ShallowCustomizable() {
    inner_ = std::make_shared<ACustomizable>("a");
    RegisterOptions("inner", &inner_, &inner_option_info);
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
  SimpleOptions* simple = base->GetOptions<SimpleOptions>();
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(A_count, 1);  // Created one A
  ASSERT_OK(base->ConfigureFromString(config_options_,
                                      "unique={id=A_1;int=1;bool=false}"));
  ASSERT_EQ(A_count, 2);  // Create another A_1
  ASSERT_OK(base->ConfigureFromString(config_options_, "unique={id=}"));
  ASSERT_EQ(simple->cu, nullptr);
  ASSERT_EQ(A_count, 2);
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

TEST_F(CustomizableTest, URLFactoryTest) {
  std::unique_ptr<TestCustomizable> unique;
  ConfigOptions ignore = config_options_;
  ignore.ignore_unsupported_options = false;
  ignore.ignore_unsupported_options = false;
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "A=1;x=y", &unique));
  ASSERT_NE(unique, nullptr);
  ASSERT_EQ(unique->GetId(), "A=1;x=y");
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "A;x=y", &unique));
  ASSERT_NE(unique, nullptr);
  ASSERT_EQ(unique->GetId(), "A;x=y");
  unique.reset();
  ASSERT_OK(TestCustomizable::CreateFromString(ignore, "A=1?x=y", &unique));
  ASSERT_NE(unique, nullptr);
  ASSERT_EQ(unique->GetId(), "A=1?x=y");
}

TEST_F(CustomizableTest, MutableOptionsTest) {
  static std::unordered_map<std::string, OptionTypeInfo> mutable_option_info = {
      {"mutable",
       OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
           0, OptionVerificationType::kNormal, OptionTypeFlags::kMutable)}};
  static std::unordered_map<std::string, OptionTypeInfo> immutable_option_info =
      {{"immutable",
        OptionTypeInfo::AsCustomSharedPtr<TestCustomizable>(
            0, OptionVerificationType::kNormal, OptionTypeFlags::kAllowNull)}};

  class MutableCustomizable : public Customizable {
   private:
    std::shared_ptr<TestCustomizable> mutable_;
    std::shared_ptr<TestCustomizable> immutable_;

   public:
    MutableCustomizable() {
      RegisterOptions("mutable", &mutable_, &mutable_option_info);
      RegisterOptions("immutable", &immutable_, &immutable_option_info);
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
  ASSERT_OK(mc.ConfigureOption(options, "mutable", "{int=22;bool=false}"));
  mm_a = mm->get()->GetOptions<AOptions>("A");
  ASSERT_EQ(mm_a->i, 22);
  ASSERT_EQ(mm_a->b, false);

  // Only the mutable options should get serialized
  options.mutable_options_only = false;
  ASSERT_OK(mc.ConfigureOption(options, "immutable", "{id=B;}"));
  options.mutable_options_only = true;

  std::string opt_str;
  ASSERT_OK(mc.GetOptionString(options, &opt_str));
  MutableCustomizable mc2;
  ASSERT_OK(mc2.ConfigureFromString(options, opt_str));
  std::string mismatch;
  ASSERT_TRUE(mc.AreEquivalent(options, &mc2, &mismatch));
  options.mutable_options_only = false;
  ASSERT_FALSE(mc.AreEquivalent(options, &mc2, &mismatch));
  ASSERT_EQ(mismatch, "immutable");
}
#endif  // !ROCKSDB_LITE

class TestSecondaryCache : public SecondaryCache {
 public:
  static const char* kClassName() { return "Test"; }
  const char* Name() const override { return kClassName(); }
  Status Insert(const Slice& /*key*/, void* /*value*/,
                const Cache::CacheItemHelper* /*helper*/) override {
    return Status::NotSupported();
  }
  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& /*key*/, const Cache::CreateCallback& /*create_cb*/,
      bool /*wait*/) override {
    return nullptr;
  }
  void Erase(const Slice& /*key*/) override {}

  // Wait for a collection of handles to become ready
  void WaitAll(std::vector<SecondaryCacheResultHandle*> /*handles*/) override {}

  std::string GetPrintableOptions() const override { return ""; }
};

#ifndef ROCKSDB_LITE
// This method loads existing test classes into the ObjectRegistry
static int RegisterTestObjects(ObjectLibrary& library,
                               const std::string& /*arg*/) {
  size_t num_types;
  library.Register<TableFactory>(
      "MockTable",
      [](const std::string& /*uri*/, std::unique_ptr<TableFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new mock::MockTableFactory());
        return guard->get();
      });
  library.Register<const Comparator>(
      test::SimpleSuffixReverseComparator::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<const Comparator>* /*guard*/,
         std::string* /* errmsg */) {
        static test::SimpleSuffixReverseComparator ssrc;
        return &ssrc;
      });

  return static_cast<int>(library.GetFactoryCount(&num_types));
}

class MockEncryptionProvider : public EncryptionProvider {
 public:
  explicit MockEncryptionProvider(const std::string& id) : id_(id) {}
  const char* Name() const override { return "Mock"; }
  size_t GetPrefixLength() const override { return 0; }
  Status CreateNewPrefix(const std::string& /*fname*/, char* /*prefix*/,
                         size_t /*prefixLength*/) const override {
    return Status::NotSupported();
  }

  Status AddCipher(const std::string& /*descriptor*/, const char* /*cipher*/,
                   size_t /*len*/, bool /*for_write*/) override {
    return Status::NotSupported();
  }

  Status CreateCipherStream(
      const std::string& /*fname*/, const EnvOptions& /*options*/,
      Slice& /*prefix*/,
      std::unique_ptr<BlockAccessCipherStream>* /*result*/) override {
    return Status::NotSupported();
  }
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override {
    if (EndsWith(id_, "://test")) {
      return EncryptionProvider::ValidateOptions(db_opts, cf_opts);
    } else {
      return Status::InvalidArgument("MockProvider not initialized");
    }
  }

 private:
  std::string id_;
};

class MockCipher : public BlockCipher {
 public:
  const char* Name() const override { return "Mock"; }
  size_t BlockSize() override { return 0; }
  Status Encrypt(char* /*data*/) override { return Status::NotSupported(); }
  Status Decrypt(char* data) override { return Encrypt(data); }
};

class TestFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
 public:
  TestFlushBlockPolicyFactory() {}

  static const char* kClassName() { return "TestFlushBlockPolicyFactory"; }
  const char* Name() const override { return kClassName(); }

  FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& /*table_options*/,
      const BlockBuilder& /*data_block_builder*/) const override {
    return nullptr;
  }
};

static int RegisterLocalObjects(ObjectLibrary& library,
                                const std::string& /*arg*/) {
  size_t num_types;
  // Load any locally defined objects here
  library.Register<EncryptionProvider>(
      "Mock(://test)?",
      [](const std::string& uri, std::unique_ptr<EncryptionProvider>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockEncryptionProvider(uri));
        return guard->get();
      });
  library.Register<BlockCipher>("Mock", [](const std::string& /*uri*/,
                                           std::unique_ptr<BlockCipher>* guard,
                                           std::string* /* errmsg */) {
    guard->reset(new MockCipher());
    return guard->get();
  });
  library.Register<FlushBlockPolicyFactory>(
      TestFlushBlockPolicyFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<FlushBlockPolicyFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new TestFlushBlockPolicyFactory());
        return guard->get();
      });

  library.Register<SecondaryCache>(
      TestSecondaryCache::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<SecondaryCache>* guard,
         std::string* /* errmsg */) {
        guard->reset(new TestSecondaryCache());
        return guard->get();
      });
  return static_cast<int>(library.GetFactoryCount(&num_types));
}
#endif  // !ROCKSDB_LITE

class LoadCustomizableTest : public testing::Test {
 public:
  LoadCustomizableTest() {
    config_options_.ignore_unsupported_options = false;
    config_options_.invoke_prepare_options = false;
  }
  bool RegisterTests(const std::string& arg) {
#ifndef ROCKSDB_LITE
    config_options_.registry->AddLibrary("custom-tests", RegisterTestObjects,
                                         arg);
    config_options_.registry->AddLibrary("local-tests", RegisterLocalObjects,
                                         arg);
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
  ColumnFamilyOptions cf_opts;
  std::shared_ptr<TableFactory> factory;
  ASSERT_NOK(
      TableFactory::CreateFromString(config_options_, "MockTable", &factory));
  ASSERT_OK(TableFactory::CreateFromString(
      config_options_, TableFactory::kBlockBasedTableName(), &factory));
  ASSERT_NE(factory, nullptr);
  ASSERT_STREQ(factory->Name(), TableFactory::kBlockBasedTableName());
#ifndef ROCKSDB_LITE
  std::string opts_str = "table_factory=";
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options_, ColumnFamilyOptions(),
      opts_str + TableFactory::kBlockBasedTableName(), &cf_opts));
  ASSERT_NE(cf_opts.table_factory.get(), nullptr);
  ASSERT_STREQ(cf_opts.table_factory->Name(),
               TableFactory::kBlockBasedTableName());
#endif  // ROCKSDB_LITE
  if (RegisterTests("Test")) {
    ASSERT_OK(
        TableFactory::CreateFromString(config_options_, "MockTable", &factory));
    ASSERT_NE(factory, nullptr);
    ASSERT_STREQ(factory->Name(), "MockTable");
#ifndef ROCKSDB_LITE
    ASSERT_OK(
        GetColumnFamilyOptionsFromString(config_options_, ColumnFamilyOptions(),
                                         opts_str + "MockTable", &cf_opts));
    ASSERT_NE(cf_opts.table_factory.get(), nullptr);
    ASSERT_STREQ(cf_opts.table_factory->Name(), "MockTable");
#endif  // ROCKSDB_LITE
  }
}

TEST_F(LoadCustomizableTest, LoadSecondaryCacheTest) {
  std::shared_ptr<SecondaryCache> result;
  ASSERT_NOK(SecondaryCache::CreateFromString(
      config_options_, TestSecondaryCache::kClassName(), &result));
  if (RegisterTests("Test")) {
    ASSERT_OK(SecondaryCache::CreateFromString(
        config_options_, TestSecondaryCache::kClassName(), &result));
    ASSERT_NE(result, nullptr);
    ASSERT_STREQ(result->Name(), TestSecondaryCache::kClassName());
  }
}

TEST_F(LoadCustomizableTest, LoadComparatorTest) {
  const Comparator* bytewise = BytewiseComparator();
  const Comparator* reverse = ReverseBytewiseComparator();

  const Comparator* result = nullptr;
  ASSERT_NOK(Comparator::CreateFromString(
      config_options_, test::SimpleSuffixReverseComparator::kClassName(),
      &result));
  ASSERT_OK(
      Comparator::CreateFromString(config_options_, bytewise->Name(), &result));
  ASSERT_EQ(result, bytewise);
  ASSERT_OK(
      Comparator::CreateFromString(config_options_, reverse->Name(), &result));
  ASSERT_EQ(result, reverse);

  if (RegisterTests("Test")) {
    ASSERT_OK(Comparator::CreateFromString(
        config_options_, test::SimpleSuffixReverseComparator::kClassName(),
        &result));
    ASSERT_NE(result, nullptr);
    ASSERT_STREQ(result->Name(),
                 test::SimpleSuffixReverseComparator::kClassName());
  }
}

#ifndef ROCKSDB_LITE
TEST_F(LoadCustomizableTest, LoadEncryptionProviderTest) {
  std::shared_ptr<EncryptionProvider> result;
  ASSERT_NOK(
      EncryptionProvider::CreateFromString(config_options_, "Mock", &result));
  ASSERT_OK(
      EncryptionProvider::CreateFromString(config_options_, "CTR", &result));
  ASSERT_NE(result, nullptr);
  ASSERT_STREQ(result->Name(), "CTR");
  ASSERT_NOK(result->ValidateOptions(DBOptions(), ColumnFamilyOptions()));
  ASSERT_OK(EncryptionProvider::CreateFromString(config_options_, "CTR://test",
                                                 &result));
  ASSERT_NE(result, nullptr);
  ASSERT_STREQ(result->Name(), "CTR");
  ASSERT_OK(result->ValidateOptions(DBOptions(), ColumnFamilyOptions()));

  if (RegisterTests("Test")) {
    ASSERT_OK(
        EncryptionProvider::CreateFromString(config_options_, "Mock", &result));
    ASSERT_NE(result, nullptr);
    ASSERT_STREQ(result->Name(), "Mock");
    ASSERT_OK(EncryptionProvider::CreateFromString(config_options_,
                                                   "Mock://test", &result));
    ASSERT_NE(result, nullptr);
    ASSERT_STREQ(result->Name(), "Mock");
    ASSERT_OK(result->ValidateOptions(DBOptions(), ColumnFamilyOptions()));
  }
}

TEST_F(LoadCustomizableTest, LoadEncryptionCipherTest) {
  std::shared_ptr<BlockCipher> result;
  ASSERT_NOK(BlockCipher::CreateFromString(config_options_, "Mock", &result));
  ASSERT_OK(BlockCipher::CreateFromString(config_options_, "ROT13", &result));
  ASSERT_NE(result, nullptr);
  ASSERT_STREQ(result->Name(), "ROT13");
  if (RegisterTests("Test")) {
    ASSERT_OK(BlockCipher::CreateFromString(config_options_, "Mock", &result));
    ASSERT_NE(result, nullptr);
    ASSERT_STREQ(result->Name(), "Mock");
  }
}
#endif  // !ROCKSDB_LITE

TEST_F(LoadCustomizableTest, LoadFlushBlockPolicyFactoryTest) {
  std::shared_ptr<TableFactory> table;
  std::shared_ptr<FlushBlockPolicyFactory> result;
  ASSERT_NOK(FlushBlockPolicyFactory::CreateFromString(
      config_options_, "TestFlushBlockPolicyFactory", &result));

  ASSERT_OK(
      FlushBlockPolicyFactory::CreateFromString(config_options_, "", &result));
  ASSERT_NE(result, nullptr);
  ASSERT_STREQ(result->Name(), FlushBlockBySizePolicyFactory::kClassName());

  ASSERT_OK(FlushBlockPolicyFactory::CreateFromString(
      config_options_, FlushBlockEveryKeyPolicyFactory::kClassName(), &result));
  ASSERT_NE(result, nullptr);
  ASSERT_STREQ(result->Name(), FlushBlockEveryKeyPolicyFactory::kClassName());

  ASSERT_OK(FlushBlockPolicyFactory::CreateFromString(
      config_options_, FlushBlockBySizePolicyFactory::kClassName(), &result));
  ASSERT_NE(result, nullptr);
  ASSERT_STREQ(result->Name(), FlushBlockBySizePolicyFactory::kClassName());
#ifndef ROCKSDB_LITE
  std::string table_opts = "id=BlockBasedTable; flush_block_policy_factory=";
  ASSERT_OK(TableFactory::CreateFromString(
      config_options_,
      table_opts + FlushBlockEveryKeyPolicyFactory::kClassName(), &table));
  auto bbto = table->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(bbto, nullptr);
  ASSERT_NE(bbto->flush_block_policy_factory.get(), nullptr);
  ASSERT_STREQ(bbto->flush_block_policy_factory->Name(),
               FlushBlockEveryKeyPolicyFactory::kClassName());
  if (RegisterTests("Test")) {
    ASSERT_OK(FlushBlockPolicyFactory::CreateFromString(
        config_options_, "TestFlushBlockPolicyFactory", &result));
    ASSERT_NE(result, nullptr);
    ASSERT_STREQ(result->Name(), "TestFlushBlockPolicyFactory");
    ASSERT_OK(TableFactory::CreateFromString(
        config_options_, table_opts + "TestFlushBlockPolicyFactory", &table));
    bbto = table->GetOptions<BlockBasedTableOptions>();
    ASSERT_NE(bbto, nullptr);
    ASSERT_NE(bbto->flush_block_policy_factory.get(), nullptr);
    ASSERT_STREQ(bbto->flush_block_policy_factory->Name(),
                 "TestFlushBlockPolicyFactory");
  }
#endif  // ROCKSDB_LITE
}

}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
