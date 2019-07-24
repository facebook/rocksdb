//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cctype>
#include <cinttypes>
#include <cstring>
#include <unordered_map>

#include "rocksdb/configurable.h"

#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace rocksdb {
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

struct SimpleOptions {
  int i = 0;
  bool b = false;
  bool d = true;
  std::string s = "";
  std::string u = "";
};

static std::unordered_map<std::string, OptionTypeInfo> simple_option_info = {
#ifndef ROCKSDB_LITE
    {"int",
     {offsetof(struct SimpleOptions, i), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"bool",
     {offsetof(struct SimpleOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"string",
     {offsetof(struct SimpleOptions, s), OptionType::kString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"unknown",
     {offsetof(struct SimpleOptions, u), OptionType::kUnknown,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0,
      [](const DBOptions& /*opts*/, const std::string& /* name */, char* addr,
         const std::string& value) {
        *reinterpret_cast<std::string*>(addr) = value;
        return Status::OK();
      },
      [](uint32_t, const std::string&, const char* addr, std::string* value) {
        *value = *reinterpret_cast<const std::string*>(addr);
        return Status::OK();
      },
      [](OptionsSanityCheckLevel, const std::string&, const char* this_addr,
         const char* that_addr) {
        return (*reinterpret_cast<const std::string*>(this_addr) ==
                *reinterpret_cast<const std::string*>(that_addr));
      }}},
#endif  // ROCKSDB_LITE
};

class SimpleConfigurable : public Configurable {
 private:
  std::string name_;
  std::string prefix_;
  std::shared_ptr<SimpleOptions> options_;
  std::unordered_map<std::string, OptionTypeInfo> map_;

 public:
  SimpleConfigurable(const std::string& name,
                     const std::shared_ptr<SimpleOptions>& options,
                     const OptionTypeMap& map)
      : name_(name), options_(options), map_(map) {
    prefix_ = "test." + name + ".";
    RegisterOptionsMap(name, options_.get(), map);
  }

 protected:
  const std::string& GetOptionsPrefix() const override { return prefix_; }

  Status SetUnknown(const DBOptions& opts, const std::string& name,
                    const std::string& value) override {
    if (name == "unknown") {
      options_->u = value;
      return Status::OK();
    } else {
      return Configurable::SetUnknown(opts, name, value);
    }
  }

  Status Validate(const DBOptions& db_opts,
                  const ColumnFamilyOptions& cf_opts) const override {
    if (!options_->b) {
      return Status::InvalidArgument("Sanitized must be true");
    } else {
      return Configurable::Validate(db_opts, cf_opts);
    }
  }

  Status Sanitize(DBOptions& db_opts, ColumnFamilyOptions& cf_opts) override {
    options_->b = true;
    return Configurable::Sanitize(db_opts, cf_opts);
  }

  Status UnknownToString(uint32_t mode, const std::string& name,
                         std::string* result) const override {
    if (name == "unknown") {
      if (!options_->u.empty()) {
        *result = options_->u;
      }
      return Status::OK();
    } else {
      return Configurable::UnknownToString(mode, name, result);
    }
  }

#ifndef ROCKSDB_LITE
  bool IsUnknownEqual(const std::string& name, const OptionTypeInfo& type_info,
                      OptionsSanityCheckLevel sanity_check_level,
                      const char* this_addr,
                      const char* that_addr) const override {
    if (name == "unknown") {
      return (*(reinterpret_cast<const std::string*>(this_addr)) ==
              *(reinterpret_cast<const std::string*>(that_addr)));
    } else {
      return Configurable::IsUnknownEqual(name, type_info, sanity_check_level,
                                          this_addr, that_addr);
    }
  }
#endif
};  // End class SimpleConfigurable

static Configurable* NewSimpleConfigurable(const std::string& name = "simple") {
  std::shared_ptr<SimpleOptions> options = std::make_shared<SimpleOptions>();
  return new SimpleConfigurable(name, options, simple_option_info);
}

// A NestedOptions/Configurable has another Configurable as part of its options
// (has-a)
struct NestedOptions : public SimpleOptions {
  NestedOptions(const std::string& inner, int mode) {
    if (mode > 0) {
      unique.reset(NewSimpleConfigurable(inner));
    } else if (mode < 0) {
      shared.reset(NewSimpleConfigurable(inner));
    } else {
      config = NewSimpleConfigurable(inner);
    }
  }

  std::unique_ptr<Configurable> unique;
  std::shared_ptr<Configurable> shared;
  Configurable* config = nullptr;
};

static NestedOptions dummy_nested_options("inner", 1);

template <typename T1>
int offset_of(T1 SimpleOptions::*member) {
  return int(size_t(&(dummy_nested_options.*member)) -
             size_t(&dummy_nested_options));
}

template <typename T1>
int offset_of(T1 NestedOptions::*member) {
  return int(size_t(&(dummy_nested_options.*member)) -
             size_t(&dummy_nested_options));
}

// A MutableOptions/Configurable has another Configurable as part of its options
struct MutableOptions : public SimpleOptions {
  MutableOptions() { unique.reset(NewSimpleConfigurable("mutable")); }
  std::unique_ptr<Configurable> unique;
};

static MutableOptions dummy_mutable_options;

template <typename T1>
int offset_of(T1 MutableOptions::*member) {
  return int(size_t(&(dummy_mutable_options.*member)) -
             size_t(&dummy_mutable_options));
}

static OptionTypeMap nested_option_info = {
#ifndef ROCKSDB_LITE
    {"int",
     {offset_of(&NestedOptions::i), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
      offset_of(&MutableOptions::i)}},
    {"bool",
     {offset_of(&NestedOptions::b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"string",
     {offset_of(&NestedOptions::s), OptionType::kString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"unique",
     {offset_of(&NestedOptions::unique), OptionType::kConfigurable,
      OptionVerificationType::kNormal, OptionTypeFlags::kMConfigurableU,
      offset_of(&MutableOptions::unique)}},
    {"shared",
     {offset_of(&NestedOptions::shared), OptionType::kConfigurable,
      OptionVerificationType::kNormal, OptionTypeFlags::kConfigurableS, 0}},
    {"config",
     {offset_of(&NestedOptions::config), OptionType::kConfigurable,
      OptionVerificationType::kNormal, OptionTypeFlags::kConfigurableP, 0}},
#endif  // ROCKSDB_LITE
};

static Configurable* NewNestedConfigurable(const std::string& outer,
                                           const std::string& inner, int mode) {
  std::shared_ptr<Configurable> result;
  std::shared_ptr<NestedOptions> options =
      std::make_shared<NestedOptions>(inner, mode);
  return new SimpleConfigurable(outer, options, nested_option_info);
}

class MutableConfigurable : public SimpleConfigurable {
 public:
  MutableConfigurable(const std::string& name)
      : SimpleConfigurable(name, std::make_shared<MutableOptions>(),
                           nested_option_info) {}

 protected:
  bool IsMutable() const override { return true; }
};

struct EmbeddedOptions : public SimpleOptions {
  EmbeddedOptions(const std::string& name = "embedded")
      : embedded(name, std::make_shared<SimpleOptions>(), simple_option_info) {}
  SimpleConfigurable embedded;
};

static EmbeddedOptions dummy_embedded_options;

template <typename T1>
int offset_of(T1 EmbeddedOptions::*member) {
  return int(size_t(&(dummy_embedded_options.*member)) -
             size_t(&dummy_embedded_options));
}

static OptionTypeMap embedded_option_info = {
#ifndef ROCKSDB_LITE
    {"int",
     {offset_of(&EmbeddedOptions::i), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"bool",
     {offset_of(&EmbeddedOptions::b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"string",
     {offset_of(&EmbeddedOptions::s), OptionType::kString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"embedded",
     {offset_of(&EmbeddedOptions::embedded), OptionType::kConfigurable,
      OptionVerificationType::kNormal, OptionTypeFlags::kConfigurable, 0}},
#endif  // ROCKSDB_LITE
};

static Configurable* NewEmbeddedConfigurable(
    const std::string& outer = "outer", const std::string& inner = "embedded") {
  std::shared_ptr<EmbeddedOptions> options =
      std::make_shared<EmbeddedOptions>(inner);
  return new SimpleConfigurable(outer, options, embedded_option_info);
}

static OptionTypeMap inherited_option_info = {
#ifndef ROCKSDB_LITE
    {"outer.int",
     {offsetof(struct SimpleOptions, i), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"outer.bool",
     {offsetof(struct SimpleOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"outer.string",
     {offsetof(struct SimpleOptions, s), OptionType::kString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
#endif  // ROCKSDB_LITE
};

// This is a class that extents a SimpleConfigurable (is-a)
class InheritedConfigurable : public SimpleConfigurable {
 private:
  SimpleOptions outer_;

 public:
  InheritedConfigurable(const std::string& name, OptionTypeMap& map,
                        const std::shared_ptr<SimpleOptions>& inner)
      : SimpleConfigurable(name, inner, map) {
    RegisterOptionsMap("outer", &outer_, inherited_option_info);
  }

  Status SetUnknown(const DBOptions& opts, const std::string& name,
                    const std::string& value) override {
    if (name == "outer.unknown") {
      outer_.u = value;
      return Status::OK();
    } else {
      return SimpleConfigurable::SetUnknown(opts, name, value);
    }
  }

  Status UnknownToString(uint32_t mode, const std::string& name,
                         std::string* result) const override {
    if (name == "other.unknown") {
      *result = outer_.u;
      return Status::OK();
    } else {
      return SimpleConfigurable::UnknownToString(mode, name, result);
    }
  }

#ifndef ROCKSDB_LITE
  bool IsUnknownEqual(const std::string& name, const OptionTypeInfo& type_info,
                      OptionsSanityCheckLevel sanity_check_level,
                      const char* this_addr,
                      const char* that_addr) const override {
    if (name == "outer.unknown") {
      return (*(reinterpret_cast<const std::string*>(this_addr)) ==
              *(reinterpret_cast<const std::string*>(that_addr)));
    } else {
      return SimpleConfigurable::IsUnknownEqual(
          name, type_info, sanity_check_level, this_addr, that_addr);
    }
  }
#endif  // ROCKSDB_LITE
};

static Configurable* NewInheritedSimpleConfigurable(const std::string& name) {
  std::shared_ptr<SimpleOptions> options = std::make_shared<SimpleOptions>();
  return new InheritedConfigurable(name, simple_option_info, options);
}

static Configurable* NewInheritedNestedConfigurable(const std::string& inner,
                                                    const std::string& nested,
                                                    int mode) {
  std::shared_ptr<NestedOptions> options =
      std::make_shared<NestedOptions>(nested, mode);
  return new InheritedConfigurable(inner, nested_option_info, options);
}

using ConfigTestFactoryFunc = std::function<Configurable*()>;

class ConfigurableTest : public testing::Test {
 public:
  DBOptions db_opts_;
};

class ConfigurableParamTest
    : public ConfigurableTest,
      virtual public ::testing::WithParamInterface<
          std::pair<std::string, ConfigTestFactoryFunc> > {
 public:
  ConfigurableParamTest() {
    configuration_ = GetParam().first;
    factory_ = GetParam().second;
    object_.reset(factory_());
  }
  ConfigTestFactoryFunc factory_;
  std::string configuration_;
  std::unique_ptr<Configurable> object_;
};

TEST_F(ConfigurableTest, GetOptionsPtrTest) {
  std::string opt_str;
  std::unique_ptr<Configurable> configurable(NewSimpleConfigurable());
  ASSERT_NE(configurable->GetOptions<SimpleOptions>("simple"), nullptr);
  ASSERT_EQ(configurable->GetOptions<SimpleOptions>("bad-opt"), nullptr);
}

#ifndef ROCKSDB_LITE  // GetOptionsFromMap is not supported in ROCKSDB_LITE
TEST_F(ConfigurableTest, ConfigureFromMapTest) {
  std::unordered_map<std::string, std::string> options_map = {
      {"int", "1"}, {"bool", "true"}, {"string", "string"}};
  std::unique_ptr<Configurable> configurable(NewSimpleConfigurable());
  ASSERT_OK(configurable->ConfigureFromMap(db_opts_, options_map));
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_EQ(simple->i, 1);
  ASSERT_EQ(simple->b, true);
  ASSERT_EQ(simple->s, "string");
}

TEST_F(ConfigurableTest, ConfigureFromStringTest) {
  std::unique_ptr<Configurable> configurable(NewSimpleConfigurable());
  ASSERT_OK(
      configurable->ConfigureFromString(db_opts_, "int=1;bool=true;string=s"));
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_EQ(simple->i, 1);
  ASSERT_EQ(simple->b, true);
  ASSERT_EQ(simple->s, "s");
}

TEST_F(ConfigurableTest, ConfigureFromOptionsTest) {
  std::unique_ptr<Configurable> configurable(NewSimpleConfigurable());
  std::unordered_map<std::string, std::string> options_map = {{"unused", "u"}};
  ASSERT_NOK(configurable->ConfigureFromMap(db_opts_, options_map));
  ASSERT_OK(configurable->ConfigureFromMap(db_opts_, options_map, false, true));
  ASSERT_NOK(configurable->ConfigureFromString(db_opts_, "unused=u"));
  ASSERT_OK(
      configurable->ConfigureFromString(db_opts_, "unused=u", false, true));
}

TEST_F(ConfigurableTest, ConfigureUnusedOptionsTest) {
  std::unique_ptr<Configurable> configurable(NewSimpleConfigurable());
  std::unordered_set<std::string> unused;
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_OK(
      configurable->ConfigureFromString(db_opts_, "unused=u", false, &unused));
  ASSERT_EQ(unused.size(), 1);
  unused.clear();
  ASSERT_OK(configurable->ConfigureOption(db_opts_, "int", "42"));
  ASSERT_EQ(simple->i, 42);
  ASSERT_OK(
      configurable->ConfigureFromString(db_opts_, "int=u", false, &unused));
  ASSERT_EQ(simple->i, 42);
  ASSERT_OK(configurable->ConfigureFromString(db_opts_, "int=33;unused=u",
                                              false, &unused));
  ASSERT_EQ(simple->i, 33);
}

TEST_F(ConfigurableTest, ConfigureFromPropsTest) {
  std::string opt_str;
  std::unique_ptr<Configurable> simple(
      NewNestedConfigurable("outer", "inner", 1));
  ASSERT_OK(simple->ConfigureFromString(
      db_opts_, "int=24;string=outer;unique={int=42;string=nested}"));
  ASSERT_OK(simple->GetOptionString(
      OptionStringMode::kOptionPrefix | OptionStringMode::kOptionDetached,
      &opt_str, "\n"));
  std::istringstream iss(opt_str);
  std::unordered_map<std::string, std::string> copy_map;
  std::string line;
  for (int line_num = 0; std::getline(iss, line); line_num++) {
    std::string name;
    std::string value;
    ASSERT_OK(
        RocksDBOptionsParser::ParseStatement(&name, &value, line, line_num));
    copy_map[name] = value;
  }
  std::unique_ptr<Configurable> copy(
      NewNestedConfigurable("outer", "inner", 1));
  ASSERT_OK(copy->ConfigureFromMap(db_opts_, copy_map));
  ASSERT_TRUE(simple->Matches(copy.get(),
                              OptionsSanityCheckLevel::kSanityLevelExactMatch));
}

TEST_F(ConfigurableTest, GetOptionsTest) {
  std::string value;
  std::unique_ptr<Configurable> simple(
      NewNestedConfigurable("outer", "inner", 1));
  ASSERT_OK(simple->ConfigureOption(db_opts_, "int", "24"));
  ASSERT_OK(simple->GetOption("int", &value));
  ASSERT_EQ(value, "24");

  ASSERT_OK(simple->ConfigureOption(db_opts_, "string", "outer"));
  ASSERT_OK(simple->GetOption("string", &value));
  ASSERT_EQ(value, "outer");

  ASSERT_OK(
      simple->ConfigureOption(db_opts_, "unique", "int=42;string=nested"));
  ASSERT_OK(simple->GetOption("unique", &value));
  std::unordered_map<std::string, std::string> map;
  ASSERT_OK(StringToMap(value, &map));
  auto iter = map.find("int");
  ASSERT_NE(iter, map.end());
  ASSERT_EQ(iter->second, "42");
  iter = map.find("string");
  ASSERT_NE(iter, map.end());
  ASSERT_EQ(iter->second, "nested");
  ASSERT_OK(simple->GetOption("unique.int", &value));
  ASSERT_EQ(value, "42");
  ASSERT_OK(simple->GetOption("shared", &value));
  ASSERT_EQ(value, "");  // There is a shared option, just no config
  ASSERT_NOK(simple->GetOption("shared.int", &value));
  ASSERT_NOK(simple->GetOption("unique.bad", &value));
  ASSERT_NOK(simple->GetOption("bad option", &value));
  ASSERT_TRUE(value.empty());
}

TEST_F(ConfigurableTest, GetOptionNamesTest) {
  std::string value;
  std::unordered_set<std::string> names;
  std::unique_ptr<Configurable> simple(
      NewNestedConfigurable("outer", "inner", 1));
  ASSERT_OK(simple->GetOptionString(
      OptionStringMode::kOptionDetached | OptionStringMode::kOptionPrefix,
      &value, "; "));
  ASSERT_OK(simple->ConfigureFromString(db_opts_, value));
  ASSERT_OK(simple->GetOptionNames(&names));
  for (auto& n : names) {
    ASSERT_OK(simple->GetOption(n, &value));
  }
  names.clear();
  ASSERT_OK(simple->GetOptionNames(OptionStringMode::kOptionPrefix, &names));
  for (auto& n : names) {
    ASSERT_OK(simple->GetOption(n, &value));
  }
}

TEST_F(ConfigurableTest, ConfigureBadOptionsTest) {
  std::unique_ptr<Configurable> configurable(NewSimpleConfigurable());
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_OK(configurable->ConfigureOption(db_opts_, "int", "42"));
  ASSERT_EQ(simple->i, 42);
  ASSERT_NOK(configurable->ConfigureOption(db_opts_, "int", "fred"));
  ASSERT_NOK(configurable->ConfigureOption(db_opts_, "bool", "fred"));
  ASSERT_NOK(configurable->ConfigureFromString(db_opts_, "int=33;unused=u"));
  ASSERT_EQ(simple->i, 42);
}

TEST_F(ConfigurableTest, ConfigureMissingOptionsTest) {
  std::unique_ptr<Configurable> simple(
      NewNestedConfigurable("outer", "inner", 1));
  ASSERT_NOK(simple->ConfigureFromString(db_opts_,
                                         "unique={missing=42;string=nested}"));
  ASSERT_NOK(simple->ConfigureFromString(
      db_opts_, "unique.missing=42;unique.string=nested"));
  ASSERT_OK(simple->ConfigureFromString(
      db_opts_, "unique={missing=42;string=nested}", false, true));
  ASSERT_OK(simple->ConfigureFromString(
      db_opts_, "unique.missing=42;unique.string=nested", false, true));
}

TEST_F(ConfigurableTest, SetUnknownTest) {
  std::string opt_str;
  std::unique_ptr<Configurable> configurable(NewSimpleConfigurable());
  ASSERT_OK(configurable->ConfigureFromString(db_opts_, "unknown=u"));
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_EQ(simple->u, "u");
  ASSERT_OK(configurable->GetOptionString(&opt_str, ";"));
  std::unique_ptr<Configurable> copy(NewSimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromString(db_opts_, opt_str));
  simple = copy->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_EQ(simple->u, "u");
}

TEST_F(ConfigurableTest, InvalidOptionTest) {
  std::unique_ptr<Configurable> configurable(NewSimpleConfigurable());
  std::unordered_map<std::string, std::string> options_map = {
      {"bad-option", "bad"}};
  ASSERT_NOK(configurable->ConfigureFromMap(db_opts_, options_map));
  ASSERT_NOK(configurable->ConfigureFromString(db_opts_, "bad-option=bad"));
  ASSERT_NOK(configurable->ConfigureOption(db_opts_, "bad-option", "bad"));
}

TEST_F(ConfigurableTest, ValidateOptionTest) {
  std::unique_ptr<Configurable> configurable(NewSimpleConfigurable());
  DBOptions db_opts;
  ColumnFamilyOptions cf_opts;
  ASSERT_OK(configurable->ConfigureOption(db_opts, "bool", "false"));
  ASSERT_NOK(configurable->ValidateOptions(db_opts, cf_opts));
  ASSERT_OK(configurable->SanitizeOptions(db_opts, cf_opts));
  ASSERT_OK(configurable->ValidateOptions(db_opts, cf_opts));
}

TEST_F(ConfigurableTest, DeprecatedOptionsTest) {
  static std::unordered_map<std::string, OptionTypeInfo>
      deprecated_option_info = {
          {"deprecated",
           {offsetof(struct SimpleOptions, b), OptionType::kBoolean,
            OptionVerificationType::kDeprecated, OptionTypeFlags::kNone, 0}}};
  std::unique_ptr<Configurable> orig(new SimpleConfigurable(
      "simple", std::make_shared<SimpleOptions>(), deprecated_option_info));
  SimpleOptions* simple = orig->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  simple->d = true;
  ASSERT_OK(orig->ConfigureOption(db_opts_, "deprecated", "false"));
  ASSERT_TRUE(simple->d);
  ASSERT_OK(orig->ConfigureFromString(db_opts_, "deprecated=false"));
  ASSERT_TRUE(simple->d);
}

TEST_F(ConfigurableTest, AliasOptionsTest) {
  static std::unordered_map<std::string, OptionTypeInfo> alias_option_info = {
      {"bool",
       {offsetof(struct SimpleOptions, b), OptionType::kBoolean,
        OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
      {"alias",
       {offsetof(struct SimpleOptions, b), OptionType::kBoolean,
        OptionVerificationType::kAlias, OptionTypeFlags::kNone, 0}}};
  std::unique_ptr<Configurable> orig(new SimpleConfigurable(
      "simple", std::make_shared<SimpleOptions>(), alias_option_info));
  SimpleOptions* simple = orig->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_OK(orig->ConfigureOption(db_opts_, "alias", "true"));
  ASSERT_TRUE(simple->b);
  std::string opts_str;
  ASSERT_OK(orig->GetOptionString(&opts_str, ";"));
  ASSERT_EQ(opts_str.find("alias"), std::string::npos);

  ASSERT_OK(orig->ConfigureOption(db_opts_, "bool", "false"));
  ASSERT_FALSE(simple->b);
  ASSERT_OK(orig->GetOption("alias", &opts_str));
  ASSERT_EQ(opts_str, "false");
}

TEST_F(ConfigurableTest, NestedUniqueConfigTest) {
  std::unique_ptr<Configurable> simple(
      NewNestedConfigurable("outer", "inner", 1));
  NestedOptions* outer = simple->GetOptions<NestedOptions>("outer");
  ASSERT_NE(outer, nullptr);
  ASSERT_NE(outer->unique, nullptr);
  ASSERT_OK(simple->ConfigureFromString(db_opts_, "int=24;string=outer"));
  ASSERT_OK(
      simple->ConfigureFromString(db_opts_, "unique={int=42;string=nested}"));
  SimpleOptions* inner = outer->unique->GetOptions<SimpleOptions>("inner");
  ASSERT_NE(inner, nullptr);
  ASSERT_EQ(outer->i, 24);
  ASSERT_EQ(outer->s, "outer");
  ASSERT_EQ(inner->i, 42);
  ASSERT_EQ(inner->s, "nested");
}

TEST_F(ConfigurableTest, NestedSharedConfigTest) {
  std::unique_ptr<Configurable> simple(
      NewNestedConfigurable("outer", "inner", -1));
  ASSERT_OK(simple->ConfigureFromString(db_opts_, "int=24;string=outer"));
  ASSERT_OK(
      simple->ConfigureFromString(db_opts_, "shared={int=42;string=nested}"));
  NestedOptions* outer = simple->GetOptions<NestedOptions>("outer");
  ASSERT_NE(outer, nullptr);
  ASSERT_NE(outer->shared, nullptr);
  SimpleOptions* inner = outer->shared->GetOptions<SimpleOptions>("inner");
  ASSERT_EQ(outer->i, 24);
  ASSERT_EQ(outer->s, "outer");
  ASSERT_EQ(inner->i, 42);
  ASSERT_EQ(inner->s, "nested");
}

TEST_F(ConfigurableTest, NestedRawConfigTest) {
  std::unique_ptr<Configurable> simple(
      NewNestedConfigurable("outer", "inner", 0));
  ASSERT_OK(simple->ConfigureFromString(db_opts_, "int=24;string=outer"));
  ASSERT_OK(
      simple->ConfigureFromString(db_opts_, "config={int=42;string=nested}"));
  NestedOptions* outer = simple->GetOptions<NestedOptions>("outer");
  ASSERT_NE(outer, nullptr);
  ASSERT_NE(outer->config, nullptr);
  SimpleOptions* inner = outer->config->GetOptions<SimpleOptions>("inner");
  ASSERT_NE(inner, nullptr);
  ASSERT_EQ(outer->i, 24);
  ASSERT_EQ(outer->s, "outer");
  ASSERT_EQ(inner->i, 42);
  ASSERT_EQ(inner->s, "nested");
  delete outer->config;
}

TEST_F(ConfigurableTest, MatchesTest) {
  std::shared_ptr<NestedOptions> n1 =
      std::make_shared<NestedOptions>("unique", 1);
  n1->shared.reset(NewSimpleConfigurable("shared"));
  std::shared_ptr<NestedOptions> n2 =
      std::make_shared<NestedOptions>("unique", 1);
  n2->shared.reset(NewSimpleConfigurable("shared"));

  std::unique_ptr<Configurable> c1(
      new SimpleConfigurable("outer", n1, nested_option_info));
  std::unique_ptr<Configurable> c2(
      new SimpleConfigurable("outer", n2, nested_option_info));

  ASSERT_OK(c1->ConfigureFromString(
      db_opts_,
      "int=11;string=outer;unique={int=22;string=u};shared={int=33;string=s}"));
  ASSERT_OK(c2->ConfigureFromString(
      db_opts_,
      "int=11;string=outer;unique={int=22;string=u};shared={int=33;string=s}"));
  ASSERT_TRUE(
      c1->Matches(c2.get(), OptionsSanityCheckLevel::kSanityLevelExactMatch));
  ASSERT_OK(c2->ConfigureOption(db_opts_, "shared", "int=44"));
  std::string mismatch;
  ASSERT_FALSE(c1->Matches(
      c2.get(), OptionsSanityCheckLevel::kSanityLevelExactMatch, &mismatch));
  ASSERT_EQ(mismatch, "shared.int");
  std::string c1value, c2value;
  ASSERT_OK(c1->GetOption(mismatch, &c1value));
  ASSERT_OK(c2->GetOption(mismatch, &c2value));
  ASSERT_NE(c1value, c2value);
}

TEST_F(ConfigurableTest, MutableOptionsTest) {
  std::unique_ptr<Configurable> base(new MutableConfigurable("outer"));
  std::string opt_str;
  ASSERT_OK(
      base->ConfigureOption(db_opts_, "int", "24"));  // This one is mutable
  ASSERT_NOK(
      base->ConfigureOption(db_opts_, "string", "s"));  // This one is not
  ASSERT_OK(base->ConfigureOption(db_opts_, "unique", "int=42;string=nested"));
  ASSERT_OK(base->GetOption("int", &opt_str));  // Can get the mutable option
  ASSERT_NOK(base->GetOption("string", &opt_str));  // Cannot get this one
  ASSERT_OK(
      base->GetOptionString(OptionStringMode::kOptionPrefix, &opt_str, ";"));
  ASSERT_EQ(opt_str.find("test.outer.string"), std::string::npos);
  std::unique_ptr<Configurable> copy(new MutableConfigurable("outer"));
  ASSERT_OK(copy->ConfigureFromString(db_opts_, opt_str));
  ASSERT_TRUE(base->Matches(copy.get(),
                            OptionsSanityCheckLevel::kSanityLevelExactMatch));
  std::unique_ptr<Configurable> nested(
      NewNestedConfigurable("outer", "mutable", 1));
  ASSERT_OK(nested->ConfigureFromString(db_opts_, opt_str));
  ASSERT_FALSE(nested->Matches(
      base.get(), OptionsSanityCheckLevel::kSanityLevelExactMatch));
  ASSERT_TRUE(base->Matches(nested.get(),
                            OptionsSanityCheckLevel::kSanityLevelExactMatch));
  ASSERT_OK(
      nested->GetOptionString(OptionStringMode::kOptionPrefix, &opt_str, ";"));
  copy.reset(new MutableConfigurable("outer"));
  ASSERT_NOK(copy->ConfigureFromString(db_opts_, opt_str));
  ASSERT_OK(copy->ConfigureFromString(db_opts_, opt_str, false, true));
  ASSERT_TRUE(copy->Matches(nested.get(),
                            OptionsSanityCheckLevel::kSanityLevelExactMatch));
}

TEST_F(ConfigurableTest, ConfigureStructTest) {
  ConfigurableStruct<SimpleOptions> config(simple_option_info);
  std::string opts_str;
  ASSERT_OK(config.ConfigureFromString(db_opts_, "int=1;bool=true;string=s"));
  ASSERT_EQ(config.GetStructOptions()->i, 1);
  ASSERT_EQ(config.GetStructOptions()->b, true);
  ASSERT_EQ(config.GetStructOptions()->s, "s");
  ASSERT_OK(config.GetOptionString(&opts_str, ";"));
  ConfigurableStruct<SimpleOptions> copy(simple_option_info);
  ASSERT_OK(copy.ConfigureFromString(db_opts_, opts_str));
  ASSERT_TRUE(
      config.Matches(&copy, OptionsSanityCheckLevel::kSanityLevelExactMatch));
}

TEST_P(ConfigurableParamTest, GetOptionsStringTest) {
  std::string opt_str;
  ASSERT_OK(object_->ConfigureFromString(db_opts_, configuration_));
  ASSERT_OK(object_->GetOptionString(&opt_str, ";"));
  std::unique_ptr<Configurable> copy(factory_());
  ASSERT_OK(copy->ConfigureFromString(db_opts_, opt_str));
  ASSERT_TRUE(object_->Matches(
      copy.get(), OptionsSanityCheckLevel::kSanityLevelExactMatch));
}

TEST_P(ConfigurableParamTest, ConfigureDetachedOptionsTest) {
  std::string opt_str;
  ASSERT_OK(object_->ConfigureFromString(db_opts_, configuration_));
  ASSERT_OK(object_->GetOptionString(OptionStringMode::kOptionDetached,
                                     &opt_str, ";"));
  std::unique_ptr<Configurable> copy(factory_());
  ASSERT_OK(copy->ConfigureFromString(db_opts_, opt_str));
  ASSERT_TRUE(object_->Matches(
      copy.get(), OptionsSanityCheckLevel::kSanityLevelExactMatch));
}

TEST_P(ConfigurableParamTest, OptionsPrefixTest) {
  std::string opt_str;
  ASSERT_OK(object_->ConfigureFromString(db_opts_, configuration_));
  ASSERT_OK(
      object_->GetOptionString(OptionStringMode::kOptionPrefix, &opt_str, ";"));
  std::unique_ptr<Configurable> copy(factory_());
  ASSERT_OK(copy->ConfigureFromString(db_opts_, opt_str));
  ASSERT_TRUE(object_->Matches(
      copy.get(), OptionsSanityCheckLevel::kSanityLevelExactMatch));
  ASSERT_OK(object_->GetOptionString(
      OptionStringMode::kOptionPrefix | OptionStringMode::kOptionDetached,
      &opt_str, ";"));
  ASSERT_TRUE(object_->Matches(
      copy.get(), OptionsSanityCheckLevel::kSanityLevelExactMatch));
}

TEST_P(ConfigurableParamTest, DumpOptionsTest) {
  std::string opt_str;
  StringLogger logger;
  ASSERT_OK(object_->ConfigureFromString(db_opts_, configuration_));
  object_->Dump(&logger);
  logger.clear();
  object_->Dump(&logger, OptionStringMode::kOptionDetached);
  logger.clear();
  object_->Dump(&logger, OptionStringMode::kOptionPrefix);
  logger.clear();
  object_->Dump(&logger, OptionStringMode::kOptionDetached |
                             OptionStringMode::kOptionPrefix);
}

#endif  // !ROCKSDB_LITE

static Configurable* SimpleFactory() { return NewSimpleConfigurable("simple"); }

static Configurable* NestedUniqueFactory() {
  return NewNestedConfigurable("simple", "unique", 1);
}

static Configurable* NestedSharedFactory() {
  return NewNestedConfigurable("simple", "shared", -1);
}

static Configurable* EmbeddedFactory() {
  return NewEmbeddedConfigurable("outer", "embedded");
}

static Configurable* InheritedSimpleFactory() {
  return NewInheritedSimpleConfigurable("inner");
}

static Configurable* InheritedNestedUniqueFactory() {
  return NewInheritedNestedConfigurable("inner", "unique", 1);
}

INSTANTIATE_TEST_CASE_P(
    ParamTest, ConfigurableParamTest,
    testing::Values(std::pair<std::string, ConfigTestFactoryFunc>(
                        "int=42;bool=true;string=s", SimpleFactory),
                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "int=33;bool=true;string=outer;"
                        "unique={int=42;string=inner}",
                        NestedUniqueFactory),
                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "int=33;bool=true;string=outer;"
                        "shared={int=42;string=inner}",
                        NestedSharedFactory),
                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "int=33;bool=true;string=outer;"
                        "embedded={int=42;string=inner}",
                        EmbeddedFactory),

                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "int=42;bool=true;string=inner;"
                        "outer.int=24;outer.bool=true;outer.string=outer",
                        InheritedSimpleFactory),
                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "outer.int=24;outer.bool=true;outer.string=other;"
                        "int=33;bool=true;string=inner;"
                        "unique={int=42;string=nested}",
                        InheritedNestedUniqueFactory)));

}  // namespace rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
