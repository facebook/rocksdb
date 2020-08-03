//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <algorithm>
#include <memory>
#include <unordered_map>

#include "options/configurable_helper.h"
#include "rocksdb/configurable.h"
#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE {
struct ColumnFamilyOptions;
struct DBOptions;

namespace test {
enum TestEnum { kTestA, kTestB };

static const std::unordered_map<std::string, int> test_enum_map = {
    {"A", TestEnum::kTestA},
    {"B", TestEnum::kTestB},
};

struct TestOptions {
  int i = 0;
  bool b = false;
  bool d = true;
  TestEnum e = TestEnum::kTestA;
  std::string s = "";
  std::string u = "";
};

static std::unordered_map<std::string, OptionTypeInfo> simple_option_info = {
#ifndef ROCKSDB_LITE
    {"int",
     {offsetof(struct TestOptions, i), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
    {"bool",
     {offsetof(struct TestOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"string",
     {offsetof(struct TestOptions, s), OptionType::kString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
#endif  // ROCKSDB_LITE
};

static std::unordered_map<std::string, OptionTypeInfo> enum_option_info = {
#ifndef ROCKSDB_LITE
    {"enum",
     OptionTypeInfo::Enum(offsetof(struct TestOptions, e), &test_enum_map)}
#endif
};

static std::unordered_map<std::string, OptionTypeInfo> unique_option_info = {
#ifndef ROCKSDB_LITE
    {"unique",
     {0, OptionType::kConfigurable, OptionVerificationType::kNormal,
      (OptionTypeFlags::kUnique | OptionTypeFlags::kMutable)}},
#endif  // ROCKSDB_LITE
};

static std::unordered_map<std::string, OptionTypeInfo> shared_option_info = {
#ifndef ROCKSDB_LITE
    {"shared",
     {0, OptionType::kConfigurable, OptionVerificationType::kNormal,
      (OptionTypeFlags::kShared)}},
#endif  // ROCKSDB_LITE
};
static std::unordered_map<std::string, OptionTypeInfo> pointer_option_info = {
#ifndef ROCKSDB_LITE
    {"pointer",
     {0, OptionType::kConfigurable, OptionVerificationType::kNormal,
      OptionTypeFlags::kRawPointer}},
#endif  // ROCKSDB_LITE
};

enum TestConfigMode {
  kEmptyMode = 0x0,            // Don't register anything
  kMutableMode = 0x01,         // Configuration is mutable
  kSimpleMode = 0x02,          // Use the simple options
  kEnumMode = 0x04,            // Use the enum options
  kDefaultMode = kSimpleMode,  // Use no inner nested configurations
  kSharedMode = 0x10,          // Use shared configuration
  kUniqueMode = 0x20,          // Use unique configuration
  kRawPtrMode = 0x40,          // Use pointer configuration
  kNestedMode = (kSharedMode | kUniqueMode | kRawPtrMode),
  kAllOptMode = (kNestedMode | kEnumMode | kSimpleMode),
};

template <typename T>
class TestConfigurable : public Configurable {
 protected:
  std::string name_;
  std::string prefix_;
  TestOptions options_;

 public:
  std::unique_ptr<T> unique_;
  std::shared_ptr<T> shared_;
  T* pointer_;

  TestConfigurable(const std::string& name, int mode,
                   const std::unordered_map<std::string, OptionTypeInfo>* map =
                       &simple_option_info)
      : name_(name), pointer_(nullptr) {
    prefix_ = "test." + name + ".";
    if ((mode & TestConfigMode::kSimpleMode) != 0) {
      ConfigurableHelper::RegisterOptions(*this, name_, &options_, map);
    }
    if ((mode & TestConfigMode::kEnumMode) != 0) {
      ConfigurableHelper::RegisterOptions(*this, name_ + "Enum", &options_,
                                          &enum_option_info);
    }
  }

  ~TestConfigurable() override { delete pointer_; }
};

}  // namespace test
}  // namespace ROCKSDB_NAMESPACE
