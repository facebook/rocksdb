//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//  @author Adam Retter

#pragma once

#include <algorithm>
#include <cstdio>
#include <initializer_list>
#include <iostream>
#include <string>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "util/coding.h"

/**
 * Simple functions which form a DSL
 * to simplify testing the CollectionMergeOperator.
 */

namespace rocksdb {

std::string Clear() {
  std::string str_clear;
  str_clear.push_back(CollectionOperation::kClear);
  return str_clear;
}

std::string Add(const std::initializer_list<std::string> records) {
  std::string str_add;
  str_add.push_back(CollectionOperation::kAdd);
  for (auto it = records.begin(); it != records.end(); ++it) {
    str_add.append(*it);
  }
  return str_add;
}

std::string Add(const std::string record) {
  return Add({record});
}

std::string Add(const std::initializer_list<uint32_t> records) {
  std::string str_add;
  str_add.push_back(CollectionOperation::kAdd);
  for (auto it = records.begin(); it != records.end(); ++it) {
    char buf[sizeof(uint32_t)];
    EncodeFixed32(buf, *it);
    str_add.append(buf, sizeof(uint32_t));
  }
  return str_add;
}

std::string Add(const uint32_t record) {
  return Add({record});
}

std::string Remove(const std::initializer_list<std::string> records) {
  std::string str_remove;
  str_remove.push_back(CollectionOperation::kRemove);
  for (auto it = records.begin(); it != records.end(); ++it) {
    str_remove.append(*it);
  }
  return str_remove;
}

std::string Remove(const std::string record) {
  return Remove({record});
}

std::string Remove(const std::initializer_list<uint32_t> records) {
  std::string str_remove;
  str_remove.push_back(CollectionOperation::kRemove);
  for (auto it = records.begin(); it != records.end(); ++it) {
    char buf[sizeof(uint32_t)];
    EncodeFixed32(buf, *it);
    str_remove.append(buf, sizeof(uint32_t));
  }
  return str_remove;
}

std::string Remove(const uint32_t record) {
  return Remove({record});
}

std::string Multi(const std::initializer_list<std::string> operations) {
  std::string str_multi;
  str_multi.push_back(CollectionOperation::_kMulti);
  for (auto it = operations.begin(); it != operations.end(); ++it) {
    std::string operation = *it;
    // _kMulti sub-operations are prefixed with records length
    char buf[sizeof(uint32_t)];
    EncodeFixed32(buf, (operation.size() & 0xFFFFFFFF) - (sizeof(CollectionOperation) & 0xFFFFFFFF));
    str_multi.append(buf, sizeof(uint32_t));
    str_multi.append(operation);
  }
  return str_multi;
}

struct OwningMergeOperationOutput {
 public:
  std::string new_value;
  Slice existing_operand;
  MergeOperator::MergeOperationOutput merge_out;
  OwningMergeOperationOutput() : merge_out(new_value, existing_operand) {}
};

OwningMergeOperationOutput EmptyMergeOut() {
  return OwningMergeOperationOutput();
}

std::string RecordString(const size_t record_len, const size_t record_count) {
  std::string str(record_len * record_count, '0');
  char n = '1';
  std::generate(str.begin(), str.end(), [record_len, n] () mutable {
    if (n > '0' + (char)record_len) {
      n = '1';
    }
    return n++;
  });
  return str;
}

std::string RecordString(const size_t record_len) {
  return RecordString(record_len, 1);
}

std::string toString(std::initializer_list<std::string>& records) {
  std::string result;
  for (auto it = records.begin(); it != records.end(); ++it) {
    result.append(*it);
  }
  return result;
}

class TestLogger : public rocksdb::Logger {
 public:
  virtual ~TestLogger() { }

  virtual void Logv(const char* format, va_list ap) {
    printf(format, ap);
    std::cout << std::endl;
  }

  virtual void Logv(const InfoLogLevel log_level, const char* format, va_list ap) {
    std::cout << "[" << log_level << "] ";
    printf(format, ap);
    std::cout << std::endl;
  }
};

}  // end rocksdb namespace
