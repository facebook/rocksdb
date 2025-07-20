//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {

constexpr auto kLongTxnTimeoutMs = 100000;
constexpr auto kShortTxnTimeoutMs = 100;

class MockColumnFamilyHandle : public ColumnFamilyHandle {
 public:
  explicit MockColumnFamilyHandle(ColumnFamilyId cf_id) : cf_id_(cf_id) {}

  ~MockColumnFamilyHandle() override {}

  const std::string& GetName() const override { return name_; }

  ColumnFamilyId GetID() const override { return cf_id_; }

  Status GetDescriptor(ColumnFamilyDescriptor*) override {
    return Status::OK();
  }

  const Comparator* GetComparator() const override {
    return BytewiseComparator();
  }

 private:
  ColumnFamilyId cf_id_;
  std::string name_ = "MockCF";
};

}  // namespace ROCKSDB_NAMESPACE
