// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "port/port.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {
class TableFactoryImpl : public TableFactory {
 public:
  explicit TableFactoryImpl() : is_mutable_(true) {}
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;
  bool IsMutable() const override;

 protected:
  Status ConfigureOptions(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opts_map,
      std::unordered_map<std::string, std::string>* unused) override;

 protected:
  mutable port::Mutex mutable_mu_;
  mutable bool is_mutable_;
};
}  // namespace ROCKSDB_NAMESPACE
