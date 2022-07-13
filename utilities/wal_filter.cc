// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/wal_filter.h"

#include <memory>

#include "rocksdb/convenience.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/customizable_util.h"

namespace ROCKSDB_NAMESPACE {
Status WalFilter::CreateFromString(const ConfigOptions& config_options,
                                   const std::string& value,
                                   WalFilter** filter) {
  Status s =
      LoadStaticObject<WalFilter>(config_options, value, nullptr, filter);
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
