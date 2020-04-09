// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <memory>

#include "options/customizable_helper.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/options.h"
#include "utilities/compaction_filters/remove_emptyvalue_compactionfilter.h"

namespace ROCKSDB_NAMESPACE {
static bool LoadCompactionFilter(const std::string& id,
                                 CompactionFilter** filter) {
  bool success = true;
  if (id.empty()) {
    *filter = nullptr;
#ifndef ROCKSDB_LITE
  } else if (id == "RemoveEmptyValueCompactionFilter") {
    *filter = new RemoveEmptyValueCompactionFilter();
#endif
  } else {
    success = false;
  }
  return success;
}

Status CompactionFilter::CreateFromString(const std::string& value,
                                          const ConfigOptions& opts,
                                          const CompactionFilter** result) {
  CompactionFilter* filter = const_cast<CompactionFilter*>(*result);
  Status status = LoadStaticObject<CompactionFilter>(
      value, LoadCompactionFilter, opts, &filter);
  if (status.ok()) {
    *result = const_cast<CompactionFilter*>(filter);
  }
  return status;
}

Status CompactionFilterFactory::CreateFromString(
    const std::string& value, const ConfigOptions& opts,
    std::shared_ptr<CompactionFilterFactory>* result) {
  Status status =
      LoadSharedObject<CompactionFilterFactory>(value, nullptr, opts, result);
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
