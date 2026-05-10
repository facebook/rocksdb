//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {

inline Status ValidateIteratorMutableOptions(
    const IteratorMutableOptions& options, const TableFactory* table_factory) {
  if (!options.pinned_block_backing.has_value()) {
    return Status::OK();
  }

  const auto* table_options =
      table_factory != nullptr
          ? table_factory->GetOptions<BlockBasedTableOptions>()
          : nullptr;
  if (table_factory != nullptr && table_options == nullptr) {
    return Status::InvalidArgument(
        "Iterator mutable options require a block-based table factory");
  }

  const auto& pinned_block_backing = *options.pinned_block_backing;
  switch (pinned_block_backing.policy) {
    case PinnedBlockBackingPolicy::kAuto:
      return Status::OK();
    case PinnedBlockBackingPolicy::kUseBlockCache:
      if (table_options == nullptr || table_options->block_cache == nullptr) {
        return Status::InvalidArgument(
            "PinnedBlockBackingPolicy::kUseBlockCache requires block cache");
      }
      return Status::OK();
    case PinnedBlockBackingPolicy::kUseRetainedBlockBuffer:
      if (pinned_block_backing.retained_block_buffer_provider != nullptr) {
        return Status::OK();
      }
      if (table_options != nullptr &&
          table_options->block_buffer_provider != nullptr) {
        return Status::OK();
      }
      return Status::InvalidArgument(
          "PinnedBlockBackingPolicy::kUseRetainedBlockBuffer requires a "
          "retained block buffer provider");
  }

  return Status::InvalidArgument("Unknown pinned block backing policy");
}

inline void ApplyIteratorMutableOptions(const IteratorMutableOptions& options,
                                        IteratorMutableOptions* current) {
  if (current == nullptr) {
    return;
  }

  if (options.pinned_block_backing.has_value()) {
    current->pinned_block_backing = options.pinned_block_backing;
  }
}

}  // namespace ROCKSDB_NAMESPACE
