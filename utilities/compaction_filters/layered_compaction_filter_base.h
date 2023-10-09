// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <memory>

#include "rocksdb/compaction_filter.h"

namespace ROCKSDB_NAMESPACE {

// Abstract base class for building layered compaction filter on top of
// user compaction filter.
// See BlobIndexCompactionFilter or TtlCompactionFilter for a basic usage.
class LayeredCompactionFilterBase : public CompactionFilter {
 public:
  LayeredCompactionFilterBase(
      const CompactionFilter* _user_comp_filter,
      std::unique_ptr<const CompactionFilter> _user_comp_filter_from_factory)
      : user_comp_filter_(_user_comp_filter),
        user_comp_filter_from_factory_(
            std::move(_user_comp_filter_from_factory)) {
    if (!user_comp_filter_) {
      user_comp_filter_ = user_comp_filter_from_factory_.get();
    }
  }

  // Return a pointer to user compaction filter
  const CompactionFilter* user_comp_filter() const { return user_comp_filter_; }

  const Customizable* Inner() const override { return user_comp_filter_; }

 protected:
  const CompactionFilter* user_comp_filter_;

 private:
  std::unique_ptr<const CompactionFilter> user_comp_filter_from_factory_;
};

}  //  namespace ROCKSDB_NAMESPACE
