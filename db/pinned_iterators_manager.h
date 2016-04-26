//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include <algorithm>
#include <memory>
#include <vector>

#include "table/internal_iterator.h"

namespace rocksdb {

// PinnedIteratorsManager will be notified whenever we need to pin an Iterator
// and it will be responsible for deleting pinned Iterators when they are
// not needed anymore.
class PinnedIteratorsManager {
 public:
  PinnedIteratorsManager() : pinning_enabled(false), pinned_iters_(nullptr) {}
  ~PinnedIteratorsManager() { assert(!pinning_enabled); }

  // Enable Iterators pinning
  void StartPinning() {
    if (!pinning_enabled) {
      pinning_enabled = true;
      if (!pinned_iters_) {
        pinned_iters_.reset(new std::vector<InternalIterator*>());
      }
    }
  }

  // Is pinning enabled ?
  bool PinningEnabled() { return pinning_enabled; }

  // Take ownership of iter if pinning is enabled and delete it when
  // ReleasePinnedIterators() is called
  void PinIteratorIfNeeded(InternalIterator* iter) {
    if (!pinning_enabled || !iter) {
      return;
    }
    pinned_iters_->push_back(iter);
  }

  // Release pinned Iterators
  void ReleasePinnedIterators() {
    if (pinning_enabled) {
      pinning_enabled = false;

      // Remove duplicate pointers
      std::sort(pinned_iters_->begin(), pinned_iters_->end());
      std::unique(pinned_iters_->begin(), pinned_iters_->end());

      for (auto& iter : *pinned_iters_) {
        delete iter;
      }
      pinned_iters_->clear();
    }
  }

 private:
  bool pinning_enabled;
  std::unique_ptr<std::vector<InternalIterator*>> pinned_iters_;
};

}  // namespace rocksdb
