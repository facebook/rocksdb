//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "util/sync_point.h"

#ifndef NDEBUG
namespace rocksdb {

SyncPoint* SyncPoint::GetInstance() {
  static SyncPoint sync_point;
  return &sync_point;
}

void SyncPoint::LoadDependency(const std::vector<Dependency>& dependencies) {
  successors_.clear();
  predecessors_.clear();
  cleared_points_.clear();
  for (const auto& dependency : dependencies) {
    successors_[dependency.predecessor].push_back(dependency.successor);
    predecessors_[dependency.successor].push_back(dependency.predecessor);
  }
}

bool SyncPoint::PredecessorsAllCleared(const std::string& point) {
  for (const auto& pred : predecessors_[point]) {
    if (cleared_points_.count(pred) == 0) {
      return false;
    }
  }
  return true;
}

void SyncPoint::EnableProcessing() {
  std::unique_lock<std::mutex> lock(mutex_);
  enabled_ = true;
}

void SyncPoint::DisableProcessing() {
  std::unique_lock<std::mutex> lock(mutex_);
  enabled_ = false;
}

void SyncPoint::ClearTrace() {
  std::unique_lock<std::mutex> lock(mutex_);
  cleared_points_.clear();
}

void SyncPoint::Process(const std::string& point) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (!enabled_) return;

  while (!PredecessorsAllCleared(point)) {
    cv_.wait(lock);
  }

  cleared_points_.insert(point);
  cv_.notify_all();
}

}  // namespace rocksdb
#endif  // NDEBUG
