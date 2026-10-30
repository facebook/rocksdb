//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <vector>

#include "rocksdb/listener.h"

namespace ROCKSDB_NAMESPACE {

// Construct before taking DB locks and destroy after releasing them. Existing
// I/O callbacks finish before the scope starts; subsequent callbacks for these
// listeners, including worker-thread callbacks, are delivered at scope exit.
// Scopes sharing listeners are exclusive through callback delivery. Callers
// must reject matching reentrant callbacks before constructing a scope.
class ScopedDeferredFileIOListeners {
 public:
  struct State;
  using FileCallback = void (EventListener::*)(const FileOperationInfo&);

  ScopedDeferredFileIOListeners(
      bool defer, const std::vector<std::shared_ptr<EventListener>>& listeners);
  ~ScopedDeferredFileIOListeners();

  ScopedDeferredFileIOListeners(const ScopedDeferredFileIOListeners&) = delete;
  ScopedDeferredFileIOListeners& operator=(
      const ScopedDeferredFileIOListeners&) = delete;
  ScopedDeferredFileIOListeners(ScopedDeferredFileIOListeners&&) = delete;
  ScopedDeferredFileIOListeners& operator=(ScopedDeferredFileIOListeners&&) =
      delete;

  static void Notify(
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      FileCallback callback, const FileOperationInfo& info);
  static void NotifyError(
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const IOErrorInfo& info);

  static bool IsInCallback(
      const std::vector<std::shared_ptr<EventListener>>& listeners);

 private:
  std::shared_ptr<State> state_;
};

}  // namespace ROCKSDB_NAMESPACE
