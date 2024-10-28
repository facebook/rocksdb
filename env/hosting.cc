//  Copyright (c) 2024-present, Meta, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "env/hosting.h"

#include <cassert>

namespace ROCKSDB_NAMESPACE {

// Default implementation of hosting interface.
class HostingImpl : public HostingInterface {
 public:
  void yield() override {}
};

// Return hosting interface.
HostingInterface* Hosting::get() {
  // Should always be set. Will catch cases when used after static shared_ptr
  // is released. All threads should be gone before static object destruction.
  assert(getPtr().get());
  return getPtr().get();
}

// Set hosting interface.
// Not thread safe! Should be called before starting other threads.
void Hosting::set(std::shared_ptr<HostingInterface> hosting) {
  getPtr() = hosting;
}

std::shared_ptr<HostingInterface>& Hosting::getPtr() {
  static std::shared_ptr<HostingInterface> hosting{
      std::make_shared<HostingImpl>()};
  return hosting;
}

}  // namespace ROCKSDB_NAMESPACE
