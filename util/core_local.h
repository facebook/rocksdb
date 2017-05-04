//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once

#include "port/likely.h"
#include "port/port.h"
#include "util/random.h"

#include <cstddef>
#include <thread>
#include <vector>

namespace rocksdb {

// An array of core-local values. Ideally the value type, T, is cache aligned to
// prevent false sharing.
template<typename T>
class CoreLocalArray {
 public:
  CoreLocalArray();

  size_t Size() const;
  // returns pointer to the element corresponding to the core that the thread
  // currently runs on.
  T* Access() const;
  // same as above, but also returns the core index, which the client can cache
  // to reduce how often core ID needs to be retrieved. Only do this if some
  // inaccuracy is tolerable, as the thread may migrate to a different core.
  std::pair<T*, size_t> AccessElementAndIndex() const;
  // returns pointer to element for the specified core index. This can be used,
  // e.g., for aggregation, or if the client caches core index.
  T* AccessAtCore(size_t core_idx) const;

 private:
  std::unique_ptr<T[]> data_;
  size_t size_;
};

template<typename T>
CoreLocalArray<T>::CoreLocalArray() {
  unsigned int num_cpus = std::thread::hardware_concurrency();
  // find a power of two >= num_cpus and >= 8
  size_ = 1 << 3;
  while (size_ < num_cpus) {
    size_ <<= 1;
  }
  data_.reset(new T[size_]);
}

template<typename T>
size_t CoreLocalArray<T>::Size() const {
  return size_;
}

template<typename T>
T* CoreLocalArray<T>::Access() const {
  return AccessElementAndIndex().first;
}

template<typename T>
std::pair<T*, size_t> CoreLocalArray<T>::AccessElementAndIndex() const {
  int cpuid = port::PhysicalCoreID();
  size_t core_idx;
  if (UNLIKELY(cpuid < 0)) {
    // cpu id unavailable, just pick randomly
    core_idx =
        Random::GetTLSInstance()->Uniform(static_cast<int>(size_));
  } else {
    core_idx = static_cast<size_t>(cpuid) % size_;
  }
  return {AccessAtCore(core_idx), core_idx};
}

template<typename T>
T* CoreLocalArray<T>::AccessAtCore(size_t core_idx) const {
  assert(core_idx < size_);
  return &data_[core_idx];
}

}  // namespace rocksdb
