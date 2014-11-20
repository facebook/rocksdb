// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <mutex>

#include "util/thread_status_impl.h"
#include "db/column_family.h"
#if ROCKSDB_USING_THREAD_STATUS

namespace rocksdb {
void ThreadStatusImpl::TEST_VerifyColumnFamilyInfoMap(
    const std::vector<ColumnFamilyHandle*>& handles) {
  std::unique_lock<std::mutex> lock(thread_list_mutex_);
  assert(cf_info_map_.size() == handles.size());
  for (auto* handle : handles) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(handle)->cfd();
    auto iter __attribute__((unused)) = cf_info_map_.find(cfd);
    assert(iter != cf_info_map_.end());
    assert(iter->second);
    assert(iter->second->cf_name == cfd->GetName());
  }
}
}  // namespace rocksdb
#endif  // ROCKSDB_USING_THREAD_STATUS
