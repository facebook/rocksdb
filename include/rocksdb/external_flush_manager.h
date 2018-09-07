//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

namespace rocksdb {

class ExternalFlushManager {
 public:
  virtual ~ExternalFlushManager() {}

  virtual void PickColumnFamiliesToFlush(
      const std::vector<uint32_t>& cf_ids,
      std::vector<std::vector<uint32_t>>* to_flush) = 0;
};

}  // namespace rocksdb
