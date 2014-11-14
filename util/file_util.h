//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <string>

#pragma once
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/env.h"

namespace rocksdb {

extern Status CopyFile(Env* env, const std::string& source,
                       const std::string& destination, uint64_t size = 0);

}  // namespace rocksdb
