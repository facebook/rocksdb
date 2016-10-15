//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "util/db_options.h"

namespace rocksdb {

extern Status CopyFile(Env* env, const std::string& source,
                       const std::string& destination, uint64_t size = 0);

extern Status CreateFile(Env* env, const std::string& destination,
                         const std::string& contents);

extern Status DeleteSSTFile(const ImmutableDBOptions* db_options,
                            const std::string& fname, uint32_t path_id);

}  // namespace rocksdb
