//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {
#ifndef NDEBUG
void TEST_SetRandomTableProperties(TableProperties* props);
#endif
}  // namespace ROCKSDB_NAMESPACE
