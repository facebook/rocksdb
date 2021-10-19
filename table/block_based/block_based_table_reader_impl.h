//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include "table/block_based/block_based_table_reader.h"

#include "table/block_based/reader_common.h"

// The file contains some member functions of BlockBasedTable that
// cannot be implemented in block_based_table_reader.cc because
// it's called by other files (e.g. block_based_iterator.h) and
// are templates.

namespace ROCKSDB_NAMESPACE {
}  // namespace ROCKSDB_NAMESPACE
