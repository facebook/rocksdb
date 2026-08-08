//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************

#pragma once

#include "rocksdb/user_defined_index.h"

namespace ROCKSDB_NAMESPACE {

using IndexFactoryBuilder = UserDefinedIndexBuilder;
using IndexFactoryIterator = UserDefinedIndexIterator;
using IndexFactoryReader = UserDefinedIndexReader;
using IndexFactoryOptions = UserDefinedIndexOption;
using IndexFactory = UserDefinedIndexFactory;

inline constexpr const char* kIndexFactoryMetaPrefix = kUserDefinedIndexPrefix;

}  // namespace ROCKSDB_NAMESPACE
