//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************
//
//  DEPRECATED: This header is a backward-compatibility shim.
//  New code should #include "rocksdb/index_factory.h" directly and
//  use the IndexFactory / IndexFactoryBuilder / IndexFactoryReader /
//  IndexFactoryIterator / IndexFactoryOptions names instead.

#pragma once

#include "rocksdb/index_factory.h"

namespace ROCKSDB_NAMESPACE {

using UserDefinedIndexBuilder = IndexFactoryBuilder;
using UserDefinedIndexIterator = IndexFactoryIterator;
using UserDefinedIndexReader = IndexFactoryReader;
using UserDefinedIndexFactory = IndexFactory;
using UserDefinedIndexOption = IndexFactoryOptions;

inline constexpr const char* kUserDefinedIndexPrefix = kIndexFactoryMetaPrefix;

}  // namespace ROCKSDB_NAMESPACE
