//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************
//
//  DEPRECATED: backward-compatibility shim. New code should
//  #include "rocksdb/index_factory.h" directly. The type aliases below
//  preserve source compatibility for code that holds the old type names
//  by value or shared_ptr. Subclasses that overrode the previous
//  no-arg NewBuilder() / single-Slice NewReader(Slice&) pure virtuals
//  must migrate to the option-taking signatures on IndexFactory
//  (see include/rocksdb/index_factory.h). On-disk format is unchanged:
//  the meta block key prefix is "rocksdb.user_defined_index."
//  (kIndexFactoryMetaPrefix).

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
