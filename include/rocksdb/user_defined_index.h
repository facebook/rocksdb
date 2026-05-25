//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************
//
//  DEPRECATED: This header is a backward-compatibility shim for the
//  experimental User-Defined Index API. New code should #include
//  "rocksdb/index_factory.h" directly and use the IndexFactory /
//  IndexFactoryBuilder / IndexFactoryReader / IndexFactoryIterator /
//  IndexFactoryOptions names instead.
//
//  Source compatibility scope (a partial shim, not a full adapter):
//
//  - Code that holds a `std::shared_ptr<UserDefinedIndexFactory>`,
//    references the type by name, or calls the option-taking virtuals
//    (`NewBuilder(const UserDefinedIndexOption&, ...)` and the matching
//    `NewReader`) continues to compile unchanged -- the names below are
//    type aliases for the new classes.
//
//  - Code that SUBCLASSES UserDefinedIndexFactory and overrode the older
//    no-arg `NewBuilder()` and/or single-Slice `NewReader(Slice&)` pure
//    virtuals will NOT compile against the new header: those signatures
//    are no longer pure virtuals on `IndexFactory`. Migrate by:
//      virtual UserDefinedIndexBuilder* NewBuilder() const  // OLD
//    -> Status NewBuilder(const IndexFactoryOptions& opt,
//                         std::unique_ptr<IndexFactoryBuilder>& out) const
//       override;                                            // NEW
//      virtual std::unique_ptr<UserDefinedIndexReader>      // OLD
//          NewReader(Slice& index_block) const;
//    -> Status NewReader(const IndexFactoryOptions& opt,
//                        Slice& index_block,
//                        std::unique_ptr<IndexFactoryReader>& out) const
//       override;                                            // NEW
//
//  On-disk format is unchanged. SSTs written by previous versions remain
//  readable -- the meta block key prefix is still
//  "rocksdb.user_defined_index." (kIndexFactoryMetaPrefix below).

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
