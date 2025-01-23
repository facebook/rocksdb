//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/secondary_index/secondary_index_iterator.h"

namespace ROCKSDB_NAMESPACE {

std::unique_ptr<Iterator> NewSecondaryIndexIterator(
    const SecondaryIndex* index, std::unique_ptr<Iterator>&& underlying_it) {
  if (!underlying_it) {
    return std::unique_ptr<Iterator>(NewErrorIterator(
        Status::InvalidArgument("Underlying iterator must be provided")));
  }

  return std::make_unique<SecondaryIndexIterator>(index,
                                                  std::move(underlying_it));
}

}  // namespace ROCKSDB_NAMESPACE
