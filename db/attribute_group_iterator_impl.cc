//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/attribute_group_iterator_impl.h"

namespace ROCKSDB_NAMESPACE {

const AttributeGroups kNoAttributeGroups;
const IterableAttributeGroups kNoIterableAttributeGroups;

void AttributeGroupIteratorImpl::AddToAttributeGroups(
    ColumnFamilyHandle* cfh, const WideColumns* columns) {
  attribute_groups_.emplace_back(cfh, columns);
}

}  // namespace ROCKSDB_NAMESPACE
