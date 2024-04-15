//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/attribute_group_iterator_impl.h"

namespace ROCKSDB_NAMESPACE {

const AttributeGroups kNoAttributeGroups;
const IteratorAttributeGroups kNoIteratorAttributeGroups;

void AttributeGroupIteratorImpl::AddToAttributeGroups(
    const autovector<MultiCfIteratorInfo>& items) {
  for (const auto& item : items) {
    attribute_groups_.emplace_back(item.cfh, &item.iterator->columns());
  }
}

}  // namespace ROCKSDB_NAMESPACE
