//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/attribute_group_iterator_impl.h"

namespace ROCKSDB_NAMESPACE {

const AttributeGroups kNoAttributeGroups;

void AttributeGroupIteratorImpl::AddToAttributeGroups(
    ColumnFamilyHandle* /*cfh*/, const WideColumns& /*columns*/) {
  // TODO - Implement AttributeGroup population
}

}  // namespace ROCKSDB_NAMESPACE
