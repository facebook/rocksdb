//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/attribute_group_iterator_impl.h"

namespace ROCKSDB_NAMESPACE {

const AttributeGroups kNoAttributeGroups;

bool AttributeGroupIteratorImpl::Valid() const { return impl_.Valid(); }
void AttributeGroupIteratorImpl::SeekToFirst() { impl_.SeekToFirst(); }
void AttributeGroupIteratorImpl::SeekToLast() { impl_.SeekToLast(); }
void AttributeGroupIteratorImpl::Seek(const Slice& target) {
  impl_.Seek(target);
}
void AttributeGroupIteratorImpl::SeekForPrev(const Slice& target) {
  impl_.SeekForPrev(target);
}
void AttributeGroupIteratorImpl::Next() { impl_.Next(); }
void AttributeGroupIteratorImpl::Prev() { impl_.Prev(); }
Slice AttributeGroupIteratorImpl::key() const { return impl_.key(); }
Status AttributeGroupIteratorImpl::status() const { return impl_.status(); }

}  // namespace ROCKSDB_NAMESPACE
