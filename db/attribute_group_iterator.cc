//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).


#include "db/attribute_group_iterator.h"

namespace ROCKSDB_NAMESPACE {

bool AttributeGroupIterator::Valid() const {
    return impl_.Valid();
}
void AttributeGroupIterator::SeekToFirst() {
  impl_.SeekToFirst();
}
void AttributeGroupIterator::SeekToLast() {
  impl_.SeekToLast();
}
void AttributeGroupIterator::Seek(const Slice& target) {
  impl_.Seek(target);
}
void AttributeGroupIterator::SeekForPrev(const Slice& target) {
  impl_.SeekForPrev(target);
}
void AttributeGroupIterator::Next() {
  impl_.Next();
}
void AttributeGroupIterator::Prev() {
  impl_.Prev();
}
Slice AttributeGroupIterator::key() const {
  return impl_.key();
}
Status AttributeGroupIterator::status() const {
  return impl_.status();
}

}  // namespace ROCKSDB_NAMESPACE
