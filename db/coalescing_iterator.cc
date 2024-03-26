//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/coalescing_iterator.h"

namespace ROCKSDB_NAMESPACE {

bool CoalescingIterator::Valid() const { return impl_.Valid(); }
void CoalescingIterator::SeekToFirst() { impl_.SeekToFirst(); }
void CoalescingIterator::SeekToLast() { impl_.SeekToLast(); }
void CoalescingIterator::Seek(const Slice& target) { impl_.Seek(target); }
void CoalescingIterator::SeekForPrev(const Slice& target) {
  impl_.SeekForPrev(target);
}
void CoalescingIterator::Next() { impl_.Next(); }
void CoalescingIterator::Prev() { impl_.Prev(); }
Slice CoalescingIterator::key() const { return impl_.key(); }
Status CoalescingIterator::status() const { return impl_.status(); }

}  // namespace ROCKSDB_NAMESPACE
