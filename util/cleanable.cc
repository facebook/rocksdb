//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/cleanable.h"

#include <atomic>
#include <cassert>
#include <utility>

namespace ROCKSDB_NAMESPACE {

Cleanable::Cleanable() {
  cleanup_.function = nullptr;
  cleanup_.next = nullptr;
}

Cleanable::~Cleanable() { DoCleanup(); }

Cleanable::Cleanable(Cleanable&& other) noexcept { *this = std::move(other); }

Cleanable& Cleanable::operator=(Cleanable&& other) noexcept {
  assert(this != &other);  // https://stackoverflow.com/a/9322542/454544
  cleanup_ = other.cleanup_;
  other.cleanup_.function = nullptr;
  other.cleanup_.next = nullptr;
  return *this;
}

// If the entire linked list was on heap we could have simply add attach one
// link list to another. However the head is an embeded object to avoid the cost
// of creating objects for most of the use cases when the Cleanable has only one
// Cleanup to do. We could put evernything on heap if benchmarks show no
// negative impact on performance.
// Also we need to iterate on the linked list since there is no pointer to the
// tail. We can add the tail pointer but maintainin it might negatively impact
// the perforamnce for the common case of one cleanup where tail pointer is not
// needed. Again benchmarks could clarify that.
// Even without a tail pointer we could iterate on the list, find the tail, and
// have only that node updated without the need to insert the Cleanups one by
// one. This however would be redundant when the source Cleanable has one or a
// few Cleanups which is the case most of the time.
// TODO(myabandeh): if the list is too long we should maintain a tail pointer
// and have the entire list (minus the head that has to be inserted separately)
// merged with the target linked list at once.
void Cleanable::DelegateCleanupsTo(Cleanable* other) {
  assert(other != nullptr);
  if (cleanup_.function == nullptr) {
    return;
  }
  Cleanup* c = &cleanup_;
  other->RegisterCleanup(c->function, c->arg1, c->arg2);
  c = c->next;
  while (c != nullptr) {
    Cleanup* next = c->next;
    other->RegisterCleanup(c);
    c = next;
  }
  cleanup_.function = nullptr;
  cleanup_.next = nullptr;
}

void Cleanable::RegisterCleanup(Cleanable::Cleanup* c) {
  assert(c != nullptr);
  if (cleanup_.function == nullptr) {
    cleanup_.function = c->function;
    cleanup_.arg1 = c->arg1;
    cleanup_.arg2 = c->arg2;
    delete c;
  } else {
    c->next = cleanup_.next;
    cleanup_.next = c;
  }
}

void Cleanable::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) {
  assert(func != nullptr);
  Cleanup* c;
  if (cleanup_.function == nullptr) {
    c = &cleanup_;
  } else {
    c = new Cleanup;
    c->next = cleanup_.next;
    cleanup_.next = c;
  }
  c->function = func;
  c->arg1 = arg1;
  c->arg2 = arg2;
}

struct SharedCleanablePtr::Impl : public Cleanable {
  std::atomic<unsigned> ref_count{1};  // Start with 1 ref
  void Ref() { ref_count.fetch_add(1, std::memory_order_relaxed); }
  void Unref() {
    if (ref_count.fetch_sub(1, std::memory_order_relaxed) == 1) {
      // Last ref
      delete this;
    }
  }
  static void UnrefWrapper(void* arg1, void* /*arg2*/) {
    static_cast<SharedCleanablePtr::Impl*>(arg1)->Unref();
  }
};

void SharedCleanablePtr::Reset() {
  if (ptr_) {
    ptr_->Unref();
    ptr_ = nullptr;
  }
}

void SharedCleanablePtr::Allocate() {
  Reset();
  ptr_ = new Impl();
}

SharedCleanablePtr::SharedCleanablePtr(const SharedCleanablePtr& from) {
  *this = from;
}

SharedCleanablePtr::SharedCleanablePtr(SharedCleanablePtr&& from) noexcept {
  *this = std::move(from);
}

SharedCleanablePtr& SharedCleanablePtr::operator=(
    const SharedCleanablePtr& from) {
  if (this != &from) {
    Reset();
    ptr_ = from.ptr_;
    if (ptr_) {
      ptr_->Ref();
    }
  }
  return *this;
}

SharedCleanablePtr& SharedCleanablePtr::operator=(
    SharedCleanablePtr&& from) noexcept {
  assert(this != &from);  // https://stackoverflow.com/a/9322542/454544
  Reset();
  ptr_ = from.ptr_;
  from.ptr_ = nullptr;
  return *this;
}

SharedCleanablePtr::~SharedCleanablePtr() { Reset(); }

Cleanable& SharedCleanablePtr::operator*() {
  return *ptr_;  // implicit upcast
}

Cleanable* SharedCleanablePtr::operator->() {
  return ptr_;  // implicit upcast
}

Cleanable* SharedCleanablePtr::get() {
  return ptr_;  // implicit upcast
}

void SharedCleanablePtr::RegisterCopyWith(Cleanable* target) {
  if (ptr_) {
    // "Virtual" copy of the pointer
    ptr_->Ref();
    target->RegisterCleanup(&Impl::UnrefWrapper, ptr_, nullptr);
  }
}

void SharedCleanablePtr::MoveAsCleanupTo(Cleanable* target) {
  if (ptr_) {
    // "Virtual" move of the pointer
    target->RegisterCleanup(&Impl::UnrefWrapper, ptr_, nullptr);
    ptr_ = nullptr;
  }
}

}  // namespace ROCKSDB_NAMESPACE
