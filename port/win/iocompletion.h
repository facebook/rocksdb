//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <utility>

namespace rocksdb {
namespace port {

class IOCompletion {
public:

  IOCompletion() noexcept :
    io_compl_(nullptr),
    tp_tasktoken_(nullptr) {
  }

  // On destruction do nothing as
  // we either cancel this explicitly
  // or it completes successfully and
  // is taken care of
  ~IOCompletion();

  IOCompletion(const IOCompletion&) = delete;
  IOCompletion& operator=(const IOCompletion&) = delete;

  IOCompletion(IOCompletion&& o) noexcept :
  IOCompletion() {
    *this = std::move(o);
  }

  IOCompletion& operator=(IOCompletion&& o) noexcept {
    std::swap(io_compl_, o.io_compl_);
    std::swap(tp_tasktoken_, o.tp_tasktoken_);
    return *this;
  }

  void Start() const;

  // Can only be called if Start()
  // was called
  void Cancel() const;

  bool Valid() const {
    return io_compl_ != nullptr;
  }

  void SetWrapper(void *ptp_io, void* tp_wrapper) {
    io_compl_ = ptp_io;
    tp_tasktoken_ = tp_wrapper;
  }

  // Called out by a thread-pool that
  // is cleaning this up and does not want
  // us to do so
  void Reset() {
    io_compl_ = nullptr;
    tp_tasktoken_ = nullptr;
  }

  // Associated file handle must be closed
  // after this
  void Close();

private:
  void*          io_compl_;
  void*          tp_tasktoken_;
};

} // namespace async
} //namespace rocksdb

