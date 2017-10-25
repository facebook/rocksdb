//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "port/win/asyncthreadpoolimpl.h"

#include <Windows.h>

namespace rocksdb {
namespace port {

class IOCompletion;
// This class is initialized by a Vista ThreadPool handle
// Creates its own TP environment for easy cleanup
// It issues IOCompletion instances used by files for async IO
// This is not a general purpose threadpool, rather it is a
// lightweight interface to issue IOCompletion instances
// for files that facilitate async IO
class WinAsyncThreadPool : public async::AsyncThreadPool {

  TP_CALLBACK_ENVIRON       env_;
  PTP_CLEANUP_GROUP         ptp_cgroup_;

public:
  explicit
  WinAsyncThreadPool(PTP_POOL ptp_pool);

  ~WinAsyncThreadPool();

  WinAsyncThreadPool(const WinAsyncThreadPool&) = delete;
  WinAsyncThreadPool& operator=(const WinAsyncThreadPool&) = delete;

  std::unique_ptr<IOCompletion>
  MakeIOCompletion(HANDLE hDevice, IOCompletionCallback cb);

  void CloseThreadPool() override;
};

} // namespace port
} //namespace rocksdb


