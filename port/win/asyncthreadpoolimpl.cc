//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port/win/asyncthreadpoolimpl.h"
#include "port/win/iocompletion.h"
#include "port/win/winasyncthreadpoolimpl.h"

namespace rocksdb {
namespace port {

ThreadPoolTaskToken::ThreadPoolTaskToken() {
}

ThreadPoolTaskToken::~ThreadPoolTaskToken() {
}

// Default destruction
void ThreadPoolTaskToken::Destroy() {
  delete this;
}

void CALLBACK ThreadPoolTaskToken::Cleanup(void* context, void*) {
  if (context != nullptr) {
    reinterpret_cast<ThreadPoolTaskToken*>(context)->Destroy();
  }
}

/////////////////////////////////////////////////////////////////////
// WinIOCompletionThreadToken

class WinIOCompletionThreadToken : public ThreadPoolTaskToken {
  IOCompletion* io_compl_;
public:
  explicit
    WinIOCompletionThreadToken(IOCompletion* io_compl) :
    io_compl_(io_compl) {
  }

  WinIOCompletionThreadToken(const WinIOCompletionThreadToken&) = delete;
  WinIOCompletionThreadToken& operator=(const WinIOCompletionThreadToken&) = delete;

  // Called when thread-pool cleans up
  // outstanding items on destruction
  void Reset() {
    io_compl_ = nullptr;
  }

  ~WinIOCompletionThreadToken() {
    if (io_compl_ != nullptr) {
      io_compl_->Reset();
    }
  }
};

///////////////////////////////////////////////////////////////////////
/// WinAsyncThreadPool

WinAsyncThreadPool::WinAsyncThreadPool(PTP_POOL ptp_pool) {

  InitializeThreadpoolEnvironment(&env_);
  SetThreadpoolCallbackPool(&env_, ptp_pool);

  ptp_cgroup_ = CreateThreadpoolCleanupGroup();

  SetThreadpoolCallbackCleanupGroup(&env_, 
    ptp_cgroup_, &ThreadPoolTaskToken::Cleanup);
}

WinAsyncThreadPool::~WinAsyncThreadPool() {
  CloseThreadPool();
}

std::unique_ptr<IOCompletion>
WinAsyncThreadPool::MakeIOCompletion(HANDLE hDevice, 
  IOCompletionCallback cb) {

  std::unique_ptr<IOCompletion> iocompl(new IOCompletion);
  auto tp_token = new port::WinIOCompletionThreadToken(iocompl.get());

  auto ptp_io = CreateThreadpoolIo(hDevice, cb, tp_token, &env_);

  if (ptp_io == nullptr) {
    tp_token->Destroy();
    return nullptr;
  }

  iocompl->SetWrapper(ptp_io, tp_token);
  return iocompl;
}

void WinAsyncThreadPool::CloseThreadPool() {
  // Wait for all of the IO to complete
  // and clean it up
  if (ptp_cgroup_ != nullptr) {

    const BOOL c_CancelPendingCallbacksFalse = FALSE;

    CloseThreadpoolCleanupGroupMembers(ptp_cgroup_, 
      c_CancelPendingCallbacksFalse, nullptr);
    CloseThreadpoolCleanupGroup(ptp_cgroup_);
    ptp_cgroup_ = nullptr;

    DestroyThreadpoolEnvironment(&env_);
  }
}

} // namepsace port
} // namespace rocksdb