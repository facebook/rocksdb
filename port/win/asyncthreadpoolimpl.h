//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/async/asyncthreadpool.h"

#include <memory>

#include <Windows.h>

namespace rocksdb {
namespace port {
// This class is a sentinel
// that can signal our user-defined
// objects that the thread-pool has cleaned
// up a TP item and thus operations on that
// IO completion object or anything else
// is no longer possible and no cleanup is
// necessary.
class ThreadPoolTaskToken {
protected:
  ThreadPoolTaskToken();
  virtual ~ThreadPoolTaskToken();
public:
  // customize destruction as it may require
  // some custom actions before the __dtor
  // is invoked
  virtual void Destroy();
  // static TP callback that is
  // called to cleanup items that are either
  // in the queue or semi-permanent objects
  // such as work items or io completion objects
  static void CALLBACK Cleanup(void* context, void*);
};

// This is a callback that is to be submitted for I/O
// completion
using
IOCompletionCallback = VOID(CALLBACK *)(
  PTP_CALLBACK_INSTANCE Instance,
  PVOID Context,
  PVOID Overlapped,
  ULONG ioResult,
  ULONG_PTR bytesTransferred,
  PTP_IO ptp_io);

} // namespace async
} //namespace rocksdb

