//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <memory>

#include "rocksdb/io_dispatcher.h"

namespace ROCKSDB_NAMESPACE {

class IODispatcherImpl : public IODispatcher {
 public:
  explicit IODispatcherImpl(int num_threads);
  ~IODispatcherImpl();

  IODispatcherImpl(IODispatcherImpl&&) = delete;
  IODispatcherImpl& operator=(IODispatcherImpl&&) = delete;

  std::shared_ptr<JobHandle> SubmitJob(std::shared_ptr<IOJob> job) override;

  unsigned int GetQueueLen() const override;

  struct Impl;

 private:
  std::unique_ptr<Impl> impl_;
};

}  // namespace ROCKSDB_NAMESPACE
