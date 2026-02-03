//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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
  IODispatcherImpl();
  explicit IODispatcherImpl(const IODispatcherOptions& options);
  ~IODispatcherImpl() override;

  Status SubmitJob(const std::shared_ptr<IOJob>& job,
                   std::shared_ptr<ReadSet>* read_set) override;

 private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace ROCKSDB_NAMESPACE
