//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <atomic>

#include "rocksdb/env.h"
#include "rocksdb/concurrent_task_limiter.h"

namespace rocksdb {

class ConcurrentTaskLimiterImpl : public ConcurrentTaskLimiter {
 public:
  explicit ConcurrentTaskLimiterImpl(const std::string& name,
                                     int32_t max_outstanding_task);

  // No copying allowed
  ConcurrentTaskLimiterImpl(const ConcurrentTaskLimiterImpl&) = delete;
  ConcurrentTaskLimiterImpl& operator=(
      const ConcurrentTaskLimiterImpl&) = delete;

  virtual ~ConcurrentTaskLimiterImpl();

  virtual const std::string& GetName() const override;

  virtual void SetMaxOutstandingTask(int32_t limit) override;

  virtual void ResetMaxOutstandingTask() override;

  virtual int32_t GetOutstandingTask() const override;
  
  virtual bool GetToken(bool force, int32_t& tasks) override;

  virtual void ReturnToken(int32_t& tasks) override;

 private:
  std::string name_;
  std::atomic<int32_t> max_outstanding_tasks_;
  std::atomic<int32_t> outstanding_tasks_;  
};


}  // namespace rocksdb
