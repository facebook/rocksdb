//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

namespace rocksdb {

/*
 * ThreadPool is a component that will spawn N background threads that will
 * be used to execute scheduled work, The number of background threads could
 * be modified by calling SetBackgroundThreads().
 * */
class ThreadPool {
 public:
  virtual ~ThreadPool() {}

  // Wait for all threads to finish.
  virtual void JoinAllThreads() = 0;

  // Set the number of background threads that will be executing the
  // scheduled jobs.
  virtual void SetBackgroundThreads(int num) = 0;

  // Get the number of jobs scheduled in the ThreadPool queue.
  virtual unsigned int GetQueueLen() const = 0;
};

// NewThreadPool() is a function that could be used to create a ThreadPool
// with `num_threads` background threads.
extern ThreadPool* NewThreadPool(int num_threads);

}  // namespace rocksdb
