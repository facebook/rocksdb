//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#pragma once

#include "rocksdb/env.h"

namespace rocksdb 
{

const int kDebugLogChunkSize = 128 * 1024;

class WinLogger : public rocksdb::Logger 
{
private:
    FILE* file_;
	uint64_t (*gettid_)();  // Return the thread id for the current thread
	std::atomic_size_t log_size_;
	int fd_;
	const static uint64_t flush_every_seconds_ = 5;
	std::atomic_uint_fast64_t last_flush_micros_;
	Env* env_;
	bool flush_pending_;

public:
	WinLogger(uint64_t(*gettid)(), Env* env, FILE * file, const InfoLogLevel log_level = InfoLogLevel::ERROR_LEVEL);
  
	virtual ~WinLogger();
	
	virtual void close();

	virtual void Flush();
  
	virtual void Logv(const char* format, va_list ap);

	virtual void DebugWriter(const char* str, int len);

	size_t GetLogFileSize() const;
};

}  // namespace rocksdb
