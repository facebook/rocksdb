//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Must not be included from any .h files to avoid polluting the namespace
// with macros.

#pragma once
#include "port/port.h"

// Helper macros that include information about file name and line number
#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define PREPEND_FILE_LINE(FMT) ("[" __FILE__ ":" TOSTRING(__LINE__) "] " FMT)

// Don't inclide file/line info in HEADER level
#define ROCKS_LOG_HEADER(LGR, FMT, ...) \
  rocksdb::Log(InfoLogLevel::HEADER_LEVEL, LGR, FMT, ##__VA_ARGS__)

#define ROCKS_LOG_DEBUG(LGR, FMT, ...)                                 \
  rocksdb::Log(InfoLogLevel::DEBUG_LEVEL, LGR, PREPEND_FILE_LINE(FMT), \
               ##__VA_ARGS__)

#define ROCKS_LOG_INFO(LGR, FMT, ...)                                 \
  rocksdb::Log(InfoLogLevel::INFO_LEVEL, LGR, PREPEND_FILE_LINE(FMT), \
               ##__VA_ARGS__)

#define ROCKS_LOG_WARN(LGR, FMT, ...)                                 \
  rocksdb::Log(InfoLogLevel::WARN_LEVEL, LGR, PREPEND_FILE_LINE(FMT), \
               ##__VA_ARGS__)

#define ROCKS_LOG_ERROR(LGR, FMT, ...)                                 \
  rocksdb::Log(InfoLogLevel::ERROR_LEVEL, LGR, PREPEND_FILE_LINE(FMT), \
               ##__VA_ARGS__)

#define ROCKS_LOG_FATAL(LGR, FMT, ...)                                 \
  rocksdb::Log(InfoLogLevel::FATAL_LEVEL, LGR, PREPEND_FILE_LINE(FMT), \
               ##__VA_ARGS__)

#define ROCKS_LOG_BUFFER(LOG_BUF, FMT, ...) \
  rocksdb::LogToBuffer(LOG_BUF, PREPEND_FILE_LINE(FMT), ##__VA_ARGS__)

#define ROCKS_LOG_BUFFER_MAX_SZ(LOG_BUF, MAX_LOG_SIZE, FMT, ...)      \
  rocksdb::LogToBuffer(LOG_BUF, MAX_LOG_SIZE, PREPEND_FILE_LINE(FMT), \
                       ##__VA_ARGS__)
