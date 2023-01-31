//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/stderr_logger.h"

#include "port/sys_time.h"

namespace ROCKSDB_NAMESPACE {
StderrLogger::~StderrLogger() {}

void StderrLogger::Logv(const char* format, va_list ap) {
  const uint64_t thread_id = Env::Default()->GetThreadID();

  port::TimeVal now_tv;
  port::GetTimeOfDay(&now_tv, nullptr);
  const time_t seconds = now_tv.tv_sec;
  struct tm t;
  port::LocalTimeR(&seconds, &t);
  fprintf(stderr, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ", t.tm_year + 1900,
          t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec,
          static_cast<int>(now_tv.tv_usec),
          static_cast<long long unsigned int>(thread_id));

  vfprintf(stderr, format, ap);
  fprintf(stderr, "\n");
}
}  // namespace ROCKSDB_NAMESPACE
