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

  // The string we eventually log has three parts: the context (time, thread),
  // optional user-supplied prefix, and the actual log message.
  //
  // We compute their lengths so that we can allocate a buffer big enough to
  // print it. The context string (with the date and thread id) is really only
  // 36 bytes, but we allocate 40 to be safe.
  size_t context_len = 40;

  va_list ap_copy;
  va_copy(ap_copy, ap);
  const size_t log_suffix_len =
      vsnprintf(nullptr, 0, format, ap_copy);
  va_end(ap_copy);

  // Allocate space for the context, log_prefix, and log itself
  // Extra byte for null termination
  size_t buf_len = context_len + log_prefix_len + log_suffix_len + 1;
  std::unique_ptr<char[]> buf(new char[buf_len]);

  // Write out the context and prefix string
  int written = snprintf(buf.get(), context_len + log_prefix_len,
          // Keep this string in sync with context_len
          "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx %s",
          t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min,
          t.tm_sec, static_cast<int>(now_tv.tv_usec),
          static_cast<long long unsigned int>(thread_id), log_prefix);
  written += vsnprintf(buf.get() + written, log_suffix_len, format, ap);
  buf[written] = '\0';

  fprintf(stderr, "%s%c", buf.get(), '\n');
}
}  // namespace ROCKSDB_NAMESPACE
