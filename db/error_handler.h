//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "rocksdb/listener.h"
#include "rocksdb/status.h"

namespace rocksdb {

class ErrorHandler {
  public:
    ErrorHandler(const ImmutableDBOptions& db_options,
        InstrumentedMutex* db_mutex)
      : db_options_(db_options),
        bg_error_(Status::OK()),
        db_mutex_(db_mutex)
      {}
    ~ErrorHandler() {}

    Status::Severity GetErrorSeverity(BackgroundErrorReason reason,
        Status::Code code, Status::SubCode subcode);

    Status SetBGError(const Status& bg_err, BackgroundErrorReason reason);

    Status GetBGError()
    {
      return bg_error_;
    }

    void ClearBGError() {
      bg_error_ = Status::OK();
    }

    bool IsDBStopped() {
      return !bg_error_.ok();
    }

    bool IsBGWorkStopped() {
      return !bg_error_.ok();
    }

  private:
    const ImmutableDBOptions& db_options_;
    Status bg_error_;
    InstrumentedMutex* db_mutex_;
};

}
