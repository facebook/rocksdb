// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// An IOStatus encapsulates the result of an operation.  It may indicate
// success, or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on an IOStatus without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same IOStatus must use
// external synchronization.

#pragma once

#include <string>
#include "rocksdb/slice.h"
#ifdef OS_WIN
#include <string.h>
#endif
#include <cstring>
#include "status.h"

namespace ROCKSDB_NAMESPACE {

class IOStatus : public Status {
 public:
  using Code = Status::Code;
  using SubCode = Status::SubCode;

  enum IOErrorScope {
    kIOErrorScopeFileSystem,
    kIOErrorScopeFile,
    kIOErrorScopeRange,
    kIOErrorScopeMax,
  };

  // Create a success status.
  IOStatus() : IOStatus(kOk, kNone) {}
  ~IOStatus() {}

  // Copy the specified status.
  IOStatus(const IOStatus& s);
  IOStatus& operator=(const IOStatus& s);
  IOStatus(IOStatus&& s)
#if !(defined _MSC_VER) || ((defined _MSC_VER) && (_MSC_VER >= 1900))
      noexcept
#endif
      ;
  IOStatus& operator=(IOStatus&& s)
#if !(defined _MSC_VER) || ((defined _MSC_VER) && (_MSC_VER >= 1900))
      noexcept
#endif
      ;
  bool operator==(const IOStatus& rhs) const;
  bool operator!=(const IOStatus& rhs) const;

  void SetRetryable(bool retryable) { retryable_ = retryable; }
  void SetDataLoss(bool data_loss) { data_loss_ = data_loss; }
  void SetScope(IOErrorScope scope) { scope_ = scope; }

  bool GetRetryable() const { return retryable_; }
  bool GetDataLoss() const { return data_loss_; }
  IOErrorScope GetScope() const { return scope_; }

  // Return a success status.
  static IOStatus OK() { return IOStatus(); }

  static IOStatus NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
    return IOStatus(kNotSupported, msg, msg2);
  }
  static IOStatus NotSupported(SubCode msg = kNone) {
    return IOStatus(kNotSupported, msg);
  }

  // Return error status of an appropriate type.
  static IOStatus NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return IOStatus(kNotFound, msg, msg2);
  }
  // Fast path for not found without malloc;
  static IOStatus NotFound(SubCode msg = kNone) {
    return IOStatus(kNotFound, msg);
  }

  static IOStatus Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
    return IOStatus(kCorruption, msg, msg2);
  }
  static IOStatus Corruption(SubCode msg = kNone) {
    return IOStatus(kCorruption, msg);
  }

  static IOStatus InvalidArgument(const Slice& msg,
                                  const Slice& msg2 = Slice()) {
    return IOStatus(kInvalidArgument, msg, msg2);
  }
  static IOStatus InvalidArgument(SubCode msg = kNone) {
    return IOStatus(kInvalidArgument, msg);
  }

  static IOStatus IOError(const Slice& msg, const Slice& msg2 = Slice()) {
    return IOStatus(kIOError, msg, msg2);
  }
  static IOStatus IOError(SubCode msg = kNone) {
    return IOStatus(kIOError, msg);
  }

  static IOStatus Busy(SubCode msg = kNone) { return IOStatus(kBusy, msg); }
  static IOStatus Busy(const Slice& msg, const Slice& msg2 = Slice()) {
    return IOStatus(kBusy, msg, msg2);
  }

  static IOStatus TimedOut(SubCode msg = kNone) {
    return IOStatus(kTimedOut, msg);
  }
  static IOStatus TimedOut(const Slice& msg, const Slice& msg2 = Slice()) {
    return IOStatus(kTimedOut, msg, msg2);
  }

  static IOStatus NoSpace() { return IOStatus(kIOError, kNoSpace); }
  static IOStatus NoSpace(const Slice& msg, const Slice& msg2 = Slice()) {
    return IOStatus(kIOError, kNoSpace, msg, msg2);
  }

  static IOStatus PathNotFound() { return IOStatus(kIOError, kPathNotFound); }
  static IOStatus PathNotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return IOStatus(kIOError, kPathNotFound, msg, msg2);
  }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  // std::string ToString() const;

 private:
  friend IOStatus status_to_io_status(Status&&);
  bool retryable_;
  bool data_loss_;
  IOErrorScope scope_;

  explicit IOStatus(Code _code, SubCode _subcode = kNone)
      : Status(_code, _subcode),
        retryable_(false),
        data_loss_(false),
        scope_(kIOErrorScopeFileSystem) {}

  IOStatus(Code _code, SubCode _subcode, const Slice& msg, const Slice& msg2);
  IOStatus(Code _code, const Slice& msg, const Slice& msg2)
      : IOStatus(_code, kNone, msg, msg2) {}
};

inline IOStatus::IOStatus(Code _code, SubCode _subcode, const Slice& msg,
                          const Slice& msg2)
    : Status(_code, _subcode),
      retryable_(false),
      data_loss_(false),
      scope_(kIOErrorScopeFileSystem) {
  assert(code_ != kOk);
  assert(subcode_ != kMaxSubCode);
  const size_t len1 = msg.size();
  const size_t len2 = msg2.size();
  const size_t size = len1 + (len2 ? (2 + len2) : 0);
  char* const result = new char[size + 1];  // +1 for null terminator
  memcpy(result, msg.data(), len1);
  if (len2) {
    result[len1] = ':';
    result[len1 + 1] = ' ';
    memcpy(result + len1 + 2, msg2.data(), len2);
  }
  result[size] = '\0';  // null terminator for C style string
  state_ = result;
}

inline IOStatus::IOStatus(const IOStatus& s) : Status(s.code_, s.subcode_) {
  retryable_ = s.retryable_;
  data_loss_ = s.data_loss_;
  scope_ = s.scope_;
  state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_);
}
inline IOStatus& IOStatus::operator=(const IOStatus& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (this != &s) {
    code_ = s.code_;
    subcode_ = s.subcode_;
    retryable_ = s.retryable_;
    data_loss_ = s.data_loss_;
    scope_ = s.scope_;
    delete[] state_;
    state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_);
  }
  return *this;
}

inline IOStatus::IOStatus(IOStatus&& s)
#if !(defined _MSC_VER) || ((defined _MSC_VER) && (_MSC_VER >= 1900))
    noexcept
#endif
    : IOStatus() {
  *this = std::move(s);
}

inline IOStatus& IOStatus::operator=(IOStatus&& s)
#if !(defined _MSC_VER) || ((defined _MSC_VER) && (_MSC_VER >= 1900))
    noexcept
#endif
{
  if (this != &s) {
    code_ = std::move(s.code_);
    s.code_ = kOk;
    subcode_ = std::move(s.subcode_);
    s.subcode_ = kNone;
    retryable_ = s.retryable_;
    data_loss_ = s.data_loss_;
    scope_ = s.scope_;
    scope_ = kIOErrorScopeFileSystem;
    delete[] state_;
    state_ = nullptr;
    std::swap(state_, s.state_);
  }
  return *this;
}

inline bool IOStatus::operator==(const IOStatus& rhs) const {
  return (code_ == rhs.code_);
}

inline bool IOStatus::operator!=(const IOStatus& rhs) const {
  return !(*this == rhs);
}

inline IOStatus status_to_io_status(Status&& status) {
  if (status.ok()) {
    // Fast path
    return IOStatus::OK();
  } else {
    const char* state = status.getState();
    if (state) {
      return IOStatus(status.code(), status.subcode(),
                      Slice(state, strlen(status.getState()) + 1), Slice());
    } else {
      return IOStatus(status.code(), status.subcode());
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
