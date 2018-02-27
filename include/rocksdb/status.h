// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#ifndef STORAGE_ROCKSDB_INCLUDE_STATUS_H_
#define STORAGE_ROCKSDB_INCLUDE_STATUS_H_

#include <string>
#include <utility>
#include "rocksdb/slice.h"

#ifdef ROCKSDB_COROUTINES
#include "rocksdb/async/coro_promise.h"
#endif // ROCKSDB_COROUTINES

namespace rocksdb {

class Status {
 public:
  // Create a success status.
  Status() : code_(kOk), subcode_(kNone), state_(nullptr) {}
  ~Status() { delete[] state_; }

  // Copy the specified status.
  Status(const Status& s);
  Status& operator=(const Status& s);
  Status(Status&& s)
#if !(defined _MSC_VER) || ((defined _MSC_VER) && (_MSC_VER >= 1900))
      noexcept
#endif
      ;
  Status& operator=(Status&& s)
#if !(defined _MSC_VER) || ((defined _MSC_VER) && (_MSC_VER >= 1900))
      noexcept
#endif
      ;
  bool operator==(const Status& rhs) const;
  bool operator!=(const Status& rhs) const;

  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
    kMergeInProgress = 6,
    kIncomplete = 7,
    kShutdownInProgress = 8,
    kTimedOut = 9,
    kAborted = 10,
    kBusy = 11,
    kExpired = 12,
    kTryAgain = 13
  };

  Code code() const { return code_; }

  enum SubCode {
    kNone = 0,
    kMutexTimeout = 1,
    kLockTimeout = 2,
    kLockLimit = 3,
    kNoSpace = 4,
    kDeadlock = 5,
    kStaleFile = 6,
    kMemoryLimit = 7,
    kMaxSubCode
  };

  SubCode subcode() const { return subcode_; }

  // Returns a C style string indicating the message of the Status
  const char* getState() const { return state_; }

  // Return a success status.
  static Status OK() { return Status(); }

  // Return error status of an appropriate type.
  static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotFound, msg, msg2);
  }
  // Fast path for not found without malloc;
  static Status NotFound(SubCode msg = kNone) { return Status(kNotFound, msg); }

  static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kCorruption, msg, msg2);
  }
  static Status Corruption(SubCode msg = kNone) {
    return Status(kCorruption, msg);
  }

  static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotSupported, msg, msg2);
  }
  static Status NotSupported(SubCode msg = kNone) {
    return Status(kNotSupported, msg);
  }

  static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kInvalidArgument, msg, msg2);
  }
  static Status InvalidArgument(SubCode msg = kNone) {
    return Status(kInvalidArgument, msg);
  }

  static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, msg, msg2);
  }
  static Status IOError(SubCode msg = kNone) { return Status(kIOError, msg); }

  static Status MergeInProgress(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kMergeInProgress, msg, msg2);
  }
  static Status MergeInProgress(SubCode msg = kNone) {
    return Status(kMergeInProgress, msg);
  }

  static Status Incomplete(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIncomplete, msg, msg2);
  }
  static Status Incomplete(SubCode msg = kNone) {
    return Status(kIncomplete, msg);
  }

  static Status ShutdownInProgress(SubCode msg = kNone) {
    return Status(kShutdownInProgress, msg);
  }
  static Status ShutdownInProgress(const Slice& msg,
                                   const Slice& msg2 = Slice()) {
    return Status(kShutdownInProgress, msg, msg2);
  }
  static Status Aborted(SubCode msg = kNone) { return Status(kAborted, msg); }
  static Status Aborted(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kAborted, msg, msg2);
  }

  static Status Busy(SubCode msg = kNone) { return Status(kBusy, msg); }
  static Status Busy(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kBusy, msg, msg2);
  }

  static Status TimedOut(SubCode msg = kNone) { return Status(kTimedOut, msg); }
  static Status TimedOut(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kTimedOut, msg, msg2);
  }

  static Status Expired(SubCode msg = kNone) { return Status(kExpired, msg); }
  static Status Expired(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kExpired, msg, msg2);
  }

  static Status TryAgain(SubCode msg = kNone) { return Status(kTryAgain, msg); }
  static Status TryAgain(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kTryAgain, msg, msg2);
  }

  static Status NoSpace() { return Status(kIOError, kNoSpace); }
  static Status NoSpace(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, kNoSpace, msg, msg2);
  }

  static Status MemoryLimit() { return Status(kAborted, kMemoryLimit); }
  static Status MemoryLimit(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kAborted, kMemoryLimit, msg, msg2);
  }

  // Returns true iff the status indicates success.
  bool ok() const { return code() == kOk; }

  // Returns true iff the status indicates a NotFound error.
  bool IsNotFound() const { return code() == kNotFound; }

  // Returns true iff the status indicates a Corruption error.
  bool IsCorruption() const { return code() == kCorruption; }

  // Returns true iff the status indicates a NotSupported error.
  bool IsNotSupported() const { return code() == kNotSupported; }

  // Returns true iff the status indicates an InvalidArgument error.
  bool IsInvalidArgument() const { return code() == kInvalidArgument; }

  // Returns true iff the status indicates an IOError.
  bool IsIOError() const { return code() == kIOError; }

  // Returns true iff the status indicates an MergeInProgress.
  bool IsMergeInProgress() const { return code() == kMergeInProgress; }

  // Returns true iff the status indicates Incomplete
  bool IsIncomplete() const { return code() == kIncomplete; }

  // Returns true iff the status indicates Shutdown In progress
  bool IsShutdownInProgress() const { return code() == kShutdownInProgress; }

  bool IsTimedOut() const { return code() == kTimedOut; }

  bool IsAborted() const { return code() == kAborted; }

  bool IsLockLimit() const {
    return code() == kAborted && subcode() == kLockLimit;
  }

  // Returns true iff the status indicates that a resource is Busy and
  // temporarily could not be acquired.
  bool IsBusy() const { return code() == kBusy; }

  bool IsDeadlock() const { return code() == kBusy && subcode() == kDeadlock; }

  // Returns true iff the status indicated that the operation has Expired.
  bool IsExpired() const { return code() == kExpired; }

  // Returns true iff the status indicates a TryAgain error.
  // This usually means that the operation failed, but may succeed if
  // re-attempted.
  bool IsTryAgain() const { return code() == kTryAgain; }

  // Returns true iff the status indicates a NoSpace error
  // This is caused by an I/O error returning the specific "out of space"
  // error condition. Stricto sensu, an NoSpace error is an I/O error
  // with a specific subcode, enabling users to take the appropriate action
  // if needed
  bool IsNoSpace() const {
    return (code() == kIOError) && (subcode() == kNoSpace);
  }

  // Returns true iff the status indicates a memory limit error.  There may be
  // cases where we limit the memory used in certain operations (eg. the size
  // of a write batch) in order to avoid out of memory exceptions.
  bool IsMemoryLimit() const {
    return (code() == kAborted) && (subcode() == kMemoryLimit);
  }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

#ifdef ROCKSDB_COROUTINES
  // Will be invoked automatically by the compiler
  // via promise_type to facilitate a customization point
  void SetCohandle(void* ch) {
    ch_ = ch;
  }

  void* GetCohandle() const {
    assert(ch_);
    return ch_;
  }
private:
  void* ch_ = nullptr;
#endif // ROCKSDB_COROUTINES

 private:
  // A nullptr state_ (which is always the case for OK) means the message
  // is empty.
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4..]  == message
  Code code_;
  SubCode subcode_;
  const char* state_;

  static const char* msgs[static_cast<int>(kMaxSubCode)];

  explicit Status(Code _code, SubCode _subcode = kNone)
      : code_(_code), subcode_(_subcode), state_(nullptr) {}

  Status(Code _code, SubCode _subcode, const Slice& msg, const Slice& msg2);
  Status(Code _code, const Slice& msg, const Slice& msg2)
      : Status(_code, kNone, msg, msg2) {}

  static const char* CopyState(const char* s);
};

inline Status::Status(const Status& s) : 
#ifdef ROCKSDB_COROUTINES
  ch_(s.ch_),
#endif // ROCKSDB_COROUTINES
  code_(s.code_),
  subcode_(s.subcode_) {
  state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_);
}
inline Status& Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (this != &s) {
#ifdef ROCKSDB_COROUTINES
    assert(!ch_);
    ch_ = s.ch_;
#endif // ROCKSDB_COROUTINES
    code_ = s.code_;
    subcode_ = s.subcode_;
    delete[] state_;
    state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_);
  }
  return *this;
}

inline Status::Status(Status&& s)
#if !(defined _MSC_VER) || ((defined _MSC_VER) && (_MSC_VER >= 1900))
    noexcept
#endif
    : Status() {
  *this = std::move(s);
}

inline Status& Status::operator=(Status&& s)
#if !(defined _MSC_VER) || ((defined _MSC_VER) && (_MSC_VER >= 1900))
    noexcept
#endif
{
  if (this != &s) {
#ifdef ROCKSDB_COROUTINES
    ch_ = s.ch_,
    s.ch_ = nullptr;
#endif // ROCKSDB_COROUTINES
    code_ = std::move(s.code_);
    s.code_ = kOk;
    subcode_ = std::move(s.subcode_);
    s.subcode_ = kNone;
    delete[] state_;
    state_ = nullptr;
    std::swap(state_, s.state_);
  }
  return *this;
}

inline bool Status::operator==(const Status& rhs) const {
  return (code_ == rhs.code_);
}

inline bool Status::operator!=(const Status& rhs) const {
  return !(*this == rhs);
}

#ifdef ROCKSDB_COROUTINES

struct StatusPromiseType : PromiseType<Status> {
  Status get_return_object() {
    Status s;
    auto ch = std::experimental::coroutine_handle<StatusPromiseType>::from_promise(*this);
    s.SetCohandle(ch.address());
    return s;
  }
};

#endif // ROCKSDB_COROUTINES

}  // namespace rocksdb

#ifdef ROCKSDB_COROUTINES

namespace std {
namespace experimental {
// traits specialization for coroutines that return
// Status which is most of them.
  //
// In coroutines, this return type does not really deliver the status
// rather it preserves the signature of the function
// and allows us to gain access to the callee handle
// The actual return Status value is delivered when co_return
// executes via return_value()/await_resume sequence exchange the
// result_ value by means of the promise_type
template<class ...ArgTypes>
struct coroutine_traits<rocksdb::Status, ArgTypes...> {
  using promise_type = rocksdb::StatusPromiseType;
}; // coroutine_traits
} // experimental
} // std

namespace rocksdb {
// Overload operator co_await for the status so we do not have to make
// Status itself awaitable
struct AwaitableStatus {

  using promise_type = StatusPromiseType;

  std::experimental::coroutine_handle<promise_type> ch_;

  AwaitableStatus() : ch_(nullptr) {}
  explicit
    AwaitableStatus(std::experimental::coroutine_handle<promise_type> ch) :
    ch_(ch) {
  }
  AwaitableStatus(const AwaitableStatus&) = delete;
  AwaitableStatus& operator=(const AwaitableStatus&) = delete;
  AwaitableStatus(AwaitableStatus&& o) noexcept
    : ch_(nullptr) {
    *this = std::move(o);
  }
  AwaitableStatus& operator=(AwaitableStatus&& o) noexcept {
    assert(!ch_);
    ch_ = o.ch_;
    o.ch_ = nullptr;
    return *this;
  }
  ~AwaitableStatus() {
    if (ch_) { ch_.destroy(); }
  }
  // Awaitable API
  bool await_ready() {
    return false;
  }
  // capture caller's handle on initial_suspend
  // and resume the callee
  void await_suspend(std::experimental::coroutine_handle<> CallerCoro) {
    ch_.promise().cb_ = CallerCoro;
    ch_.resume();
  }
  // Deliver the return value once we are resumed
  // this becomes the result of the co_await
  // resume occurs essentially on callback when
  // the callee is in its final_suspend
  rocksdb::Status await_resume() {
    return ch_.promise().result_;
  }
};

inline
AwaitableStatus operator co_await(const rocksdb::Status& s) {
  auto ch = std::experimental::coroutine_handle<AwaitableStatus::promise_type>::from_address(s.GetCohandle());
  return AwaitableStatus(ch);
}
} // rocksdb

#endif // ROCKSDB_COROUTINES

// XXX:  Put it here for now, should be some place in port
#ifdef ROCKSDB_COROUTINES
  #define RDB_AWAIT  co_await
  #define RDB_RETURN co_return
#else
#define RDB_AWAIT
#define RDB_RETURN return
#endif // ROCKSDB_COROUTINES

#endif  // STORAGE_ROCKSDB_INCLUDE_STATUS_H_
