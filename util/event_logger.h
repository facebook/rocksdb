//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <chrono>

#include "rocksdb/env.h"

namespace rocksdb {

// JSONWritter doesn't support objects in arrays yet. There wasn't a need for
// that.
class JSONWritter {
 public:
  JSONWritter() : state_(kExpectKey), first_element_(true) { stream_ << "{"; }

  void AddKey(const std::string& key) {
    assert(state_ == kExpectKey);
    if (!first_element_) {
      stream_ << ", ";
    }
    stream_ << "\"" << key << "\": ";
    state_ = kExpectValue;
    first_element_ = false;
  }

  void AddValue(const char* value) {
    assert(state_ == kExpectValue || state_ == kInArray);
    if (state_ == kInArray && !first_element_) {
      stream_ << ", ";
    }
    stream_ << "\"" << value << "\"";
    if (state_ != kInArray) {
      state_ = kExpectKey;
    }
    first_element_ = false;
  }

  template <typename T>
  void AddValue(const T& value) {
    assert(state_ == kExpectValue || state_ == kInArray);
    if (state_ == kInArray && !first_element_) {
      stream_ << ", ";
    }
    stream_ << value;
    if (state_ != kInArray) {
      state_ = kExpectKey;
    }
    first_element_ = false;
  }

  void StartArray() {
    assert(state_ == kExpectKey);
    state_ = kInArray;
    if (!first_element_) {
      stream_ << ", ";
    }
    stream_ << "[";
    first_element_ = true;
  }

  void EndArray() {
    assert(state_ == kInArray);
    state_ = kExpectKey;
    stream_ << "]";
    first_element_ = false;
  }

  void StartObject() {
    assert(state_ == kExpectValue);
    stream_ << "{";
    first_element_ = true;
  }

  void EndObject() {
    assert(state_ == kExpectKey);
    stream_ << "}";
    first_element_ = false;
  }

  std::string Get() const { return stream_.str(); }

  JSONWritter& operator<<(const char* val) {
    if (state_ == kExpectKey) {
      AddKey(val);
    } else {
      AddValue(val);
    }
    return *this;
  }

  JSONWritter& operator<<(const std::string& val) {
    return *this << val.c_str();
  }

  template <typename T>
  JSONWritter& operator<<(const T& val) {
    assert(state_ != kExpectKey);
    AddValue(val);
    return *this;
  }

 private:
  enum JSONWritterState {
    kExpectKey,
    kExpectValue,
    kInArray,
  };
  JSONWritterState state_;
  bool first_element_;
  std::ostringstream stream_;
};

class EventLoggerStream {
 public:
  template <typename T>
  EventLoggerStream& operator<<(const T& val) {
    MakeStream();
    *json_writter_ << val;
    return *this;
  }
  ~EventLoggerStream();

 private:
  void MakeStream() {
    if (!json_writter_) {
      json_writter_ = new JSONWritter();
      *this << "time_micros"
            << std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::system_clock::now().time_since_epoch()).count();
    }
  }
  friend class EventLogger;
  explicit EventLoggerStream(Logger* logger);
  Logger* logger_;
  // ownership
  JSONWritter* json_writter_;
};

// here is an example of the output that will show up in the LOG:
// 2015/01/15-14:13:25.788019 1105ef000 EVENT_LOG_v1 {"time_micros":
// 1421360005788015, "event": "table_file_creation", "file_number": 12,
// "file_size": 1909699}
class EventLogger {
 public:
  explicit EventLogger(Logger* logger) : logger_(logger) {}
  EventLoggerStream Log() { return EventLoggerStream(logger_); }

 private:
  Logger* logger_;
};

}  // namespace rocksdb
