/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef _THRIFT_TRANSPORT_TTRANSPORTEXCEPTION_H_
#define _THRIFT_TRANSPORT_TTRANSPORTEXCEPTION_H_ 1

#include <string>
#include "thrift/lib/cpp/Thrift.h"

namespace apache { namespace thrift { namespace transport {

/**
 * Class to encapsulate all the possible types of transport errors that may
 * occur in various transport systems. This provides a sort of generic
 * wrapper around the shitty UNIX E_ error codes that lets a common code
 * base of error handling to be used for various types of transports, i.e.
 * pipes etc.
 *
 */
class TTransportException : public apache::thrift::TLibraryException {
 public:
  /**
   * Error codes for the various types of exceptions.
   */
  enum TTransportExceptionType
  { UNKNOWN = 0
  , NOT_OPEN = 1
  , ALREADY_OPEN = 2
  , TIMED_OUT = 3
  , END_OF_FILE = 4
  , INTERRUPTED = 5
  , BAD_ARGS = 6
  , CORRUPTED_DATA = 7
  , INTERNAL_ERROR = 8
  , NOT_SUPPORTED = 9
  , INVALID_STATE = 10
  , INVALID_FRAME_SIZE = 11
  , SSL_ERROR = 12
  };

  TTransportException() :
    apache::thrift::TLibraryException(),
      type_(UNKNOWN), errno_(0) {}

  TTransportException(TTransportExceptionType type) :
    apache::thrift::TLibraryException(),
    type_(type), errno_(0) {}

  TTransportException(const std::string& message) :
    apache::thrift::TLibraryException(message),
    type_(UNKNOWN), errno_(0) {}

  TTransportException(TTransportExceptionType type, const std::string& message) :
    apache::thrift::TLibraryException(message),
    type_(type), errno_(0) {}

  TTransportException(TTransportExceptionType type,
                      const std::string& message,
                      int errno_copy) :
    apache::thrift::TLibraryException(getMessage(message, errno_copy)),
      type_(type), errno_(errno_copy) {}

  virtual ~TTransportException() throw() {}

  /**
   * Returns an error code that provides information about the type of error
   * that has occurred.
   *
   * @return Error code
   */
  TTransportExceptionType getType() const throw() {
    return type_;
  }

  virtual const char* what() const throw() {
    if (message_.empty()) {
      switch (type_) {
        case UNKNOWN        : return "TTransportException: Unknown transport exception";
        case NOT_OPEN       : return "TTransportException: Transport not open";
        case ALREADY_OPEN   : return "TTransportException: Transport already open";
        case TIMED_OUT      : return "TTransportException: Timed out";
        case END_OF_FILE    : return "TTransportException: End of file";
        case INTERRUPTED    : return "TTransportException: Interrupted";
        case BAD_ARGS       : return "TTransportException: Invalid arguments";
        case CORRUPTED_DATA : return "TTransportException: Corrupted Data";
        case INTERNAL_ERROR : return "TTransportException: Internal error";
        case NOT_SUPPORTED  : return "TTransportException: Not supported";
        case INVALID_STATE  : return "TTransportException: Invalid state";
        case INVALID_FRAME_SIZE:
          return "TTransportException: Invalid frame size";
        case SSL_ERROR      : return "TTransportException: SSL error";
        default             : return "TTransportException: (Invalid exception type)";
      }
    } else {
      return message_.c_str();
    }
  }

  int getErrno() const { return errno_; }

 protected:
  /** Just like strerror_r but returns a C++ string object. */
  std::string strerror_s(int errno_copy);

  /** Return a message based on the input. */
  static std::string getMessage(const std::string &message,
                                int errno_copy) {
    if (errno_copy != 0) {
      return message + ": " + TOutput::strerror_s(errno_copy);
    } else {
      return message;
    }
  }

  /** Error code */
  TTransportExceptionType type_;

  /** A copy of the errno. */
  int errno_;
};

}}} // apache::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TTRANSPORTEXCEPTION_H_
