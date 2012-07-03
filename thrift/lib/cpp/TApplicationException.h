// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TAPPLICATIONEXCEPTION_H_
#define _THRIFT_TAPPLICATIONEXCEPTION_H_ 1

#include "thrift/lib/cpp/Thrift.h"


namespace apache { namespace thrift {

namespace protocol {
  class TProtocol;
}

/**
 * This class is thrown when some high-level communication errors with
 * the remote peer occur, and also when a server throws an unexpected
 * exception from a handler method.  Because of the latter case, this
 * class can be serialized.
 */
class TApplicationException : public TException {
 public:

  /**
   * Error codes for the various types of exceptions.
   */
  enum TApplicationExceptionType
  { UNKNOWN = 0
  , UNKNOWN_METHOD = 1
  , INVALID_MESSAGE_TYPE = 2
  , WRONG_METHOD_NAME = 3
  , BAD_SEQUENCE_ID = 4
  , MISSING_RESULT = 5
  , INVALID_TRANSFORM = 6
  , INVALID_PROTOCOL = 7
  , UNSUPPORTED_CLIENT_TYPE = 8
  };


  TApplicationException() :
    type_(UNKNOWN) {}

  TApplicationException(TApplicationExceptionType type) :
    type_(type) {}

  TApplicationException(const std::string& message) :
    message_(message),
    type_(UNKNOWN) {}

  TApplicationException(TApplicationExceptionType type,
                        const std::string& message) :
    message_(message),
    type_(type) {}

  virtual ~TApplicationException() throw() {}

  /**
   * Returns an error code that provides information about the type of error
   * that has occurred.
   *
   * @return Error code
   */
  TApplicationExceptionType getType() {
    return type_;
  }

  virtual const char* what() const throw() {
    if (message_.empty()) {
      switch (type_) {
        case UNKNOWN              : return "TApplicationException: Unknown application exception";
        case UNKNOWN_METHOD       : return "TApplicationException: Unknown method";
        case INVALID_MESSAGE_TYPE : return "TApplicationException: Invalid message type";
        case WRONG_METHOD_NAME    : return "TApplicationException: Wrong method name";
        case BAD_SEQUENCE_ID      : return "TApplicationException: Bad sequence identifier";
        case MISSING_RESULT       : return "TApplicationException: Missing result";
        case INVALID_TRANSFORM    :
          return "TApplicationException: Invalid transform";
        case INVALID_PROTOCOL     :
          return "TApplicationException: Invalid protocol";
        case UNSUPPORTED_CLIENT_TYPE:
          return "TApplicationException: Unsupported client type";
        default                   : return "TApplicationException: (Invalid exception type)";
      };
    } else {
      return message_.c_str();
    }
  }

  uint32_t read(protocol::TProtocol* iprot);
  uint32_t write(protocol::TProtocol* oprot) const;

 protected:
  std::string message_;

  /**
   * Error code
   */
  TApplicationExceptionType type_;

};

}} // apache::thrift

#endif // #ifndef _THRIFT_TAPPLICATIONEXCEPTION_H_
