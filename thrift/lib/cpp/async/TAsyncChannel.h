// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef THRIFT_ASYNC_TASYNCCHANNEL_H_
#define THRIFT_ASYNC_TASYNCCHANNEL_H_ 1

#include <tr1/functional>
#include "thrift/lib/cpp/Thrift.h"
#include "thrift/lib/cpp/transport/TTransportUtils.h"

namespace apache { namespace thrift { namespace async {

class TAsyncTransport;

/**
 * TAsyncChannel defines an asynchronous API for message-based I/O.
 */
class TAsyncChannel {
 public:
  typedef std::tr1::function<void()> VoidCallback;

  virtual ~TAsyncChannel() {}

  // is the channel readable (possibly closed by the remote site)?
  virtual bool readable() const = 0;
  // is the channel in a good state?
  virtual bool good() const = 0;
  virtual bool error() const = 0;
  virtual bool timedOut() const = 0;

  /**
   * Send a message over the channel.
   *
   * @return  call "cob" on success, "errorCob" on fail.  Caller must be ready
   *          for either cob to be called before return.  Only one cob will be
   *          called and it will be called exactly once per invocation.
   */
  virtual void sendMessage(
                        const VoidCallback& cob,
                        const VoidCallback& errorCob,
                        apache::thrift::transport::TMemoryBuffer* message) = 0;

  /**
   * Receive a message from the channel.
   *
   * @return  call "cob" on success, "errorCob" on fail.  Caller must be ready
   *          for either cob to be called before return.  Only one cob will be
   *          called and it will be called exactly once per invocation.
   */
  virtual void recvMessage(
                        const VoidCallback& cob,
                        const VoidCallback& errorCob,
                        apache::thrift::transport::TMemoryBuffer* message) = 0;

  /**
   * Send a message over the channel and receive a response.
   *
   * @return  call "cob" on success, "errorCob" on fail.  Caller must be ready
   *          for either cob to be called before return.  Only one cob will be
   *          called and it will be called exactly once per invocation.
   */
  virtual void sendAndRecvMessage(
                        const VoidCallback& cob,
                        const VoidCallback& errorCob,
                        apache::thrift::transport::TMemoryBuffer* sendBuf,
                        apache::thrift::transport::TMemoryBuffer* recvBuf) = 0;

  /**
   * Send a message over the channel, single cob version.  (See above.)
   *
   * @return  call "cob" on success or fail; channel status must be queried
   *          by the cob.
   */
  void sendMessage(const VoidCallback& cob,
                   apache::thrift::transport::TMemoryBuffer* message) {
    return sendMessage(cob, cob, message);
  }

  /**
   * Receive a message from the channel, single cob version.  (See above.)
   *
   * @return  call "cob" on success or fail; channel status must be queried
   *          by the cob.
   */
  void recvMessage(const VoidCallback& cob,
                          apache::thrift::transport::TMemoryBuffer* message) {
    return recvMessage(cob, cob, message);
  }

  /**
   * Send a message over the channel and receive response, single cob version.
   * (See above.)
   *
   * @return  call "cob" on success or fail; channel status must be queried
   *          by the cob.
   */
  void sendAndRecvMessage(const VoidCallback& cob,
                          apache::thrift::transport::TMemoryBuffer* sendBuf,
                          apache::thrift::transport::TMemoryBuffer* recvBuf) {
    return sendAndRecvMessage(cob, cob, sendBuf, recvBuf);
  }

  // TODO(dreiss): Make this nonvirtual when TFramedSocketAsyncChannel gets
  // renamed to TFramedAsyncChannel.
  virtual boost::shared_ptr<TAsyncTransport> getTransport() = 0;
};

}}} // apache::thrift::async

#endif // #ifndef THRIFT_ASYNC_TASYNCCHANNEL_H_
