// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef THRIFT_ASYNC_TEVENTCONNECTION_H_
#define THRIFT_ASYNC_TEVENTCONNECTION_H_ 1

#include "thrift/lib/cpp/server/TConnectionContext.h"
#include "thrift/lib/cpp/transport/TSocketAddress.h"
#include "thrift/lib/cpp/async/TEventServer.h"
#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>

namespace apache { namespace thrift {

class TProcessor;

namespace protocol {
class TProtocol;
}

namespace server {
class TServerEventHandler;
}

namespace transport {
class TMemoryBuffer;
}

namespace async {

class TAsyncEventChannel;
class TAsyncProcessor;
class TEventWorker;
class TAsyncSocket;
class TaskCompletionMessage;

/**
 * Represents a connection that is handled via libevent. This connection
 * essentially encapsulates a socket that has some associated libevent state.
 */
class TEventConnection : private boost::noncopyable,
                         public TEventBase::LoopCallback {
 public:

  /**
   * Constructor for TEventConnection.
   *
   * @param asyncSocket shared pointer to the async socket
   * @param address the peer address of this connection
   * @param worker the worker instance that is handling this connection
   */
  TEventConnection(boost::shared_ptr<TAsyncSocket> asyncSocket,
                   const transport::TSocketAddress* address,
                   TEventWorker* worker, TEventServer::TransportType transport);

  /**
   * (Re-)Initialize a TEventConnection.  We break this out from the
   * constructor to allow for pooling.
   *
   * @param asyncSocket shared pointer to the async socket
   * @param address the peer address of this connection
   * @param worker the worker instance that is handling this connection
   */
  void init(boost::shared_ptr<TAsyncSocket> asyncSocket,
            const transport::TSocketAddress* address,
            TEventWorker* worker, TEventServer::TransportType transport);

  /// First cause -- starts i/o on connection
  void start();

  /// Shut down the connection even if it's OK; used for load reduction.
  void stop() {
    shutdown_ = true;
  }

  /// Return a pointer to the worker that owns us
  TEventWorker* getWorker() const {
    return worker_;
  }

  /// cause the notification callback to occur within the appropriate context
  bool notifyCompletion(TaskCompletionMessage &&msg);

  /// Run scheduled read when there are too many reads on the stack
  void runLoopCallback() THRIFT_NOEXCEPT;

  boost::shared_ptr<apache::thrift::TProcessor> getProcessor() const {
    return processor_;
  }

  boost::shared_ptr<apache::thrift::protocol::TProtocol>
   getInputProtocol() const {
    return inputProtocol_;
  }

  boost::shared_ptr<apache::thrift::protocol::TProtocol>
   getOutputProtocol() const {
    return outputProtocol_;
  }

  /// Get the per-server event handler set for this server, if any
  boost::shared_ptr<apache::thrift::server::TServerEventHandler>
   getServerEventHandler() const {
    return serverEventHandler_;
  }

  /// Get the TConnectionContext for this connection
  server::TConnectionContext* getConnectionContext() {
    return &context_;
  }

  /// Destructor -- close down the connection.
  ~TEventConnection();

  /**
   * Check the size of our memory buffers and resize if needed.  Do not call
   * when a call is in progress.
   */
  void checkBufferMemoryUsage();

 private:
  class ConnContext : public server::TConnectionContext {
   public:
    void init(const transport::TSocketAddress* address,
              boost::shared_ptr<protocol::TProtocol> inputProtocol,
              boost::shared_ptr<protocol::TProtocol> outputProtocol) {
      address_ = *address;
      inputProtocol_ = inputProtocol;
      outputProtocol_ = outputProtocol;
    }

    virtual const transport::TSocketAddress* getPeerAddress() const {
      return &address_;
    }

    void reset() {
      address_.reset();
      cleanupUserData();
    }

    // TODO(dsanduleac): implement the virtual getInputProtocol() & such

    virtual boost::shared_ptr<protocol::TProtocol> getInputProtocol() const {
      // from TEventConnection
      return inputProtocol_;
    }

    virtual boost::shared_ptr<protocol::TProtocol> getOutputProtocol() const {
      return outputProtocol_;
    }

   private:
    transport::TSocketAddress address_;
    boost::shared_ptr<protocol::TProtocol> inputProtocol_;
    boost::shared_ptr<protocol::TProtocol> outputProtocol_;
  };

  void readNextRequest();
  void handleReadSuccess();
  void handleReadFailure();
  void handleAsyncTaskComplete(bool success);
  void handleSendSuccess();
  void handleSendFailure();

  void handleEOF();
  void handleFailure(const char* msg);
  void cleanup();

  //! The worker instance handling this connection.
  TEventWorker* worker_;

  //! This connection's socket.
  boost::shared_ptr<TAsyncSocket> asyncSocket_;

  //! Transport that the processor reads from.
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> inputTransport_;

  //! Transport that the processor writes to.
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> outputTransport_;

  /// Largest size of read buffer seen since buffer was constructed
  size_t largestReadBufferSize_;

  /// Largest size of write buffer seen since buffer was constructed
  size_t largestWriteBufferSize_;

  /// Count of the number of calls for use with getResizeBufferEveryN().
  int32_t callsForResize_;

  //! Protocol decoder.
  boost::shared_ptr<apache::thrift::protocol::TProtocol> inputProtocol_;

  //! Protocol encoder.
  boost::shared_ptr<apache::thrift::protocol::TProtocol> outputProtocol_;

  //! Channel that actually performs the socket I/O and callbacks.
  TAsyncEventChannel* asyncChannel_;

  /// Count of outstanding processor callbacks (generally 0 or 1).
  int32_t processorActive_;

  //! Count of the number of handleReadSuccess frames on the stack
  int32_t readersActive_;

  /// Sync processor if we're in queuing mode
  boost::shared_ptr<apache::thrift::TProcessor> processor_;

  /// Flag used to shut down connection (used for load-shedding mechanism).
  bool shutdown_;

  /// Flag indicating that we have deferred closing down (processor was active)
  bool closing_;

  /// The per-server event handler set for this erver, if any
  boost::shared_ptr<apache::thrift::server::TServerEventHandler>
   serverEventHandler_;

  /// per-connection context
  ConnContext context_;

  /// Our processor
  boost::shared_ptr<TAsyncProcessor> asyncProcessor_;

  /// So that TEventWorker can call handleAsyncTaskComplete();
  friend class TEventWorker;

  /// Make the server a friend so it can manage tasks when overloaded
  friend class TEventServer;

  /// Make an async task a friend so it can communicate a cleanup() to us.
  friend class TEventTask;
};

}}} // apache::thrift::async

#endif // #ifndef THRIFT_ASYNC_TEVENTCONNECTION_H_
