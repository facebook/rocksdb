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
#ifndef THRIFT_ASYNC_TEVENTSERVER_H_
#define THRIFT_ASYNC_TEVENTSERVER_H_ 1

#include "thrift/lib/cpp/Thrift.h"
#include "thrift/lib/cpp/server/TServer.h"
#include "thrift/lib/cpp/async/TAsyncProcessor.h"
#include "thrift/lib/cpp/transport/TTransportUtils.h"
#include "thrift/lib/cpp/transport/TSSLSocket.h"
#include "thrift/lib/cpp/protocol/THeaderProtocol.h"
#include "thrift/lib/cpp/concurrency/Mutex.h"
#include "thrift/lib/cpp/concurrency/ThreadLocal.h"
#include "thrift/lib/cpp/async/TEventBase.h"
#include "thrift/lib/cpp/async/TEventBaseManager.h"
#include <vector>
#include <map>
#include <cstdlib>
#include <boost/scoped_ptr.hpp>

namespace apache { namespace thrift {

namespace concurrency {
class ThreadFactory;
class ThreadManager;
}

namespace async {

using apache::thrift::protocol::TDualProtocolFactory;

// Forward declaration of classes
class TAsyncServerSocket;
class TEventConnection;
class TEventWorker;

/**
   This is a non-blocking event-based server for high performance that
   operates an I/O thread for each cpu core and uses callbacks for
   notification of processing or I/O operations.

   It does not use the TServerTransport framework, but rather has socket
   operations hardcoded for use with libevent and implements framing
   compatible with TFramedTransport.  A single "listener" thread accepts
   connections and sends them to the server threads via a single socketpair();
   the server threads are each responsible for allocating and pooling actual
   connection objects, avoiding the need for the locks required by a common
   pool.

   The original Thrift server was by Mark Slee <mcslee@facebook.com>.
   A non-blocking variant was produced which allowed for a large number
   of processing threads but restricted network I/O to a single thread --
   suboptimal on a multi-core CPU.  David Reiss <dreiss@facebook.com>
   and Mark Rabkin <mrabkin@facebook.com> refactored this server into a
   callback-driven event-based configuration.  Ed Hall <edhall@facebook.com>
   elaborated on this to support network I/O on multiple threads (ideally
   one per CPU core).

   @author Mark Slee <mcslee@facebook.com>
   @author David Reiss <dreiss@facebook.com>
   @author Mark Rabkin <mrabkin@facebook.com>
   @author Ed Hall <edhall@facebook.com>
 */

class TEventServer : public apache::thrift::server::TServer {

 public:
   enum TransportType {
    FRAMED = 0,
    HEADER = 1,

    /*********** Deprecation Warning *******************
     *                                                 *
     *    The unframed transports are deprecated !     *
     * They should be used for legancy services only   *
     * Also note: they only works with TBinaryProtocol *
     ***************************************************/

    UNFRAMED_BINARY = 2
  };

 protected:
  //! Default max size of per-worker connection pool.
  static const uint32_t T_ASYNC_MAX_CONNECTION_POOL_SIZE = 64;

  /// Starting size of a TEventConnection's read buffer
  static const int T_ASYNC_READ_BUFFER_DEFAULT_SIZE = 1024;

  /// Starting size of a TEventConnection's write buffer
  static const int T_ASYNC_WRITE_BUFFER_DEFAULT_SIZE = 1024;

  /// Maximum size of read buffer allocated to idle connection (0 = unlimited)
  static const int T_ASYNC_IDLE_READ_BUFFER_LIMIT = 0;

  /// Maximum size of write buffer allocated to idle connection (0 = unlimited)
  static const int T_ASYNC_IDLE_WRITE_BUFFER_LIMIT = 0;

  /// # of calls before resizing oversized buffers (0 = check only on close)
  static const int T_ASYNC_RESIZE_BUFFER_EVERY_N = 0;

  //! Default number of worker threads (should be # of processor cores).
  static const int T_ASYNC_DEFAULT_WORKER_THREADS = 4;

  //! Maximum size of a frame we'll accept (default = 64MB)
  static const int T_ASYNC_DEFAULT_MAX_FRAME_SIZE = 67108864;

  static const uint32_t T_MAX_NUM_MESSAGES_IN_PIPE = 0xffffffff;

  /// Listen backlog
  static const int T_LISTEN_BACKLOG = 1024;

  //! Transport type
  static const TransportType T_ASYNC_DEFAULT_TRANSPORT_TYPE = FRAMED;

 private:
  struct WorkerInfo {
    boost::shared_ptr<TEventWorker> worker;
    boost::shared_ptr<apache::thrift::concurrency::Thread> thread;
  };

  //! Max size of per-worker connection pool (may be set).
  uint32_t maxConnectionPoolSize_;

  //! SSL context
  boost::shared_ptr<transport::SSLContext> sslContext_;

  //! Factory that creates connection processor objects.
  boost::shared_ptr<TAsyncProcessorFactory> asyncProcessorFactory_;

  //! Port to listen on
  uint16_t port_;

  //! Listen socket
  TAsyncServerSocket* socket_;

  //! The TEventBase currently driving serve().  NULL when not serving.
  TEventBase* serveEventBase_;

  //! Number of worker threads (may be set) (should be # of CPU cores)
  int nWorkers_;

  //! Milliseconds we'll wait for data to appear (0 = infinity)
  int timeout_;

  //! Manager of per-thread TEventBase objects.
  TEventBaseManager eventBaseManager_;

  //! Last worker chosen -- used to select workers in round-robin sequence.
  uint32_t workerChoice_;

  //! List of workers.
  typedef std::vector<WorkerInfo> WorkerVector;
  WorkerVector workers_;

  //! Maximum number of bytes accepted in a frame.
  uint32_t maxFrameSize_;

  /// We initialize (and reinitialize) TEventConnection's read buffer to
  /// this size.
  size_t readBufferDefaultSize_;

  /// We initialize (and reinitialize) TEventConnection's write buffer to
  /// this size.
  size_t writeBufferDefaultSize_;

  /**
   * Max read buffer size for an idle TConnection.  When we place an idle
   * TConnection into TEventWorker::connectionStack_ or on every
   * resizeBufferEveryN_ calls, we insure that its read buffer is <= to
   * this size; otherwise we replace it with a new one to insure that idle
   * connections don't hog memory.  0 disables this check.
   */
  size_t idleReadBufferLimit_;

  /**
   * Max write buffer size for an idle connection.  When we place an idle
   * TConnection into TEventWorker::connectionStack_ or on every
   * resizeBufferEveryN_ calls, we insure that its write buffer is <= to
   * this size; otherwise we replace it with a new one to insure that idle
   * connections don't hog memory.  0 disables this check.
   */
  size_t idleWriteBufferLimit_;

  /**
   * Every N calls we check the buffer size limits on a connected
   * TEventConnection.  0 disables (i.e. the checks are only done when a
   * connection closes).
   */
  int32_t resizeBufferEveryN_;

  /**
   * Call timeout in ms.  When nonzero, limits the amount of time we allow
   * between the start of a call and the actual invokation of its processor.
   * The connection closes if it is exceeded.
   */
  int32_t callTimeout_;

  /**
   * The thread manager used when we're in queuing mode.
   */
  boost::shared_ptr<concurrency::ThreadManager> threadManager_;

  /**
   * Thread local storage to track the current connection being processed
   */
  concurrency::ThreadLocal<TEventConnection,
      concurrency::NoopThreadLocalManager<TEventConnection> >
    currentConnection_;

  /**
   * The time in milliseconds before an unperformed task expires --
   * queuing mode only. (0 == infinite)
   */
  uint64_t taskExpireTime_;

  /**
   * Set true if we are in queuing mode, false if not.
   */
  bool queuingMode_;

  /**
   * The speed for adjusting connection accept rate.
   * 0 for disabling auto adjusting connection accept rate.
   */
  double acceptRateAdjustSpeed_;

  /**
   * The maximum number of unprocessed messages which a NotificationPipe
   * can hold.
   */
  uint32_t maxNumMsgsInPipe_;

  /**
   * The max number of active connections for each worker
   */
  int32_t maxNumActiveConnectionsPerWorker_;

  /**
   * The transport type to use
   */
  TransportType transportType_;

  void addWorker(concurrency::ThreadFactory* threadFactory);

  /**
   * No-op signal handler (for SIGPIPE)
   */
  static void sigNoOp(int signo) {
    (void)signo;
  }

  /**
   * Set the current connection
   */
  void setCurrentConnection(TEventConnection* conn) {
    assert(currentConnection_.get() == NULL);
    currentConnection_.set(conn);
  }

  /**
   * Clear the current connection
   */
  void clearCurrentConnection() {
    currentConnection_.clear();
  }
  // Allow TEventConnection and TEventTask to access setCurrentConnection()
  // and clearCurrentConnection(). Only these two private
  // methods are meant to be used by TEventConnection and TEventTask.
  friend class TEventConnection;
  friend class TEventTask;

 public:
  /** Construct an async Thrift server.
      You need to compile your thrift configuration with thrift_cpp_options =
      "cob_style" to get the required TAsyncProcessor class; this differs
      from the usual TProcessor object by adding a completion callback.
      TBinaryProtocol is assumed for both input and output with this
      constructor.
      @param processor the TAsyncProcessor object for this service
      @param port the TCP port number for this service
      @param nWorkers the number of worker threads -- should be the same
                      as the number of CPU cores, though if a process has
                      more than one TEventServer the cores can be split
                      between them.
  */
  template<typename AsyncProcessor>
  TEventServer(boost::shared_ptr<AsyncProcessor> processor,
               int port,
               int nWorkers = T_ASYNC_DEFAULT_WORKER_THREADS,
               THRIFT_OVERLOAD_IF(AsyncProcessor, TAsyncProcessor)) :
    apache::thrift::server::TServer(boost::shared_ptr<TProcessor>()),
    maxConnectionPoolSize_(T_ASYNC_MAX_CONNECTION_POOL_SIZE),
    asyncProcessorFactory_(new TAsyncSingletonProcessorFactory(processor)),
    port_(port),
    socket_(NULL),
    serveEventBase_(NULL),
    nWorkers_(nWorkers),
    timeout_(0),
    eventBaseManager_(),
    workerChoice_(0),
    maxFrameSize_(T_ASYNC_DEFAULT_MAX_FRAME_SIZE),
    readBufferDefaultSize_(T_ASYNC_READ_BUFFER_DEFAULT_SIZE),
    writeBufferDefaultSize_(T_ASYNC_WRITE_BUFFER_DEFAULT_SIZE),
    idleReadBufferLimit_(T_ASYNC_IDLE_READ_BUFFER_LIMIT),
    idleWriteBufferLimit_(T_ASYNC_IDLE_WRITE_BUFFER_LIMIT),
    resizeBufferEveryN_(T_ASYNC_RESIZE_BUFFER_EVERY_N),
    callTimeout_(0),
    taskExpireTime_(0),
    queuingMode_(false),
    acceptRateAdjustSpeed_(0),
    maxNumMsgsInPipe_(T_MAX_NUM_MESSAGES_IN_PIPE),
    maxNumActiveConnectionsPerWorker_(0),
    transportType_(T_ASYNC_DEFAULT_TRANSPORT_TYPE) {
    processor->setAsyncServer(this);
  }

  /** Construct an async Thrift server for a particular TProtocol.
      See above; adds a "protocolFactory" parameter to replace the
      default TBinaryProtocol.
      @param processor the TAsyncProcessor object for this service
      @param protocolFactory the TProtocolFactory to use for input & output
      @param port the TCP port number for this service
      @param nWorkers the number of worker threads
  */
  template<typename AsyncProcessor>
  TEventServer(boost::shared_ptr<AsyncProcessor> processor,
               boost::shared_ptr<apache::thrift::protocol::TProtocolFactory>
                protocolFactory,
               int port,
               int nWorkers = T_ASYNC_DEFAULT_WORKER_THREADS,
               THRIFT_OVERLOAD_IF(AsyncProcessor, TAsyncProcessor)) :
    apache::thrift::server::TServer(boost::shared_ptr<TProcessor>()),
    maxConnectionPoolSize_(T_ASYNC_MAX_CONNECTION_POOL_SIZE),
    asyncProcessorFactory_(new TAsyncSingletonProcessorFactory(processor)),
    port_(port),
    socket_(NULL),
    serveEventBase_(NULL),
    nWorkers_(nWorkers),
    timeout_(0),
    eventBaseManager_(),
    workerChoice_(0),
    maxFrameSize_(T_ASYNC_DEFAULT_MAX_FRAME_SIZE),
    readBufferDefaultSize_(T_ASYNC_READ_BUFFER_DEFAULT_SIZE),
    writeBufferDefaultSize_(T_ASYNC_WRITE_BUFFER_DEFAULT_SIZE),
    idleReadBufferLimit_(T_ASYNC_IDLE_READ_BUFFER_LIMIT),
    idleWriteBufferLimit_(T_ASYNC_IDLE_WRITE_BUFFER_LIMIT),
    resizeBufferEveryN_(T_ASYNC_RESIZE_BUFFER_EVERY_N),
    callTimeout_(0),
    taskExpireTime_(0),
    queuingMode_(false),
    acceptRateAdjustSpeed_(0),
    maxNumMsgsInPipe_(T_MAX_NUM_MESSAGES_IN_PIPE),
    maxNumActiveConnectionsPerWorker_(0),
    transportType_(T_ASYNC_DEFAULT_TRANSPORT_TYPE) {
    processor->setAsyncServer(this);

    setProtocolFactory(protocolFactory);
  }

  /** Construct an async Thrift server with different input & output TProtocol.
      See above; adds "inputProtocolFactory" and "outputProtocolFactory"
      parameters.
      @param processor the TAsyncProcessor object for this service
      @param inputProtocolFactory the TProtocolFactory to use for input
      @param outputProtocolFactory the TProtocolFactory to use for output
      @param port the TCP port number for this service
      @param nWorkers the number of worker threads

      @deprecated use TDuplex* ctor below
  */
  template<typename AsyncProcessor>
  TEventServer(boost::shared_ptr<AsyncProcessor> processor,
               boost::shared_ptr<apache::thrift::protocol::TProtocolFactory>
                inputProtocolFactory,
               boost::shared_ptr<apache::thrift::protocol::TProtocolFactory>
                outputProtocolFactory,
               int port,
               int nWorkers = T_ASYNC_DEFAULT_WORKER_THREADS,
               THRIFT_OVERLOAD_IF(AsyncProcessor, TAsyncProcessor)) :
    apache::thrift::server::TServer(boost::shared_ptr<TProcessor>()),
    maxConnectionPoolSize_(T_ASYNC_MAX_CONNECTION_POOL_SIZE),
    asyncProcessorFactory_(new TAsyncSingletonProcessorFactory(processor)),
    port_(port),
    socket_(NULL),
    serveEventBase_(NULL),
    nWorkers_(nWorkers),
    timeout_(0),
    eventBaseManager_(),
    workerChoice_(0),
    maxFrameSize_(T_ASYNC_DEFAULT_MAX_FRAME_SIZE),
    readBufferDefaultSize_(T_ASYNC_READ_BUFFER_DEFAULT_SIZE),
    writeBufferDefaultSize_(T_ASYNC_WRITE_BUFFER_DEFAULT_SIZE),
    idleReadBufferLimit_(T_ASYNC_IDLE_READ_BUFFER_LIMIT),
    idleWriteBufferLimit_(T_ASYNC_IDLE_WRITE_BUFFER_LIMIT),
    resizeBufferEveryN_(T_ASYNC_RESIZE_BUFFER_EVERY_N),
    callTimeout_(0),
    taskExpireTime_(0),
    queuingMode_(false),
    acceptRateAdjustSpeed_(0),
    maxNumMsgsInPipe_(T_MAX_NUM_MESSAGES_IN_PIPE),
    maxNumActiveConnectionsPerWorker_(0),
    transportType_(T_ASYNC_DEFAULT_TRANSPORT_TYPE) {
    processor->setAsyncServer(this);

    setDuplexProtocolFactory(
      boost::shared_ptr<TDualProtocolFactory>(
        new TDualProtocolFactory(inputProtocolFactory, outputProtocolFactory)));
  }


  /** Construct an async Thrift server with custom input & output TProtocol.
      See above; Replaces protocolFactory with duplexProtocolFactory
      parameters.
      @param processor the TAsyncProcessor object for this service
      @param duplexProtocolFactory the TProtocolFactory to use for input/output
      @param port the TCP port number for this service
      @param nWorkers the number of worker threads
  */
  template<typename AsyncProcessor>
  TEventServer(
      boost::shared_ptr<AsyncProcessor> processor,
      boost::shared_ptr<apache::thrift::protocol::TDuplexProtocolFactory>
        duplexProtocolFactory,
      int port,
      int nWorkers = T_ASYNC_DEFAULT_WORKER_THREADS,
      THRIFT_OVERLOAD_IF(AsyncProcessor, TAsyncProcessor)):
    apache::thrift::server::TServer(boost::shared_ptr<TProcessor>()),
    maxConnectionPoolSize_(T_ASYNC_MAX_CONNECTION_POOL_SIZE),
    asyncProcessorFactory_(new TAsyncSingletonProcessorFactory(processor)),
    port_(port),
    socket_(NULL),
    serveEventBase_(NULL),
    nWorkers_(nWorkers),
    timeout_(0),
    eventBaseManager_(),
    workerChoice_(0),
    maxFrameSize_(T_ASYNC_DEFAULT_MAX_FRAME_SIZE),
    readBufferDefaultSize_(T_ASYNC_READ_BUFFER_DEFAULT_SIZE),
    writeBufferDefaultSize_(T_ASYNC_WRITE_BUFFER_DEFAULT_SIZE),
    idleReadBufferLimit_(T_ASYNC_IDLE_READ_BUFFER_LIMIT),
    idleWriteBufferLimit_(T_ASYNC_IDLE_WRITE_BUFFER_LIMIT),
    resizeBufferEveryN_(T_ASYNC_RESIZE_BUFFER_EVERY_N),
    callTimeout_(0),
    taskExpireTime_(0),
    queuingMode_(false),
    acceptRateAdjustSpeed_(0),
    maxNumMsgsInPipe_(T_MAX_NUM_MESSAGES_IN_PIPE),
    maxNumActiveConnectionsPerWorker_(0),
    transportType_(T_ASYNC_DEFAULT_TRANSPORT_TYPE) {
    processor->setAsyncServer(this);

    setDuplexProtocolFactory(duplexProtocolFactory);
  }


  /** Construct a task-queuing Thrift server for a particular TProtocol.
      Largely compatible with TNonblockingServer.

      @param processor the TProcessor object for this service
      @param protocolFactory the TProtocolFactory to use for input & output
      @param port the TCP port number for this service
      @param threadManager the thread manager we use for task queuing
      @param nWorkers the number of worker threads
  */
  template<typename Processor>
  TEventServer(
      boost::shared_ptr<Processor> processor,
      boost::shared_ptr<apache::thrift::protocol::TProtocolFactory>
       protocolFactory,
      int port,
      boost::shared_ptr<concurrency::ThreadManager> const& threadManager =
        boost::shared_ptr<concurrency::ThreadManager>(),
      int nWorkers = T_ASYNC_DEFAULT_WORKER_THREADS,
      THRIFT_OVERLOAD_IF(Processor, TProcessor)) :
    apache::thrift::server::TServer(processor),
    maxConnectionPoolSize_(T_ASYNC_MAX_CONNECTION_POOL_SIZE),
    port_(port),
    socket_(NULL),
    serveEventBase_(NULL),
    nWorkers_(nWorkers),
    timeout_(0),
    eventBaseManager_(),
    workerChoice_(0),
    maxFrameSize_(T_ASYNC_DEFAULT_MAX_FRAME_SIZE),
    readBufferDefaultSize_(T_ASYNC_READ_BUFFER_DEFAULT_SIZE),
    writeBufferDefaultSize_(T_ASYNC_WRITE_BUFFER_DEFAULT_SIZE),
    idleReadBufferLimit_(T_ASYNC_IDLE_READ_BUFFER_LIMIT),
    idleWriteBufferLimit_(T_ASYNC_IDLE_WRITE_BUFFER_LIMIT),
    resizeBufferEveryN_(T_ASYNC_RESIZE_BUFFER_EVERY_N),
    callTimeout_(0),
    taskExpireTime_(0),
    queuingMode_(true),
    acceptRateAdjustSpeed_(0),
    maxNumMsgsInPipe_(T_MAX_NUM_MESSAGES_IN_PIPE),
    maxNumActiveConnectionsPerWorker_(0),
    transportType_(T_ASYNC_DEFAULT_TRANSPORT_TYPE) {
    setProtocolFactory(protocolFactory);
    setThreadManager(threadManager);
  }

  /** Construct a task-queuing Thrift server for a particular TProtocol.
      Largely compatible with TNonblockingServer.

      @param processor the TProcessor object for this service
      @param protocolFactory the TProtocolFactory to use for input & output
      @param port the TCP port number for this service
      @param threadManager the thread manager we use for task queuing
      @param nWorkers the number of worker threads
  */
  template<typename Processor>
  TEventServer(
      boost::shared_ptr<Processor> processor,
      boost::shared_ptr<apache::thrift::protocol::TDuplexProtocolFactory>
        duplexProtocolFactory,
      int port,
      boost::shared_ptr<concurrency::ThreadManager> const& threadManager =
        boost::shared_ptr<concurrency::ThreadManager>(),
      int nWorkers = T_ASYNC_DEFAULT_WORKER_THREADS,
      THRIFT_OVERLOAD_IF(Processor, TProcessor)) :
    apache::thrift::server::TServer(processor),
    maxConnectionPoolSize_(T_ASYNC_MAX_CONNECTION_POOL_SIZE),
    port_(port),
    socket_(NULL),
    serveEventBase_(NULL),
    nWorkers_(nWorkers),
    timeout_(0),
    eventBaseManager_(),
    workerChoice_(0),
    maxFrameSize_(T_ASYNC_DEFAULT_MAX_FRAME_SIZE),
    readBufferDefaultSize_(T_ASYNC_READ_BUFFER_DEFAULT_SIZE),
    writeBufferDefaultSize_(T_ASYNC_WRITE_BUFFER_DEFAULT_SIZE),
    idleReadBufferLimit_(T_ASYNC_IDLE_READ_BUFFER_LIMIT),
    idleWriteBufferLimit_(T_ASYNC_IDLE_WRITE_BUFFER_LIMIT),
    resizeBufferEveryN_(T_ASYNC_RESIZE_BUFFER_EVERY_N),
    callTimeout_(0),
    taskExpireTime_(0),
    queuingMode_(true),
    acceptRateAdjustSpeed_(0),
    maxNumMsgsInPipe_(T_MAX_NUM_MESSAGES_IN_PIPE),
    maxNumActiveConnectionsPerWorker_(0),
    transportType_(T_ASYNC_DEFAULT_TRANSPORT_TYPE) {
    setDuplexProtocolFactory(duplexProtocolFactory);
    setThreadManager(threadManager);
  }

  virtual ~TEventServer() {
  }

  /**
   *
   */
  void setSSLContext(boost::shared_ptr<transport::SSLContext> context) {
    sslContext_ = context;
  }

  boost::shared_ptr<transport::SSLContext> getSSLContext() const {
    return sslContext_;
  }

  /**
   * Use the provided socket rather than binding to port_.  The caller must
   * call ::bind on this socket, but should not call ::listen.
   *
   * NOTE: TEventServe takes ownership of this 'socket' so if binding fails
   *       we destroy this socket, while cleaning itself up. So, 'accept' better
   *       work the first time :)
   */
  void useExistingSocket(int socket);

  /**
   * Return the file descriptor associated with the listening socket
   */
  int getListenSocket() const;

  /**
   * Get the TAsyncProcessorFactory object used by this server.
   *
   * @return a pointer to the processorFactory.
   */
  boost::shared_ptr<TAsyncProcessorFactory> getAsyncProcessorFactory() const {
    return asyncProcessorFactory_;
  }

  /**
   * Set the TAsyncProcessor object used by this server.
   */
  void setAsyncProcessorFactory(boost::shared_ptr<TAsyncProcessorFactory> pf) {
    asyncProcessorFactory_ = pf;
  }

  /**
   * Get the TEventBase used by the current thread.
   * This will be different between each worker and the listener.  Use this
   * for any event monitoring within a processor and be careful NOT to
   * cache between connections (since they may be executed by different
   * workers).
   *
   * @return a pointer to the TEventBase.
   */
  TEventBase* getEventBase() const {
    return eventBaseManager_.getEventBase();
  }

  /**
   * Get the TEventServer's main event base.
   *
   * @return a pointer to the TEventBase.
   */
  TEventBase* getServeEventBase() const {
    return serveEventBase_;
  }

  /**
   * Get the TEventBaseManager used by this server.
   * This can be used to find or create the TEventBase associated with
   * any given thread, including any new threads created by clients.
   *
   * @return a pointer to the TEventBaseManager.
   */
  TEventBaseManager* getEventBaseManager() {
    return &eventBaseManager_;
  }
  const TEventBaseManager* getEventBaseManager() const {
    return &eventBaseManager_;
  }

  /**
   * Set the port to serve
   */
  void setPort(uint16_t port) {
    port_ = port ;
  }

  /**
   *Set the maximum number of inactive connection objects pooled.
   * Since these objects consume memory, we need to limit how many we keep.
   * You can disable pooling altogether by setting this to zero.  Note that
   * the actual maximum is nWorkers*size since each worker thread maintains
   * its own pool (to avoid the need for locks).
   *
   * @param size the maximum number of inactive connections to pool.
  */
  void setMaxConnectionPoolSize(uint32_t size) {
    maxConnectionPoolSize_ = size;
  }

  /** Get the maximum number of inactive connection objects pooled.
      @return the maximum pool size.
  */
  uint32_t getMaxConnectionPoolSize() const {
    return maxConnectionPoolSize_;
  }

  /**
   * Get the maximum number of unprocessed messages which a NotificationPipe
   * can hold.
   */
  uint32_t getMaxNumMessagesInPipe() const {
    return maxNumMsgsInPipe_;
  }
  /**
   * Set the maximum number of unprocessed messages in NotificationPipe.
   * No new message will be sent to that NotificationPipe if there are more
   * than such number of unprocessed messages in that pipe.
   */
  void setMaxNumMessagesInPipe(uint32_t num) {
    maxNumMsgsInPipe_ = num;
  }

  /**
   * Get the maxmum number of active connections each TAsyncWorker can have
   */
  int32_t getMaxNumActiveConnectionsPerWorker() const {
    return maxNumActiveConnectionsPerWorker_;
  }

  /**
   * Set the maxmum number of active connections each TAsyncWorker can have.
   * Zero means unlimited
   */
  void setMaxNumActiveConnectionsPerWorker(int32_t num) {
    maxNumActiveConnectionsPerWorker_ = num;
  }

  /**
   * Get the speed of adjusting connection accept rate.
   */
  double getAcceptRateAdjustSpeed() const {
    return acceptRateAdjustSpeed_;
  }

  /**
   * Set the speed of adjusting connection accept rate.
   */
  void setAcceptRateAdjustSpeed(double speed) {
    acceptRateAdjustSpeed_ = speed;
  }

  /**
   * Get the number of connections dropped by the TAsyncServerSocket
   */
  uint64_t getNumDroppedConnections() const;

  /** Reset the maximum number of inactive connection objects to the default.
  */
  void resetMaxConnectionPoolSize() {
    setMaxConnectionPoolSize(T_ASYNC_MAX_CONNECTION_POOL_SIZE);
  }

  /** Get maximum number of milliseconds we'll wait for data (0 = infinity).
   *
   *  @return number of milliseconds, or 0 if no timeout set.
   */
  int getRecvTimeout() const {
    return timeout_;
  }

  /** Set maximum number of milliseconds we'll wait for data (0 = infinity).
   *  Note: existing connections are unaffected by this call.
   *
   *  @param timeout number of milliseconds, or 0 to disable timeouts.
   */
  void setRecvTimeout(int timeout) {
    timeout_ = timeout;
  }

  /** Set the maximum frame size server will accept.
   *
   * @param size the maximum size in bytes of a frame we'll accept.
   */
  void setMaxFrameSize(uint32_t size) {
    maxFrameSize_ = size;
  }

  /** Get the maximum frame size server will accept.
   *
   *  @return the maximum pool size.
   */
  uint32_t getMaxFrameSize() const {
    return maxFrameSize_;
  }

  /**
   * Get the starting size of a TEventConnection object's read buffer.
   *
   * @return # bytes we init a TEventConnection object's read buffer to.
   */
  size_t getReadBufferDefaultSize() const {
    return readBufferDefaultSize_;
  }

  /**
   * Set the starting size of a TEventConnection object's read buffer.
   *
   * @param size # bytes we init a TEventConnection object's read buffer to.
   */
  void setReadBufferDefaultSize(size_t size) {
    readBufferDefaultSize_ = size;
  }

  /**
   * Get the starting size of a TEventConnection object's write buffer.
   *
   * @return # bytes we init a TEventConnection object's write buffer to.
   */
  size_t getWriteBufferDefaultSize() const {
    return writeBufferDefaultSize_;
  }

  /**
   * Set the starting size of a TEventConnection object's write buffer.
   *
   * @param size # bytes we init a TEventConnection object's write buffer to.
   */
  void setWriteBufferDefaultSize(size_t size) {
    writeBufferDefaultSize_ = size;
  }

  /**
   * Get the maximum size of read buffer allocated to idle TConnection objects.
   *
   * @return # bytes beyond which we will shrink buffers when idle.
   */
  size_t getIdleReadBufferLimit() const {
    return idleReadBufferLimit_;
  }

  /**
   * Set the maximum size read buffer allocated to idle TEventConnection
   * objects.  If a TEventConnection object is found (either on connection
   * close or between calls when resizeBufferEveryN_ is set) with more than
   * this much memory
   * allocated to its read buffer, we shrink it to this value.
   *
   * @param limit of bytes beyond which we will shrink buffers when checked.
   */
  void setIdleReadBufferLimit(size_t limit) {
    idleReadBufferLimit_ = limit;
  }

  /**
   * Get the maximum size of write buffer allocated to idle TEventConnection
   * objects.
   *
   * @return # bytes beyond which we will reallocate buffers when checked.
   */
  size_t getIdleWriteBufferLimit() const {
    return idleWriteBufferLimit_;
  }

  /**
   * Set the maximum size write buffer allocated to idle TEventConnection
   * objects.  If a TEventConnection object is found (either on connection
   * close or between calls when resizeBufferEveryN_ is set) with more than
   * this much memory allocated to its write buffer, we destroy and construct
   * that buffer.
   *
   * @param limit of bytes beyond which we will shrink buffers when idle.
   */
  void setIdleWriteBufferLimit(size_t limit) {
    idleWriteBufferLimit_ = limit;
  }

  /**
   * Get # of calls made between buffer size checks.  0 means disabled.
   *
   * @return # of calls between buffer size checks.
   */
  int32_t getResizeBufferEveryN() const {
    return resizeBufferEveryN_;
  }

  /**
   * Check buffer sizes every "count" calls.  This allows buffer limits
   * to be enforced for persistant connections with a controllable degree
   * of overhead. 0 disables checks except at connection close.
   *
   * @param count the number of calls between checks, or 0 to disable
   */
  void setResizeBufferEveryN(int32_t count) {
    resizeBufferEveryN_ = count;
  }

  /**
   * Set a call timeout in milliseconds.
   *
   * When a worker's TEventBase starts taking longer than this amount of time
   * to process a single loop, start dropping connections to reduce loadj
   *
   * TODO: This should be renamed something other than "call timeout"
   *
   * @param milliseconds the call timeout (0 inhibits)
   */
  void setCallTimeout(int32_t milliseconds) {
    callTimeout_ = milliseconds;
  }

  /**
   * Get the call timeout in milliseconds.  0 (default) disables.
   *
   * @return the call timeout in milliseconds
   */
  int32_t getCallTimeout() const {
    return callTimeout_;
  }

  /**
   * Set Thread Manager (for queuing mode).
   *
   * @param threadManager a shared pointer to the thread manager
   */
  void setThreadManager(boost::shared_ptr<concurrency::ThreadManager>
                        threadManager) {
    threadManager_ = threadManager;
  }

  /**
   * Get Thread Manager (for queuing mode).
   *
   * @return a shared pointer to the thread manager
   */
  boost::shared_ptr<concurrency::ThreadManager> getThreadManager() {
    return threadManager_;
  }

  /**
   * Get the task expire time (for queuing mode).
   *
   * @return task expire time
   */
  int64_t getTaskExpireTime() const {
    return taskExpireTime_;
  }

  /**
   * Return whether we are in queuing mode or not.
   *
   * @return true if we are in queuing mode, false if not.
   */
  bool queuingMode() const {
    return queuingMode_;
  }

  /**
   * Set the transport type to use
   *
   * @param transportType transport type
   */
  void setTransportType(TransportType transportType) {

    /*********** Deprecation Warning *******************
     *                                                 *
     *    The unframed transports are deprecated !     *
     * They should be used for legancy services only   *
     * Also note: they only works with TBinaryProtocol *
     ***************************************************/

    if (transportType == UNFRAMED_BINARY &&
        !dynamic_cast<apache::thrift::protocol::TBinaryProtocolFactoryBase*>(
          getDuplexProtocolFactory()->getInputProtocolFactory().get())) {
      throw TLibraryException(
        "UnFramedTransport can only be used with TBinaryProtocol");
    } else if (transportType == HEADER &&
        !dynamic_cast<apache::thrift::protocol::THeaderProtocolFactory*>(
          getDuplexProtocolFactory()->getInputProtocolFactory().get())) {
      throw TLibraryException(
        "HEADER transport can only be used with THeaderProtocol");
    }

    transportType_ = transportType;
  }

  /**
   * Get the transport type to use
   *
   * @return transport type
   */
  TransportType getTransportType() {
    return transportType_;
  }


  /**
   * Call this to complete initialization
   */
  void setup();

  /**
   * Kill the workers and wait for listeners to quit
   */
  void cleanUp();

  /**
   * One stop solution:
   *
   * starts worker threads, enters accept loop; when
   * the accept loop exits, shuts down and joins workers.
   */
  void serve();

  /**
   * Call this to stop the server, if started by serve()
   *
   * This causes the main serve() function to stop listening for new
   * connections, closes existing connections, shut down the worker threads,
   * and then return.
   */
  void stop();

  /**
   * Call this to stop listening on the server port.
   *
   * This causes the main serve() function to stop listening for new
   * connections while still allows the worker threads to process
   * existing connections. stop() still needs to be called to clear
   * up the worker threads.
   */
  void stopListening();

  /**
   * Terminate a given pending task.  Callable by the thread manager or
   * from the server context.
   */
  void expireClose(
                boost::shared_ptr<apache::thrift::concurrency::Runnable> task);

  /**
   * In task queue mode, drop a task from the head of the queue and shut
   * down the associated connection.
   */
  bool drainPendingTask();

  /**
   * Get the TConnectionContext for the connection currently being processed.
   *
   * This is intended to be invoked from within the TAsyncProcessor (or the
   * handler used by the TProcessor).
   *
   * @return Return a pointer to the TConnectionContext for the current
   *         connection, or NULL if invoked outside of a call to
   *         TAsyncProcessor::process().  The returned TConnectionContext
   *         object is guaranteed to remain valid until the
   *         TAsyncProcessor invokes its success or error callback.  However,
   *         getConnectionContext() may return NULL when invoked after
   *         process() has returned.
   *
   *         In other words, async handlers may continue using the
   *         TConnectionContext object for the duration of the handler
   *         processing.  However, they must retrieve the TConnectionContext
   *         inside the call to process() and cache it for later use if
   *         they need it later.
   */
  virtual server::TConnectionContext* getConnectionContext() const;

  // We use this to get the processor when in task queue mode
  using TServer::getProcessor;
};

}}} // apache::thrift::async

#endif // #ifndef THRIFT_ASYNC_TEVENTSERVER_H_
