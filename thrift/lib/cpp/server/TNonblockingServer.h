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

#ifndef THRIFT_SERVER_TNONBLOCKINGSERVER_H_
#define THRIFT_SERVER_TNONBLOCKINGSERVER_H_ 1

#include "thrift/lib/cpp/Thrift.h"
#include "thrift/lib/cpp/server/TServer.h"
#include "thrift/lib/cpp/transport/TBufferTransports.h"
#include "thrift/lib/cpp/transport/TSocket.h"
#include "thrift/lib/cpp/concurrency/ThreadManager.h"
#include "thrift/lib/cpp/concurrency/Thread.h"
#include "thrift/lib/cpp/concurrency/PosixThreadFactory.h"
#include "thrift/lib/cpp/concurrency/Mutex.h"
#include "thrift/lib/cpp/concurrency/ThreadLocal.h"
#include <stack>
#include <vector>
#include <string>
#include <errno.h>
#include <cstdlib>
#include <unistd.h>
#include <limits.h>
#include <event.h>  // libevent
#include <tr1/unordered_set>


namespace apache { namespace thrift { namespace server {

using apache::thrift::transport::TMemoryBuffer;
using apache::thrift::transport::TSocket;
using apache::thrift::protocol::TProtocol;
using apache::thrift::concurrency::Runnable;
using apache::thrift::concurrency::ThreadManager;
using apache::thrift::concurrency::PosixThreadFactory;
using apache::thrift::concurrency::ThreadFactory;
using apache::thrift::concurrency::Thread;
using apache::thrift::concurrency::Mutex;
using apache::thrift::concurrency::Guard;

/**
 * This is a non-blocking server in C++ for high performance that
 * operates a set of IO threads (by default only one). It assumes that
 * all incoming requests are framed with a 4 byte length indicator and
 * writes out responses using the same framing.
 *
 * It does not use the TServerTransport framework, but rather has socket
 * operations hardcoded for use with select.
 *
 */


/// Overload condition actions.
enum TOverloadAction {
  T_OVERLOAD_NO_ACTION,        ///< Don't handle overload */
  T_OVERLOAD_CLOSE_ON_ACCEPT,  ///< Drop new connections immediately */
  T_OVERLOAD_DRAIN_TASK_QUEUE, ///< Drop some tasks from head of task queue */
  T_OVERLOAD_PAUSE_ACCEPTING   ///< do not accept any new connections
};

class TNonblockingIOThread;
class TNonblockingServerObserver;

class TNonblockingServer : public TServer {
 private:
  class TConnection;

  friend class TNonblockingIOThread;
 private:
  /// Listen backlog
  static const int LISTEN_BACKLOG = 1024;

  /// Default limit on size of idle connection pool
  static const int CONNECTION_STACK_LIMIT = -1;

  /// Default limit on frame size
  static const int MAX_FRAME_SIZE = 256 * 1024 * 1024;

  /// Default limit on total number of connected sockets
  static const int MAX_CONNECTIONS = INT_MAX;

  /// Default limit on connections in handler/task processing
  static const int MAX_ACTIVE_PROCESSORS = INT_MAX;

  /// Default limit on the number of active requests on the server.
  static const int MAX_ACTIVE_REQUESTS = INT_MAX;

  /// Default limit on memory usage
  static const uint64_t MAX_MEMORY_USAGE_BYTES = ULONG_MAX;

  /// Default size of write buffer
  static const int WRITE_BUFFER_DEFAULT_SIZE = 1024;

  /// Maximum size of read buffer allocated to idle connection (0 = unlimited)
  static const int IDLE_READ_BUFFER_LIMIT = 8192;

  /// Maximum size of write buffer allocated to idle connection (0 = unlimited)
  static const int IDLE_WRITE_BUFFER_LIMIT = 0;

  /// # of calls before resizing oversized buffers (0 = check only on close)
  static const int RESIZE_BUFFER_EVERY_N = 0;

  /// # of IO threads to use by default
  static const int DEFAULT_IO_THREADS = 1;

  /// File descriptor of an invalid socket
  static const int INVALID_SOCKET = -1;

  /// # of IO threads this server will use
  size_t numIOThreads_;

  /// Whether to set high scheduling priority for IO threads
  bool useHighPriorityIOThreads_;

  /// Server socket file descriptor
  int serverSocket_;

  /// Port server runs on
  int port_;

  /// For processing via thread pool, may be NULL
  boost::shared_ptr<ThreadManager> threadManager_;

  /// Is thread pool processing?
  bool threadPoolProcessing_;

  // Factory to create the IO threads
  boost::shared_ptr<PosixThreadFactory> ioThreadFactory_;

  // Vector of IOThread objects that will handle our IO
  std::vector<boost::shared_ptr<TNonblockingIOThread> > ioThreads_;

  // Index of next IO Thread to be used (for round-robin)
  int nextIOThread_;

  // Synchronizes access to connection stack and similar data, such as
  // numActiveRequests_.
  Mutex connMutex_;

  /// Number of TConnection object we've created
  size_t numTConnections_;

  /// Number of Connections processing or waiting to process
  size_t numActiveProcessors_;

  /// Number of active/outstanding requests on the server. I.e., sum of requests
  /// that are being processed, waiting to be processed and are waiting for the
  /// reply to be sent out over the wire.
  size_t numActiveRequests_;

  /// Limit for the number of requests that are being processed, waiting to be
  /// processed, or are waiting for the reply to be sent out over the wire.
  size_t maxActiveRequests_;

  /// listen backlog
  int32_t listenBacklog_;

  /// Limit for how many TConnection objects to cache
  /// 0 = infinite, -1 = none.
  size_t connectionStackLimit_;

  /// Limit for number of connections processing or waiting to process
  size_t maxActiveProcessors_;

  /// Limit for number of open connections
  size_t maxConnections_;

  /// Limit for memory usage
  uint64_t maxMemoryUsageBytes_;

  /// Limit for frame size
  size_t maxFrameSize_;

  /// Time in milliseconds before an unperformed task expires (0 == infinite).
  int64_t taskExpireTime_;

  /// Time in milliseconds to pause accepts for OVERLOAD_PAUSE_ACCEPTING
  int pauseAcceptDuration_;

  /**
   * Hysteresis for overload state.  This is the fraction of the overload
   * value that needs to be reached before the overload state is cleared;
   * must be <= 1.0.
   */
  double overloadHysteresis_;

  /// Action to take when we're overloaded.
  TOverloadAction overloadAction_;

  /**
   * The write buffer is initialized (and when idleWriteBufferLimit_ is checked
   * and found to be exceeded, reinitialized) to this size.
   */
  size_t writeBufferDefaultSize_;

  /**
   * Max read buffer size for an idle TConnection.  When we place an idle
   * TConnection into connectionStack_ or on every resizeBufferEveryN_ calls,
   * we will free the buffer (such that it will be reinitialized by the next
   * received frame) if it has exceeded this limit.  0 disables this check.
   */
  size_t idleReadBufferLimit_;

  /**
   * Max write buffer size for an idle connection.  When we place an idle
   * TConnection into connectionStack_ or on every resizeBufferEveryN_ calls,
   * we insure that its write buffer is <= to this size; otherwise we
   * replace it with a new one of writeBufferDefaultSize_ bytes to insure that
   * idle connections don't hog memory. 0 disables this check.
   */
  size_t idleWriteBufferLimit_;

  /**
   * Every N calls we check the buffer size limits on a connected TConnection.
   * 0 disables (i.e. the checks are only done when a connection closes).
   */
  int32_t resizeBufferEveryN_;

  /// Set if we are currently in an overloaded state.
  bool overloaded_;

  /// Count of connections dropped since overload started
  uint32_t nConnectionsDropped_;

  /// Count of connections dropped on overload since server started
  uint64_t nTotalConnectionsDropped_;

  // Notification of various server events
  boost::shared_ptr<TNonblockingServerObserver> observer_;

  /**
   * This is a stack of all the objects that have been created but that
   * are NOT currently in use. When we close a connection, we place it on this
   * stack so that the object can be reused later, rather than freeing the
   * memory and reallocating a new object later.
   */
  std::stack<TConnection*> connectionStack_;

  /**
   * This contains all existing TConnections, including the ones in the
   * connectionStack_.
   */
  std::tr1::unordered_set<TConnection*>  connectionSet_;

  /**
   * Struct to help set socket options for
   * TNonblockingServer::TConnection->TSocket sockets
   */
  TSocket::Options sockOptHelper_;

  /**
   * Thread-local data storage to track the current connection being processed.
   */
  typedef concurrency::ThreadLocal<
      TConnection,
      concurrency::NoopThreadLocalManager<TConnection> >
    ThreadLocalConnection;
  ThreadLocalConnection currentConnection_;

  /**
   * Called when server socket had something happen.  We accept all waiting
   * client connections on listen socket fd and assign TConnection objects
   * to handle those requests.
   *
   * @param fd the listen socket.
   * @param which the event flag that triggered the handler.
   */
  void handleEvent(int fd, short which);

  void init(int port) {
    serverSocket_ = -1;
    numIOThreads_ = DEFAULT_IO_THREADS;
    nextIOThread_ = 0;
    useHighPriorityIOThreads_ = false;
    port_ = port;
    threadPoolProcessing_ = false;
    numTConnections_ = 0;
    numActiveProcessors_ = 0;
    numActiveRequests_ = 0;
    connectionStackLimit_ = CONNECTION_STACK_LIMIT;
    maxActiveProcessors_ = MAX_ACTIVE_PROCESSORS;
    maxActiveRequests_ = MAX_ACTIVE_REQUESTS;
    maxConnections_ = MAX_CONNECTIONS;
    maxMemoryUsageBytes_ = MAX_MEMORY_USAGE_BYTES;
    maxFrameSize_ = MAX_FRAME_SIZE;
    taskExpireTime_ = 0;
    pauseAcceptDuration_ = 1;
    overloadHysteresis_ = 0.8;
    overloadAction_ = T_OVERLOAD_NO_ACTION;
    writeBufferDefaultSize_ = WRITE_BUFFER_DEFAULT_SIZE;
    idleReadBufferLimit_ = IDLE_READ_BUFFER_LIMIT;
    idleWriteBufferLimit_ = IDLE_WRITE_BUFFER_LIMIT;
    resizeBufferEveryN_ = RESIZE_BUFFER_EVERY_N;
    overloaded_ = false;
    nConnectionsDropped_ = 0;
    nTotalConnectionsDropped_ = 0;
    listenBacklog_ = LISTEN_BACKLOG;

  }

  size_t getWriteBufferBytesImpl() const;
  size_t getReadBufferBytesImpl() const;

 protected:

  /// Increment the count of connections currently processing.
  void incrementActiveProcessors() {
    Guard g(connMutex_);
    ++numActiveProcessors_;
  }

  /// Decrement the count of connections currently processing.
  void decrementActiveProcessors() {
    Guard g(connMutex_);
    if (numActiveProcessors_ > 0) {
      --numActiveProcessors_;
    }
  }

  /// Increment the number of active requests on the server.
  void incrementNumActiveRequests() {
    Guard g(connMutex_);
    ++numActiveRequests_;
  }

  /// Decrement the number of active requests on the server.
  void decrementNumActiveRequests() {
    Guard g(connMutex_);
    if (numActiveRequests_ > 0) {
      --numActiveRequests_;
    }
  }

 public:
  template<typename ProcessorFactory>
  TNonblockingServer(
      const boost::shared_ptr<ProcessorFactory>& processorFactory,
      int port,
      THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory)) :
    TServer(processorFactory) {
    init(port);
  }

  template<typename Processor>
  TNonblockingServer(const boost::shared_ptr<Processor>& processor,
                     int port,
                     THRIFT_OVERLOAD_IF(Processor, TProcessor)) :
    TServer(processor) {
    init(port);
  }

  template<typename ProcessorFactory>
  TNonblockingServer(
      const boost::shared_ptr<ProcessorFactory>& processorFactory,
      const boost::shared_ptr<TProtocolFactory>& protocolFactory,
      int port,
      const boost::shared_ptr<ThreadManager>& threadManager =
        boost::shared_ptr<ThreadManager>(),
      THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory)) :
    TServer(processorFactory) {

    init(port);

    setProtocolFactory(protocolFactory);
    setThreadManager(threadManager);
  }

  template<typename ProcessorFactory>
  TNonblockingServer(
      const boost::shared_ptr<ProcessorFactory>& processorFactory,
      const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
      int port,
      const boost::shared_ptr<ThreadManager>& threadManager =
        boost::shared_ptr<ThreadManager>(),
      THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory)) :
    TServer(processorFactory) {

    init(port);

    setDuplexProtocolFactory(duplexProtocolFactory);
    setThreadManager(threadManager);
  }

  template<typename Processor>
  TNonblockingServer(
      const boost::shared_ptr<Processor>& processor,
      const boost::shared_ptr<TProtocolFactory>& protocolFactory,
      int port,
      const boost::shared_ptr<ThreadManager>& threadManager =
        boost::shared_ptr<ThreadManager>(),
      THRIFT_OVERLOAD_IF(Processor, TProcessor)) :
    TServer(processor) {

    init(port);

    setProtocolFactory(protocolFactory);
    setThreadManager(threadManager);
  }

  template<typename Processor>
  TNonblockingServer(
      const boost::shared_ptr<Processor>& processor,
      const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
      int port,
      const boost::shared_ptr<ThreadManager>& threadManager =
        boost::shared_ptr<ThreadManager>(),
      THRIFT_OVERLOAD_IF(Processor, TProcessor)) :
    TServer(processor) {

    init(port);

    setDuplexProtocolFactory(duplexProtocolFactory);
    setThreadManager(threadManager);
  }

  template<typename ProcessorFactory>
  TNonblockingServer(
      const boost::shared_ptr<ProcessorFactory>& processorFactory,
      const boost::shared_ptr<TDuplexTransportFactory>& duplexTransportFactory,
      const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
      int port,
      const boost::shared_ptr<ThreadManager>& threadManager =
        boost::shared_ptr<ThreadManager>(),
      THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory)):
    TServer(processorFactory) {

    init(port);

    setDuplexTransportFactory(duplexTransportFactory);
    setDuplexProtocolFactory(duplexProtocolFactory);
    setThreadManager(threadManager);
  }

  template<typename Processor>
  TNonblockingServer(
      const boost::shared_ptr<Processor>& processor,
      const boost::shared_ptr<TDuplexTransportFactory>& duplexTransportFactory,
      const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
      int port,
      const boost::shared_ptr<ThreadManager>& threadManager =
        boost::shared_ptr<ThreadManager>(),
      THRIFT_OVERLOAD_IF(Processor, TProcessor)):
    TServer(processor) {

    init(port);

    setDuplexTransportFactory(duplexTransportFactory);
    setDuplexProtocolFactory(duplexProtocolFactory);
    setThreadManager(threadManager);
  }

  void setThreadManager(boost::shared_ptr<ThreadManager> threadManager);

  boost::shared_ptr<ThreadManager> getThreadManager() {
    return threadManager_;
  }

  /**
   * Sets the number of IO threads used by this server. Can only be used before
   * the call to serve() and has no effect afterwards.  We always use a
   * PosixThreadFactory for the IO worker threads, because they must joinable
   * for clean shutdown.
   */
  void setNumIOThreads(size_t numThreads) {
    numIOThreads_ = numThreads;
  }

  /** Return whether the IO threads will get high scheduling priority */
  bool useHighPriorityIOThreads() const {
    return useHighPriorityIOThreads_;
  }

  /** Set whether the IO threads will get high scheduling priority. */
  void setUseHighPriorityIOThreads(bool val) {
    useHighPriorityIOThreads_ = val;
  }

  /** Return the number of IO threads used by this server. */
  size_t getNumIOThreads() const {
    return numIOThreads_;
  }

  /**
   * Get the maximum number of unused TConnection we will hold in reserve.
   *
   * @return the current limit on TConnection pool size.
   */
  size_t getConnectionStackLimit() const {
    return connectionStackLimit_;
  }

  /**
   * Set the maximum number of unused TConnection we will hold in reserve.
   *
   * @param sz the new limit for TConnection pool size.
   */
  void setConnectionStackLimit(size_t sz) {
    connectionStackLimit_ = sz;
  }

  bool isThreadPoolProcessing() const {
    return threadPoolProcessing_;
  }

  /**
    * set the listen backlog
    *
    * @param listenBacklog the new listen backlog
    */
  void setListenBacklog(int32_t listenBacklog) {
    listenBacklog_ = listenBacklog;
  }

  /**
   * Return the count of sockets currently connected to.
   *
   * @return count of connected sockets.
   */
  size_t getNumConnections() const {
    return numTConnections_;
  }

  /**
   * Return the count of sockets currently connected to.
   *
   * @return count of connected sockets.
   */
  size_t getNumActiveConnections() const {
    return getNumConnections() - getNumIdleConnections();
  }

  /**
   * Return the bytes in all connection's write buffer.
   *
   * @return bytes
   */
  size_t getWriteBufferBytes() const {
    concurrency::Guard g(connMutex_);
    return getWriteBufferBytesImpl();
  }

  /**
   * Return the bytes in all connection's read buffer.
   *
   * @return bytes
   */
  size_t getReadBufferBytes() const {
    concurrency::Guard g(connMutex_);
    return getReadBufferBytesImpl();
  }

  /**
   * Return the count of connection objects allocated but not in use.
   *
   * @return count of idle connection objects.
   */
  size_t getNumIdleConnections() const {
    return connectionStack_.size();
  }

  /**
   * Return count of number of connections which are currently processing.
   * This is defined as a connection where all data has been received and
   * either assigned a task (when threading) or passed to a handler (when
   * not threading), and where the handler has not yet returned.
   *
   * @return # of connections currently processing.
   */
  size_t getNumActiveProcessors() const {
    return numActiveProcessors_;
  }

  /**
   * Return the number of active/outstanding requests on the server. This
   * includes requests that are processing, waiting to be processed and have
   * been processed, but are waiting for the reply to be sent out over the wire.
   *
   * @return # of currently active requests.
   */
  size_t getNumActiveRequests() const {
    return numActiveRequests_;
  }

  /**
   * Get the maximum number of active requests.
   *
   * @return current setting.
   */
  size_t getMaxActiveRequests() const {
    return maxActiveRequests_;
  }

  /**
   * Set the maximum # of active requests allowed before overload.
   *
   * @param maxActiveRequsts new setting for maximum # of active requests.
   */
  void setMaxActiveRequests(size_t maxActiveRequests) {
    maxActiveRequests_ = maxActiveRequests;
  }

  /**
   * Get the maximum # of connections allowed before overload.
   *
   * @return current setting.
   */
  size_t getMaxConnections() const {
    return maxConnections_;
  }

  /**
   * Set the maximum # of connections allowed before overload.
   *
   * @param maxConnections new setting for maximum # of connections.
   */
  void setMaxConnections(size_t maxConnections) {
    maxConnections_ = maxConnections;
  }

  /**
   * Get the maximum # of connections waiting in handler/task before overload.
   *
   * @return current setting.
   */
  size_t getMaxActiveProcessors() const {
    return maxActiveProcessors_;
  }

  /**
   * Set the maximum # of connections waiting in handler/task before overload.
   *
   * @param maxActiveProcessors new setting for maximum # of active processes.
   */
  void setMaxActiveProcessors(size_t maxActiveProcessors) {
    maxActiveProcessors_ = maxActiveProcessors;
  }

 /**
   * Get the maximum memory usage allowed before overload.
   *
   * @return current setting.
   */
  uint64_t getMaxMemoryUsageBytes() const {
    return maxMemoryUsageBytes_;
  }

  /**
   * Set the maximum memory usage allowed before overload.
   *
   * @param maxMemoryUsage new setting for maximum memory usage.
   */
  void setMaxMemoryUsageBytes(uint64_t maxMemoryUsageBytes) {
    maxMemoryUsageBytes_ = maxMemoryUsageBytes;
  }

  /**
   * Get the maximum allowed frame size.
   *
   * If a client tries to send a message larger than this limit,
   * its connection will be closed.
   *
   * @return Maxium frame size, in bytes.
   */
  size_t getMaxFrameSize() const {
    return maxFrameSize_;
  }

  /**
   * Set the maximum allowed frame size, up to 2^31-1
   *
   * @param maxFrameSize The new maximum frame size.
   */
  void setMaxFrameSize(size_t maxFrameSize) {
    if (maxFrameSize > 0x7fffffff) {
      maxFrameSize = 0x7fffffff;
    }
    maxFrameSize_ = maxFrameSize;
  }

  /**
   * Get fraction of maximum limits before an overload condition is cleared.
   *
   * @return hysteresis fraction
   */
  double getOverloadHysteresis() const {
    return overloadHysteresis_;
  }

  /**
   * Set fraction of maximum limits before an overload condition is cleared.
   * A good value would probably be between 0.5 and 0.9.
   *
   * @param hysteresisFraction fraction <= 1.0.
   */
  void setOverloadHysteresis(double hysteresisFraction) {
    if (hysteresisFraction <= 1.0 && hysteresisFraction > 0.0) {
      overloadHysteresis_ = hysteresisFraction;
    }
  }

  /**
   * Get duration of stopping accepts when overload
   *
   * @return pauseAcceptDuration_
   */
  int getPauseAcceptDuration() const {
    return pauseAcceptDuration_;
  }

  /**
   * Set the accept pause duration (when overload) in ms
   *
   * @param pauseDuration > 0
   */
  void setPauseAcceptDuration(int pauseDuration) {
    if (pauseDuration > 0) {
      pauseAcceptDuration_ = pauseDuration;
    }
  }

  /**
   * Get the action the server will take on overload.
   *
   * @return a TOverloadAction enum value for the currently set action.
   */
  TOverloadAction getOverloadAction() const {
    return overloadAction_;
  }

  /**
   * Set the action the server is to take on overload.
   *
   * @param overloadAction a TOverloadAction enum value for the action.
   */
  void setOverloadAction(TOverloadAction overloadAction) {
    overloadAction_ = overloadAction;
  }

  /**
   * Get the time in milliseconds after which a task expires (0 == infinite).
   *
   * @return a 64-bit time in milliseconds.
   */
  int64_t getTaskExpireTime() const {
    return taskExpireTime_;
  }

  /**
   * Set the time in milliseconds after which a task expires (0 == infinite).
   *
   * @param taskExpireTime a 64-bit time in milliseconds.
   */
  void setTaskExpireTime(int64_t taskExpireTime) {
    taskExpireTime_ = taskExpireTime;
  }

  /**
   * Determine if the server is currently overloaded.
   * This function checks the maximums for open connections and connections
   * currently in processing, and sets an overload condition if they are
   * exceeded.  The overload will persist until both values are below the
   * current hysteresis fraction of their maximums.
   *
   * @return true if an overload condition exists, false if not.
   */
  bool serverOverloaded();

  /**
   * Check if the server is overloaded and overloadAction_ is
   * T_OVERLOAD_PAUSE_ACCEPTING, and if so, pause the accept on the listening
   * socket
   *
   * @return true if overloadAction_ != OVERLOAD_PAUSE_ACCEPTING, or
   * if overload condition doesn't exist.
   */
  bool canContinueToAccept();

  /**
   * Check if the server is overloaded, and if so, take the necessary action.
   *
   * @param conn  The connection currently being processed that is triggering
   *     the overload check.
   *
   * Returns true if the current connection has been closed due to overload
   * processing, and false otherwise.  (Note that a false return value does not
   * mean that the server is not overloaded.)
   */
  bool checkForOverload(TConnection* conn);


  /** Pop and discard next task on threadpool wait queue.
   *
   * @return true if a task was discarded, false if the wait queue was empty.
   */
  bool drainPendingTask();

  /**
   * Get the starting size of a TConnection object's write buffer.
   *
   * @return # bytes we initialize a TConnection object's write buffer to.
   */
  size_t getWriteBufferDefaultSize() const {
    return writeBufferDefaultSize_;
  }

  /**
   * Set the starting size of a TConnection object's write buffer.
   *
   * @param size # bytes we initialize a TConnection object's write buffer to.
   */
  void setWriteBufferDefaultSize(size_t size) {
    writeBufferDefaultSize_ = size;
  }

  /**
   * Get the maximum size of read buffer allocated to idle TConnection objects.
   *
   * @return # bytes beyond which we will dealloc idle buffer.
   */
  size_t getIdleReadBufferLimit() const {
    return idleReadBufferLimit_;
  }

  /**
   * [NOTE: This is for backwards compatibility, use getIdleReadBufferLimit().]
   * Get the maximum size of read buffer allocated to idle TConnection objects.
   *
   * @return # bytes beyond which we will dealloc idle buffer.
   */
  size_t getIdleBufferMemLimit() const {
    return idleReadBufferLimit_;
  }

  /**
   * Set the maximum size read buffer allocated to idle TConnection objects.
   * If a TConnection object is found (either on connection close or between
   * calls when resizeBufferEveryN_ is set) with more than this much memory
   * allocated to its read buffer, we free it and allow it to be reinitialized
   * on the next received frame.
   *
   * @param limit of bytes beyond which we will shrink buffers when checked.
   */
  void setIdleReadBufferLimit(size_t limit) {
    idleReadBufferLimit_ = limit;
  }

  /**
   * [NOTE: This is for backwards compatibility, use setIdleReadBufferLimit().]
   * Set the maximum size read buffer allocated to idle TConnection objects.
   * If a TConnection object is found (either on connection close or between
   * calls when resizeBufferEveryN_ is set) with more than this much memory
   * allocated to its read buffer, we free it and allow it to be reinitialized
   * on the next received frame.
   *
   * @param limit of bytes beyond which we will shrink buffers when checked.
   */
  void setIdleBufferMemLimit(size_t limit) {
    idleReadBufferLimit_ = limit;
  }



  /**
   * Get the maximum size of write buffer allocated to idle TConnection objects.
   *
   * @return # bytes beyond which we will reallocate buffers when checked.
   */
  size_t getIdleWriteBufferLimit() const {
    return idleWriteBufferLimit_;
  }

  /**
   * Set the maximum size write buffer allocated to idle TConnection objects.
   * If a TConnection object is found (either on connection close or between
   * calls when resizeBufferEveryN_ is set) with more than this much memory
   * allocated to its write buffer, we destroy and construct that buffer with
   * writeBufferDefaultSize_ bytes.
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
   * Sets the sockOptHelper_
   * Socket options on underlying TSockets are set by passing
   * the sockOptHelper_ to TSocket::setSocketOptions() function
   * in the TConnection constructor
   */
  void setSocketOptions(const TSocket::Options& oh) {
    sockOptHelper_ = oh;
  }

  void setObserver(
               const boost::shared_ptr<TNonblockingServerObserver>& observer) {
    observer_ = observer;
  }

  /**
   * Main workhorse function, starts up the server listening on a port and
   * loops over the libevent handler.
   */
  void serve();

  /**
   * Causes the server to terminate gracefully (can be called from any thread).
   */
  void stop();

  TConnectionContext* getConnectionContext() const;

 private:
  void addTask(boost::shared_ptr<Runnable> task) {
    threadManager_->add(task, 0LL, taskExpireTime_);
  }

  void setCurrentConnection(TConnection* conn) {
    assert(currentConnection_.get() == NULL);
    currentConnection_.set(conn);
  }
  void clearCurrentConnection() {
    currentConnection_.clear();
  }

  /**
   * Callback function that the threadmanager calls when a task reaches
   * its expiration time.  It is needed to clean up the expired connection.
   *
   * @param task the runnable associated with the expired task.
   */
  void expireClose(boost::shared_ptr<Runnable> task);

  const boost::shared_ptr<TNonblockingServerObserver>& getObserver() const {
    return observer_;
  }

  /// Creates a socket to listen on and binds it to the local port.
  void createAndListenOnSocket();

  /**
   * Takes a socket created by createAndListenOnSocket() and sets various
   * options on it to prepare for use in the server.
   *
   * @param fd descriptor of socket to be initialized/
   */
  void listenSocket(int fd);
  /**
   * Return an initialized connection object.  Creates or recovers from
   * pool a TConnection and initializes it with the provided socket FD
   * and flags.
   *
   * @param socket FD of socket associated with this connection.
   * @param addr the sockaddr of the client
   * @param addrLen the length of addr
   * @return pointer to initialized TConnection object.
   */
  TConnection* createConnection(int socket, const sockaddr* addr,
                                            socklen_t addrLen);

  /**
   * Returns a connection to pool or deletion.  If the connection pool
   * (a stack) isn't full, place the connection object on it, otherwise
   * just delete it.
   *
   * @param connection the TConection being returned.
   */
  void returnConnection(TConnection* connection);
};

class TNonblockingIOThread : public Runnable {
 public:
  // Creates an IO thread and sets up the event base.  The listenSocket should
  // be a valid FD on which listen() has already been called.  If the
  // listenSocket is < 0, accepting will not be done.
  TNonblockingIOThread(TNonblockingServer* server,
                       int number,
                       int listenSocket,
                       bool useHighPriority);

  ~TNonblockingIOThread();

  // Returns the event-base for this thread.
  event_base* getEventBase() const { return eventBase_; }

  // Returns the server for this thread.
  TNonblockingServer* getServer() const { return server_; }

  // Returns the number of this IO thread.
  int getThreadNumber() const { return number_; }

  // Returns the thread id associated with this object.  This should
  // only be called after the thread has been started.
  pthread_t getThreadId() const { return threadId_; }

  // Returns the send-fd for task complete notifications.
  int getNotificationSendFD() const { return notificationPipeFDs_[1]; }

  // Returns the read-fd for task complete notifications.
  int getNotificationRecvFD() const { return notificationPipeFDs_[0]; }

  // Returns the actual thread object associated with this IO thread.
  boost::shared_ptr<Thread> getThread() const { return thread_; }

  // Sets the actual thread object associated with this IO thread.
  void setThread(const boost::shared_ptr<Thread>& t) { thread_ = t; }

  // Used by TConnection objects to indicate processing has finished.
  bool notify(TNonblockingServer::TConnection* conn);

  // Enters the event loop and does not return until a call to stop().
  virtual void run();

  // Exits the event loop as soon as possible.
  void stop();

  // Ensures that the event-loop thread is fully finished and shut down.
  void join();

  /// Maintains a count of requests for sampling purposes
  /// request counter is maintained module the sample rate
  uint32_t incrementRequestCounter(uint32_t sampleRate);

  /// Return true if the currently running thread is this I/O thread
  bool isInIOThread() const {
    return pthread_equal(pthread_self(), threadId_);
  }

  /// Pauses accept event handling for pauseDuration milliseconds
  void pauseAcceptHandling(int pauseDuration);

 private:
  /**
   * C-callable event handler for signaling task completion.  Provides a
   * callback that libevent can understand that will read a connection
   * object's address from a pipe and call connection->transition() for
   * that object.
   *
   * @param fd the descriptor the event occurred on.
   */
  static void notifyHandler(int fd, short which, void* v);

  /**
   * C-callable event handler for listener events.  Provides a callback
   * that libevent can understand which invokes server->handleEvent().
   *
   * @param fd the descriptor the event occurred on.
   * @param which the flags associated with the event.
   * @param v void* callback arg where we placed TNonblockingServer's "this".
   */
  static void listenHandler(int fd, short which, void* v) {
    ((TNonblockingServer*)v)->handleEvent(fd, which);
  }

  /**
   * C-callable event handler to re-enable accept handling.
   * Provides a callback that libevent can understand.
   *
   * @param fd unused.
   * @param which the flags associated with the event.
   * @param v void* callback arg where we placed TNonblockingIOThread's "this".
   */
  static void reenableAcceptHandler(int fd, short which, void* v) {
    ((TNonblockingIOThread*)v)->reenableAccept();
  }

  /// Re-enables accept event handling.
  void reenableAccept();

  /// Exits the loop ASAP in case of shutdown or error.
  void breakLoop(bool error);

  /// Registers the events for the notification & listen sockets
  void registerEvents();

  /// Create the pipe used to notify I/O process of task completion.
  void createNotificationPipe();

  /// Unregisters our events for notification and listen sockets.
  void cleanupEvents();

  /// Sets (or clears) high priority scheduling status for the current thread.
  void setCurrentThreadHighPriority(bool value);

 private:
  /// associated server
  TNonblockingServer* server_;

  /// thread number (for debugging).
  const int number_;

  /// The actual physical thread id.
  pthread_t threadId_;

  /// If listenSocket_ >= 0, adds an event on the event_base to accept conns
  int listenSocket_;

  /// Sets a high scheduling priority when running
  bool useHighPriority_;

  /// pointer to eventbase to be used for looping
  event_base* eventBase_;

  /// Whether the server is currently accepting connections
  bool acceptingConnections_;

  /// Used with eventBase_ for connection events (only in listener thread)
  struct event serverEvent_;

  /// Used with eventBase_ for backing off on accept handling until there are
  /// available fds
  struct event acceptBackoffEvent_;

  /// Used with eventBase_ for task completion notification
  struct event notificationEvent_;

 /// File descriptors for pipe used for task completion notification.
  int notificationPipeFDs_[2];

  /// Actual IO Thread
  boost::shared_ptr<Thread> thread_;

  /// Call counter
  uint32_t requestCounter_;
};


}}} // apache::thrift::server

#endif // #ifndef THRIFT_SERVER_TNONBLOCKINGSERVER_H_
