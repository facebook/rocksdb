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
#ifndef THRIFT_UTIL_TNONBLOCKINGSERVERCREATOR_H_
#define THRIFT_UTIL_TNONBLOCKINGSERVERCREATOR_H_ 1

#include "thrift/lib/cpp/util/ServerCreatorBase.h"

#include "thrift/lib/cpp/server/TNonblockingServer.h"
#include "thrift/lib/cpp/transport/TSocket.h"

namespace apache { namespace thrift {

class TProcessor;

namespace util {

class TNonblockingServerCreator : public ServerCreatorBase {
 public:
  typedef server::TNonblockingServer ServerType;

  /// Use 8 task threads by default.
  static const size_t DEFAULT_NUM_TASK_THREADS = 8;

  /// Use 1 I/O thread by default.
  static const size_t DEFAULT_NUM_IO_THREADS = 1;

  /**
   * By default, just run the I/O threads with the same priority as other
   * threads.
   */
  static const bool DEFAULT_HI_PRI_IO_THREADS = false;

  /// Default limit on the size of the idle connection pool
  static const size_t DEFAULT_CONN_STACK_LIMIT = 1024;

  /// Default limit on the total number of connected sockets
  static const size_t DEFAULT_MAX_CONNECTIONS = INT_MAX;

  /// Default limit on the number of outstanding requests
  static const size_t DEFAULT_MAX_ACTIVE_PROCESSORS = INT_MAX;

  /**
   * By default, reject requests over 256MB.
   *
   * This avoids allocating giant buffers if a client sends a bogus frame
   * length.
   */
  static const size_t DEFAULT_MAX_FRAME_SIZE = 256 * 1024 * 1024;

  /// Default overload hysteresis fraction
  static const double DEFAULT_HYSTERESIS_FRACTION;

  /**
   * Default overload action.
   *
   * TODO: None of the TNonblockingServer OverloadAction behaviors seem like
   * reasonable defaults.  If the overload is occurring because we hit the
   * maxConnections limit, it seems like the best behavior is to try and close
   * idle connections.  If the overload is occurring due to the
   * maxActiveProcessors limit, it seems like the best behavior may be to stop
   * calling accept() and stop reading on sockets until the # of active
   * processors drops to a reasonable limit.
   */
  static const server::TOverloadAction DEFAULT_OVERLOAD_ACTION =
    server::T_OVERLOAD_NO_ACTION;

  /**
   * By default, give up on a request if we can't find a thread to process it
   * within 30 seconds.
   */
  static const int64_t DEFAULT_TASK_EXPIRE_TIME = 30000;

  /// Default size of write buffer
  static const size_t DEFAULT_WRITE_BUFFER_SIZE = 1024;

  /// Maximum size of read buffer allocated to each idle connection
  static const size_t DEFAULT_IDLE_READ_BUF_LIMIT = 8192;

  /// Maximum size of write buffer allocated to each idle connection
  static const size_t DEFAULT_IDLE_WRITE_BUF_LIMIT = 8192;

  /**
   * By check to see if we should shrink oversized read/write buffers after
   * every 64 calls on connection.
   */
  static const int DEFAULT_RESIZE_EVERY_N = 64;

  /// listen backlog
  static const size_t DEFAULT_LISTEN_BACKLOG = 1024;

  /**
   * Create a new TNonblockingServerCreator.
   */
  TNonblockingServerCreator(const boost::shared_ptr<TProcessor>& processor,
                            uint16_t port,
                            size_t numTaskThreads = DEFAULT_NUM_TASK_THREADS);

  /**
   * Set the number of threads to use for processing requests.
   *
   * Setting this to 0 causes all requests to be processed in the I/O
   * thread(s).  You should only set this to 0 if the handler responds to all
   * requests immediately, without ever blocking.
   */
  void setNumTaskThreads(size_t numThreads) {
    numTaskThreads_ = numThreads;
  }

  /**
   * Set the ThreadFactory to use for creating the task threads.
   */
  void setTaskThreadFactory(
      const boost::shared_ptr<concurrency::ThreadFactory>& threadFactory) {
    taskThreadFactory_ = threadFactory;
  }

  /**
   * Set the number of threads to use for performing I/O.
   */
  void setNumIOThreads(size_t numThreads) {
    numIOThreads_ = numThreads;
  }

  /**
   * Set whether or not the I/O threads should be given a higher scheduling
   * priority.
   */
  void setUseHighPriorityIOThreads(bool useHiPri) {
    useHiPriIOThreads_ = useHiPri;
  }

  /**
   * Set the maximum number of TConnection objects to cache.
   *
   * By default, when a connection is closed TNonblockingServer caches the
   * TConnection object and re-uses it when a new connection comes in, instead
   * of deleting it and allocating a new one for the next connection.  This
   * value caps the number of unused TConnection objects that will be cached.
   */
  void setConnectionStackLimit(size_t limit) {
    connectionStackLimit_ = limit;
  }

  /// Set the maximum number of open connections
  void setMaxConnections(size_t maxConnections) {
    maxConnections_ = maxConnections;
  }

  /// Set the maximum number of active requests
  void setMaxActiveProcessors(size_t maxActiveProcessors) {
    maxActiveProcessors_ = maxActiveProcessors;
  }

  /// Set the maximum allowed request frame size.
  void setMaxFrameSize(size_t maxFrameSize) {
    maxFrameSize_ = maxFrameSize;
  }

  /// Set the overload hysteresis fraction
  void setOverloadHysteresis(double hysteresisFraction) {
    hysteresisFraction_ = hysteresisFraction;
  }

  /// Set the overload action
  void setOverloadAction(server::TOverloadAction action) {
    overloadAction_ = action;
  }

  /**
   * Set the task expiration time.
   *
   * If no task thread is available to process a request within taskExpireTime
   * milliseconds, close the connection rather than continuing to wait for a
   * thread.  If the server is overloaded, this prevents it from processing
   * old requests that the client has already given up on.
   */
  void setTaskExpireTime(int64_t taskExpireTime) {
    taskExpireTime_ = taskExpireTime;
  }

  /**
   * Set the socket options to use for accepted connections
   */
  void setSocketOptions(const transport::TSocket::Options& options) {
    socketOptions_ = options;
  }

  /// Set the default write buffer size for new connections
  void setWriteBufferDefaultSize(size_t size) {
    defaultWriteBufferSize_ = size;
  }

  /**
   * Set the maximum read buffer size for idle connections.
   *
   * This value is checked when the connection is closed, and also every N
   * connections if setResizeBufferEveryN() has been set.  If the read buffer
   * is larger than the allowed limit, free it.
   *
   * This prevents a single large request on a connection from continuing to
   * taking up a lot of buffer space.
   */
  void setIdleReadBufferLimit(size_t limit) {
    idleReadBufferLimit_ = limit;
  }

  /**
   * Set the maximum write buffer size for idle connections.
   *
   * This value is checked when the connection is closed, and also every N
   * connections if setResizeBufferEveryN() has been set.  If the write buffer
   * is larger than the allowed limit, free it.
   *
   * This prevents a single large response on a connection from continuing to
   * taking up a lot of buffer space.
   */
  void setIdleWriteBufferLimit(size_t limit) {
    idleWriteBufferLimit_ = limit;
  }

  /// Check a connection's buffer size limits every count requests.
  void setResizeBufferEveryN(int32_t count) {
    resizeBufferEveryN_ = count;
  }

  /// listen backlog
  void setListenBacklog(int32_t listenBacklog) {
    listenBacklog_ = listenBacklog;
  }

  virtual boost::shared_ptr<server::TServer> createServer();

  boost::shared_ptr<server::TNonblockingServer> createNonblockingServer();

 private:
  boost::shared_ptr<TProcessor> processor_;
  uint16_t port_;
  size_t numTaskThreads_;
  size_t numIOThreads_;
  bool useHiPriIOThreads_;
  size_t connectionStackLimit_;
  size_t maxConnections_;
  size_t maxActiveProcessors_;
  size_t maxFrameSize_;
  double hysteresisFraction_;
  server::TOverloadAction overloadAction_;
  int64_t taskExpireTime_;
  size_t defaultWriteBufferSize_;
  size_t idleReadBufferLimit_;
  size_t idleWriteBufferLimit_;
  int32_t resizeBufferEveryN_;
  int32_t listenBacklog_;
  transport::TSocket::Options socketOptions_;
  boost::shared_ptr<concurrency::ThreadFactory> taskThreadFactory_;
};

}}} // apache::thrift::util

#endif // THRIFT_UTIL_TNONBLOCKINGSERVERCREATOR_H_
