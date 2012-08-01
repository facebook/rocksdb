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
#ifndef THRIFT_ASYNC_TASYNCSOCKET_H_
#define THRIFT_ASYNC_TASYNCSOCKET_H_ 1

#include <sys/types.h>
#include <sys/socket.h>
#include "thrift/lib/cpp/async/TAsyncTimeout.h"
#include "thrift/lib/cpp/async/TAsyncTransport.h"
#include "thrift/lib/cpp/async/TDelayedDestruction.h"
#include "thrift/lib/cpp/async/TEventHandler.h"

#include <list>
#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift { namespace async {

/**
 * A class for performing asynchronous I/O on a socket.
 *
 * TAsyncSocket allows users to asynchronously wait for data on a socket, and
 * to asynchronously send data.
 *
 * The APIs for reading and writing are intentionally asymmetric.  Waiting for
 * data to read is a persistent API: a callback is installed, and is notified
 * whenever new data is available.  It continues to be notified of new events
 * until it is uninstalled.
 *
 * TAsyncSocket does not provide read timeout functionality, because it
 * typically cannot determine when the timeout should be active.  Generally, a
 * timeout should only be enabled when processing is blocked waiting on data
 * from the remote endpoint.  For server sockets, the timeout should not be
 * active if the server is currently processing one or more outstanding
 * requests for this socket.  For client sockets, the timeout should not be
 * active if there are no requests pending on the socket.  Additionally, if a
 * client has multiple pending requests, it will ususally want a separate
 * timeout for each request, rather than a single read timeout.
 *
 * The write API is fairly intuitive: a user can request to send a block of
 * data, and a callback will be informed once the entire block has been
 * transferred to the kernel, or on error.  TAsyncSocket does provide a send
 * timeout, since most callers want to give up if the remote end stops
 * responding and no further progress can be made sending the data.
 */
class TAsyncSocket : public TAsyncTransport,
                     public TDelayedDestruction {
 public:
#if THRIFT_HAVE_UNIQUE_PTR
  typedef std::unique_ptr<TAsyncSocket, Destructor> UniquePtr;
#endif

  class ConnectCallback {
   public:
    virtual ~ConnectCallback() {}

    /**
     * connectSuccess() will be invoked when the connection has been
     * successfully established.
     */
    virtual void connectSuccess() THRIFT_NOEXCEPT = 0;

    /**
     * connectError() will be invoked if the connection attempt fails.
     *
     * @param ex        An exception describing the error that occurred.
     */
    virtual void connectError(const transport::TTransportException& ex)
      THRIFT_NOEXCEPT = 0;
  };

  /**
   * Create a new unconnected TAsyncSocket.
   *
   * connect() must later be called on this socket to establish a connection.
   */
  explicit TAsyncSocket(TEventBase* evb);

  /**
   * Create a new TAsyncSocket and begin the connection process.
   *
   * @param evb             EventBase that will manage this socket.
   * @param address         The address to connect to.
   * @param connectTimeout  Optional timeout in milliseconds for the connection
   *                        attempt.
   */
  TAsyncSocket(TEventBase* evb,
               const transport::TSocketAddress& address,
               uint32_t connectTimeout = 0);

  /**
   * Create a new TAsyncSocket and begin the connection process.
   *
   * @param evb             EventBase that will manage this socket.
   * @param ip              IP address to connect to (dotted-quad).
   * @param port            Destination port in host byte order.
   * @param connectTimeout  Optional timeout in milliseconds for the connection
   *                        attempt.
   */
  TAsyncSocket(TEventBase* evb,
               const std::string& ip,
               uint16_t port,
               uint32_t connectTimeout = 0);

  /**
   * Create a TAsyncSocket from an already connected socket file descriptor.
   *
   * Note that while TAsyncSocket enables TCP_NODELAY for sockets it creates
   * when connecting, it does not change the socket options when given an
   * existing file descriptor.  If callers want TCP_NODELAY enabled when using
   * this version of the constructor, they need to explicitly call
   * setNoDelay(true) after the constructor returns.
   *
   * @param evb EventBase that will manage this socket.
   * @param fd  File descriptor to take over (should be a connected socket).
   */
  TAsyncSocket(TEventBase* evb, int fd);

  /**
   * Helper function to create a shared_ptr<TAsyncSocket>.
   *
   * This passes in the correct destructor object, since TAsyncSocket's
   * destructor is protected and cannot be invoked directly.
   */
  static boost::shared_ptr<TAsyncSocket> newSocket(TEventBase* evb) {
    return boost::shared_ptr<TAsyncSocket>(new TAsyncSocket(evb),
                                           Destructor());
  }

  /**
   * Helper function to create a shared_ptr<TAsyncSocket>.
   */
  static boost::shared_ptr<TAsyncSocket> newSocket(
      TEventBase* evb,
      const transport::TSocketAddress& address,
      uint32_t connectTimeout = 0) {
    return boost::shared_ptr<TAsyncSocket>(
        new TAsyncSocket(evb, address, connectTimeout),
        Destructor());
  }

  /**
   * Helper function to create a shared_ptr<TAsyncSocket>.
   */
  static boost::shared_ptr<TAsyncSocket> newSocket(
      TEventBase* evb,
      const std::string& ip,
      uint16_t port,
      uint32_t connectTimeout = 0) {
    return boost::shared_ptr<TAsyncSocket>(
        new TAsyncSocket(evb, ip, port, connectTimeout),
        Destructor());
  }

  /**
   * Helper function to create a shared_ptr<TAsyncSocket>.
   */
  static boost::shared_ptr<TAsyncSocket> newSocket(TEventBase* evb, int fd) {
    return boost::shared_ptr<TAsyncSocket>(new TAsyncSocket(evb, fd),
                                           Destructor());
  }

  /**
   * Destroy the socket.
   *
   * TAsyncSocket::destroy() must be called to destroy the socket.
   * The normal destructor is private, and should not be invoked directly.
   * This prevents callers from deleting a TAsyncSocket while it is invoking a
   * callback.
   */
  virtual void destroy();

  /**
   * Get the TEventBase used by this socket.
   */
  virtual TEventBase* getEventBase() const {
    return eventBase_;
  }

  /**
   * Get the file descriptor used by the TAsyncSocket.
   */
  int getFd() const {
    return fd_;
  }

  /**
   * Extract the file descriptor from the TAsyncSocket.
   *
   * This will immediately cause any installed callbacks to be invoked with an
   * error.  The TAsyncSocket may no longer be used after the file descriptor
   * has been extracted.
   *
   * Returns the file descriptor.  The caller assumes ownership of the
   * descriptor, and it will not be closed when the TAsyncSocket is destroyed.
   */
  int detachFd();

  /**
   * Class that consists of the input parameters for setsockopt().
   *
   * The memory referenced by optval should be valid throughout the
   * life cycle of the SocketOption object.
   */
  class SocketOption {
   public:
    SocketOption(): level_(0), optname_(0), optval_(NULL), size_(0) {}

    template <class T>
      SocketOption(int level, int optname, const T* optval):
    level_(level), optname_(optname), optval_(optval), size_(sizeof(T)) {}

    int apply(int fd) const {
      return setsockopt(fd, level_, optname_, optval_, size_);
    }

   protected:
    int level_;
    int optname_;
    const void *optval_;
    size_t size_;
  };

  typedef std::list<SocketOption> OptionList;

  static OptionList emptyOptionList;

  /**
   * Initiate a connection.
   *
   * @param callback  The callback to inform when the connection attempt
   *                  completes.
   * @param address   The address to connect to.
   * @param timeout   A timeout value, in milliseconds.  If the connection
   *                  does not succeed within this period,
   *                  callback->connectError() will be invoked.
   */
  virtual void connect(ConnectCallback* callback,
               const transport::TSocketAddress& address,
               int timeout = 0,
               const OptionList &options = emptyOptionList) THRIFT_NOEXCEPT;
  void connect(ConnectCallback* callback, const std::string& ip, uint16_t port,
               int timeout = 00,
               const OptionList &options = emptyOptionList) THRIFT_NOEXCEPT;

  /**
   * Set the send timeout.
   *
   * If write requests do not make any progress for more than the specified
   * number of milliseconds, fail all pending writes and close the socket.
   *
   * If write requests are currently pending when setSendTimeout() is called,
   * the timeout interval is immediately restarted using the new value.
   *
   * (See the comments for TAsyncSocket for an explanation of why TAsyncSocket
   * provides setSendTimeout() but not setRecvTimeout().)
   *
   * @param milliseconds  The timeout duration, in milliseconds.  If 0, no
   *                      timeout will be used.
   */
  void setSendTimeout(uint32_t milliseconds);

  /**
   * Get the send timeout.
   *
   * @return Returns the current send timeout, in milliseconds.  A return value
   *         of 0 indicates that no timeout is set.
   */
  uint32_t getSendTimeout() const {
    return sendTimeout_;
  }

  /**
   * Set the maximum number of reads to execute from the underlying
   * socket each time the TEventBase detects that new ingress data is
   * available. The default is unlimited, but callers can use this method
   * to limit the amount of data read from the socket per event loop
   * iteration.
   *
   * @param maxReads  Maximum number of reads per data-available event;
   *                  a value of zero means unlimited.
   */
  void setMaxReadsPerEvent(uint16_t maxReads) {
    maxReadsPerEvent_ = maxReads;
  }

  /**
   * Get the maximum number of reads this object will execute from
   * the underlying socket each time the TEventBase detects that new
   * ingress data is available.
   *
   * @returns Maximum number of reads per data-available event; a value
   *          of zero means unlimited.
   */
  uint16_t getMaxReadsPerEvent() const {
    return maxReadsPerEvent_;
  }

  // Methods inherited from TAsyncTransport
  // See the documentation in TAsyncTransport.h
  virtual void setReadCallback(ReadCallback* callback);
  virtual ReadCallback* getReadCallback() const;

  virtual void write(WriteCallback* callback, const void* buf, size_t bytes);
  virtual void writev(WriteCallback* callback, const iovec* vec, size_t count);
  virtual void writeChain(WriteCallback* callback,
                          std::unique_ptr<folly::IOBuf>&& buf,
                          bool cork = false);

  virtual void close();
  virtual void closeNow();
  virtual void closeWithReset();
  virtual void shutdownWrite();
  virtual void shutdownWriteNow();

  virtual bool readable() const;
  virtual bool good() const;
  virtual bool error() const;
  virtual void attachEventBase(TEventBase* eventBase);
  virtual void detachEventBase();

  virtual void getLocalAddress(transport::TSocketAddress* address) const;
  virtual void getPeerAddress(transport::TSocketAddress* address) const;

  virtual bool connecting() const {
    return (state_ == STATE_CONNECTING);
  }

  // Methods controlling socket options

  /**
   * Force writes to be transmitted immediately.
   *
   * This controls the TCP_NODELAY socket option.  When enabled, TCP segments
   * are sent as soon as possible, even if it is not a full frame of data.
   * When disabled, the data may be buffered briefly to try and wait for a full
   * frame of data.
   *
   * By default, TCP_NODELAY is enabled for TAsyncSocket objects.
   *
   * This method will fail if the socket is not currently open.
   *
   * @return Returns 0 if the TCP_NODELAY flag was successfully updated,
   *         or a non-zero errno value on error.
   */
  int setNoDelay(bool noDelay);

  /*
   * Set the Flavor of Congestion Control to be used for this Socket
   * Please check '/lib/modules/<kernel>/kernel/net/ipv4' for tcp_*.ko
   * first to make sure the module is available for plugging in
   * Alternatively you can choose from net.ipv4.tcp_allowed_congestion_control
   */
  int setCongestionFlavor(const std::string &cname);

  /*
   * Forces ACKs to be sent immediately
   *
   * @return Returns 0 if the TCP_QUICKACK flag was successfully updated,
   *         or a non-zero errno value on error.
   */
  int setQuickAck(bool quickack);

  /**
   * Set the send bufsize
   */
  int setSendBufSize(size_t bufsize);

  /**
   * Set the recv bufsize
   */
  int setRecvBufSize(size_t bufsize);

  /**
   * Generic API for reading a socket option.
   *
   * @param level     same as the "level" parameter in getsockopt().
   * @param optname   same as the "optname" parameter in getsockopt().
   * @param optval    pointer to the variable in which the option value should
   *                  be returned.
   * @return          same as the return value of getsockopt().
   */
  template <typename T>
  int  getSockOpt(int level, int optname, T *optval) {
    return getsockopt(fd_, level, optname, optval, sizeof(T));
  }

  /**
   * Generic API for setting a socket option.
   *
   * @param level     same as the "level" parameter in getsockopt().
   * @param optname   same as the "optname" parameter in getsockopt().
   * @param optval    the option value to set.
   * @return          same as the return value of setsockopt().
   */
  template <typename T>
  int setSockOpt(int  level,  int  optname,  const T *optval) {
    return setsockopt(fd_, level, optname, optval, sizeof(T));
  }

 protected:
  enum ReadResultEnum {
    READ_EOF = 0,
    READ_ERROR = -1,
    READ_BLOCKING = -2,
  };

  /**
   * Protected destructor.
   *
   * Users of TAsyncSocket must never delete it directly.  Instead, invoke
   * destroy() instead.  (See the documentation in TDelayedDestruction.h for
   * more details.)
   */
  ~TAsyncSocket();

  enum StateEnum {
    STATE_UNINIT,
    STATE_CONNECTING,
    STATE_ESTABLISHED,
    STATE_CLOSED,
    STATE_ERROR
  };
  enum ShutdownFlags {
    /// shutdownWrite() called, but we are still waiting on writes to drain
    SHUT_WRITE_PENDING = 0x01,
    /// writes have been completely shut down
    SHUT_WRITE = 0x02,
    /**
     * Reads have been shutdown.
     *
     * At the moment we don't distinguish between remote read shutdown
     * (received EOF from the remote end) and local read shutdown.  We can
     * only receive EOF when a read callback is set, and we immediately inform
     * it of the EOF.  Therefore there doesn't seem to be any reason to have a
     * separate state of "received EOF but the local side may still want to
     * read".
     *
     * We also don't currently provide any API for only shutting down the read
     * side of a socket.  (This is a no-op as far as TCP is concerned, anyway.)
     */
    SHUT_READ = 0x04,
  };

  class WriteRequest;

  class WriteTimeout : public TAsyncTimeout {
   public:
    WriteTimeout(TAsyncSocket* socket, TEventBase* eventBase)
      : TAsyncTimeout(eventBase)
      , socket_(socket) {}

    virtual void timeoutExpired() THRIFT_NOEXCEPT {
      socket_->timeoutExpired();
    }

   private:
    TAsyncSocket* socket_;
  };

  class IoHandler : public TEventHandler {
   public:
    IoHandler(TAsyncSocket* socket, TEventBase* eventBase)
      : TEventHandler(eventBase, -1)
      , socket_(socket) {}
    IoHandler(TAsyncSocket* socket, TEventBase* eventBase, int fd)
      : TEventHandler(eventBase, fd)
      , socket_(socket) {}

    virtual void handlerReady(uint16_t events) THRIFT_NOEXCEPT {
      socket_->ioReady(events);
    }

   private:
    TAsyncSocket* socket_;
  };

  void init();

  // event notification methods
  void ioReady(uint16_t events) THRIFT_NOEXCEPT;
  virtual void checkForImmediateRead() THRIFT_NOEXCEPT;
  virtual void handleInitialReadWrite() THRIFT_NOEXCEPT;
  virtual void handleRead() THRIFT_NOEXCEPT;
  virtual void handleWrite() THRIFT_NOEXCEPT;
  virtual void handleConnect() THRIFT_NOEXCEPT;
  void timeoutExpired() THRIFT_NOEXCEPT;

  /**
   * Attempt to read from the socket.
   *
   * @param buf      The buffer to read data into.
   * @param buflen   The length of the buffer.
   *
   * @return Returns the number of bytes read, or READ_EOF on EOF, or
   * READ_ERROR on error, or READ_BLOCKING if the operation will
   * block.
   */
  virtual ssize_t performRead(void* buf, size_t buflen);

  /**
   * Populate an iovec array from an IOBuf and attempt to write it.
   *
   * @param callback Write completion/error callback.
   * @param vec      Target iovec array; caller retains ownership.
   * @param count    Number of IOBufs to write, beginning at start of buf.
   * @param buf      Chain of iovecs.
   * @param cork     Whether to delay the output until a subsequent
   *                 non-corked write.
   */
  void writeChainImpl(WriteCallback* callback, iovec* vec,
      size_t count, std::unique_ptr<folly::IOBuf>&& buf, bool cork);

  /**
   * Write as much data as possible to the socket without blocking,
   * and queue up any leftover data to send when the socket can
   * handle writes again.
   *
   * @param callback The callback to invoke when the write is completed.
   * @param vec      Array of buffers to write; this method will make a
   *                 copy of the vector (but not the buffers themselves)
   *                 if the write has to be completed asynchronously.
   * @param count    Number of elements in vec.
   * @param buf      The IOBuf that manages the buffers referenced by
   *                 vec, or a pointer to NULL if the buffers are not
   *                 associated with an IOBuf.  Note that ownership of
   *                 the IOBuf is transferred here; upon completion of
   *                 the write, the TAsyncSocket deletes the IOBuf.
   * @param cork     Whether to delay the write until the next non-corked
   *                 write operation. (Note: may not be supported in all
   *                 subclasses or on all platforms.)
   */
  void writeImpl(WriteCallback* callback, const iovec* vec, size_t count,
                 std::unique_ptr<folly::IOBuf>&& buf, bool cork = false);

  /**
   * Attempt to write to the socket.
   *
   * @param vec             The iovec array pointing to the buffers to write.
   * @param count           The length of the iovec array.
   * @param haveMore        This flag is inherited from TAsyncSocket but is
   *                        not handled here.
   * @param countWritten    On return, the value pointed to by this parameter
   *                          will contain the number of iovec entries that were
   *                          fully written.
   * @param partialWritten  On return, the value pointed to by this parameter
   *                          will contain the number of bytes written in the
   *                          partially written iovec entry.
   *
   * @return Returns the total number of bytes written, or -1 on error.  If no
   *     data can be written immediately, 0 is returned.
   */
  virtual ssize_t performWrite(const iovec* vec, uint32_t count,
                               bool haveMore, uint32_t* countWritten,
                               uint32_t* partialWritten);

  bool updateEventRegistration();

  /**
   * Update event registration.
   *
   * @param enable Flags of events to enable. Set it to 0 if no events
   * need to be enabled in this call.
   * @param disable Flags of events
   * to disable. Set it to 0 if no events need to be disabled in this
   * call.
   *
   * @return true iff the update is successful.
   */
  bool updateEventRegistration(uint16_t enable, uint16_t disable);

  // error handling methods
  void startFail();
  void finishFail();
  void fail(const char* fn, const transport::TTransportException& ex);
  void failConnect(const char* fn, const transport::TTransportException& ex);
  void failRead(const char* fn, const transport::TTransportException& ex);
  void failWrite(const char* fn, WriteCallback* callback, size_t bytesWritten,
                 const transport::TTransportException& ex);
  void failWrite(const char* fn, const transport::TTransportException& ex);
  void failAllWrites(const transport::TTransportException& ex);
  void invalidState(ConnectCallback* callback);
  void invalidState(ReadCallback* callback);
  void invalidState(WriteCallback* callback);

  uint8_t state_;                       ///< StateEnum describing current state
  uint8_t shutdownFlags_;               ///< Shutdown state (ShutdownFlags)
  uint16_t eventFlags_;                 ///< TEventBase::HandlerFlags settings
  int fd_;                              ///< The socket file descriptor
  uint32_t sendTimeout_;                ///< The send timeout, in milliseconds
  uint16_t maxReadsPerEvent_;           ///< Max reads per event loop iteration
  TEventBase* eventBase_;               ///< The TEventBase
  WriteTimeout writeTimeout_;           ///< A timeout for connect and write
  IoHandler ioHandler_;                 ///< A TEventHandler to monitor the fd

  ConnectCallback* connectCallback_;    ///< ConnectCallback
  ReadCallback* readCallback_;          ///< ReadCallback
  WriteRequest* writeReqHead_;          ///< Chain of WriteRequests
  WriteRequest* writeReqTail_;          ///< End of WriteRequest chain
};

}}} // apache::thrift::async

#endif // #ifndef THRIFT_ASYNC_TASYNCSOCKET_H_
