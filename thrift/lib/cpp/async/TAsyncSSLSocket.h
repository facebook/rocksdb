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
#ifndef THRIFT_ASYNC_TASYNCSSLSOCKET_H_
#define THRIFT_ASYNC_TASYNCSSLSOCKET_H_ 1

#include "thrift/lib/cpp/async/TAsyncSocket.h"
#include "thrift/lib/cpp/async/TAsyncTimeout.h"
#include "thrift/lib/cpp/transport/TSSLSocket.h"
#include "thrift/lib/cpp/transport/TTransportException.h"

namespace apache { namespace thrift {

namespace async {

class TSSLException: public apache::thrift::transport::TTransportException {
 public:
  TSSLException(int sslError, int errno_copy);

  int getSSLError() const { return error_; }

 protected:
  int error_;
};

/**
 * A class for performing asynchronous I/O on an SSL connection.
 *
 * TAsyncSSLSocket allows users to asynchronously wait for data on an
 * SSL connection, and to asynchronously send data.
 *
 * The APIs for reading and writing are intentionally asymmetric.
 * Waiting for data to read is a persistent API: a callback is
 * installed, and is notified whenever new data is available.  It
 * continues to be notified of new events until it is uninstalled.
 *
 * TAsyncSSLSocket does not provide read timeout functionality,
 * because it typically cannot determine when the timeout should be
 * active.  Generally, a timeout should only be enabled when
 * processing is blocked waiting on data from the remote endpoint.
 * For server connections, the timeout should not be active if the
 * server is currently processing one or more outstanding requests for
 * this connection.  For client connections, the timeout should not be
 * active if there are no requests pending on the connection.
 * Additionally, if a client has multiple pending requests, it will
 * ususally want a separate timeout for each request, rather than a
 * single read timeout.
 *
 * The write API is fairly intuitive: a user can request to send a
 * block of data, and a callback will be informed once the entire
 * block has been transferred to the kernel, or on error.
 * TAsyncSSLSocket does provide a send timeout, since most callers
 * want to give up if the remote end stops responding and no further
 * progress can be made sending the data.
 */
class TAsyncSSLSocket : public TAsyncSocket {
 public:

#if THRIFT_HAVE_UNIQUE_PTR
  typedef std::unique_ptr<TAsyncSSLSocket, Destructor> UniquePtr;
#endif

  class HandshakeCallback {
   public:
    virtual ~HandshakeCallback() {}

    /**
     * handshakeSuccess() is called when a new SSL connection is
     * established, i.e., after SSL_accept/connect() returns successfully.
     *
     * The HandshakeCallback will be uninstalled before handshakeSuccess()
     * is called.
     *
     * @param sock  SSL socket on which the handshake was initiated
     */
    virtual void handshakeSuccess(TAsyncSSLSocket *sock) THRIFT_NOEXCEPT = 0;

    /**
     * handshakeError() is called if an error occurs while
     * establishing the SSL connection.
     *
     * The HandshakeCallback will be uninstalled before handshakeError()
     * is called.
     *
     * @param sock  SSL socket on which the handshake was initiated
     * @param ex  An exception representing the error.
     */
    virtual void handshakeError(
      TAsyncSSLSocket *sock,
      const apache::thrift::transport::TTransportException& ex)
      THRIFT_NOEXCEPT = 0;
  };

  class HandshakeTimeout : public TAsyncTimeout {
   public:
    HandshakeTimeout(TAsyncSSLSocket* sslSocket, TEventBase* eventBase)
      : TAsyncTimeout(eventBase)
      , sslSocket_(sslSocket) {}

    virtual void timeoutExpired() THRIFT_NOEXCEPT {
      sslSocket_->timeoutExpired();
    }

   private:
    TAsyncSSLSocket* sslSocket_;
  };

  /**
   * These are passed to the application via errno, so values have to be
   * outside the valid errno range
   */
  enum SSLError {
    SSL_CLIENT_RENEGOTIATION_ATTEMPT = 0x8001
  };

  /**
   * Create a client TAsyncSSLSocket
   */
  TAsyncSSLSocket(const boost::shared_ptr<transport::SSLContext> &ctx,
                  TEventBase* evb) :
      TAsyncSocket(evb),
      corked_(false),
      server_(false),
      handshakeComplete_(false),
      renegotiateAttempted_(false),
      sslState_(STATE_UNINIT),
      ctx_(ctx),
      handshakeCallback_(NULL),
      ssl_(NULL),
      sslSession_(NULL),
      handshakeTimeout_(this, evb) {
  }

  /**
   * Create a TAsyncSSLSocket from an already connected socket file descriptor.
   *
   * Note that while TAsyncSSLSocket enables TCP_NODELAY for sockets it creates
   * when connecting, it does not change the socket options when given an
   * existing file descriptor.  If callers want TCP_NODELAY enabled when using
   * this version of the constructor, they need to explicitly call
   * setNoDelay(true) after the constructor returns.
   *
   * @param ctx             SSL context for this connection.
   * @param evb EventBase that will manage this socket.
   * @param fd  File descriptor to take over (should be a connected socket).
   */
  TAsyncSSLSocket(const boost::shared_ptr<transport::
                  SSLContext>& ctx,
                  TEventBase* evb, int fd, bool server = true);

  /**
   * Helper function to create a shared_ptr<TAsyncSSLSocket>.
   */
  static boost::shared_ptr<TAsyncSSLSocket> newSocket(
    const boost::shared_ptr<transport::SSLContext>& ctx,
    TEventBase* evb, int fd, bool server=true) {
    return boost::shared_ptr<TAsyncSSLSocket>(
      new TAsyncSSLSocket(ctx, evb, fd, server),
      Destructor());
  }

  /**
   * Helper function to create a shared_ptr<TAsyncSSLSocket>.
   */
  static boost::shared_ptr<TAsyncSSLSocket> newSocket(
    const boost::shared_ptr<transport::SSLContext>& ctx,
    TEventBase* evb) {
    return boost::shared_ptr<TAsyncSSLSocket>(
      new TAsyncSSLSocket(ctx, evb),
      Destructor());
  }

  /**
   * TODO: implement support for SSL renegotiation.
   *
   * This involves proper handling of the SSL_ERROR_WANT_READ/WRITE
   * code as a result of SSL_write/read(), instead of returning an
   * error. In that case, the READ/WRITE event should be registered,
   * and a flag (e.g., writeBlockedOnRead) should be set to indiciate
   * the condition. In the next invocation of read/write callback, if
   * the flag is on, performWrite()/performRead() should be called in
   * addition to the normal call to performRead()/performWrite(), and
   * the flag should be reset.
   */

  // Inherit TAsyncTransport methods from TAsyncSocket except the
  // following.
  // See the documentation in TAsyncTransport.h
  // TODO: implement graceful shutdown in close()
  // TODO: implement detachSSL() that returns the SSL connection
  virtual void closeNow();
  virtual void shutdownWrite();
  virtual void shutdownWriteNow();
  virtual bool good() const;
  virtual bool connecting() const;

  /**
   * Accept an SSL connection on the socket.
   *
   * The callback will be invoked and uninstalled when an SSL
   * connection has been established on the underlying socket.
   *
   * @param callback callback object to invoke on success/failure
   * @param timeout timeout for this function in milliseconds, or 0 for no
   *                timeout
   */
  void sslAccept(HandshakeCallback* callback, uint32_t timeout = 0);

  /**
   * Invoke SSL accept following an asynchronous session cache lookup
   */
  void restartSSLAccept();

  /**
   * Connect to the given address, invoking callback when complete or on error
   *
   * Note timeout applies to TCP + SSL connection time
   */
  void connect(ConnectCallback* callback,
               const transport::TSocketAddress& address,
               int timeout = 0,
               const OptionList &options = emptyOptionList) THRIFT_NOEXCEPT;

  using TAsyncSocket::connect;

  /**
   * Initiate an SSL connection on the socket
   * THe callback will be invoked and uninstalled when an SSL connection
   * has been establshed on the underlying socket.
   *
   * @param callback callback object to invoke on success/failure
   * @param timeout timeout for this function in milliseconds, or 0 for no
   *                timeout
   */
  void sslConnect(HandshakeCallback *callback, uint64_t timeout = 0);

  enum SSLStateEnum {
    STATE_UNINIT,
    STATE_ACCEPTING,
    STATE_CACHE_LOOKUP,
    STATE_CONNECTING,
    STATE_ESTABLISHED,
    STATE_REMOTE_CLOSED, /// remote end closed; we can still write
    STATE_CLOSING,       ///< close() called, but waiting on writes to complete
    /// close() called with pending writes, before connect() has completed
    STATE_CONNECTING_CLOSING,
    STATE_CLOSED,
    STATE_ERROR
  };

  SSLStateEnum getSSLState() const { return sslState_;}

  /**
   * Get a handle to the negotiated SSL session.  This increments the session
   * refcount and must be deallocated by the caller.
   */
  SSL_SESSION *getSSLSession();

  /**
   * Set the SSL session to be used during sslConnect.  TAsyncSSLSocket will
   * hold a reference to the session until it is destroyed or released by the
   * underlying SSL structure.
   *
   * @param takeOwnership if true, TAsyncSSLSocket will assume the caller's
   *                      reference count to session.
   */
  void setSSLSession(SSL_SESSION *session, bool takeOwnership = false);

#ifdef OPENSSL_NPN_NEGOTIATED
  /**
   * Get the name of the protocol selected by the client during
   * Next Protocol Negotiation (NPN)
   *
   * @param protoName      Name of the protocol (not guaranteed to be
   *                       null terminated); will be set to NULL if
   *                       the client did not negotiate a protocol.
   *                       Note: the TAsyncSSLSocket retains ownership
   *                       of this string.
   * @param protoNameLen   Length of the name.
   */
  void getSelectedNextProtocol(const unsigned char** protoName,
      unsigned* protoLen);
#endif // OPENSSL_NPN_NEGOTIATED

  /**
   * Determine if the session specified during setSSLSession was reused
   * or if the server rejected it and issued a new session.
   */
  bool getSSLSessionReused() const;

  /**
   * Get the negociated cipher name for this SSL connection.
   * Returns the cipher used or the constant value "NONE" when no SSL session
   * has been established.
   */
  const char *getNegotiatedCipherName() const;

  /**
   * Get the SSL version for this connection.
   * Possible return values are SSL2_VERSION, SSL3_VERSION, TLS1_VERSION,
   * with hexa representations 0x200, 0x300, 0x301,
   * or 0 if no SSL session has been established.
   */
  int getSSLVersion() const;

  /* Get the number of bytes read from the wire (including protocol
   * overhead). Returns 0 once the connection has been closed.
   */
  unsigned long getBytesRead() const {
    if (ssl_ != NULL) {
      return BIO_number_read(SSL_get_rbio(ssl_));
    }
    return 0;
  }

  /* Get the number of bytes written to the wire (including protocol
   * overhead).  Returns 0 once the connection has been closed.
   */
  unsigned long getBytesWritten() const {
    if (ssl_ != NULL) {
      return BIO_number_written(SSL_get_wbio(ssl_));
    }
    return 0;
  }

  virtual void attachEventBase(TEventBase* eventBase) {
    TAsyncSocket::attachEventBase(eventBase);
    handshakeTimeout_.attachEventBase(eventBase);
  }

  virtual void detachEventBase() {
    TAsyncSocket::detachEventBase();
    handshakeTimeout_.detachEventBase();
  }

  void timeoutExpired() THRIFT_NOEXCEPT;

 protected:

  /**
   * Protected destructor.
   *
   * Users of TAsyncSSLSocket must never delete it directly.  Instead, invoke
   * destroy() instead.  (See the documentation in TDelayedDestruction.h for
   * more details.)
   */
  ~TAsyncSSLSocket();

  // Inherit event notification methods from TAsyncSocket except
  // the following.

  void handleRead() THRIFT_NOEXCEPT;
  void handleWrite() THRIFT_NOEXCEPT;
  void handleAccept() THRIFT_NOEXCEPT;
  void handleConnect() THRIFT_NOEXCEPT;

  void invalidState(HandshakeCallback* callback);
  bool willBlock(int ret, int *errorOut) THRIFT_NOEXCEPT;

  // TAsyncSocket calls this at the wrong time for SSL
  void handleInitialReadWrite() THRIFT_NOEXCEPT {}

  ssize_t performRead(void* buf, size_t buflen);
  ssize_t performWrite(const iovec* vec, uint32_t count, bool haveMore,
                       uint32_t* countWritten, uint32_t* partialWritten);

  // Inherit error handling methods from TAsyncSocket, plus the following.
  void failHandshake(const char* fn, const transport::TTransportException& ex);

  void invokeHandshakeCallback();

  static void sslInfoCallback(const SSL *ssl, int type, int val);

  // Whether we've applied the TCP_CORK option to the socket
  bool corked_;
  // SSL related members.
  bool server_;
  // Used to prevent client-initiated renegotiation.  Note that TAsyncSSLSocket
  // doesn't fully support renegotiation, so we could just fail all attempts
  // to enforce this.  Once it is supported, we should make it an option
  // to disable client-initiated renegotiation.
  bool handshakeComplete_;
  bool renegotiateAttempted_;
  SSLStateEnum sslState_;
  boost::shared_ptr<transport::SSLContext> ctx_;
  // Callback for SSL_accept() or SSL_connect()
  HandshakeCallback* handshakeCallback_;
  SSL* ssl_;
  SSL_SESSION *sslSession_;
  HandshakeTimeout handshakeTimeout_;
};

}}} // apache::thrift::async

#endif // #ifndef THRIFT_ASYNC_TASYNCSSLSOCKET_H_
