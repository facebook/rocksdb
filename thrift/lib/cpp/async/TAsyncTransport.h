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
#ifndef THRIFT_ASYNC_TASYNCTRANSPORT_H_
#define THRIFT_ASYNC_TASYNCTRANSPORT_H_ 1

#include "thrift/lib/cpp/thrift_config.h"
#include <sys/uio.h>
#include <inttypes.h>
#include <memory>

namespace folly {
class IOBuf;
}

namespace apache { namespace thrift {

namespace transport {
class TSocketAddress;
class TTransportException;
}

namespace async {

class TEventBase;

/**
 * TAsyncTransport defines an asynchronous API for streaming I/O.
 *
 * This class provides an API to for asynchronously waiting for data
 * on a streaming transport, and for asynchronously sending data.
 *
 * The APIs for reading and writing are intentionally asymmetric.  Waiting for
 * data to read is a persistent API: a callback is installed, and is notified
 * whenever new data is available.  It continues to be notified of new events
 * until it is uninstalled.
 *
 * TAsyncTransport does not provide read timeout functionality, because it
 * typically cannot determine when the timeout should be active.  Generally, a
 * timeout should only be enabled when processing is blocked waiting on data
 * from the remote endpoint.  For server-side applications, the timeout should
 * not be active if the server is currently processing one or more outstanding
 * requests on this transport.  For client-side applications, the timeout
 * should not be active if there are no requests pending on the transport.
 * Additionally, if a client has multiple pending requests, it will ususally
 * want a separate timeout for each request, rather than a single read timeout.
 *
 * The write API is fairly intuitive: a user can request to send a block of
 * data, and a callback will be informed once the entire block has been
 * transferred to the kernel, or on error.  TAsyncTransport does provide a send
 * timeout, since most callers want to give up if the remote end stops
 * responding and no further progress can be made sending the data.
 */
class TAsyncTransport {
 public:
  class ReadCallback {
   public:
    virtual ~ReadCallback() {}

    /**
     * When data becomes available, getReadBuffer() will be invoked to get the
     * buffer into which data should be read.
     *
     * This method allows the ReadCallback to delay buffer allocation until
     * data becomes available.  This allows applications to manage large
     * numbers of idle connections, without having to maintain a separate read
     * buffer for each idle connection.
     *
     * It is possible that in some cases, getReadBuffer() may be called
     * multiple times before readDataAvailable() is invoked.  In this case, the
     * data will be written to the buffer returned from the most recent call to
     * readDataAvailable().  If the previous calls to readDataAvailable()
     * returned different buffers, the ReadCallback is responsible for ensuring
     * that they are not leaked.
     *
     * If getReadBuffer() throws an exception, returns a NULL buffer, or
     * returns a 0 length, the ReadCallback will be uninstalled and its
     * readError() method will be invoked.
     *
     * getReadBuffer() is not allowed to change the transport state before it
     * returns.  (For example, it should never uninstall the read callback, or
     * set a different read callback.)
     *
     * @param bufReturn getReadBuffer() should update *bufReturn to contain the
     *                  address of the read buffer.  This parameter will never
     *                  be NULL.
     * @param lenReturn getReadBuffer() should update *lenReturn to contain the
     *                  maximum number of bytes that may be written to the read
     *                  buffer.  This parameter will never be NULL.
     */
    virtual void getReadBuffer(void** bufReturn, size_t* lenReturn) = 0;

    /**
     * readDataAvailable() will be invoked when data has been successfully read
     * into the buffer returned by the last call to getReadBuffer().
     *
     * The read callback remains installed after readDataAvailable() returns.
     * It must be explicitly uninstalled to stop receiving read events.
     * getReadBuffer() will be called at least once before each call to
     * readDataAvailable().  getReadBuffer() will also be called before any
     * call to readEOF().
     *
     * @param len       The number of bytes placed in the buffer.
     */
    virtual void readDataAvailable(size_t len) THRIFT_NOEXCEPT = 0;

    /**
     * readEOF() will be invoked when the transport is closed.
     *
     * The read callback will be automatically uninstalled immediately before
     * readEOF() is invoked.
     */
    virtual void readEOF() THRIFT_NOEXCEPT = 0;

    /**
     * readError() will be invoked if an error occurs reading from the
     * transport.
     *
     * The read callback will be automatically uninstalled immediately before
     * readError() is invoked.
     *
     * @param ex        An exception describing the error that occurred.
     */
    virtual void readError(const transport::TTransportException& ex)
      THRIFT_NOEXCEPT = 0;
  };

  class WriteCallback {
   public:
    virtual ~WriteCallback() {}

    /**
     * writeSuccess() will be invoked when all of the data has been
     * successfully written.
     *
     * Note that this mainly signals that the buffer containing the data to
     * write is no longer needed and may be freed or re-used.  It does not
     * guarantee that the data has been fully transmitted to the remote
     * endpoint.  For example, on socket-based transports, writeSuccess() only
     * indicates that the data has been given to the kernel for eventual
     * transmission.
     */
    virtual void writeSuccess() THRIFT_NOEXCEPT = 0;

    /**
     * writeError() will be invoked if an error occurs writing the data.
     *
     * @param bytesWritten      The number of bytes that were successfull
     * @param ex                An exception describing the error that occurred.
     */
    virtual void writeError(size_t bytesWritten,
                            const transport::TTransportException& ex)
      THRIFT_NOEXCEPT = 0;
  };

  virtual ~TAsyncTransport() {}

  /**
   * Set the read callback.
   *
   * See the documentation for ReadCallback above for a description of how the
   * callback will be invoked.  Note that the callback remains installed until
   * it is explicitly uninstalled, or until an error occurs.
   *
   * If a ReadCallback is already installed, it is replaced with the new
   * callback.
   *
   * @param callback    The callback to invoke when data is available.
   *                    This parameter may be NULL to uninstall the current
   *                    read callback.
   */
  virtual void setReadCallback(ReadCallback* callback) = 0;

  /**
   * Get the currently installed read callback.
   *
   * @return Returns a pointer to the installed ReadCallback, or NULL if no
   *         ReadCallback is installed.
   */
  virtual ReadCallback* getReadCallback() const = 0;

  /**
   * Write data to the transport.
   *
   * write() will always return immediately.  The WriteCallback will later be
   * invoked from the main TEventBase loop when the write has completed.
   *
   * Additional write attempts may be started before the first write completes.
   * The subsequent write requests will be queued, and processed in the order
   * in which they were called.
   *
   * @param callback    The callback to invoke when the data has been written.
   *                    The callback may not be NULL.
   * @param buf         The buffer containing the data to write.  The caller is
   *                    responsible for ensuring that this buffer remains valid
   *                    until the callback is invoked.  This parameter may not
   *                    be NULL.
   * @param bytes       The number of bytes to write.
   */
  virtual void write(WriteCallback* callback,
                     const void* buf, size_t bytes) = 0;

  /**
   * Write non-contiguous data to the transport.
   *
   * writev() will always return immediately.  The WriteCallback will later be
   * invoked from the main TEventBase loop when the write has completed.
   *
   * Additional write attempts may be started before the first write completes.
   * The subsequent write requests will be queued, and processed in the order
   * in which they were called.
   *
   * @param callback    The callback to invoke when the data has been written.
   *                    The callback may not be NULL.
   * @param vec         A pointer to an array of iovec objects.  The caller is
   *                    responsible for ensuring that the buffers remain valid
   *                    until the callback is invoked.  This parameter may not
   *                    be NULL.
   * @param count       The number of iovec objects in the vec array.
   */
  virtual void writev(WriteCallback* callback,
                      const iovec* vec, size_t count) = 0;

  /**
   * Write a chain of IOBufs to the transport.
   *
   * writeChain() will always return immediately.  The WriteCallback will
   * later be invoked from the main TEventBase loop when the write has
   * completed.
   *
   * Additional write attempts may be started before the first write completes.
   * The subsequent write requests will be queued, and processed in the order
   * in which they were called.
   *
   * @param callback    The callback to invoke when the data has been written.
   *                    The callback may not be NULL.
   * @param iob         The head of an IOBuf chain.  The TAsyncTransport
   *                    will take ownership of this chain and delete it
   *                    after writing.
   * @param cork        Whether to delay the write until the next non-corked
   *                    write operation. (Note: may not be supported in all
   *                    subclasses or on all platforms.)
   */
  virtual void writeChain(WriteCallback* callback,
                          std::unique_ptr<folly::IOBuf>&& iob,
                          bool cork = false) = 0;

  /**
   * Close the transport.
   *
   * This gracefully closes the transport, waiting for all pending write
   * requests to complete before actually closing the underlying transport.
   *
   * If a read callback is set, readEOF() will be called immediately.  If there
   * are outstanding write requests, the close will be delayed until all
   * remaining writes have completed.  No new writes may be started after
   * close() has been called.
   */
  virtual void close() = 0;

  /**
   * Close the transport immediately.
   *
   * This closes the transport immediately, dropping any outstanding data
   * waiting to be written.
   *
   * If a read callback is set, readEOF() will be called immediately.
   * If there are outstanding write requests, these requests will be aborted
   * and writeError() will be invoked immediately on all outstanding write
   * callbacks.
   */
  virtual void closeNow() = 0;

  /**
   * Reset the transport immediately.
   *
   * This closes the transport immediately, sending a reset to the remote peer
   * if possible to indicate abnormal shutdown.
   *
   * Note that not all subclasses implement this reset functionality: some
   * subclasses may treat reset() the same as closeNow().  Subclasses that use
   * TCP transports should terminate the connection with a TCP reset.
   */
  virtual void closeWithReset() {
    closeNow();
  }

  /**
   * Perform a half-shutdown of the write side of the transport.
   *
   * The caller should not make any more calls to write() or writev() after
   * shutdownWrite() is called.  Any future write attempts will fail
   * immediately.
   *
   * Not all transport types support half-shutdown.  If the underlying
   * transport does not support half-shutdown, it will fully shutdown both the
   * read and write sides of the transport.  (Fully shutting down the socket is
   * better than doing nothing at all, since the caller may rely on the
   * shutdownWrite() call to notify the other end of the connection that no
   * more data can be read.)
   *
   * If there is pending data still waiting to be written on the transport,
   * the actual shutdown will be delayed until the pending data has been
   * written.
   *
   * Note: There is no corresponding shutdownRead() equivalent.  Simply
   * uninstall the read callback if you wish to stop reading.  (On TCP sockets
   * at least, shutting down the read side of the socket is a no-op anyway.)
   */
  virtual void shutdownWrite() = 0;

  /**
   * Perform a half-shutdown of the write side of the transport.
   *
   * shutdownWriteNow() is identical to shutdownWrite(), except that it
   * immediately performs the shutdown, rather than waiting for pending writes
   * to complete.  Any pending write requests will be immediately failed when
   * shutdownWriteNow() is called.
   */
  virtual void shutdownWriteNow() = 0;

  /**
   * Determine if transport is open and ready to read or write.
   *
   * Note that this function returns false on EOF; you must also call error()
   * to distinguish between an EOF and an error.
   *
   * @return  true iff the transport is open and ready, false otherwise.
   */
  virtual bool good() const = 0;

  /**
   * Determine if the transport is readable or not.
   *
   * @return  true iff the transport is readable, false otherwise.
   */
  virtual bool readable() const = 0;

  /**
   * Determine if transport is connected to the endpoint
   *
   * @return  false iff the transport is connected, otherwise true
   */
  virtual bool connecting() const = 0;

  /**
   * Determine if an error has occurred with this transport.
   *
   * @return  true iff an error has occurred (not EOF).
   */
  virtual bool error() const = 0;

  /**
   * Attach the transport to a TEventBase.
   *
   * This may only be called if the transport is not currently attached to a
   * TEventBase (by an earlier call to detachEventBase()).
   *
   * This method must be invoked in the TEventBase's thread.
   */
  virtual void attachEventBase(TEventBase* eventBase) = 0;

  /**
   * Detach the transport from its TEventBase.
   *
   * This may only be called when the transport is idle and has no reads or
   * writes pending.  Once detached, the transport may not be used again until
   * it is re-attached to a TEventBase by calling attachEventBase().
   *
   * This method must be called from the current TEventBase's thread.
   */
  virtual void detachEventBase() = 0;

  /**
   * Get the TEventBase used by this transport.
   *
   * Returns NULL if this transport is not currently attached to a TEventBase.
   */
  virtual TEventBase* getEventBase() const = 0;

  /**
   * Set the send timeout.
   *
   * If write requests do not make any progress for more than the specified
   * number of milliseconds, fail all pending writes and close the transport.
   *
   * If write requests are currently pending when setSendTimeout() is called,
   * the timeout interval is immediately restarted using the new value.
   *
   * @param milliseconds  The timeout duration, in milliseconds.  If 0, no
   *                      timeout will be used.
   */
  virtual void setSendTimeout(uint32_t milliseconds) = 0;

  /**
   * Get the send timeout.
   *
   * @return Returns the current send timeout, in milliseconds.  A return value
   *         of 0 indicates that no timeout is set.
   */
  virtual uint32_t getSendTimeout() const = 0;

  /**
   * Get the address of the local endpoint of this transport.
   *
   * This function may throw TTransportException on error.
   *
   * @param address  The local address will be stored in the specified
   *                 TSocketAddress.
   */
  virtual void getLocalAddress(transport::TSocketAddress* address) const = 0;

  /**
   * Get the address of the remote endpoint to which this transport is
   * connected.
   *
   * This function may throw TTransportException on error.
   *
   * @param address  The remote endpoint's address will be stored in the
   *                 specified TSocketAddress.
   */
  virtual void getPeerAddress(transport::TSocketAddress* address) const = 0;
};

}}} // apache::thrift::async

#endif // #ifndef THRIFT_ASYNC_TASYNCTRANSPORT_H_
