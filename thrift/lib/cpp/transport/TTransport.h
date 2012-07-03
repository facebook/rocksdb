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

#ifndef THRIFT_TRANSPORT_TTRANSPORT_H
#define THRIFT_TRANSPORT_TTRANSPORT_H 1

#include "thrift/lib/cpp/Thrift.h"
#include <boost/shared_ptr.hpp>
#include "thrift/lib/cpp/transport/TTransportException.h"
#include <string>

namespace apache { namespace thrift { namespace transport {

/**
 * Helper template to hoist readAll implementation out of TTransport
 */
template <class Transport_>
uint32_t readAll(Transport_ &trans, uint8_t* buf, uint32_t len) {
  uint32_t have = 0;
  uint32_t get = 0;

  while (have < len) {
    get = trans.read(buf+have, len-have);
    if (get <= 0) {
      throw TTransportException(TTransportException::END_OF_FILE,
                                "No more data to read.");
    }
    have += get;
  }

  return have;
}


/**
 * Generic interface for a method of transporting data. A TTransport may be
 * capable of either reading or writing, but not necessarily both.
 *
 */
class TTransport {
 public:
  /**
   * Virtual deconstructor.
   */
  virtual ~TTransport() {}

  /**
   * Whether this transport is open.
   */
  virtual bool isOpen() {
    return false;
  }

  /**
   * Tests whether there is more data to read or if the remote side is
   * still open. By default this is true whenever the transport is open,
   * but implementations should add logic to test for this condition where
   * possible (i.e. on a socket).
   * This is used by a server to check if it should listen for another
   * request.
   */
  virtual bool peek() {
    return isOpen();
  }

  /**
   * Opens the transport for communications.
   *
   * @return bool Whether the transport was successfully opened
   * @throws TTransportException if opening failed
   */
  virtual void open() {
    throw TTransportException(TTransportException::NOT_OPEN, "Cannot open base TTransport.");
  }

  /**
   * Closes the transport.
   */
  virtual void close() {
    throw TTransportException(TTransportException::NOT_OPEN, "Cannot close base TTransport.");
  }

  /**
   * Attempt to read up to the specified number of bytes into the string.
   *
   * @param buf  Reference to the location to write the data
   * @param len  How many bytes to read
   * @return How many bytes were actually read
   * @throws TTransportException If an error occurs
   */
  uint32_t read(uint8_t* buf, uint32_t len) {
    T_VIRTUAL_CALL();
    return read_virt(buf, len);
  }
  virtual uint32_t read_virt(uint8_t* /* buf */, uint32_t /* len */) {
    throw TTransportException(TTransportException::NOT_OPEN,
                              "Base TTransport cannot read.");
  }

  /**
   * Reads the given amount of data in its entirety no matter what.
   *
   * @param s     Reference to location for read data
   * @param len   How many bytes to read
   * @return How many bytes read, which must be equal to size
   * @throws TTransportException If insufficient data was read
   */
  uint32_t readAll(uint8_t* buf, uint32_t len) {
    T_VIRTUAL_CALL();
    return readAll_virt(buf, len);
  }
  virtual uint32_t readAll_virt(uint8_t* buf, uint32_t len) {
    return apache::thrift::transport::readAll(*this, buf, len);
  }

  /**
   * Called when read is completed.
   * This can be over-ridden to perform a transport-specific action
   * e.g. logging the request to a file
   *
   * @return number of bytes read if available, 0 otherwise.
   */
  virtual uint32_t readEnd() {
    // default behavior is to do nothing
    return 0;
  }

  /**
   * Writes the string in its entirety to the buffer.
   *
   * Note: You must call flush() to ensure the data is actually written,
   * and available to be read back in the future.  Destroying a TTransport
   * object does not automatically flush pending data--if you destroy a
   * TTransport object with written but unflushed data, that data may be
   * discarded.
   *
   * @param buf  The data to write out
   * @throws TTransportException if an error occurs
   */
  void write(const uint8_t* buf, uint32_t len) {
    T_VIRTUAL_CALL();
    write_virt(buf, len);
  }
  virtual void write_virt(const uint8_t* /* buf */, uint32_t /* len */) {
    throw TTransportException(TTransportException::NOT_OPEN,
                              "Base TTransport cannot write.");
  }

  /**
   * Called when write is completed.
   * This can be over-ridden to perform a transport-specific action
   * at the end of a request.
   *
   * @return number of bytes written if available, 0 otherwise
   */
  virtual uint32_t writeEnd() {
    // default behaviour is to do nothing
    return 0;
  }

  /**
   * Flushes any pending data to be written. Typically used with buffered
   * transport mechanisms.
   *
   * @throws TTransportException if an error occurs
   */
  virtual void flush() {
    // default behaviour is to do nothing
  }

  /**
   * Attempts to return a pointer to \c len bytes, possibly copied into \c buf.
   * Does not consume the bytes read (i.e.: a later read will return the same
   * data).  This method is meant to support protocols that need to read
   * variable-length fields.  They can attempt to borrow the maximum amount of
   * data that they will need, then consume (see next method) what they
   * actually use.  Some transports will not support this method and others
   * will fail occasionally, so protocols must be prepared to use read if
   * borrow fails.
   *
   * @oaram buf  A buffer where the data can be stored if needed.
   *             If borrow doesn't return buf, then the contents of
   *             buf after the call are undefined.  This parameter may be
   *             NULL to indicate that the caller is not supplying storage,
   *             but would like a pointer into an internal buffer, if
   *             available.
   * @param len  *len should initially contain the number of bytes to borrow.
   *             If borrow succeeds, *len will contain the number of bytes
   *             available in the returned pointer.  This will be at least
   *             what was requested, but may be more if borrow returns
   *             a pointer to an internal buffer, rather than buf.
   *             If borrow fails, the contents of *len are undefined.
   * @return If the borrow succeeds, return a pointer to the borrowed data.
   *         This might be equal to \c buf, or it might be a pointer into
   *         the transport's internal buffers.
   * @throws TTransportException if an error occurs
   */
  const uint8_t* borrow(uint8_t* buf, uint32_t* len) {
    T_VIRTUAL_CALL();
    return borrow_virt(buf, len);
  }
  virtual const uint8_t* borrow_virt(uint8_t* /* buf */, uint32_t* /* len */) {
    return NULL;
  }

  /**
   * Remove len bytes from the transport.  This should always follow a borrow
   * of at least len bytes, and should always succeed.
   * TODO(dreiss): Is there any transport that could borrow but fail to
   * consume, or that would require a buffer to dump the consumed data?
   *
   * @param len  How many bytes to consume
   * @throws TTransportException If an error occurs
   */
  void consume(uint32_t len) {
    T_VIRTUAL_CALL();
    consume_virt(len);
  }
  virtual void consume_virt(uint32_t /* len */) {
    throw TTransportException(TTransportException::NOT_OPEN,
                              "Base TTransport cannot consume.");
  }

 protected:
  /**
   * Simple constructor.
   */
  TTransport() {}
};

/**
 * Generic factory class to make an input and output transport out of a
 * source transport. Commonly used inside servers to make input and output
 * streams out of raw clients.
 *
 */
class TTransportFactory {
 public:
  TTransportFactory() {}

  virtual ~TTransportFactory() {}

  /**
   * Default implementation does nothing, just returns the transport given.
   */
  virtual boost::shared_ptr<TTransport> getTransport(boost::shared_ptr<TTransport> trans) {
    return trans;
  }

};

/**
 * A duplex transport factory used to make input and output transports in a
 * single call.  This can be used to ensure the input and output transports
 * are the pointers to the same object, for example.
 *
 * TTransportPair.first = Input Transport
 * TTransportPair.second = Output Transport
 */
typedef std::pair<boost::shared_ptr<TTransport>,
                  boost::shared_ptr<TTransport> > TTransportPair;

class TDuplexTransportFactory {
 public:
  TDuplexTransportFactory() {}

  virtual ~TDuplexTransportFactory() {}

  virtual TTransportPair getTransport(boost::shared_ptr<TTransport> trans) {
    return std::make_pair(trans, trans);
  }

  virtual TTransportPair getTransport(TTransportPair transports) {
    return std::make_pair(transports.first, transports.second);
  }

};

/**
 * Adapts a TTransportFactory to a TDuplexTransportFactory that returns
 * a new transport object for both input and output
 */
template <class Factory_>
class TSingleTransportFactory : public TDuplexTransportFactory {
 public:
  TSingleTransportFactory() {
    factory_.reset(new Factory_());
  }

  explicit TSingleTransportFactory(
    boost::shared_ptr<Factory_> factory) :
      factory_(factory) {}

  virtual TTransportPair getTransport(boost::shared_ptr<TTransport> trans) {
    return std::make_pair(factory_->getTransport(trans),
                          factory_->getTransport(trans));
  }

  virtual TTransportPair getTransport(TTransportPair transports) {
    return std::make_pair(factory_->getTransport(transports.first),
                          factory_->getTransport(transports.second));
  }
 private:

  boost::shared_ptr<Factory_> factory_;
};

/**
 * Use TDualTransportFactory to construct input and output transports from
 * different factories.
 */
class TDualTransportFactory : public TDuplexTransportFactory {
 public:
  TDualTransportFactory(
    boost::shared_ptr<TTransportFactory> inputFactory,
    boost::shared_ptr<TTransportFactory> outputFactory) :
      inputFactory_(inputFactory),
      outputFactory_(outputFactory) {}

  virtual TTransportPair getTransport(boost::shared_ptr<TTransport> trans) {
    return std::make_pair(inputFactory_->getTransport(trans),
                          outputFactory_->getTransport(trans));
  }

  virtual TTransportPair getTransport(TTransportPair transports) {
    return std::make_pair(inputFactory_->getTransport(transports.first),
                          outputFactory_->getTransport(transports.second));
  }
 private:

  boost::shared_ptr<TTransportFactory> inputFactory_;
  boost::shared_ptr<TTransportFactory> outputFactory_;
};

}}} // apache::thrift::transport

#endif // #ifndef THRIFT_TRANSPORT_TTRANSPORT_H
