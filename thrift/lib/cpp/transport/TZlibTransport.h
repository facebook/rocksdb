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

#ifndef _THRIFT_TRANSPORT_TZLIBTRANSPORT_H_
#define _THRIFT_TRANSPORT_TZLIBTRANSPORT_H_ 1

#include <boost/lexical_cast.hpp>
#include "thrift/lib/cpp/transport/TBufferTransports.h"
#include "thrift/lib/cpp/transport/TVirtualTransport.h"

struct z_stream_s;

namespace apache { namespace thrift { namespace transport {

class TZlibTransportException : public TTransportException {
 public:
  TZlibTransportException(int status, const char* msg) :
    TTransportException(TTransportException::INTERNAL_ERROR,
                        errorMessage(status, msg)),
    zlib_status_(status),
    zlib_msg_(msg == NULL ? "(null)" : msg) {}

  virtual ~TZlibTransportException() throw() {}

  int getZlibStatus() { return zlib_status_; }
  std::string getZlibMessage() { return zlib_msg_; }

  static std::string errorMessage(int status, const char* msg) {
    std::string rv = "zlib error: ";
    if (msg) {
      rv += msg;
    } else {
      rv += "(no message)";
    }
    rv += " (status = ";
    rv += boost::lexical_cast<std::string>(status);
    rv += ")";
    return rv;
  }

  int zlib_status_;
  std::string zlib_msg_;
};

/**
 * This transport uses zlib's compressed format on the "far" side.
 *
 * There are two kinds of TZlibTransport objects:
 * - Standalone objects are used to encode self-contained chunks of data
 *   (like structures).  They include checksums.
 * - Non-standalone transports are used for RPC.  They are not implemented yet.
 *
 * TODO(dreiss): Don't do an extra copy of the compressed data if
 *               the underlying transport is TBuffered or TMemory.
 *
 */
class TZlibTransport : public TVirtualTransport<TZlibTransport> {
 public:

  /**
   * @param transport    The transport to read compressed data from
   *                     and write compressed data to.
   * @param urbuf_size   Uncompressed buffer size for reading.
   * @param crbuf_size   Compressed buffer size for reading.
   * @param uwbuf_size   Uncompressed buffer size for writing.
   * @param cwbuf_size   Compressed buffer size for writing.
   *
   * TODO(dreiss): Write a constructor that isn't a pain.
   */
  TZlibTransport(boost::shared_ptr<TTransport> transport,
                 int urbuf_size = DEFAULT_URBUF_SIZE,
                 int crbuf_size = DEFAULT_CRBUF_SIZE,
                 int uwbuf_size = DEFAULT_UWBUF_SIZE,
                 int cwbuf_size = DEFAULT_CWBUF_SIZE) :
    transport_(transport),
    urpos_(0),
    uwpos_(0),
    input_ended_(false),
    output_finished_(false),
    urbuf_size_(urbuf_size),
    crbuf_size_(crbuf_size),
    uwbuf_size_(uwbuf_size),
    cwbuf_size_(cwbuf_size),
    urbuf_(NULL),
    crbuf_(NULL),
    uwbuf_(NULL),
    cwbuf_(NULL),
    rstream_(NULL),
    wstream_(NULL)
  {
    if (uwbuf_size_ < MIN_DIRECT_DEFLATE_SIZE) {
      // Have to copy this into a local because of a linking issue.
      int minimum = MIN_DIRECT_DEFLATE_SIZE;
      throw TTransportException(
          TTransportException::BAD_ARGS,
          "TZLibTransport: uncompressed write buffer must be at least"
          + boost::lexical_cast<std::string>(minimum) + ".");
    }

    try {
      urbuf_ = new uint8_t[urbuf_size];
      crbuf_ = new uint8_t[crbuf_size];
      uwbuf_ = new uint8_t[uwbuf_size];
      cwbuf_ = new uint8_t[cwbuf_size];

      // Don't call this outside of the constructor.
      initZlib();

    } catch (...) {
      delete[] urbuf_;
      delete[] crbuf_;
      delete[] uwbuf_;
      delete[] cwbuf_;
      throw;
    }
  }

  // Don't call this outside of the constructor.
  void initZlib();

  /**
   * TZlibTransport destructor.
   *
   * Warning: Destroying a TZlibTransport object may discard any written but
   * unflushed data.  You must explicitly call flush() or finish() to ensure
   * that data is actually written and flushed to the underlying transport.
   */
  ~TZlibTransport();

  bool isOpen();
  bool peek();

  void open() {
    transport_->open();
  }

  void close() {
    transport_->close();
  }

  uint32_t read(uint8_t* buf, uint32_t len);

  void write(const uint8_t* buf, uint32_t len);

  void flush();

  /**
   * Finalize the zlib stream.
   *
   * This causes zlib to flush any pending write data and write end-of-stream
   * information, including the checksum.  Once finish() has been called, no
   * new data can be written to the stream.
   */
  void finish();

  const uint8_t* borrow(uint8_t* buf, uint32_t* len);

  void consume(uint32_t len);

  /**
   * Verify the checksum at the end of the zlib stream.
   *
   * This may only be called after all data has been read.
   * It verifies the checksum that was written by the finish() call.
   */
  void verifyChecksum();

   /**
    * TODO(someone_smart): Choose smart defaults.
    */
  static const int DEFAULT_URBUF_SIZE = 128;
  static const int DEFAULT_CRBUF_SIZE = 1024;
  static const int DEFAULT_UWBUF_SIZE = 128;
  static const int DEFAULT_CWBUF_SIZE = 1024;

 protected:

  inline void checkZlibRv(int status, const char* msg);
  inline void checkZlibRvNothrow(int status, const char* msg);
  inline int readAvail();
  void flushToTransport(int flush);
  void flushToZlib(const uint8_t* buf, int len, int flush);
  bool readFromZlib();

 private:
  // Deprecated constructor signature.
  //
  // This used to be the constructor signature.  If you are getting a compile
  // error because you are trying to use this constructor, you need to update
  // your code as follows:
  // - Remove the use_for_rpc argument in the constructur.
  //   There is no longer any distinction between RPC and standalone zlib
  //   transports.  (Previously, only standalone was allowed, anyway.)
  // - Replace TZlibTransport::flush() calls with TZlibTransport::finish()
  //   in your code.  Previously, flush() used to finish the zlib stream.
  //   Now flush() only flushes out pending data, so more writes can be
  //   performed after a flush().  The finish() method can be used to finalize
  //   the zlib stream.
  //
  // If we don't declare this constructor, old code written as
  // TZlibTransport(trans, false) still compiles but behaves incorrectly.
  // The second bool argument is converted to an integer and used as the
  // urbuf_size.
  TZlibTransport(boost::shared_ptr<TTransport> transport,
                 bool use_for_rpc,
                 int urbuf_size = DEFAULT_URBUF_SIZE,
                 int crbuf_size = DEFAULT_CRBUF_SIZE,
                 int uwbuf_size = DEFAULT_UWBUF_SIZE,
                 int cwbuf_size = DEFAULT_CWBUF_SIZE);

 protected:
  // Writes smaller than this are buffered up.
  // Larger (or equal) writes are dumped straight to zlib.
  static const int MIN_DIRECT_DEFLATE_SIZE = 32;

  boost::shared_ptr<TTransport> transport_;

  int urpos_;
  int uwpos_;

  /// True iff zlib has reached the end of the input stream.
  bool input_ended_;
  /// True iff we have finished the output stream.
  bool output_finished_;

  int urbuf_size_;
  int crbuf_size_;
  int uwbuf_size_;
  int cwbuf_size_;

  uint8_t* urbuf_;
  uint8_t* crbuf_;
  uint8_t* uwbuf_;
  uint8_t* cwbuf_;

  struct z_stream_s* rstream_;
  struct z_stream_s* wstream_;
};


/**
 * Wraps a transport into a zlibbed one.
 *
 */
class TZlibTransportFactory : public TTransportFactory {
 public:
  TZlibTransportFactory() {}

  virtual ~TZlibTransportFactory() {}

  virtual boost::shared_ptr<TTransport> getTransport(
                                         boost::shared_ptr<TTransport> trans) {
    return boost::shared_ptr<TTransport>(new TZlibTransport(trans));
  }
};

/**
 * Wraps a transport into a framed, zlibbed one.
 */
class TFramedZlibTransportFactory : public TTransportFactory {
 public:
  TFramedZlibTransportFactory() {}

  virtual ~TFramedZlibTransportFactory() {}

  virtual boost::shared_ptr<TTransport> getTransport(
                                         boost::shared_ptr<TTransport> trans) {
    boost::shared_ptr<TTransport> framedTransport(new TFramedTransport(trans));
    return boost::shared_ptr<TTransport>(new TZlibTransport(framedTransport));
  }
};


}}} // apache::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TZLIBTRANSPORT_H_
