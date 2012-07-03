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

#ifndef _THRIFT_TRANSPORT_TMEMPAGEDTRANSPORT_H_
#define _THRIFT_TRANSPORT_TMEMPAGEDTRANSPORT_H_ 1

#include "thrift/lib/cpp/transport/TVirtualTransport.h"
#include "thrift/lib/cpp/transport/TMemPagedFactory.h"
#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift { namespace transport {

using namespace boost;

/**
 * Class TTransport for direct usage of paged memory
 * NOT thread safe - for a higher performance trade-off
 */
class TMemPagedTransport : public TVirtualTransport<TMemPagedTransport> {
 public:
  /**
   * Constructor
   */
  TMemPagedTransport(shared_ptr<FixedSizeMemoryPageFactory>& factory);

  /**
   * Destructor
   */
  ~TMemPagedTransport();

  /**
   * Whether this transport is open.
   */
  virtual bool isOpen();

  /**
   * Opens the transport.
   */
  virtual void open();

  /**
   * Closes the transport.
   */
  virtual void close();

  /**
   * Called when read is completed.
   * This can be over-ridden to perform a transport-specific action
   * e.g. logging the request to a file
   *
   * @return number of bytes read if available, 0 otherwise.
   */
  virtual uint32_t readEnd();

  /**
   * Called when write is completed.
   * This can be over-ridden to perform a transport-specific action
   * at the end of a request.
   *
   * @return number of bytes written if available, 0 otherwise
   */
  virtual uint32_t writeEnd();

  /**
   * Implementations of the base functions
   */
  inline uint32_t read(uint8_t* buf, uint32_t len);
  inline void write(const uint8_t* buf, uint32_t len);
  inline const uint8_t* borrow(uint8_t* buf, uint32_t* len);
  inline void consume(uint32_t len);
  inline uint32_t readAll(uint8_t* buf, uint32_t len);
  /**
   * interface extension, not part of the TTrasport
   * make sure there are some bytes for write at the same page available
   */
  inline uint8_t* reserve(uint32_t* len, bool throwOnError = true);
  /**
   * interface extension, not part of the TTrasport
   * similar to consume but for the write
   */
  inline void skip(uint32_t len);

  /**
   * Reset buffer for read
   * Sets read offset to zero
   */
  inline void resetForRead();

  /**
   * Reset buffer for write
   * Sets write offset to zero and releases extra memory pages
   */
  inline void resetForWrite(bool releaseAllPages = false);

  /**
   * Returns number of bytes already read from buffer
   */
  inline uint32_t getReadBytes() const;

  /**
   * Returns current bytes written in buffer
   */
  inline uint32_t getWrittenBytes() const;

 private:
  /**
   * helper function
   * write or skip bytes
   */
  inline void set(const uint8_t* buf, uint32_t len);

  /**
   * helper function
   * read or skip bytes
   */
  inline uint32_t get(uint8_t* buf, uint32_t len);


 private:
  shared_ptr<FixedSizeMemoryPageFactory> factory_; //!< memory pages factory
  const uint32_t pageSize_; //!< page size
  FixedSizeMemoryPage* headPage_; //!< head of the stack
  FixedSizeMemoryPage* currentReadPage_; //!< current read page
  uint32_t currentReadOffset_; //!< bytes already read
  FixedSizeMemoryPage* currentWritePage_; //!< current write page
  uint32_t currentWriteOffset_;   //!< bytes already written
};

}}} // apache::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TMEMPAGEDTRANSPORT_H_
