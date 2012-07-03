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

#ifndef _THRIFT_TRANSPORT_TMEMPAGEDTRANSPORT_HPP_
#define _THRIFT_TRANSPORT_TMEMPAGEDTRANSPORT_HPP_ 1

#include "thrift/lib/cpp/transport/TMemPagedTransport.h"
#include <cstring>

namespace apache { namespace thrift { namespace transport {

/**
 * Reset buffer for read
 */
inline void TMemPagedTransport::resetForRead() {
  currentReadPage_ = headPage_;
  currentReadOffset_ = 0;
}

/**
 * Reset buffer for write
 */
inline void TMemPagedTransport::resetForWrite(bool releaseAllPages) {
  FixedSizeMemoryPage* discardPage = NULL;
  if (releaseAllPages) { // release all pages
    discardPage = headPage_;
    headPage_ = NULL;
  } else { // make sure head page left or created
    if (!headPage_) { // allocate page if any
      headPage_ = factory_->getPage();
    } else {
      discardPage = headPage_->next_;
      headPage_->next_ = NULL;
    }
  }
  while (discardPage) {
    FixedSizeMemoryPage* pageForDeletion = discardPage;
    discardPage = discardPage->next_;
    factory_->returnPage(pageForDeletion);
  }

  // reset
  currentReadPage_ = currentWritePage_ = headPage_;
  currentReadOffset_ = currentWriteOffset_ = 0;
}

/**
 * Returns number of bytes already read from buffer
 */
inline uint32_t TMemPagedTransport::getReadBytes() const {
  uint32_t ret = 0;
  FixedSizeMemoryPage* page = headPage_;
  while (page) {
    if (page == currentReadPage_) {
      ret += currentReadOffset_;
      break;
    } else {
      ret += pageSize_;
    }
    page = page->next_;
  }
  // return result
  return ret;
}

/**
 * Returns current bytes written in buffer
 */
inline uint32_t TMemPagedTransport::getWrittenBytes() const {
  uint32_t ret = 0;
  FixedSizeMemoryPage* page = headPage_;
  while (page) {
    if (page == currentWritePage_) {
      ret += currentWriteOffset_;
      break;
    } else {
      ret += pageSize_;
    }
    page = page->next_;
  }
  // return result
  return ret;
}

/**
 * Implementations of the base functions
 */
inline uint32_t TMemPagedTransport::read(uint8_t* buf, uint32_t len) {
  if (!buf) {
    throw TTransportException(TTransportException::BAD_ARGS);
  }
  return get(buf, len);
}

inline void TMemPagedTransport::write(const uint8_t* buf, uint32_t len) {
  if (!buf) {
    throw TTransportException(TTransportException::BAD_ARGS);
  }
  set(buf, len);
}

/**
 * interface extension, not part of the TTrasport
 * similar to consume but for the write
 */
inline void TMemPagedTransport::skip(uint32_t len) {
  set(NULL, len);
}

inline const uint8_t* TMemPagedTransport::borrow(uint8_t* buf, uint32_t* len) {
  // - possible scenarios
  // - buf is NULL
  // -- available bytes for current page >= len - (return internal pointer)
  // -- available bytes for current page < len - return NULL, set len 0
  // - buf is not NULL
  // -- available bytes for current page >= len - (return internal pointer)
  // -- available bytes for current page < len - copy to the external buffer
  FixedSizeMemoryPage* page = currentReadPage_;
  uint32_t readOffset = currentReadOffset_;
  if (!page) { // no memory available
    *len = 0;
    return NULL;
  }

  uint32_t capacity = *len; // requested capacity
  uint32_t available = 0;
  if (page == currentWritePage_) {
    available = currentWriteOffset_ - readOffset;
  } else {
    available = pageSize_ - readOffset;
  }

  if (available >= capacity) { // enough available bytes
    *len = available; // assign available bytes
    return page->buffer_ + readOffset; // return readable memory pointer
  }
  // not enough available bytes on the current page
  if (!buf) { // buffer is not provided
    *len = 0;
    return NULL;
  }
  // buffer is provided, make memory copy from current page if any
  available = std::min(available, capacity);
  if (available) {
    std::memcpy(buf + *len - capacity,
                  page->buffer_ + readOffset,
                  available);
    // adjust capacity
    capacity -= available;
    // adjust offset
    readOffset += available;
  }
  // copy the rest if available
  while (capacity) { // read until all buffer filled
    if (page == currentWritePage_ &&
        readOffset == currentWriteOffset_) {
      *len = 0;
      return NULL; // no more available bytes for read
    } else {
      // if read and write on the same page
      if (page == currentWritePage_) {
        available = currentWriteOffset_ - readOffset;
      } else if (!(available = pageSize_ - readOffset)) {
        // move to next page
        page = page->next_;
        assert(page);
        readOffset = 0;
        continue;
      }
    }
    // how many bytes we can read
    available = std::min(capacity, available);
    // memory copy
    std::memcpy(buf + *len - capacity,
                page->buffer_ + readOffset,
                available);
    // adjust capacity
    capacity -= available;
    // adjust page read offset
    readOffset += available;
    // move to the next page if end is reached
    // and more pages for read available
    if (readOffset == pageSize_ &&
        page != currentWritePage_) {
      page = page->next_;
      assert(page);
      // reset offset
      readOffset = 0;
    }
  }
  // return result, leave len as it is
  return buf;
}

/**
 * interface extension, not part of the TTrasport
 * make sure there are some bytes for write at the same page available
 */
inline uint8_t* TMemPagedTransport::reserve(uint32_t* len, bool throwOnError) {
  if (!currentWritePage_ || pageSize_ == currentWriteOffset_) {
    FixedSizeMemoryPage* newPage = factory_->getPage(throwOnError);
    if (!newPage) { // no memory
      return NULL;
    }
    if (currentWritePage_) {
      currentWritePage_->next_ = newPage;
    } else {
      headPage_ = currentReadPage_ = newPage;
    }
    currentWritePage_ = newPage;
    // reset write offset
    currentWriteOffset_ = 0;
  }
  if (len) { // it can be a call just to reserve a new page
    *len = pageSize_ - currentWriteOffset_;
  }

  return currentWritePage_->buffer_ + currentWriteOffset_;
}

inline void TMemPagedTransport::consume(uint32_t len) {
  get(NULL, len);
}

inline uint32_t TMemPagedTransport::readAll(uint8_t* buf, uint32_t len) {
  // try to read requested bytes
  uint32_t res = read(buf, len);
  if (res != len) { // not all bytes can be read
    // interface description requires throwing exception
    throw TTransportException(TTransportException::END_OF_FILE);
  }
  return res;
}


/**
 * helper function
 * write or skip bytes
 */
inline void TMemPagedTransport::set(const uint8_t* buf, uint32_t len) {
  uint32_t capacity = len; // requested capacity
  // prepare for rollback in case of out of memory
  FixedSizeMemoryPage* saveWritePage = currentWritePage_;
  uint32_t saveWriteOffset = currentWriteOffset_;
  while (capacity) { // write until all buffer bytes get sent
    uint32_t available = 0;
    if (!reserve(&available, false)) { // reserve page if any
      // rollback
      currentWriteOffset_ = saveWriteOffset;
      if ((currentWritePage_ = saveWritePage) != NULL) { // there were pages
        // remove extra allocated pages if any
        // even we remove previously existed pages it won't hurt
        FixedSizeMemoryPage* pageForDelete = saveWritePage->next_;
        currentWritePage_->next_ = NULL; // cut the tail
        while (pageForDelete) {
          saveWritePage = pageForDelete;
          pageForDelete = pageForDelete->next_;
          factory_->returnPage(saveWritePage);
        }
      } else {
        // remove all pages
        resetForWrite(true);
      }
      // throw
      throw TTransportException(TTransportException::INTERNAL_ERROR);
    }
    // how many bytes we can write
    available = std::min(capacity, available);
    if (buf) {
      // memory copy if any
      std::memcpy(currentWritePage_->buffer_ + currentWriteOffset_,
                  buf + len - capacity,
                  available);
    }
    // adjust capacity
    capacity -= available;
    // adjust page write iterator
    currentWriteOffset_ += available;
  } // while
}

/**
 * Implementations of the base functions
 */
inline uint32_t TMemPagedTransport::get(uint8_t* buf, uint32_t len) {
  uint32_t capacity = len; // requested memory
  uint32_t available = 0;
  if (!currentReadPage_) {
    return 0; // nothing to read, did not reset or write bytes
  }
  while (capacity) { // read until all buffer filled
    if (currentReadPage_ == currentWritePage_) {
      // if read and write on the same page
      if (!(available = currentWriteOffset_ - currentReadOffset_)) {
        break; // no more available bytes for read
      }
    } else if (!(available = pageSize_ - currentReadOffset_)) {
      // not the last page
      // move to next page
      currentReadPage_ = currentReadPage_->next_;
      assert(currentReadPage_);
      currentReadOffset_ = 0;
      available = pageSize_ - currentReadOffset_;
    }
    // how many bytes we can read
    available = std::min(capacity, available);
    // memory copy if any
    if (buf) {
      std::memcpy(buf + len - capacity,
                  currentReadPage_->buffer_ + currentReadOffset_,
                  available);
    }
    // adjust capacity
    capacity -= available;
    // adjust page read iterator
    currentReadOffset_ += available;
    // move to the next page if end is reached
    // and more pages for read available
    if (currentReadOffset_ == pageSize_ &&
        currentReadPage_ != currentWritePage_) {
      currentReadPage_ = currentReadPage_->next_;
      assert(currentReadPage_);
      // reset offset
      currentReadOffset_ = 0;
    }
  }
  // return result
  return len - capacity;
}

}}} // apache::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TMEMPAGEDTRANSPORT_HPP_
