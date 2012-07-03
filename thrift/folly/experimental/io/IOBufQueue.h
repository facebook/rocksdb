/*
 * Copyright 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FOLLY_IO_IOBUF_QUEUE_H
#define FOLLY_IO_IOBUF_QUEUE_H

#include "folly/experimental/io/IOBuf.h"

#include <stdexcept>
#include <string>

namespace folly {

/**
 * An IOBufQueue encapsulates a chain of IOBufs and provides
 * convenience functions to append data to the back of the chain
 * and remove data from the front.
 */
class IOBufQueue {
 public:
  struct Options {
    Options() : cacheChainLength(false) { }
    bool cacheChainLength;
  };

  explicit IOBufQueue(const Options& options = Options());

  /**
   * Add a buffer or buffer chain to the end of this queue. The
   * queue takes ownership of buf.
   */
  void append(std::unique_ptr<folly::IOBuf>&& buf);

  /**
   * Add a queue to the end of this queue. The queue takes ownership of
   * all buffers from the other queue.
   */
  void append(IOBufQueue& other);
  void append(IOBufQueue&& other) {
    append(other);  // call lvalue reference overload, above
  }

  /**
   * Copy len bytes, starting at buf, to the end of this queue.
   * The caller retains ownership of the source data.
   */
  void append(const void* buf, size_t len);

  /**
   * Copy a string to the end of this queue.
   * The caller retains ownership of the source data.
   */
  void append(const std::string& buf) {
    append(buf.data(), buf.length());
  }

  /**
   * Append a chain of IOBuf objects that point to consecutive regions
   * within buf.
   *
   * Just like IOBuf::wrapBuffer, this should only be used when the caller
   * knows ahead of time and can ensure that all IOBuf objects that will point
   * to this buffer will be destroyed before the buffer itself is destroyed;
   * all other caveats from wrapBuffer also apply.
   *
   * Every buffer except for the last will wrap exactly blockSize bytes.
   * Importantly, this method may be used to wrap buffers larger than 4GB.
   */
  void wrapBuffer(const void* buf, size_t len,
                  uint32_t blockSize=(1U << 31));  // default block size: 2GB

  /**
   * Obtain a writable block of contiguous bytes at the end of this
   * queue, allocating more space if necessary.  The amount of space
   * reserved will be between min and max, inclusive; the IOBufQueue
   * implementation may pick a value in that range that makes efficient
   * use of already-allocated internal space.
   *
   * If the caller subsequently writes anything into the returned space,
   * it must call the postallocate() method.
   *
   * @return The starting address of the block and the length in bytes.
   *
   * @note The point of the preallocate()/postallocate() mechanism is
   *       to support I/O APIs such as Thrift's TAsyncSocket::ReadCallback
   *       that request a buffer from the application and then, in a later
   *       callback, tell the application how much of the buffer they've
   *       filled with data.
   */
  std::pair<void*,uint32_t> preallocate(uint32_t min, uint32_t max);

  /**
   * Tell the queue that the caller has written data into the first n
   * bytes provided by the previous preallocate() call.
   *
   * @note n should be less than or equal to the size returned by
   *       preallocate().  If n is zero, the caller may skip the call
   *       to postallocate().  If n is nonzero, the caller must not
   *       invoke any other non-const methods on this IOBufQueue between
   *       the call to preallocate and the call to postallocate().
   */
  void postallocate(uint32_t n);

  /**
   * Obtain a writable block of n contiguous bytes, allocating more space
   * if necessary, and mark it as used.  The caller can fill it later.
   */
  void* allocate(uint32_t n) {
    void* p = preallocate(n, n).first;
    postallocate(n);
    return p;
  }

  /**
   * Split off the first n bytes of the queue into a separate IOBuf chain,
   * and transfer ownership of the new chain to the caller.  The IOBufQueue
   * retains ownership of everything after the split point.
   *
   * @warning If the split point lies in the middle of some IOBuf within
   *          the chain, this function may, as an implementation detail,
   *          clone that IOBuf.
   *
   * @throws std::underflow_error if n exceeds the number of bytes
   *         in the queue.
   */
  std::unique_ptr<folly::IOBuf> split(size_t n);

  /**
   * Similar to IOBuf::trimStart, but works on the whole queue.  Will
   * pop off buffers that have been completely trimmed.
   */
  void trimStart(size_t amount);

  /**
   * Similar to IOBuf::trimEnd, but works on the whole queue.  Will
   * pop off buffers that have been completely trimmed.
   */
  void trimEnd(size_t amount);

  /**
   * Transfer ownership of the queue's entire IOBuf chain to the caller.
   */
  std::unique_ptr<folly::IOBuf>&& move() {
    chainLength_ = 0;
    return std::move(head_);
  }

  /**
   * Access
   */
  const folly::IOBuf* front() const {
    return head_.get();
  }

  /**
   * Total chain length, only valid if cacheLength was specified in the
   * constructor.
   */
  size_t chainLength() const {
    if (!options_.cacheChainLength) {
      throw std::invalid_argument("IOBufQueue: chain length not cached");
    }
    return chainLength_;
  }

  const Options& options() const {
    return options_;
  }

  /** Movable */
  IOBufQueue(IOBufQueue&&);
  IOBufQueue& operator=(IOBufQueue&&);

 private:
  static const size_t kChainLengthNotCached = (size_t)-1;
  /** Not copyable */
  IOBufQueue(const IOBufQueue&) = delete;
  IOBufQueue& operator=(const IOBufQueue&) = delete;

  Options options_;
  size_t chainLength_;
  /** Everything that has been appended but not yet discarded or moved out */
  std::unique_ptr<folly::IOBuf> head_;
};

} // folly

#endif // FOLLY_IO_IOBUF_QUEUE_H
