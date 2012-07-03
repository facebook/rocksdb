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

#ifndef FOLLY_IO_IOBUF_H_
#define FOLLY_IO_IOBUF_H_

#include <atomic>
#include <cassert>
#include <cinttypes>
#include <cstddef>
#include <cstring>
#include <memory>
#include <limits>
#include <type_traits>
#include <folly/experimental/io/check.h>

namespace folly {

/**
 * An IOBuf is a pointer to a buffer of data.
 *
 * IOBuf objects are intended to be used primarily for networking code, and are
 * modelled somewhat after FreeBSD's mbuf data structure, and Linux's sk_buff
 * structure.
 *
 * IOBuf objects facilitate zero-copy network programming, by allowing multiple
 * IOBuf objects to point to the same underlying buffer of data, using a
 * reference count to track when the buffer is no longer needed and can be
 * freed.
 *
 *
 * Data Layout
 * -----------
 *
 * The IOBuf itself is a small object containing a pointer to the buffer and
 * information about which segment of the buffer contains valid data.
 *
 * The data layout looks like this:
 *
 *  +-------+
 *  | IOBuf |
 *  +-------+
 *   /
 *  |
 *  v
 *  +------------+--------------------+-----------+
 *  | headroom   |        data        |  tailroom |
 *  +------------+--------------------+-----------+
 *  ^            ^                    ^           ^
 *  buffer()   data()               tail()      bufferEnd()
 *
 *  The length() method returns the length of the valid data; capacity()
 *  returns the entire capacity of the buffer (from buffer() to bufferEnd()).
 *  The headroom() and tailroom() methods return the amount of unused capacity
 *  available before and after the data.
 *
 *
 * Buffer Sharing
 * --------------
 *
 * The buffer itself is reference counted, and multiple IOBuf objects may point
 * to the same buffer.  Each IOBuf may point to a different section of valid
 * data within the underlying buffer.  For example, if multiple protocol
 * requests are read from the network into a single buffer, a separate IOBuf
 * may be created for each request, all sharing the same underlying buffer.
 *
 * In other words, when multiple IOBufs share the same underlying buffer, the
 * data() and tail() methods on each IOBuf may point to a different segment of
 * the data.  However, the buffer() and bufferEnd() methods will point to the
 * same location for all IOBufs sharing the same underlying buffer.
 *
 *       +-----------+     +---------+
 *       |  IOBuf 1  |     | IOBuf 2 |
 *       +-----------+     +---------+
 *        |         | _____/        |
 *   data |    tail |/    data      | tail
 *        v         v               v
 *  +-------------------------------------+
 *  |     |         |               |     |
 *  +-------------------------------------+
 *
 * If you only read data from an IOBuf, you don't need to worry about other
 * IOBuf objects possibly sharing the same underlying buffer.  However, if you
 * ever write to the buffer you need to first ensure that no other IOBufs point
 * to the same buffer.  The unshare() method may be used to ensure that you
 * have an unshared buffer.
 *
 *
 * IOBuf Chains
 * ------------
 *
 * IOBuf objects also contain pointers to next and previous IOBuf objects.
 * This can be used to represent a single logical piece of data that its stored
 * in non-contiguous chunks in separate buffers.
 *
 * A single IOBuf object can only belong to one chain at a time.
 *
 * IOBuf chains are always circular.  The "prev" pointer in the head of the
 * chain points to the tail of the chain.  However, it is up to the user to
 * decide which IOBuf is the head.  Internally the IOBuf code does not care
 * which element is the head.
 *
 * The lifetime of all IOBufs in the chain are linked: when one element in the
 * chain is deleted, all other chained elements are also deleted.  Conceptually
 * it is simplest to treat this as if the head of the chain owns all other
 * IOBufs in the chain.  When you delete the head of the chain, it will delete
 * the other elements as well.  For this reason, prependChain() and
 * appendChain() take ownership of of the new elements being added to this
 * chain.
 *
 * When the coalesce() method is used to coalesce an entire IOBuf chain into a
 * single IOBuf, all other IOBufs in the chain are eliminated and automatically
 * deleted.  The unshare() method may coalesce the chain; if it does it will
 * similarly delete all IOBufs eliminated from the chain.
 *
 * As discussed in the following section, it is up to the user to maintain a
 * lock around the entire IOBuf chain if multiple threads need to access the
 * chain.  IOBuf does not provide any internal locking.
 *
 *
 * Synchronization
 * ---------------
 *
 * When used in multithread programs, a single IOBuf object should only be used
 * in a single thread at a time.  If a caller uses a single IOBuf across
 * multiple threads the caller is responsible for using an external lock to
 * synchronize access to the IOBuf.
 *
 * Two separate IOBuf objects may be accessed concurrently in separate threads
 * without locking, even if they point to the same underlying buffer.  The
 * buffer reference count is always accessed atomically, and no other
 * operations should affect other IOBufs that point to the same data segment.
 * The caller is responsible for using unshare() to ensure that the data buffer
 * is not shared by other IOBufs before writing to it, and this ensures that
 * the data itself is not modified in one thread while also being accessed from
 * another thread.
 *
 * For IOBuf chains, no two IOBufs in the same chain should be accessed
 * simultaneously in separate threads.  The caller must maintain a lock around
 * the entire chain if the chain, or individual IOBufs in the chain, may be
 * accessed by multiple threads.
 *
 *
 * IOBuf Object Allocation/Sharing
 * -------------------------------
 *
 * IOBuf objects themselves are always allocated on the heap.  The IOBuf
 * constructors are private, so IOBuf objects may not be created on the stack.
 * In part this is done since some IOBuf objects use small-buffer optimization
 * and contain the buffer data immediately after the IOBuf object itself.  The
 * coalesce() and unshare() methods also expect to be able to delete subsequent
 * IOBuf objects in the chain if they are no longer needed due to coalescing.
 *
 * The IOBuf structure also does not provide room for an intrusive refcount on
 * the IOBuf object itself, only the underlying data buffer is reference
 * counted.  If users want to share the same IOBuf object between multiple
 * parts of the code, they are responsible for managing this sharing on their
 * own.  (For example, by using a shared_ptr.  Alternatively, users always have
 * the option of using clone() to create a second IOBuf that points to the same
 * underlying buffer.)
 *
 * With jemalloc, allocating small objects like IOBuf objects should be
 * relatively fast, and the cost of allocating IOBuf objects on the heap and
 * cloning new IOBufs should be relatively cheap.
 */
namespace detail {
// Is T a unique_ptr<> to a standard-layout type?
template <class T, class Enable=void> struct IsUniquePtrToSL
  : public std::false_type { };
template <class T, class D>
struct IsUniquePtrToSL<
  std::unique_ptr<T, D>,
  typename std::enable_if<std::is_standard_layout<T>::value>::type>
  : public std::true_type { };
}  // namespace detail

class IOBuf {
 public:
  typedef void (*FreeFunction)(void* buf, void* userData);

  /**
   * Allocate a new IOBuf object with the requested capacity.
   *
   * Returns a new IOBuf object that must be (eventually) deleted by the
   * caller.  The returned IOBuf may actually have slightly more capacity than
   * requested.
   *
   * The data pointer will initially point to the start of the newly allocated
   * buffer, and will have a data length of 0.
   *
   * Throws std::bad_alloc on error.
   */
  static std::unique_ptr<IOBuf> create(uint32_t capacity);

  /**
   * Create a new IOBuf pointing to an existing data buffer.
   *
   * The new IOBuffer will assume ownership of the buffer, and free it by
   * calling the specified FreeFunction when the last IOBuf pointing to this
   * buffer is destroyed.  The function will be called with a pointer to the
   * buffer as the first argument, and the supplied userData value as the
   * second argument.  The free function must never throw exceptions.
   *
   * If no FreeFunction is specified, the buffer will be freed using free().
   *
   * The IOBuf data pointer will initially point to the start of the buffer,
   * and the length will be the full capacity of the buffer.
   *
   * On error, std::bad_alloc will be thrown.  If freeOnError is true (the
   * default) the buffer will be freed before throwing the error.
   */
  static std::unique_ptr<IOBuf> takeOwnership(void* buf, uint32_t capacity,
                                              FreeFunction freeFn = NULL,
                                              void* userData = NULL,
                                              bool freeOnError = true);

  /**
   * Create a new IOBuf pointing to an existing data buffer made up of
   * count objects of a given standard-layout type.
   *
   * This is dangerous -- it is essentially equivalent to doing
   * reinterpret_cast<unsigned char*> on your data -- but it's often useful
   * for serialization / deserialization.
   *
   * The new IOBuffer will assume ownership of the buffer, and free it
   * appropriately (by calling the UniquePtr's custom deleter, or by calling
   * delete or delete[] appropriately if there is no custom deleter)
   * when the buffer is destroyed.  The custom deleter, if any, must never
   * throw exceptions.
   *
   * The IOBuf data pointer will initially point to the start of the buffer,
   * and the length will be the full capacity of the buffer (count *
   * sizeof(T)).
   *
   * On error, std::bad_alloc will be thrown, and the buffer will be freed
   * before throwing the error.
   */
  template <class UniquePtr>
  static typename std::enable_if<detail::IsUniquePtrToSL<UniquePtr>::value,
                                 std::unique_ptr<IOBuf>>::type
  takeOwnership(UniquePtr&& buf, size_t count=1);

  /**
   * Create a new IOBuf object that points to an existing user-owned buffer.
   *
   * This should only be used when the caller knows the lifetime of the IOBuf
   * object ahead of time and can ensure that all IOBuf objects that will point
   * to this buffer will be destroyed before the buffer itself is destroyed.
   *
   * This buffer will not be freed automatically when the last IOBuf
   * referencing it is destroyed.  It is the caller's responsibility to free
   * the buffer after the last IOBuf has been destroyed.
   *
   * The IOBuf data pointer will initially point to the start of the buffer,
   * and the length will be the full capacity of the buffer.
   *
   * An IOBuf created using wrapBuffer() will always be reported as shared.
   * unshare() may be used to create a writable copy of the buffer.
   *
   * On error, std::bad_alloc will be thrown.
   */
  static std::unique_ptr<IOBuf> wrapBuffer(const void* buf, uint32_t capacity);

  /**
   * Convenience function to create a new IOBuf object that copies data from a
   * user-supplied buffer, optionally allocating a given amount of
   * headroom and tailroom.
   */
  static std::unique_ptr<IOBuf> copyBuffer(const void* buf, uint32_t size,
                                           uint32_t headroom=0,
                                           uint32_t minTailroom=0);

  /**
   * Convenience function to free a chain of IOBufs held by a unique_ptr.
   */
  static void destroy(std::unique_ptr<IOBuf>&& data) {
    auto destroyer = std::move(data);
  }

  /**
   * Destroy this IOBuf.
   *
   * Deleting an IOBuf will automatically destroy all IOBufs in the chain.
   * (See the comments above regarding the ownership model of IOBuf chains.
   * All subsequent IOBufs in the chain are considered to be owned by the head
   * of the chain.  Users should only explicitly delete the head of a chain.)
   *
   * When each individual IOBuf is destroyed, it will release its reference
   * count on the underlying buffer.  If it was the last user of the buffer,
   * the buffer will be freed.
   */
  ~IOBuf();

  /**
   * Check whether the chain is empty (i.e., whether the IOBufs in the
   * chain have a total data length of zero).
   *
   * This method is semantically equivalent to
   *   i->computeChainDataLength()==0
   * but may run faster because it can short-circuit as soon as it
   * encounters a buffer with length()!=0
   */
  bool empty() const;

  /**
   * Get the pointer to the start of the data.
   */
  const uint8_t* data() const {
    return data_;
  }

  /**
   * Get a writable pointer to the start of the data.
   *
   * The caller is responsible for calling unshare() first to ensure that it is
   * actually safe to write to the buffer.
   */
  uint8_t* writableData() {
    return data_;
  }

  /**
   * Get the pointer to the end of the data.
   */
  const uint8_t* tail() const {
    return data_ + length_;
  }

  /**
   * Get a writable pointer to the end of the data.
   *
   * The caller is responsible for calling unshare() first to ensure that it is
   * actually safe to write to the buffer.
   */
  uint8_t* writableTail() {
    return data_ + length_;
  }

  /**
   * Get the data length.
   */
  uint32_t length() const {
    return length_;
  }

  /**
   * Get the amount of head room.
   *
   * Returns the number of bytes in the buffer before the start of the data.
   */
  uint32_t headroom() const {
    return data_ - buffer();
  }

  /**
   * Get the amount of tail room.
   *
   * Returns the number of bytes in the buffer after the end of the data.
   */
  uint32_t tailroom() const {
    return bufferEnd() - tail();
  }

  /**
   * Get the pointer to the start of the buffer.
   *
   * Note that this is the pointer to the very beginning of the usable buffer,
   * not the start of valid data within the buffer.  Use the data() method to
   * get a pointer to the start of the data within the buffer.
   */
  const uint8_t* buffer() const {
    return (flags_ & kFlagExt) ? ext_.buf : int_.buf;
  }

  /**
   * Get a writable pointer to the start of the buffer.
   *
   * The caller is responsible for calling unshare() first to ensure that it is
   * actually safe to write to the buffer.
   */
  uint8_t* writableBuffer() {
    return (flags_ & kFlagExt) ? ext_.buf : int_.buf;
  }

  /**
   * Get the pointer to the end of the buffer.
   *
   * Note that this is the pointer to the very end of the usable buffer,
   * not the end of valid data within the buffer.  Use the tail() method to
   * get a pointer to the end of the data within the buffer.
   */
  const uint8_t* bufferEnd() const {
    return (flags_ & kFlagExt) ?
      ext_.buf + ext_.capacity :
      int_.buf + kMaxInternalDataSize;
  }

  /**
   * Get the total size of the buffer.
   *
   * This returns the total usable length of the buffer.  Use the length()
   * method to get the length of the actual valid data in this IOBuf.
   */
  uint32_t capacity() const {
    return (flags_ & kFlagExt) ?  ext_.capacity : kMaxInternalDataSize;
  }

  /**
   * Get a pointer to the next IOBuf in this chain.
   */
  IOBuf* next() {
    return next_;
  }
  const IOBuf* next() const {
    return next_;
  }

  /**
   * Get a pointer to the previous IOBuf in this chain.
   */
  IOBuf* prev() {
    return prev_;
  }
  const IOBuf* prev() const {
    return prev_;
  }

  /**
   * Shift the data forwards in the buffer.
   *
   * This shifts the data pointer forwards in the buffer to increase the
   * headroom.  This is commonly used to increase the headroom in a newly
   * allocated buffer.
   *
   * The caller is responsible for ensuring that there is sufficient
   * tailroom in the buffer before calling advance().
   *
   * If there is a non-zero data length, advance() will use memmove() to shift
   * the data forwards in the buffer.  In this case, the caller is responsible
   * for making sure the buffer is unshared, so it will not affect other IOBufs
   * that may be sharing the same underlying buffer.
   */
  void advance(uint32_t amount) {
    // In debug builds, assert if there is a problem.
    assert(amount <= tailroom());

    if (length_ > 0) {
      memmove(data_ + amount, data_, length_);
    }
    data_ += amount;
  }

  /**
   * Shift the data backwards in the buffer.
   *
   * The caller is responsible for ensuring that there is sufficient headroom
   * in the buffer before calling retreat().
   *
   * If there is a non-zero data length, retreat() will use memmove() to shift
   * the data backwards in the buffer.  In this case, the caller is responsible
   * for making sure the buffer is unshared, so it will not affect other IOBufs
   * that may be sharing the same underlying buffer.
   */
  void retreat(uint32_t amount) {
    // In debug builds, assert if there is a problem.
    assert(amount <= headroom());

    if (length_ > 0) {
      memmove(data_ - amount, data_, length_);
    }
    data_ -= amount;
  }

  /**
   * Adjust the data pointer to include more valid data at the beginning.
   *
   * This moves the data pointer backwards to include more of the available
   * buffer.  The caller is responsible for ensuring that there is sufficient
   * headroom for the new data.  The caller is also responsible for populating
   * this section with valid data.
   *
   * This does not modify any actual data in the buffer.
   */
  void prepend(uint32_t amount) {
    CHECK(amount <= headroom());
    data_ -= amount;
  }

  /**
   * Adjust the tail pointer to include more valid data at the end.
   *
   * This moves the tail pointer forwards to include more of the available
   * buffer.  The caller is responsible for ensuring that there is sufficient
   * tailroom for the new data.  The caller is also responsible for populating
   * this section with valid data.
   *
   * This does not modify any actual data in the buffer.
   */
  void append(uint32_t amount) {
    CHECK(amount <= tailroom());
    length_ += amount;
  }

  /**
   * Adjust the data pointer forwards to include less valid data.
   *
   * This moves the data pointer forwards so that the first amount bytes are no
   * longer considered valid data.  The caller is responsible for ensuring that
   * amount is less than or equal to the actual data length.
   *
   * This does not modify any actual data in the buffer.
   */
  void trimStart(uint32_t amount) {
    CHECK(amount <= length_);
    data_ += amount;
    length_ -= amount;
  }

  /**
   * Adjust the tail pointer backwards to include less valid data.
   *
   * This moves the tail pointer backwards so that the last amount bytes are no
   * longer considered valid data.  The caller is responsible for ensuring that
   * amount is less than or equal to the actual data length.
   *
   * This does not modify any actual data in the buffer.
   */
  void trimEnd(uint32_t amount) {
    CHECK(amount <= length_);
    length_ -= amount;
  }

  /**
   * Clear the buffer.
   *
   * Postcondition: headroom() == 0, length() == 0, tailroom() == capacity()
   */
  void clear() {
    data_ = writableBuffer();
    length_ = 0;
  }

  /**
   * Ensure that this buffer has at least minHeadroom headroom bytes and at
   * least minTailroom tailroom bytes.  The buffer must be writable
   * (you must call unshare() before this, if necessary).
   *
   * Postcondition: headroom() >= minHeadroom, tailroom() >= minTailroom,
   * the data (between data() and data() + length()) is preserved.
   */
  void reserve(uint32_t minHeadroom, uint32_t minTailroom) {
    // Maybe we don't need to do anything.
    if (headroom() >= minHeadroom && tailroom() >= minTailroom) {
      return;
    }
    // If the buffer is empty but we have enough total room (head + tail),
    // move the data_ pointer around.
    if (length() == 0 &&
        headroom() + tailroom() >= minHeadroom + minTailroom) {
      data_ = writableBuffer() + minHeadroom;
      return;
    }
    // Bah, we have to do actual work.
    reserveSlow(minHeadroom, minTailroom);
  }

  /**
   * Return true if this IOBuf is part of a chain of multiple IOBufs, or false
   * if this is the only IOBuf in its chain.
   */
  bool isChained() const {
    assert((next_ == this) == (prev_ == this));
    return next_ != this;
  }

  /**
   * Get the number of IOBufs in this chain.
   *
   * Beware that this method has to walk the entire chain.
   * Use isChained() if you just want to check if this IOBuf is part of a chain
   * or not.
   */
  uint32_t countChainElements() const;

  /**
   * Get the length of all the data in this IOBuf chain.
   *
   * Beware that this method has to walk the entire chain.
   */
  uint64_t computeChainDataLength() const;

  /**
   * Insert another IOBuf chain immediately before this IOBuf.
   *
   * For example, if there are two IOBuf chains (A, B, C) and (D, E, F),
   * and B->prependChain(D) is called, the (D, E, F) chain will be subsumed
   * and become part of the chain starting at A, which will now look like
   * (A, D, E, F, B, C)
   *
   * Note that since IOBuf chains are circular, head->prependChain(other) can
   * be used to append the other chain at the very end of the chain pointed to
   * by head.  For example, if there are two IOBuf chains (A, B, C) and
   * (D, E, F), and A->prependChain(D) is called, the chain starting at A will
   * now consist of (A, B, C, D, E, F)
   *
   * The elements in the specified IOBuf chain will become part of this chain,
   * and will be owned by the head of this chain.  When this chain is
   * destroyed, all elements in the supplied chain will also be destroyed.
   *
   * For this reason, appendChain() only accepts an rvalue-reference to a
   * unique_ptr(), to make it clear that it is taking ownership of the supplied
   * chain.  If you have a raw pointer, you can pass in a new temporary
   * unique_ptr around the raw pointer.  If you have an existing,
   * non-temporary unique_ptr, you must call std::move(ptr) to make it clear
   * that you are destroying the original pointer.
   */
  void prependChain(std::unique_ptr<IOBuf>&& iobuf);

  /**
   * Append another IOBuf chain immediately after this IOBuf.
   *
   * For example, if there are two IOBuf chains (A, B, C) and (D, E, F),
   * and B->appendChain(D) is called, the (D, E, F) chain will be subsumed
   * and become part of the chain starting at A, which will now look like
   * (A, B, D, E, F, C)
   *
   * The elements in the specified IOBuf chain will become part of this chain,
   * and will be owned by the head of this chain.  When this chain is
   * destroyed, all elements in the supplied chain will also be destroyed.
   *
   * For this reason, appendChain() only accepts an rvalue-reference to a
   * unique_ptr(), to make it clear that it is taking ownership of the supplied
   * chain.  If you have a raw pointer, you can pass in a new temporary
   * unique_ptr around the raw pointer.  If you have an existing,
   * non-temporary unique_ptr, you must call std::move(ptr) to make it clear
   * that you are destroying the original pointer.
   */
  void appendChain(std::unique_ptr<IOBuf>&& iobuf) {
    // Just use prependChain() on the next element in our chain
    next_->prependChain(std::move(iobuf));
  }

  /**
   * Remove this IOBuf from its current chain.
   *
   * Since ownership of all elements an IOBuf chain is normally maintained by
   * the head of the chain, unlink() transfers ownership of this IOBuf from the
   * chain and gives it to the caller.  A new unique_ptr to the IOBuf is
   * returned to the caller.  The caller must store the returned unique_ptr (or
   * call release() on it) to take ownership, otherwise the IOBuf will be
   * immediately destroyed.
   *
   * Since unlink transfers ownership of the IOBuf to the caller, be careful
   * not to call unlink() on the head of a chain if you already maintain
   * ownership on the head of the chain via other means.  The pop() method
   * is a better choice for that situation.
   */
  std::unique_ptr<IOBuf> unlink() {
    next_->prev_ = prev_;
    prev_->next_ = next_;
    prev_ = this;
    next_ = this;
    return std::unique_ptr<IOBuf>(this);
  }

  /**
   * Remove this IOBuf from its current chain and return a unique_ptr to
   * the IOBuf that formerly followed it in the chain.
   */
  std::unique_ptr<IOBuf> pop() {
    IOBuf *next = next_;
    next_->prev_ = prev_;
    prev_->next_ = next_;
    prev_ = this;
    next_ = this;
    return std::unique_ptr<IOBuf>((next == this) ? NULL : next);
  }

  /**
   * Remove a subchain from this chain.
   *
   * Remove the subchain starting at head and ending at tail from this chain.
   *
   * Returns a unique_ptr pointing to head.  (In other words, ownership of the
   * head of the subchain is transferred to the caller.)  If the caller ignores
   * the return value and lets the unique_ptr be destroyed, the subchain will
   * be immediately destroyed.
   *
   * The subchain referenced by the specified head and tail must be part of the
   * same chain as the current IOBuf, but must not contain the current IOBuf.
   * However, the specified head and tail may be equal to each other (i.e.,
   * they may be a subchain of length 1).
   */
  std::unique_ptr<IOBuf> separateChain(IOBuf* head, IOBuf* tail) {
    assert(head != this);
    assert(tail != this);

    head->prev_->next_ = tail->next_;
    tail->next_->prev_ = head->prev_;

    head->prev_ = tail;
    tail->next_ = head;

    return std::unique_ptr<IOBuf>(head);
  }

  /**
   * Return true if at least one of the IOBufs in this chain are shared,
   * or false if all of the IOBufs point to unique buffers.
   *
   * Use isSharedOne() to only check this IOBuf rather than the entire chain.
   */
  bool isShared() const {
    const IOBuf* current = this;
    while (true) {
      if (current->isSharedOne()) {
        return true;
      }
      current = current->next_;
      if (current == this) {
        return false;
      }
    }
  }

  /**
   * Return true if other IOBufs are also pointing to the buffer used by this
   * IOBuf, and false otherwise.
   *
   * If this IOBuf points at a buffer owned by another (non-IOBuf) part of the
   * code (i.e., if the IOBuf was created using wrapBuffer(), or was cloned
   * from such an IOBuf), it is always considered shared.
   *
   * This only checks the current IOBuf, and not other IOBufs in the chain.
   */
  bool isSharedOne() const {
    // If this is a user-owned buffer, it is always considered shared
    if (flags_ & kFlagUserOwned) {
      return true;
    }

    if (flags_ & kFlagExt) {
      return ext_.sharedInfo->refcount.load(std::memory_order_acquire) > 1;
    } else {
      return false;
    }
  }

  /**
   * Ensure that this IOBuf has a unique buffer that is not shared by other
   * IOBufs.
   *
   * unshare() operates on an entire chain of IOBuf objects.  If the chain is
   * shared, it may also coalesce the chain when making it unique.  If the
   * chain is coalesced, subsequent IOBuf objects in the current chain will be
   * automatically deleted.
   *
   * Note that buffers owned by other (non-IOBuf) users are automatically
   * considered shared.
   *
   * Throws std::bad_alloc on error.  On error the IOBuf chain will be
   * unmodified.
   *
   * Currently unshare may also throw std::overflow_error if it tries to
   * coalesce.  (TODO: In the future it would be nice if unshare() were smart
   * enough not to coalesce the entire buffer if the data is too large.
   * However, in practice this seems unlikely to become an issue.)
   */
  void unshare() {
    if (isChained()) {
      unshareChained();
    } else {
      unshareOne();
    }
  }

  /**
   * Ensure that this IOBuf has a unique buffer that is not shared by other
   * IOBufs.
   *
   * unshareOne() operates on a single IOBuf object.  This IOBuf will have a
   * unique buffer after unshareOne() returns, but other IOBufs in the chain
   * may still be shared after unshareOne() returns.
   *
   * Throws std::bad_alloc on error.  On error the IOBuf will be unmodified.
   */
  void unshareOne() {
    if (isSharedOne()) {
      unshareOneSlow();
    }
  }

  /**
   * Coalesce this IOBuf chain into a single buffer.
   *
   * This method moves all of the data in this IOBuf chain into a single
   * contiguous buffer, if it is not already in one buffer.  After coalesce()
   * returns, this IOBuf will be a chain of length one.  Other IOBufs in the
   * chain will be automatically deleted.
   *
   * After coalescing, the IOBuf will have at least as much headroom as the
   * first IOBuf in the chain, and at least as much tailroom as the last IOBuf
   * in the chain.
   *
   * Throws std::bad_alloc on error.  On error the IOBuf chain will be
   * unmodified.  Throws std::overflow_error if the length of the entire chain
   * larger than can be described by a uint32_t capacity.
   */
  void coalesce() {
    if (!isChained()) {
      return;
    }
    coalesceSlow();
  }

  /**
   * Ensure that this chain has at least maxLength bytes available as a
   * contiguous memory range.
   *
   * This method coalesces whole buffers in the chain into this buffer as
   * necessary until this buffer's length() is at least maxLength.
   *
   * After coalescing, the IOBuf will have at least as much headroom as the
   * first IOBuf in the chain, and at least as much tailroom as the last IOBuf
   * that was coalesced.
   *
   * Throws std::bad_alloc on error.  On error the IOBuf chain will be
   * unmodified.  Throws std::overflow_error if the length of the coalesced
   * portion of the chain is larger than can be described by a uint32_t
   * capacity.  (Although maxLength is uint32_t, gather() doesn't split
   * buffers, so coalescing whole buffers may result in a capacity that can't
   * be described in uint32_t.
   *
   * Upon return, either enough of the chain was coalesced into a contiguous
   * region, or the entire chain was coalesced.  That is,
   * length() >= maxLength || !isChained() is true.
   */
  void gather(uint32_t maxLength) {
    if (!isChained() || length_ >= maxLength) {
      return;
    }
    coalesceSlow(maxLength);
  }

  /**
   * Return a new IOBuf chain sharing the same data as this chain.
   *
   * The new IOBuf chain will normally point to the same underlying data
   * buffers as the original chain.  (The one exception to this is if some of
   * the IOBufs in this chain contain small internal data buffers which cannot
   * be shared.)
   */
  std::unique_ptr<IOBuf> clone() const;

  /**
   * Return a new IOBuf with the same data as this IOBuf.
   *
   * The new IOBuf returned will not be part of a chain (even if this IOBuf is
   * part of a larger chain).
   */
  std::unique_ptr<IOBuf> cloneOne() const;

  // Overridden operator new and delete.
  // These directly use malloc() and free() to allocate the space for IOBuf
  // objects.  This is needed since IOBuf::create() manually uses malloc when
  // allocating IOBuf objects with an internal buffer.
  void* operator new(size_t size);
  void* operator new(size_t size, void* ptr);
  void operator delete(void* ptr);

 private:
  enum FlagsEnum {
    kFlagExt = 0x1,
    kFlagUserOwned = 0x2,
    kFlagFreeSharedInfo = 0x4,
  };

  // Values for the ExternalBuf type field.
  // We currently don't really use this for anything, other than to have it
  // around for debugging purposes.  We store it at the moment just because we
  // have the 4 extra bytes in the ExternalBuf struct that would just be
  // padding otherwise.
  enum ExtBufTypeEnum {
    kExtAllocated = 0,
    kExtUserSupplied = 1,
    kExtUserOwned = 2,
  };

  struct SharedInfo {
    SharedInfo();
    SharedInfo(FreeFunction fn, void* arg);

    // A pointer to a function to call to free the buffer when the refcount
    // hits 0.  If this is NULL, free() will be used instead.
    FreeFunction freeFn;
    void* userData;
    std::atomic<uint32_t> refcount;
  };
  struct ExternalBuf {
    uint32_t capacity;
    uint32_t type;
    uint8_t* buf;
    // SharedInfo may be NULL if kFlagUserOwned is set.  It is non-NULL
    // in all other cases.
    SharedInfo* sharedInfo;
  };
  struct InternalBuf {
    uint8_t buf[] __attribute__((aligned));
  };

  // The maximum size for an IOBuf object, including any internal data buffer
  static const uint32_t kMaxIOBufSize = 256;
  static const uint32_t kMaxInternalDataSize;

  // Forbidden copy constructor and assignment opererator
  IOBuf(IOBuf const &);
  IOBuf& operator=(IOBuf const &);

  /**
   * Create a new IOBuf with internal data.
   *
   * end is a pointer to the end of the IOBuf's internal data buffer.
   */
  explicit IOBuf(uint8_t* end);

  /**
   * Create a new IOBuf pointing to an external buffer.
   *
   * The caller is responsible for holding a reference count for this new
   * IOBuf.  The IOBuf constructor does not automatically increment the
   * reference count.
   */
  IOBuf(ExtBufTypeEnum type, uint32_t flags,
        uint8_t* buf, uint32_t capacity,
        uint8_t* data, uint32_t length,
        SharedInfo* sharedInfo);

  void unshareOneSlow();
  void unshareChained();
  void coalesceSlow(size_t maxLength=std::numeric_limits<size_t>::max());
  void decrementRefcount();
  void reserveSlow(uint32_t minHeadroom, uint32_t minTailroom);

  static size_t goodExtBufferSize(uint32_t minCapacity);
  static void initExtBuffer(uint8_t* buf, size_t mallocSize,
                            SharedInfo** infoReturn,
                            uint32_t* capacityReturn);
  static void allocExtBuffer(uint32_t minCapacity,
                             uint8_t** bufReturn,
                             SharedInfo** infoReturn,
                             uint32_t* capacityReturn);

  /*
   * Member variables
   */

  /*
   * Links to the next and the previous IOBuf in this chain.
   *
   * The chain is circularly linked (the last element in the chain points back
   * at the head), and next_ and prev_ can never be NULL.  If this IOBuf is the
   * only element in the chain, next_ and prev_ will both point to this.
   */
  IOBuf* next_;
  IOBuf* prev_;

  /*
   * A pointer to the start of the data referenced by this IOBuf, and the
   * length of the data.
   *
   * This may refer to any subsection of the actual buffer capacity.
   */
  uint8_t* data_;
  uint32_t length_;
  uint32_t flags_;

  union {
    ExternalBuf ext_;
    InternalBuf int_;
  };

  struct DeleterBase {
    virtual ~DeleterBase() { }
    virtual void dispose(void* p) = 0;
  };

  template <class UniquePtr>
  struct UniquePtrDeleter : public DeleterBase {
    typedef typename UniquePtr::pointer Pointer;
    typedef typename UniquePtr::deleter_type Deleter;

    explicit UniquePtrDeleter(Deleter deleter) : deleter_(std::move(deleter)){ }
    void dispose(void* p) {
      try {
        deleter_(static_cast<Pointer>(p));
        delete this;
      } catch (...) {
        abort();
      }
    }

   private:
    Deleter deleter_;
  };

  static void freeUniquePtrBuffer(void* ptr, void* userData) {
    static_cast<DeleterBase*>(userData)->dispose(ptr);
  }
};

template <class UniquePtr>
typename std::enable_if<detail::IsUniquePtrToSL<UniquePtr>::value,
                        std::unique_ptr<IOBuf>>::type
IOBuf::takeOwnership(UniquePtr&& buf, size_t count) {
  size_t size = count * sizeof(typename UniquePtr::element_type);
  CHECK_LT(size, size_t(std::numeric_limits<uint32_t>::max()));
  auto deleter = new UniquePtrDeleter<UniquePtr>(buf.get_deleter());
  return takeOwnership(buf.release(),
                       size,
                       &IOBuf::freeUniquePtrBuffer,
                       deleter);
}

inline std::unique_ptr<IOBuf> IOBuf::copyBuffer(
    const void* data, uint32_t size, uint32_t headroom,
    uint32_t minTailroom) {
  uint32_t capacity = headroom + size + minTailroom;
  std::unique_ptr<IOBuf> buf = create(capacity);
  buf->advance(headroom);
  memcpy(buf->writableData(), data, size);
  buf->append(size);
  return buf;
}

} // folly

#endif // FOLLY_IO_IOBUF_H_
