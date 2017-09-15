#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "cachelib_nvm/common/CompilerUtils.hpp"

namespace facebook {
namespace cachelib {

// node information for the double linked list modelling the lru. It has the
// previous, next information with the last time the item was updated in the
// LRU.
template <typename T>
struct CACHELIB_PACKED_ATTR DListHook {
  using Time = uint32_t;
  using CompressedPtr = typename T::CompressedPtr;
  using PtrCompressor = typename T::PtrCompressor;

  void setNext(T* const n, const PtrCompressor& compressor) noexcept {
    next_ = compressor.compress(n);
  }

  void setPrev(T* const p, const PtrCompressor& compressor) noexcept {
    prev_ = compressor.compress(p);
  }

  T* getNext(const PtrCompressor& compressor) const noexcept {
    return compressor.unCompress(next_);
  }

  T* getPrev(const PtrCompressor& compressor) const noexcept {
    return compressor.unCompress(prev_);
  }

  // set and get the time when the node was updated in the lru.
  void setUpdateTime(Time time) noexcept { updateTime_ = time; }
  Time getUpdateTime() const noexcept { return updateTime_; }

 private:
  CompressedPtr next_{}; // next node in the linked list
  CompressedPtr prev_{}; // previous node in the linked list
  // timestamp when this was last updated to the head of the list
  Time updateTime_{0};
};

// uses a double linked list to implement an LRU. T must be have a public
// member of type Hook and HookPtr must point to that.
template <typename T, DListHook<T> T::*HookPtr>
class DList {
 public:
  using CompressedPtr = typename T::CompressedPtr;
  using PtrCompressor = typename T::PtrCompressor;

  DList() = default;
  DList(const DList&) = delete;
  DList& operator=(const DList&) = delete;

  explicit DList(PtrCompressor compressor) noexcept
      : compressor_(std::move(compressor)) {}

  T* getNext(const T& node) const noexcept {
    return (node.*HookPtr).getNext(compressor_);
  }

  T* getPrev(const T& node) const noexcept {
    return (node.*HookPtr).getPrev(compressor_);
  }

  void setNext(T& node, T* next) noexcept {
    (node.*HookPtr).setNext(next, compressor_);
  }

  void setPrev(T& node, T* prev) noexcept {
    (node.*HookPtr).setPrev(prev, compressor_);
  }

  // Links the passed node to the head of the double linked list
  // @param node node to be linked at the head
  void linkAtHead(T& node) noexcept;

  // Links the passed node to the tail of the double linked list
  // @param node node to be linked at the tail
  void linkAtTail(T& node) noexcept;

  // Add node before nextNode.
  //
  // @param nextNode    node before which to insert
  // @param node        node to insert
  // @note nextNode must be in the list and node must not be in the list
  void insertBefore(T& nextNode, T& node) noexcept;

  // removes the node completely from the linked list and cleans up the node
  // appropriately by setting its next and prev as nullptr.
  void remove(T& node) noexcept;

  // Unlinks the destination node and replaces it with the source node
  //
  // @param oldNode   destination node
  // @param newNode   source node
  void replace(T& oldNode, T& newNode) noexcept;

  // moves a node that belongs to the linked list to the head of the linked
  // list.
  void moveToHead(T& node) noexcept;

  T* getHead() const noexcept { return head_; }
  T* getTail() const noexcept { return tail_; }

  size_t size() const noexcept { return size_; }

  // Iterator interface for the double linked list. Supports both iterating
  // from the tail and head.
  class Iterator {
   public:
    enum class Direction { FROM_HEAD, FROM_TAIL };

    Iterator(T* p, Direction d, const DList<T, HookPtr>& dlist) noexcept
        : curr_(p), dir_(d), dlist_(&dlist) {}
    virtual ~Iterator() = default;

    // copyable and movable
    Iterator(const Iterator&) = default;
    Iterator& operator=(const Iterator&) = default;
    Iterator(Iterator&&) noexcept = default;
    Iterator& operator=(Iterator&&) noexcept = default;

    // moves the iterator forward and backward. Calling ++ once the iterator
    // has reached the end is undefined.
    Iterator& operator++() noexcept;
    Iterator& operator--() noexcept;

    T* operator->() const noexcept { return curr_; }
    T& operator*() const noexcept { return *curr_; }

    bool operator==(const Iterator& other) const noexcept {
      return dlist_ == other.dlist_ && curr_ == other.curr_ &&
             dir_ == other.dir_;
    }

    bool operator!=(const Iterator& other) const noexcept {
      return !(*this == other);
    }

    explicit operator bool() const noexcept {
      return curr_ != nullptr && dlist_ != nullptr;
    }

    T* get() const noexcept { return curr_; }

    // Invalidates this iterator
    void reset() noexcept { curr_ = nullptr; }

    // Reset the iterator back to the beginning
    void resetToBegin() noexcept {
      curr_ = dir_ == Direction::FROM_HEAD ? dlist_->head_ : dlist_->tail_;
    }

   protected:
    void goForward() noexcept;
    void goBackward() noexcept;

    // the current position of the iterator in the list
    T* curr_{nullptr};
    // the direction we are iterating.
    Direction dir_{Direction::FROM_HEAD};
    const DList<T, HookPtr>* dlist_{nullptr};
  };

  // provides an iterator starting from the head of the linked list.
  Iterator begin() const noexcept;

  // provides an iterator starting from the tail of the linked list.
  Iterator rbegin() const noexcept;

  // Iterator to compare against for the end.
  Iterator end() const noexcept;
  Iterator rend() const noexcept;

 private:
  // unlinks the node from the linked list. Does not correct the next and
  // previous.
  void unlink(const T& node) noexcept;

  const PtrCompressor compressor_{};

  // head of the linked list
  T* head_{nullptr};

  // tail of the linked list
  T* tail_{nullptr};

  // size of the list
  size_t size_{0};
};
}
}

#include "DList-inl.hpp"
