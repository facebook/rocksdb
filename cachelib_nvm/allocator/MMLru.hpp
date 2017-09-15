#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>

#include "cachelib_nvm/allocator/Cache.hpp"
#include "cachelib_nvm/allocator/CacheStats.hpp"
#include "cachelib_nvm/allocator/Util.hpp"
#include "cachelib_nvm/allocator/datastruct/DList.hpp"
#include "cachelib_nvm/common/CompilerUtils.hpp"
#include "cachelib_nvm/common/Logging.hpp"

namespace facebook {
namespace cachelib {

class MMLru {
 public:
  // unique identifier per MMType
  static const int kId;

  template <typename T>
  using Hook = DListHook<T>;

  struct Config {
    Config(uint32_t time, bool updateOnW, bool updateOnR)
        : Config(time, updateOnW, updateOnR, false, 0) {}

    Config(uint32_t time, bool updateOnW, bool updateOnR, uint8_t ipSpec)
        : Config(time, updateOnW, updateOnR, false, ipSpec) {}

    Config(uint32_t time,
           bool updateOnW,
           bool updateOnR,
           bool tryLockU,
           uint8_t ipSpec)
        : lruRefreshTime(time),
          updateOnWrite(updateOnW),
          updateOnRead(updateOnR),
          tryLockUpdate(tryLockU),
          lruInsertionPointSpec(ipSpec) {}

    Config() = default;
    Config(const Config& rhs) = default;
    Config(Config&& rhs) = default;

    Config& operator=(const Config& rhs) = default;
    Config& operator=(Config&& rhs) = default;

    // threshold value in seconds to compare with a node's update time to
    // determine if we need to update the position of the node in the linked
    // list. By default this is 60s to reduce the contention on the lru lock.
    uint32_t lruRefreshTime{60};

    // whether the lru needs to be updated on writes for recordAccess. If
    // false, accessing the cache for writes does not promote the cached item
    // to the head of the lru.
    bool updateOnWrite{false};

    // whether the lru needs to be updated on reads for recordAccess. If
    // false, accessing the cache for reads does not promote the cached item
    // to the head of the lru.
    bool updateOnRead{true};

    // whether to tryLock or lock the lru lock when attempting promotion on
    // access. If set, and tryLock fails, access will not result in promotion.
    bool tryLockUpdate{false};

    // By default insertions happen at the head of the LRU. If we need
    // insertions at the middle of lru we can adjust this to be a non-zero.
    // Ex: lruInsertionPointSpec = 1, we insert at the middle (1/2 from end)
    //     lruInsertionPointSpec = 2, we insert at a point 1/4th from tail
    uint8_t lruInsertionPointSpec{0};
  };

  // The container object which can be used to keep track of objects of type
  // T. T must have a public member of type Hook. This object is wrapper
  // around DList, is thread safe and can be accessed from multiple threads.
  // The current implementation models an LRU using the above DList
  // implementation.
  template <typename T, Hook<T> T::*HookPtr>
  struct Container {
   private:
    using LruList = DList<T, HookPtr>;
    using Mutex = std::mutex;
    using LockHolder = std::unique_lock<Mutex>;
    using PtrCompressor = typename T::PtrCompressor;
    using Time = typename Hook<T>::Time;
    using CompressedPtr = typename T::CompressedPtr;
    using Flags = typename T::Flags;

    // protects all operations on the lru. We never really just read the state
    // of the LRU. Hence we dont really require a RW mutex at this point of
    // time.
    mutable Mutex lruMutex_;

    const PtrCompressor compressor_{};

    // the lru
    LruList lru_{};

    // insertion point
    T* insertionPoint_{nullptr};

    // size of tail after insertion point
    size_t tailSize_{0};

    uint64_t evictions_{0};

    // number of inserts into the LRU
    uint64_t numLockByInserts_{0};

    // number of lock hits by calling recordAccess
    uint64_t numLockByRecordAccesses_{0};

    // number of lock hits by calling recordAccess on nodes not in mmContainer
    uint64_t numLockByRemoves_{0};

    // Config for this lru.
    // Write access to the MMLru Config is serialized.
    // Reads may be racy.
    Config config_{};

    static Time getUpdateTime(const T& node) noexcept {
      return (node.*HookPtr).getUpdateTime();
    }

    static void setUpdateTime(T& node, Time time) noexcept {
      (node.*HookPtr).setUpdateTime(time);
    }

    // temporarily public until CacheAllocator integration in TAO
   public:
    // remove node from lru and adjust insertion points
    //
    // @param node          node to remove
    // @parma ctx           RemoveContext
    void removeLocked(T& node, RemoveContext ctxt) noexcept;

   private:
    // This function is invoked by remove or record access prior to
    // removing the node (or moving the node to head) to ensure that
    // the node being moved is not the insertion point and if it is
    // adjust it accordingly.
    void ensureNotInsertionPoint(T& node) noexcept;

    // update the lru insertion point after doing an insert or removal.
    // We need to ensure the insertionPoint_ is set to the correct node
    // to maintain the tailSize_, for the next insertion.
    void updateLruInsertionPoint() noexcept;

    // Bit MM_BIT_0 is used to record if the item is in tail. This
    // is used to implement LRU insertion points
    void markTail(T& node) noexcept {
      node.template setFlag<Flags::MM_FLAG_0>();
    }
    void unmarkTail(T& node) noexcept {
      node.template unSetFlag<Flags::MM_FLAG_0>();
    }
    bool isTail(T& node) const noexcept {
      return node.template isFlagSet<Flags::MM_FLAG_0>();
    }

    // Bit MM_BIT_1 is used to record if the item has been accessed since being
    // written in cache. Unaccessed items are ignored when determining projected
    // update time.
    void markAccessed(T& node) noexcept {
      node.template setFlag<Flags::MM_FLAG_1>();
    }
    void unmarkAccessed(T& node) noexcept {
      node.template unSetFlag<Flags::MM_FLAG_1>();
    }
    bool isAccessed(const T& node) const noexcept {
      return node.template isFlagSet<Flags::MM_FLAG_1>();
    }

    // returns age of the nth node from the tail of the list,
    // where n = 0 is the tail
    //
    // @param n number of items to bypass from the tail
    // @param ignoreUntouched if true, only elements that have been marked
    //                        'accessed' are considered after bypassing
    //                        n items from the tail
    // @param limit the maximum number of items (accessed or unaccessed) to
    //              iterate over
    Time getNthFromTailAgeLocked(size_t n,
                                 bool ignoreUntouched,
                                 size_t limit,
                                 Time currTime) const noexcept;

   public:
    Container() = default;
    Container(Config c, PtrCompressor compressor)
        : compressor_(std::move(compressor)),
          lru_(compressor_),
          config_(std::move(c)) {}

    Container(const Container&) = delete;
    Container& operator=(const Container&) = delete;

    // context for iterating the MM container. At any given point of time,
    // there can be only one iterator active since we need to lock the LRU for
    // iteration. we can support multiple iterators at same time, by using a
    // shared ptr in the context for the lock holder in the future.
    using NodeT = T;

    class LruIterator : public LruList::Iterator {
     public:
      // noncopyable but movable.
      LruIterator(const LruIterator&) = delete;
      LruIterator& operator=(const LruIterator&) = delete;

      LruIterator(LruIterator&&) noexcept = default;

      // 1. Invalidate this iterator
      // 2. Unlock
      void destroy() {
        LruList::Iterator::reset();
        if (l_.owns_lock()) {
          l_.unlock();
        }
      }

      // Reset this iterator to the beginning
      void resetToBegin() {
        if (!l_.owns_lock()) {
          l_.lock();
        }
        LruList::Iterator::resetToBegin();
      }

     private:
      // private because it's easy to misuse and cause deadlock for MMLru
      LruIterator& operator=(LruIterator&&) noexcept = default;

      // create an lru iterator with the lock being held.
      LruIterator(LockHolder l,
                  const typename LruList::Iterator& iter) noexcept;

      // only the container can create iterators
      friend Container<T, HookPtr>;

      // lock protecting the validity of the iterator
      LockHolder l_;
    };

    using Iterator = LruIterator;

    Config getConfig() { return config_; }

    void setConfig(const Config& newConfig) {
      LockHolder l(lruMutex_);
      config_ = newConfig;
      if (config_.lruInsertionPointSpec == 0 && insertionPoint_ != nullptr) {
        auto curr = insertionPoint_;
        while (tailSize_ != 0) {
          assert(curr != nullptr);
          unmarkTail(*curr);
          tailSize_--;
          curr = lru_.getNext(*curr);
        }
        insertionPoint_ = nullptr;
      }
    }

    bool isEmpty() const noexcept {
      LockHolder l(lruMutex_);
      return lru_.size() == 0;
    }

    size_t size() const noexcept { return lru_.size(); }

    // TODO: to be removed once Tao moves fully onto cachelib rebalancing
    //
    // returns the nth node from the tail of the list, where n = 0 is the tail;
    //
    // @param n number of items to bypass from the tail
    // @param ignoreUntouched if true, only elements that have been marked
    //                        'accessed' are considered after bypassing
    //                        n items from the tail
    // @param limit the maximum number of items (accessed or unaccessed) to
    //              iterate over
    // @return the nth node from the tail of the list, subject to above
    //         considerations; return nullptr if no accessed node is found
    //         after iterating through 'limit' items

    // LRU lock must be held when calling this
    const T* getNthFromTailLocked(size_t n,
                                  bool ignoreUntouched,
                                  size_t limit) const noexcept;

    // Returns the eviction age stats. See CacheStats.hpp for details
    EvictionAgeStat getEvictionAgeStat(uint64_t limit,
                                       uint64_t limitMax,
                                       bool ignoreUntouched) const noexcept;

    // records the information that the node was accessed. This could bump up
    // the node to the head of the lru depending on the time when the node was
    // last updated in lru and the kLruRefreshTime. If the node was moved to
    // the head in the lru, the node's updateTime will be updated
    // accordingly.
    //
    // @param node  node that we want to mark as relevant/accessed
    // @param mode  the mode for the access operation.
    void recordAccess(T& node, AccessMode mode) noexcept;

    // adds the given node into the container and marks it as being present in
    // the container. The node is added to the head of the lru.
    //
    // @param node  The node to be added to the container.
    // @return  True if the node was successfully added to the container. False
    //          if the node was already in the contianer. On error state of node
    //          is unchanged.
    bool add(T& node) noexcept;

    // removes the node from the lru and sets it previous and next to nullptr.
    //
    // @param node  The node to be removed from the container.
    // @param ctxt  The context for removal.
    // @return  True if the node was successfully removed from the container.
    //          False if the node was not part of the container. On error, the
    //          state of node is unchanged.
    bool remove(T& node, RemoveContext ctxt = RemoveContext::kNormal) noexcept;

    // same as the above but uses an iterator context. The iterator is updated
    // on removal of the corresponding node to point to the next node. The
    // iterator context holds the lock on the lru.
    //
    // iterator will be advanced to the next node after removing the node
    //
    // @param it    Iterator that will be removed
    // @param ctxt  The context for removal. If this is an eviction, then
    //              bumps up the appropriate stats.
    void remove(LruIterator& it,
                RemoveContext ctxt = RemoveContext::kNormal) noexcept;

    // replaces one node with another, at the same position
    //
    // @param oldNode   node being replaced
    // @param newNode   node to replace oldNode with
    //
    // @return true  If the replace was successful. Returns false if the
    //               destination node did not exist in the container, or if the
    //               source node already existed.
    bool replace(T& oldNode, T& newNode) noexcept;

    // Obtain an iterator that start from the tail and can be used
    // to search for evictions. This iterator holds a lock to this
    // container and only one such iterator can exist at a time
    LruIterator getEvictionIterator() const noexcept;

    // return the stats for this container.
    MMContainerStat getStats() const noexcept;
  };
};
}
}

#include "MMLru-inl.hpp"
