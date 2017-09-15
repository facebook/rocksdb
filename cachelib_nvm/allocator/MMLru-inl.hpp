namespace facebook {
namespace cachelib {

template <typename T, MMLru::Hook<T> T::*HookPtr>
void MMLru::Container<T, HookPtr>::recordAccess(T& node,
                                                AccessMode mode) noexcept {
  if ((mode == AccessMode::kWrite && !config_.updateOnWrite) ||
      (mode == AccessMode::kRead && !config_.updateOnRead)) {
    return;
  }

  const auto curr = static_cast<Time>(util::getCurrentTimeSec());
  // check if the node is still being memory managed
  if (node.isInMMContainer() &&
      ((curr >= getUpdateTime(node) + config_.lruRefreshTime) ||
       !isAccessed(node))) {
    if (!isAccessed(node)) {
      markAccessed(node);
    }
    LockHolder l(lruMutex_, std::defer_lock);
    if (config_.tryLockUpdate) {
      l.try_lock();
    } else {
      l.lock();
    }
    if (l.owns_lock()) {
      ensureNotInsertionPoint(node);
      ++numLockByRecordAccesses_;
      if (node.isInMMContainer()) {
        lru_.moveToHead(node);
        setUpdateTime(node, curr);
      }
      if (isTail(node)) {
        unmarkTail(node);
        tailSize_--;
        DCHECK_LE(0, tailSize_);
        updateLruInsertionPoint();
      }
    }
  }
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat MMLru::Container<T, HookPtr>::getEvictionAgeStat(
    uint64_t limit, uint64_t limitMax, bool ignoreUntouched) const noexcept {
  EvictionAgeStat stat;
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());

  LockHolder l(lruMutex_);
  stat.oldestElementAge =
      getNthFromTailAgeLocked(0, ignoreUntouched, limitMax, currTime);
  stat.projectedAge =
      getNthFromTailAgeLocked(limit, ignoreUntouched, limitMax, currTime);
  return stat;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
typename MMLru::Container<T, HookPtr>::Time
MMLru::Container<T, HookPtr>::getNthFromTailAgeLocked(size_t n,
                                                      bool ignoreUntouched,
                                                      size_t limit,
                                                      Time currTime) const
    noexcept {
  const auto* node = getNthFromTailLocked(n, ignoreUntouched, limit);
  return node ? currTime - getUpdateTime(*node) : currTime;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
const T* MMLru::Container<T, HookPtr>::getNthFromTailLocked(
    size_t n, bool ignoreUntouched, size_t limit) const noexcept {
  const T* node = lru_.getTail();
  for (size_t numSeen = 0; numSeen < limit && node != nullptr;
       numSeen++, node = lru_.getPrev(*node)) {
    if (ignoreUntouched && !isAccessed(*node)) {
      continue;
    }
    if (numSeen >= n) {
      return node;
    }
  }
  return nullptr;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
void MMLru::Container<T, HookPtr>::updateLruInsertionPoint() noexcept {
  if (config_.lruInsertionPointSpec == 0) {
    return;
  }

  // If insertionPoint_ is nullptr initialize it to tail first
  if (insertionPoint_ == nullptr) {
    insertionPoint_ = lru_.getTail();
    tailSize_ = 0;
    if (insertionPoint_ != nullptr) {
      markTail(*insertionPoint_);
      tailSize_++;
    }
  }

  if (lru_.size() <= 1) {
    // we are done;
    return;
  }

  DCHECK_NE(static_cast<T*>(nullptr), insertionPoint_);

  const auto expectedSize = lru_.size() >> config_.lruInsertionPointSpec;
  auto curr = insertionPoint_;

  while (tailSize_ < expectedSize && curr != lru_.getHead()) {
    curr = lru_.getPrev(*curr);
    markTail(*curr);
    tailSize_++;
  }

  while (tailSize_ > expectedSize && curr != lru_.getTail()) {
    unmarkTail(*curr);
    tailSize_--;
    curr = lru_.getNext(*curr);
  }

  insertionPoint_ = curr;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
bool MMLru::Container<T, HookPtr>::add(T& node) noexcept {
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());
  LockHolder l(lruMutex_);
  ++numLockByInserts_;
  if (node.isInMMContainer()) {
    return false;
  }
  if (config_.lruInsertionPointSpec == 0 || insertionPoint_ == nullptr) {
    lru_.linkAtHead(node);
  } else {
    lru_.insertBefore(*insertionPoint_, node);
  }
  node.markInMMContainer();
  setUpdateTime(node, currTime);
  unmarkAccessed(node);
  updateLruInsertionPoint();
  return true;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
typename MMLru::Container<T, HookPtr>::LruIterator
MMLru::Container<T, HookPtr>::getEvictionIterator() const noexcept {
  LockHolder l(lruMutex_);
  return LruIterator{std::move(l), lru_.rbegin()};
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
void MMLru::Container<T, HookPtr>::ensureNotInsertionPoint(T& node) noexcept {
  // If we are removing the insertion point node, grow tail before we remove
  // so that insertionPoint_ is valid (or nullptr) after removal
  if (&node == insertionPoint_) {
    insertionPoint_ = lru_.getPrev(*insertionPoint_);
    if (insertionPoint_ != nullptr) {
      tailSize_++;
      markTail(*insertionPoint_);
    } else {
      DCHECK_EQ(lru_.size(), 1);
    }
  }
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
void MMLru::Container<T, HookPtr>::removeLocked(T& node,
                                                RemoveContext ctxt) noexcept {
  ensureNotInsertionPoint(node);
  lru_.remove(node);
  unmarkAccessed(node);
  if (ctxt == RemoveContext::kEviction) {
    evictions_++;
  }
  if (isTail(node)) {
    unmarkTail(node);
    tailSize_--;
  }
  node.unmarkInMMContainer();
  updateLruInsertionPoint();
  return;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
bool MMLru::Container<T, HookPtr>::remove(T& node,
                                          RemoveContext ctxt) noexcept {
  LockHolder l(lruMutex_);
  ++numLockByRemoves_;
  if (!node.isInMMContainer()) {
    return false;
  }
  removeLocked(node, ctxt);
  return true;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
void MMLru::Container<T, HookPtr>::remove(LruIterator& it,
                                          RemoveContext ctxt) noexcept {
  T& node = *it;
  assert(node.isInMMContainer());
  ++it;
  removeLocked(node, ctxt);
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
bool MMLru::Container<T, HookPtr>::replace(T& oldNode, T& newNode) noexcept {
  LockHolder l(lruMutex_);
  if (!oldNode.isInMMContainer() || newNode.isInMMContainer()) {
    return false;
  }
  const auto updateTime = getUpdateTime(oldNode);
  lru_.replace(oldNode, newNode);
  oldNode.unmarkInMMContainer();
  newNode.markInMMContainer();
  setUpdateTime(newNode, updateTime);
  if (isAccessed(oldNode)) {
    markAccessed(newNode);
  } else {
    unmarkAccessed(newNode);
  }
  assert(!isTail(newNode));
  if (isTail(oldNode)) {
    markTail(newNode);
    unmarkTail(oldNode);
  } else {
    unmarkTail(newNode);
  }
  if (insertionPoint_ == &oldNode) {
    insertionPoint_ = &newNode;
  }
  return true;
}

template <typename T, MMLru::Hook<T> T::*HookPtr>
MMContainerStat MMLru::Container<T, HookPtr>::getStats() const noexcept {
  LockHolder l(lruMutex_);
  auto* tail = lru_.getTail();
  return {lru_.size(),
          tail == nullptr ? 0 : getUpdateTime(*tail),
          evictions_,
          numLockByInserts_,
          numLockByRecordAccesses_,
          numLockByRemoves_,
          0,
          0,
          0};
}

// Iterator Context Implementation
template <typename T, MMLru::Hook<T> T::*HookPtr>
MMLru::Container<T, HookPtr>::LruIterator::LruIterator(
    LockHolder l, const typename LruList::Iterator& iter) noexcept
    : LruList::Iterator(iter), l_(std::move(l)) {}
}
}
