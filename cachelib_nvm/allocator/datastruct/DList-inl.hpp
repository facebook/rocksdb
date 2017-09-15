namespace facebook {
namespace cachelib {

/* Linked list implemenation */
template <typename T, DListHook<T> T::*HookPtr>
void DList<T, HookPtr>::linkAtHead(T& node) noexcept {
  assert(&node != head_);

  setNext(node, head_);
  setPrev(node, nullptr);
  // fix the prev ptr of head
  if (head_ != nullptr) {
    setPrev(*head_, &node);
  }
  head_ = &node;
  if (tail_ == nullptr) {
    tail_ = &node;
  }
  size_++;
}

template <typename T, DListHook<T> T::*HookPtr>
void DList<T, HookPtr>::linkAtTail(T& node) noexcept {
  assert(&node != tail_);

  setNext(node, nullptr);
  setPrev(node, tail_);
  // Fix the next ptr for tail
  if (tail_ != nullptr) {
    setNext(*tail_, &node);
  }
  tail_ = &node;
  if (head_ == nullptr) {
    head_ = &node;
  }
  size_++;
}

template <typename T, DListHook<T> T::*HookPtr>
void DList<T, HookPtr>::insertBefore(T& nextNode, T& node) noexcept {
  assert(&nextNode != &node);
  assert(getNext(node) == nullptr);
  assert(getPrev(node) == nullptr);

  auto* const prev = getPrev(nextNode);

  assert(prev != &node);

  setPrev(node, prev);
  if (prev != nullptr) {
    setNext(*prev, &node);
  } else {
    head_ = &node;
  }

  setPrev(nextNode, &node);
  setNext(node, &nextNode);
  size_++;
}

template <typename T, DListHook<T> T::*HookPtr>
void DList<T, HookPtr>::unlink(const T& node) noexcept {
  assert(size_ > 0);
  // fix head_ and tail_ if the node is either of that.
  auto* const prev = getPrev(node);
  auto* const next = getNext(node);

  if (&node == head_) {
    head_ = next;
  }
  if (&node == tail_) {
    tail_ = prev;
  }

  // fix the next and prev ptrs of the node before and after us.
  if (prev != nullptr) {
    setNext(*prev, next);
  }
  if (next != nullptr) {
    setPrev(*next, prev);
  }
  size_--;
}

template <typename T, DListHook<T> T::*HookPtr>
void DList<T, HookPtr>::remove(T& node) noexcept {
  unlink(node);
  setNext(node, nullptr);
  setPrev(node, nullptr);
}

template <typename T, DListHook<T> T::*HookPtr>
void DList<T, HookPtr>::replace(T& oldNode, T& newNode) noexcept {
  // Update head and tail links if needed
  if (&oldNode == head_) {
    head_ = &newNode;
  }
  if (&oldNode == tail_) {
    tail_ = &newNode;
  }

  // Make the previous and next nodes point to the new node
  auto* const prev = getPrev(oldNode);
  auto* const next = getNext(oldNode);
  if (prev != nullptr) {
    setNext(*prev, &newNode);
  }
  if (next != nullptr) {
    setPrev(*next, &newNode);
  }

  // Make the new node point to the previous and next nodes
  setPrev(newNode, prev);
  setNext(newNode, next);

  // Cleanup the old node
  setPrev(oldNode, nullptr);
  setNext(oldNode, nullptr);
}

template <typename T, DListHook<T> T::*HookPtr>
void DList<T, HookPtr>::moveToHead(T& node) noexcept {
  if (&node == head_) {
    return;
  }
  unlink(node);
  linkAtHead(node);
}

/* Iterator Implementation */
template <typename T, DListHook<T> T::*HookPtr>
void DList<T, HookPtr>::Iterator::goForward() noexcept {
  if (dir_ == Direction::FROM_TAIL) {
    curr_ = dlist_->getPrev(*curr_);
  } else {
    curr_ = dlist_->getNext(*curr_);
  }
}

template <typename T, DListHook<T> T::*HookPtr>
void DList<T, HookPtr>::Iterator::goBackward() noexcept {
  if (dir_ == Direction::FROM_TAIL) {
    curr_ = dlist_->getNext(*curr_);
  } else {
    curr_ = dlist_->getPrev(*curr_);
  }
}

template <typename T, DListHook<T> T::*HookPtr>
typename DList<T, HookPtr>::Iterator& DList<T, HookPtr>::Iterator::
operator++() noexcept {
  assert(curr_ != nullptr);
  if (curr_ != nullptr) {
    goForward();
  }
  return *this;
}

template <typename T, DListHook<T> T::*HookPtr>
typename DList<T, HookPtr>::Iterator& DList<T, HookPtr>::Iterator::
operator--() noexcept {
  assert(curr_ != nullptr);
  if (curr_ != nullptr) {
    goBackward();
  }
  return *this;
}

template <typename T, DListHook<T> T::*HookPtr>
typename DList<T, HookPtr>::Iterator DList<T, HookPtr>::begin() const noexcept {
  return DList<T, HookPtr>::Iterator(head_, Iterator::Direction::FROM_HEAD,
                                     *this);
}

template <typename T, DListHook<T> T::*HookPtr>
typename DList<T, HookPtr>::Iterator DList<T, HookPtr>::rbegin() const
    noexcept {
  return DList<T, HookPtr>::Iterator(tail_, Iterator::Direction::FROM_TAIL,
                                     *this);
}

template <typename T, DListHook<T> T::*HookPtr>
typename DList<T, HookPtr>::Iterator DList<T, HookPtr>::end() const noexcept {
  return DList<T, HookPtr>::Iterator(nullptr, Iterator::Direction::FROM_HEAD,
                                     *this);
}

template <typename T, DListHook<T> T::*HookPtr>
typename DList<T, HookPtr>::Iterator DList<T, HookPtr>::rend() const noexcept {
  return DList<T, HookPtr>::Iterator(nullptr, Iterator::Direction::FROM_TAIL,
                                     *this);
}
}
}
