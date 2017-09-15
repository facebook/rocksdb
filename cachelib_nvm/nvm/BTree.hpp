#pragma once

#include <type_traits>

#include "cachelib_nvm/nvm/BTreeNode.hpp"
#include "cachelib_nvm/nvm/Utils.hpp"

namespace facebook {
namespace cachelib {
namespace details {
inline constexpr size_t bucketSize(size_t bytes) {
  // This extra branch is never real for B tree
  // if (bytes <= 8) {
  //   return 8;
  // }
  if (bytes <= 128) {
    return util::alignSize(bytes, 16);
  }
  if (bytes <= 512) {
    return util::alignSize(bytes, 64);
  }
  if (bytes <= 4096) {
    return util::alignSize(bytes, 256);
  }
  // This is not exact, but shouldn't be needed.
  return util::alignSize(bytes, 4096);
}
}

struct BTreeMemoryStats {
  size_t leafUsed{};
  size_t leafAllocated{};
  size_t nodeUsed{};
  size_t nodeAllocated{};

  size_t totalUsed() const { return leafUsed + nodeUsed; }
  size_t totalAllocated() const { return leafAllocated + nodeAllocated; }
};

// B+ tree (actually, variation of (a,b)-tree).
//
// Use void* in all places where it is not known if the pointer is a node or
// a leaf. It makes all sites where we decide about type explicit.
template <typename KeyT, typename ValueT>
class BTree {
 public:
  static_assert(
      std::is_integral<KeyT>::value && std::is_trivial<ValueT>::value,
      "B tree type requirements fail");

  using Key = KeyT;
  using Value = ValueT;

  using Node = BTreeNode<BTreeNodeType::Node, Key, void*>;
  using Leaf = BTreeNode<BTreeNodeType::Leaf, Key, Value>;

  BTree(uint32_t nodeSize, uint32_t smallLeafSize, uint32_t largeLeafSize)
      : nodeSize_{nodeSize},
        smallLeafSize_{smallLeafSize},
        largeLeafSize_{largeLeafSize} {
    root_ = trackNew(Leaf::create(smallLeafSize_));
  }

  BTree(const BTree& other) = delete;
  BTree(BTree&& other) noexcept
      : nodeSize_{other.nodeSize_},
        smallLeafSize_{other.smallLeafSize_},
        largeLeafSize_{other.largeLeafSize_},
        root_{other.root_},
        height_{other.height_},
        size_{other.size_} {
    memoryUsed_[0] = other.memoryUsed_[0];
    memoryUsed_[1] = other.memoryUsed_[1];
    memoryAllocated_[0] = other.memoryAllocated_[0];
    memoryAllocated_[1] = other.memoryAllocated_[1];
    other.reset();
  }

  BTree& operator=(const BTree& other) = delete;
  BTree& operator=(BTree&& other) noexcept {
    nodeSize_ = other.nodeSize_;
    smallLeafSize_ = other.smallLeafSize_;
    largeLeafSize_ = other.largeLeafSize_;
    root_ = other.root_;
    height_ = other.height_;
    size_ = other.size_;
    memoryUsed_[0] = other.memoryUsed_[0];
    memoryUsed_[1] = other.memoryUsed_[1];
    memoryAllocated_[0] = other.memoryAllocated_[0];
    memoryAllocated_[1] = other.memoryAllocated_[1];
    other.reset();
    return *this;
  }

  ~BTree() { destroy(); }

  void CACHELIB_NOINLINE destroy() {
    destroyRec(height_, root_);
    reset();
  }

  void CACHELIB_NOINLINE insert(Key key, Value value) {
    auto r = insertRec(key, value, height_, root_);
    if (r.ptr2 != nullptr) {
      auto root = trackNew(Node::create(nodeSize_));
      root->size_ = 1;
      root->keys_[0] = r.median;
      root->data_[0] = r.ptr1;
      root->data_[1] = r.ptr2;
      root_ = root;
      height_++;
    } else if (r.ptr1 != nullptr) {
      root_ = r.ptr1;
    }
  }

  bool CACHELIB_NOINLINE remove(Key key) {
    auto r = removeRec(key, height_, root_);
    if (r.merge && height_ > 0) {
      auto root = reinterpret_cast<Node*>(root_);
      if (root->size() == 0) {
        root_ = root->data_[0];
        trackDelete(root)->destroy();
        height_--;
      }
    } else if (r.subst != nullptr) {
      root_ = r.subst;
    }
    return r.found;
  }

  bool CACHELIB_NOINLINE lookup(Key key, Value& value) const {
    auto p = root_;
    for (auto h = height_; h != 0; h--) {
      assert(p != nullptr);
      auto node = reinterpret_cast<Node*>(p);
      p = node->data_[node->find(key)];
    }
    auto leaf = reinterpret_cast<Leaf*>(p);
    auto i = leaf->find(key);
    if (i < leaf->size() && leaf->keyAt(i) == key) {
      value = leaf->data_[i];
      return true;
    }
    return false;
  }

  size_t size() const { return size_; }
  uint32_t height() const { return height_; }

  BTreeMemoryStats getMemoryStats() const {
    BTreeMemoryStats stats;
    stats.leafUsed = memoryUsed_[0];
    stats.leafAllocated = memoryAllocated_[0];
    stats.nodeUsed = memoryUsed_[1];
    stats.nodeAllocated = memoryAllocated_[1];
    return stats;
  }

  // Simplified tree merge. Every key is the this tree must be less that every
  // key in the other.
  void mergeWith(BTree&& other) {
    assert(nodeSize_ == other.nodeSize_ &&
           smallLeafSize_ == other.smallLeafSize_ &&
           largeLeafSize_ == other.largeLeafSize_);
    // Ok even if @root_ is a leaf
    auto root = reinterpret_cast<Node*>(root_);
    auto splitKey = maxKey();
    assert(splitKey < other.minKey());
    if (height_ == other.height_) {
      // Grow tree up
      // TODO: Check roots conform Btree node load factor condition
      root = trackNew(Node::create(nodeSize_));
      root->size_ = 1;
      root->keys_[0] = splitKey;
      root->data_[0] = root_;
      root->data_[1] = other.root_;
      root_ = root;
      height_++;
    } else if (height_ > other.height()) {
      for (auto h = height_; h > other.height_ + 1; h--) {
        root = reinterpret_cast<Node*>(root->data_[root->size_]);
        assert(root != nullptr);
      }
      assert(root->size() < root->capacity());
      root->keys_[root->size_] = splitKey;
      root->data_[root->size_ + 1] = other.root_;
      root->size_ += 1;
    } else {
      // Right tree has more height
      assert(false); // Not implemented
      return;
    }
    size_ += other.size();
    memoryUsed_[0] += other.memoryUsed_[0];
    memoryUsed_[1] += other.memoryUsed_[1];
    memoryAllocated_[0] += other.memoryAllocated_[0];
    memoryAllocated_[1] += other.memoryAllocated_[1];
    other.reset();
  }

  Key minKey() const {
    auto p = root_;
    for (auto h = height_; h != 0; h--) {
      auto node = reinterpret_cast<Node*>(p);
      p = node->data_[0];
    }
    auto leaf = reinterpret_cast<Leaf*>(p);
    return leaf->keys_[0];
  }

  Key maxKey() const {
    auto p = root_;
    // Rightmost node always has a pointer @size_
    for (auto h = height_; h != 0; h--) {
      auto node = reinterpret_cast<Node*>(p);
      p = node->data_[node->size_];
      assert(p != nullptr);
    }
    assert(p != nullptr);
    auto leaf = reinterpret_cast<Leaf*>(p);
    return leaf->keys_[leaf->size_ - 1];
  }

  // For debug/test purposes only
  void* root() { return root_; }

 private:
  Node* trackNew(Node* node) {
    trackNew(1, Node::allocSize(node->capacity()));
    return node;
  }
  Leaf* trackNew(Leaf* leaf) {
    trackNew(0, Leaf::allocSize(leaf->capacity()));
    return leaf;
  }
  void trackNew(uint32_t i, size_t bytes) {
    memoryUsed_[i] += bytes;
    memoryAllocated_[i] += details::bucketSize(bytes);
  }

  Node* trackDelete(Node* node) {
    trackDelete(1, Node::allocSize(node->capacity()));
    return node;
  }
  Leaf* trackDelete(Leaf* leaf) {
    trackDelete(0, Leaf::allocSize(leaf->capacity()));
    return leaf;
  }
  void trackDelete(uint32_t i, size_t bytes) {
    memoryUsed_[i] -= bytes;
    memoryAllocated_[i] -= details::bucketSize(bytes);
  }

  struct InsertOutput {
    Key median{};
    void* ptr1{};
    void* ptr2{};
  };
  InsertOutput insertRec(Key key, Value value, uint32_t height, void* ptr);

  struct RemoveOutput {
    void* subst{}; // Substitute node (reallocated to a smaller one)
    bool merge{false};
    bool found{false};
  };
  // Returns true if node became too small needs merge
  RemoveOutput removeRec(Key key, uint32_t height, void* ptr);

  template <typename Child>
  void rebalance(Node* node,
                 uint32_t i,
                 uint32_t smallCapacity,
                 uint32_t largeCapacity) {
    if (i > 0 && i + 1 <= node->size() && node->data_[i + 1] != nullptr) {
      // If both neighbours present, we can choose a better one.
      auto prev = reinterpret_cast<Child*>(node->data_[i - 1]);
      auto next = reinterpret_cast<Child*>(node->data_[i + 1]);
      if (prev->size() < largeCapacity / 2 ||
          next->size() < largeCapacity / 2) {
        // If any of them is smaller than half, choose the smallest and merge
        // into one large node.
        if (prev->size() < next->size()) {
          assert(prev->capacity() == smallCapacity);
          merge<Child>(node, largeCapacity, i - 1);
        } else {
          assert(next->capacity() == smallCapacity);
          merge<Child>(node, largeCapacity, i);
        }
      } else {
        // One of two neighbours is more than 50% populated. Pick more populated
        // and replace with two small leaves. Merges are stopped.
        if (prev->size() > next->size()) {
          mergeAndSplit<Child>(node, smallCapacity, i - 1);
        } else {
          mergeAndSplit<Child>(node, smallCapacity, i);
        }
      }
    } else if (i > 0) {
      // Deal with prev neighbour only
      auto prev = reinterpret_cast<Child*>(node->data_[i - 1]);
      if (prev->size() < largeCapacity / 2) {
        merge<Child>(node, largeCapacity, i - 1);
      } else {
        mergeAndSplit<Child>(node, smallCapacity, i - 1);
      }
    } else {
      assert(i == 0);
      auto next = reinterpret_cast<Child*>(node->data_[i + 1]);
      if (next->size() < largeCapacity / 2) {
        // If any of them is smaller than half, choose the smallest and merge
        // into one large node.
        merge<Child>(node, largeCapacity, i);
      } else {
        mergeAndSplit<Child>(node, smallCapacity, i);
      }
    }
  }

  template <typename Child>
  void mergeAndSplit(Node* node, uint32_t capacity, uint32_t first) {
    auto l1 = reinterpret_cast<Child*>(node->data_[first]);
    auto l2 = reinterpret_cast<Child*>(node->data_[first + 1]);
    auto d1 = trackNew(Child::create(capacity));
    auto d2 = trackNew(Child::create(capacity));
    Child::rebalance(*l1, *l2, (l1->size() + l2->size()) / 2, *d1, *d2);
    node->data_[first] = d1;
    node->keys_[first] = d1->lastKey();
    node->data_[first + 1] = d2;
    trackDelete(l1)->destroy();
    trackDelete(l2)->destroy();
  }

  template <typename Child>
  void merge(Node* node, uint32_t capacity, uint32_t first) {
    auto large = trackNew(Child::create(capacity));
    auto l1 = reinterpret_cast<Child*>(node->data_[first]);
    auto l2 = reinterpret_cast<Child*>(node->data_[first + 1]);
    large->merge(*l1, *l2);
    node->remove(first);
    // When I removed first, next will have index @first
    node->data_[first] = large;
    // Check if this is a child with a key
    trackDelete(l1)->destroy();
    trackDelete(l2)->destroy();
  }

  void destroyRec(uint32_t height, void* ptr) {
    if (ptr == nullptr) {
      return;
    }
    if (height == 0) {
      trackDelete(reinterpret_cast<Leaf*>(ptr))->destroy();
    } else {
      auto node = reinterpret_cast<Node*>(ptr);
      for (uint32_t i = 0; i <= node->size_; i++) {
        destroyRec(height - 1, node->data_[i]);
      }
      trackDelete(node)->destroy();
    }
  }

  void reset() {
    root_ = nullptr;
    size_ = 0;
    height_ = 0;
    memoryUsed_[0] = 0;
    memoryUsed_[1] = 0;
    memoryAllocated_[0] = 0;
    memoryAllocated_[1] = 0;
  }

  uint32_t nodeSize_{};
  uint32_t smallLeafSize_{};
  uint32_t largeLeafSize_{};
  void* root_{};
  size_t height_{}; // Grows from leaf: leaf has height of 0
  size_t size_{};
  size_t memoryUsed_[2]{};
  size_t memoryAllocated_[2]{};
};

template <typename K, typename V>
auto BTree<K, V>::insertRec(K key, V value, uint32_t height, void* ptr)
    -> InsertOutput {
  assert(ptr != nullptr);
  InsertOutput res;
  if (height == 0) {
    auto leaf = reinterpret_cast<Leaf*>(ptr);
    auto i = leaf->find(key);
    if (i < leaf->size() && leaf->keyAt(i) == key) {
      leaf->data_[i] = value;
      return res;
    }
    if (leaf->size() < leaf->capacity()) {
      leaf->insert(i, key, value);
    } else if (leaf->capacity() < largeLeafSize_) {
      auto largeLeaf = trackNew(Leaf::create(largeLeafSize_));
      largeLeaf->copyFrom(*leaf);
      largeLeaf->insert(i, key, value);
      res.ptr1 = largeLeaf;
      trackDelete(leaf)->destroy();
    } else {
      auto leaf1 = trackNew(Leaf::create(smallLeafSize_));
      auto leaf2 = trackNew(Leaf::create(smallLeafSize_));
      leaf->split(*leaf1, *leaf2, leaf->size() / 2);
      trackDelete(leaf)->destroy();
      res.median = leaf1->lastKey();
      res.ptr1 = leaf1;
      res.ptr2 = leaf2;
      if (key <= res.median) {
        leaf1->insert(i, key, value);
      } else {
        leaf2->insert(i - leaf1->size(), key, value);
      }
    }
    size_++;
  } else {
    auto node = reinterpret_cast<Node*>(ptr);
    auto i = node->find(key);
    auto r = insertRec(key, value, height - 1, node->data_[i]);
    if (r.ptr2 != nullptr) {
      assert(node->capacity() == nodeSize_);
      if (node->size() < node->capacity()) {
        node->insert(i, r.median, r.ptr1);
        node->data_[i + 1] = r.ptr2;
      } else {
        auto node1 = trackNew(Node::create(nodeSize_));
        auto node2 = trackNew(Node::create(nodeSize_));
        node->split(*node1, *node2, node->size() / 2);
        trackDelete(node)->destroy();
        res.median = node1->lastKey();
        res.ptr1 = node1;
        res.ptr2 = node2;
        if (r.median <= res.median) {
          node1->insert(i, r.median, r.ptr1);
          node1->data_[i + 1] = r.ptr2;
        } else {
          i -= node1->size();
          node2->insert(i, r.median, r.ptr1);
          node2->data_[i + 1] = r.ptr2;
        }
      }
    } else if (r.ptr1 != nullptr) {
      // Leaf was resized
      node->data_[i] = r.ptr1;
    }
  }
  return res;
}

template <typename K, typename V>
auto BTree<K, V>::removeRec(K key, uint32_t height, void* ptr) -> RemoveOutput {
  RemoveOutput res;
  if (height == 0) {
    auto leaf = reinterpret_cast<Leaf*>(ptr);
    auto i = leaf->find(key);
    if (i < leaf->size() && leaf->keyAt(i) == key) {
      res.found = true;
      // Remove it anyways, let code above the stack merge
      leaf->remove(i);
      size_--;
      if (leaf->size() < largeLeafSize_ / 2 &&
          leaf->capacity() != smallLeafSize_) {
        auto smallLeaf = trackNew(Leaf::create(smallLeafSize_));
        smallLeaf->copyFrom(*leaf);
        res.subst = smallLeaf;
        trackDelete(leaf)->destroy();
        leaf = smallLeaf;
      }
      res.merge = leaf->size() < smallLeafSize_ / 2;
    }
    return res;
  }

  auto node = reinterpret_cast<Node*>(ptr);
  auto i = node->find(key);
  auto r = removeRec(key, height - 1, node->data_[i]);
  res.found = r.found;
  if (!r.merge) {
    if (r.subst != nullptr) {
      node->data_[i] = r.subst;
    }
  } else {
    assert(r.subst == nullptr);
    if (height == 1) {
      rebalance<Leaf>(node, i, smallLeafSize_, largeLeafSize_);
    } else {
      rebalance<Node>(node, i, nodeSize_, nodeSize_);
    }
    res.merge = node->size() < std::max(nodeSize_ / 3, 1u);
  }
  return res;
}
}
}
