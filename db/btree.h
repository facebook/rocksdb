//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the BTree will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.(B-link tree)
//
//

#pragma once
#include <assert.h>
#include <stdlib.h>
#include <stack>
#include "util/arena.h"
#include "port/port.h"
#include "util/arena.h"

namespace rocksdb {

template<typename Key, class Comparator>
class BTree {
 private:
  struct Node;
  struct IndexEntry;

 public:
  // Create a new BTree object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  //
  // TODO Actually I'll create node from existing node when inserting, and after relink other references,
  // I'll remove the original node from memory. Is it possible to do that with arena?
  explicit BTree(Comparator cmp, Arena* arena, 
    int32_t fanOutFactor = 128, int32_t maxNodeSize = 128);

  // Insert key into the tree.
  // REQUIRES: nothing that compares equal to key is currently in the tree.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the tree.
  bool Contains(const Key& key) const;

  // Iteration over the contents of a btree
  class Iterator {
   public:
    // Initialize an iterator over the specified tree.
    // The returned iterator is not valid.
    explicit Iterator(const BTree* tree);

    // Change the underlying btree used for this iterator
    // This enables us not changing the iterator without deallocating
    // an old one and then allocating a new one
    void SetTree(const BTree* tree);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in tree.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in tree.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const BTree* tree_;
    Node* node_; // leaf node logically used as the first on the delta nodes chain
    int offset_; // cursor into aggretaged_ vector
    
    // TODO 
    // In order to prevent invalidating the iterator, I'll use Epoch mechanism [Levandoski]
    // to defer garbage collect unused nodes. Since the only two access patterns are when inserting and
    // Accessing Iterator, one can implement Epoch around those two usecases

  // Maps from logical node id to physical pointer to a Node. Adopted from Bw-Tree.
  class MappingTable {
  public:
    MappingTable(Arena* arena, int capacity = 10000) 
    : arena_(arena), capacity_(capacity) {
      char* mem = arena_->AllocateAligned(sizeof(Node*) * capacity);
      table_ = (Node**) mem; 
      size_ = 0;
    }

    Node* getNode(int pid) const {
      assert(pid < size_);
      return table_[pid];
    }

    // No need to synchronize since externally synchronized, and newly being added
    int addNode(Node* node) {
      assert(size_ < capacity_);
      int pid = size_;
      table_[pid] = node;
      size_++;
      return pid;
    }

    void updateNode(int pid, Node* node) {
      assert(pid < size_);
      // TODO Do I need memory barrier here?
      table_[pid] = node;
    }
    
  private:
    Arena* const arena_;
    Node** table_;
    int capacity_;
    int size_;
  };

 private:
  const int32_t kFanOut_;
  const int32_t kMaxNodeSize_;

  // Immutable after construction
  Comparator const compare_;
  Arena* const arena_;    // Arena used for allocations of nodes
  MappingTable mappingTable_;
  int rootPid_;

  Node* NewNode(int numEntries, int8_t type);
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return the earliest node that comes at or after key.
  // Return nullptr for first element if there is no such node.
  std::tuple<Node*, int, std::unique_ptr<class std::stack<Node*> > > FindGreaterOrEqual(const Key& key) const;

  // Return the latest node with a key < key.
  // Return nullptr if there is no such node.
  std::tuple<Node*, int> FindLessThan(const Key& key) const;

  // Return nullptr if tree is empty
  std::tuple<Node*, int> FindFirst() const;

  // Return the last node in the tree.
  // Return nullptr if tree is empty.
  std::tuple<Node*, int, std::unique_ptr<std::stack<Node*> > > FindLast() const;

  bool splitIsNecessary(Node* node);

  std::tuple<Node*, Node*> splitWithAddedEntry(Node* node, int depthIndex, int entryIndex, const IndexEntry & entry);

  // No copying allowed
  BTree(const BTree&);
  void operator=(const BTree&);
};

template<typename Key, class Comparator>
struct BTree<Key, Comparator>::IndexEntry {
  Key key;
  union {
    int32_t pid; // logical pointer to next level node at mapping table
    Key obj; // actual pointer to object  
  };
};

// Implementation details follow
template<typename Key, class Comparator>
struct BTree<Key, Comparator>::Node {
  explicit Node(int32_t ne, int8_t t)
  : type(t), numEntries(ne) {
    nextDelta = nullptr;
    highKey = nullptr;
    // lowKey = nullptr;
    linkPtrPid = -1;
    lowPtrPid = -1;
  }

  static const uint8_t NODE_TYPE_INDEX = 0;
  static const uint8_t NODE_TYPE_LEAF = 1;

  int32_t pid;
  int8_t type;
  Node* nextDelta;
  Key highKey; // inclusive, upper bound of current node's keys(except at root node)
  // Key lowKey; // exclusive
  int32_t linkPtrPid;
  int32_t lowPtrPid; // Pointer to child node with range (-infty, k0]
  int32_t const numEntries;
  IndexEntry entries[1];

  // Depth of btree delta nodes chain
  int depth() {
    int depth = 0;
    Node* node = this;
    while (node != nullptr) {
      depth++;
      node = node->nextDelta;
    }
    return depth;
  }

  Node* operator[](int index) {
    assert(index >= 0);
    Node* ret = this;
    while (index > 0) {
      ret = ret->nextDelta;
      if (ret == nullptr) return nullptr;
      index--;
    }
    return ret;
  }

  int numEntriesSum() {
    int sum = 0;
    Node* node = this;
    while (node != nullptr) {
      sum += node->numEntries;
      node = node->nextDelta;
    }
    return sum;
  }
 private: 
};

template<typename Key, class Comparator>
typename BTree<Key, Comparator>::Node*
BTree<Key, Comparator>::NewNode(int numEntries, int8_t type) {
  char* mem = arena_->AllocateAligned(sizeof(Node) + sizeof(IndexEntry) * (numEntries - 1));
  return new (mem) Node(numEntries, type);
}

template<typename Key, class Comparator>
inline BTree<Key, Comparator>::Iterator::Iterator(const BTree* tree) {
  SetTree(tree);
}

template<typename Key, class Comparator>
inline void BTree<Key, Comparator>::Iterator::SetTree(const BTree* tree) {
  tree_ = tree;
  node_ = nullptr;
  offset_ = -1;
}

template<typename Key, class Comparator>
inline bool BTree<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
  // TODO return fase if we are pointing to rightmost index entry of the rightmost node
}

template<typename Key, class Comparator>
inline const Key& BTree<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return aggregated_[offset_].obj;
}

template<typename Key, class Comparator>
inline void BTree<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  if (offset_ == (int)(aggregated_.size() - 1)) {
    node_ = tree_->mappingTable_.getNode(node_->linkPtrPid);
    if (node_ == nullptr) {
      offset_ = -1;
      return;
    }
    aggregateToVector();
    offset_ = 0;
  } else {
    offset_++;
  }
}

template<typename Key, class Comparator>
inline void BTree<Key, Comparator>::Iterator::Prev() {
  // We search for the last node that falls before key.
  assert(Valid());
  if (offset_ == 0) {
    Key currentKey = aggregated_[0].key;
    auto tuple = tree_->FindLessThan(currentKey);
    node_ = std::get<0>(tuple);
    if (node_ == nullptr) { // Reached beginning of the tree.
      offset_ = -1;
      return;
    }
    aggregateToVector();
    // A possible corner case would be that a new key lower than current 
    // node's lowest was added and it was prepended in the current node.
    // Then we shouldn't just start from the back end of the vector
    offset_ = -1;
    for (int i = aggregated_.size() - 1; i >= 0; i--) {
      if (tree_->compare_(currentKey, aggregated_[i].key) < 0) {
        offset_ = i;
        break;
      }
    }
    assert(offset != -1);
  } else {
    offset_--;
  }
}

template<typename Key, class Comparator>
inline void BTree<Key, Comparator>::Iterator::Seek(const Key& target) {
  auto tuple = tree_->FindGreaterOrEqual(target);
  node_ = std::get<0>(tuple);
  if (node_ == nullptr) {
    offset_ = -1;
    return;
  }
  Key key = (*node_)[std::get<1>(tuple)]->entries[std::get<2>(tuple)].key;
  aggregateToVector();
  //TODO Could improve by using Binary search
  offset_ = -1;
  for (int i = 0; i < (int)aggregated_.size(); i++) {
    if (tree_->compare_(aggregated_[i].key, key) == 0) {
      offset_ = i;
      break;
    }
  }
  assert(offset_ != -1);
}

template<typename Key, class Comparator>
inline void BTree<Key, Comparator>::Iterator::SeekToFirst() {
  auto tuple = tree_->FindFirst();
  node_ = std::get<0>(tuple);
  if (node_ == nullptr) {
    offset_ = -1;  
    return;
  }
  aggregateToVector();
  offset_ = 0;
}

template<typename Key, class Comparator>
inline void BTree<Key, Comparator>::Iterator::SeekToLast() {
  auto tuple = tree_->FindLast();
  node_ = std::get<0>(tuple);
  if (node_ == nullptr) {
    offset_ = -1;
    return;
  }
  aggregateToVector();
  offset_ = aggregated_.size() - 1;
}

template<typename Key, class Comparator>
std::tuple<typename BTree<Key, Comparator>::Node*, int, int, std::unique_ptr<class std::stack<typename BTree<Key, Comparator>::Node*> > > 
BTree<Key, Comparator>::FindGreaterOrEqual(const Key& key) const {
  Node* x = mappingTable_.getNode(rootPid_);
  auto stack = std::unique_ptr<class std::stack<Node*> >(new std::stack<Node*>());
  
  int depthIndex = 0;
  int entryIndex = -1;
  while (true) {
    assert(x != nullptr);
    // TODO In case parent routed us to wrong(being splitted) node,
    // We might need to look at the next linked node
    // We can conveniently check with high_key
    if (x->highKey != nullptr && compare_(key, x->highKey) > 0) {
      // searching key is greater than highest key in this node
      if (x->linkPtrPid == -1) {
        // Reached the end of the tree
        return std::make_tuple(nullptr, -1, -1, std::move(stack));
      }
      // Move to next node on the same level
      x = mappingTable_.getNode(x->linkPtrPid);
      continue;
    }
    stack->push(x);

    Key curLowestKey = nullptr;
    int depth = x->depth();
    for (int i = 0; i < depth; i++) {// Iterate through delta chains
      int left = 0;
      int right = (*x)[i]->numEntries;
      int middle = -1;
      // Binary search
      while (left < right) {
        middle = (left + right) / 2;
        int compared = compare_(key, (*x)[i]->entries[middle].key);
        if (compared == 0) {
          break;
        } else if (compared < 0) {
          // Search left
          right = middle;
        } else {
          // Search right
          left = middle + 1;
        }
      }
      assert(left == right);
      int selectedIndex = left;// This index is either equal key, or least greatest than search key on this depth
      // In case we are at root node, and highKey is not defined but key is greater than any root node's keys
      if (selectedIndex < (*x)[i]->numEntries) {
        Key selectedKey = (*x)[i]->entries[selectedIndex].key;
        assert(compare_(selectedKey, key) >= 0); // If we got here, selectedKey should always greater than or equal to search key
        if (curLowestKey == nullptr || compare_(selectedKey, curLowestKey) < 0) {
          // key <= selectedKey < curLowestKey
          curLowestKey = selectedKey;
          depthIndex = i;
          entryIndex = selectedIndex;
        }
      }
    }

    if ((*x)[depthIndex]->type == Node::NODE_TYPE_INDEX) {
      if (entryIndex == 0) {
        // our search key is less than any of the nodes' entries
        x = mappingTable_.getNode((*x)[depthIndex]->lowPtrPid);
      } else if (curLowestKey == nullptr) { 
        // We are at root and search key is greater than any root nodes' entries
        int numEntries = (*x)[depthIndex]->numEntries;
        x = mappingTable_.getNode((*x)[depthIndex]->entries[numEntries - 1].pid);
      } else {
        x = mappingTable_.getNode((*x)[depthIndex]->entries[entryIndex - 1].pid);
      }
    } else {
      // We are at the leaf
      if (curLowestKey == nullptr) {
        // Current node is root node, and search key is greater than highest key in root node
        // Reached the end of the tree, and greater or equal key not found
        return std::make_tuple(nullptr, -1, -1, std::move(stack));
      } else {
        return std::make_tuple(x, depthIndex, entryIndex, std::move(stack)); 
      }
    }
  }
}

template<typename Key, class Comparator>
typename std::tuple<typename BTree<Key, Comparator>::Node*, int, int>
BTree<Key, Comparator>::FindLessThan(const Key& key) const {
  // TODO TBD. Similar to FindGreaterOrEqualTo
  return std::make_tuple(nullptr, -1, -1);
}

template<typename Key, class Comparator>
typename std::tuple<typename BTree<Key, Comparator>::Node*, int, int>
BTree<Key, Comparator>::FindFirst()
    const {
  return std::make_tuple(nullptr, -1, -1);
}

template<typename Key, class Comparator>
typename std::tuple<typename BTree<Key, Comparator>::Node*, int, int, std::unique_ptr<class std::stack<typename BTree<Key, Comparator>::Node*> > >
BTree<Key, Comparator>::FindLast()
    const {
  return std::make_tuple(nullptr, -1, -1, nullptr);
}

template<typename Key, class Comparator>
BTree<Key, Comparator>::BTree(const Comparator cmp, Arena* arena, int32_t fanOutFactor, int32_t maxNodeSize)
    : kFanOut_(fanOutFactor),
      kMaxNodeSize_(maxNodeSize),
      compare_(cmp),
      arena_(arena),
      mappingTable_(arena) {
  assert(kFanOut_ > 0);
  assert(kMaxNodeSize_ > 0);
  Node* root = NewNode(0, Node::NODE_TYPE_LEAF);
  rootPid_ = mappingTable_.addNode(root);
}

template<typename Key, class Comparator>
bool BTree<Key, Comparator>::splitIsNecessary(Node* node) {
  assert(node != nullptr);
  return node->numEntriesSum() >= kMaxNodeSize_;
}

// Create two new Nodes 
template<typename Key, class Comparator>
typename std::tuple<typename BTree<Key, Comparator>::Node*, typename BTree<Key, Comparator>::Node*> 
BTree<Key, Comparator>::splitWithAddedEntry(Node* node, int entryIndex, const IndexEntry & entry) {
  // Split function must consolidate the original and new node, so that calculating numEntries
  // would return correct value.
  int newSize = 1 + node->numEntries; // number of total elements in node including the element to be added
  
  Node* left = NewNode(newSize / 2, node->type);
  Node* right = NewNode(newSize - left->numEntries, node->type);

  bool newEntryAdded = false;
  Node* cur;
  int nodeIndex = 0;
  for (int i = 0; i < newSize; i++) {
    if (i < left->numEntries) {
      cur = left;
      nodeIndex = i;
    } else {
      cur = right;
      nodeIndex = i - left->numEntries;
    }

    Key min = nullptr;
    int minNodeIdx = -1;
    for (int j = 0; j < chainDepth; j++) {
      if (idx[j] < numEntries[j]) {
        Key compared = node->entries[idx[j]].key;
        if (min == nullptr || compare_(compared, min) < 0) {
          min = compared;
          minNodeIdx = j;
        }  
      }
    }
    if (!newEntryAdded && compare_(entry.key, min) < 0) {
      // First time entry's key is smaller than min key
      cur->entries[nodeIndex] = entry;
      newEntryAdded = true;
      continue;//Do calculation again
    }

    assert(minNodeIdx != -1 && min != nullptr);
    int accesingIdx = idx[minNodeIdx]++;
    cur->entries[nodeIndex] = (*node)[minNodeIdx]->entries[accesingIdx];
  }

  // Extract right node's low ptr, only if node is non-leaf
  if (node->type == Node::NODE_TYPE_INDEX) {
    right->lowPtrPid = left->entries[left->numEntries - 1].pid;
    left->entries[left->numEntries - 1].pid = -1;
  }

  right->pid = mappingTable_.addNode(right);
  right->nextDelta = nullptr;
  right->highKey = node->highKey;
  right->linkPtrPid = node->linkPtrPid;

  left->pid = node->pid;
  left->nextDelta = nullptr;
  left->highKey = left->entries[left->numEntries - 1].key;
  left->linkPtrPid = right->pid;
  left->lowPtrPid = node->lowPtrPid;  

  mappingTable_.updateNode(left->pid, left);
  return std::make_tuple(left, right);
}

template<typename Key, class Comparator>
void BTree<Key, Comparator>::Insert(const Key& key) {
  Node* node;
  int depthIndex;
  int entryIndex;
  unique_ptr<std::stack<Node*> > stack;
  std::tie(node, depthIndex, entryIndex, stack) = FindGreaterOrEqual(key);

  // Our data structure does not allow duplicate insertion
  assert(node == nullptr || !Equal(key, (*node)[depthIndex]->entries[entryIndex].key));

  if (node == nullptr) {
    // Need to insert at the end of the tree
    std::tie(node, depthIndex, entryIndex, stack) = FindLast();
  }
  
  IndexEntry newEntry;
  newEntry.key = key;
  newEntry.obj = key;
  while (!stack->empty()) {
    node = stack->top();
    stack->pop();
    if (splitIsNecessary(node)) {
      Node* left;
      Node* right;
      std::tie(left, right) = splitWithAddedEntry(node, depthIndex, entryIndex, newEntry);
      // left->right->old left's next are all connected at this point.
      // IndexEntry to insert on the upper level
      newEntry.key = left->highKey;
      newEntry.pid = right->pid;
      if (stack->empty()) {
        // We are at the top of the tree, but need to split.(We need to create a new root)
        // TODO
      }
    } else {
      // Append a delta to current node and then exit
      // TODO In case delta chain depth is too deep, we should consolidate the node,
      // instead of attaching yet another delta node.
      Node* delta = NewNode(1, node->type);
      delta->pid = node->pid;
      delta->nextDelta = node;
      delta->highKey = node->highKey;
      delta->linkPtrPid = node->linkPtrPid;
      delta->lowPtrPid = node->lowPtrPid;
      assert(delta->numEntries == 1);
      delta->entries[0] = newEntry;

      mappingTable_.updateNode(delta->pid, delta);
      break;
    }
  }
}

template<typename Key, class Comparator>
bool BTree<Key, Comparator>::Contains(const Key& key) const {
  Node* node;
  int depthIndex;
  int entryIndex;
  std::tie(node, depthIndex, entryIndex, std::ignore) = FindGreaterOrEqual(key);
  if (node != nullptr && Equal(key, (*node)[depthIndex]->entries[entryIndex].key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace rocksdb
