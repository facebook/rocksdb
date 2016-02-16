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
#include "util/allocator.h"
#include <iostream>
#include <atomic>
#include "port/port.h"

namespace rocksdb {

template<typename Key, class Comparator>
class BTree {
 private:
  struct Node;
  struct IndexEntry;

 public:
  // Create a new BTree object that will use "cmp" for comparing keys,
  // and will allocate memory using "*allocator".  Objects allocated in the
  // allocator must remain allocated for the lifetime of the bw-tree object.
  //
  // TODO Figure out how to deallocate allocated memory when using allocator(Epoch mechanism from original paper)
  explicit BTree(Comparator cmp, Allocator* allocator, int32_t maxNodeSize = 64);

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
    int offset_; // offset within the current btree node
    
    // TODO 
    // In order to prevent invalidating the iterator, I'll use Epoch mechanism [Levandoski]
    // to defer garbage collect unused nodes. Since the only two access patterns are when inserting and
    // Accessing Iterator, one can implement Epoch around those two usecases
  };
  // Maps from logical node id to physical pointer to a Node. Adopted from Bw-Tree.
  class MappingTable {
  public:
    MappingTable(Allocator* allocator, int capacity = 400000) 
    : allocator_(allocator), capacity_(capacity), size_(0) {
      char* mem = allocator_->AllocateAligned(sizeof(std::atomic<Node*>) * capacity);
      table_ = (std::atomic<Node*>*) mem; 
    }

    Node* getNode(int pid) const {
      return table_[pid].load(std::memory_order_consume);
    }

    // No need to synchronize since externally synchronized, and newly being added
    int addNode(Node* node) {
      assert(size_.load(std::memory_order_consume) < capacity_);
      int pid = size_.load(std::memory_order_consume);
      table_[pid].store(node, std::memory_order_release);
      size_.store(pid + 1, std::memory_order_release);
      return pid;
    }

    void updateNode(int pid, Node* node) {
      table_[pid].store(node, std::memory_order_release);
    }
    
  private:
    Allocator* const allocator_;
    std::atomic<Node*>* table_;
    int capacity_;
    std::atomic<int> size_;
  };

 private:
  const int32_t kMaxNodeSize_;

  // Immutable after construction
  Comparator const compare_;
  Allocator* const allocator_;    // Allocator used for allocations of nodes
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
  // Else, return the left-most leaf node
  std::tuple<Node*, int> FindFirst() const;

  // Return the last node in the tree.
  // Return nullptr if tree is empty.
  std::tuple<Node*, int, std::unique_ptr<std::stack<Node*> > > FindLast() const;

  bool splitIsNecessary(Node* node);

  std::tuple<Node*, Node*> splitWithAddedEntry(Node* node, const IndexEntry & entry, bool shouldUpdateEntryKey, const Key updateEntryKey);

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
    linkPtrPid = -1;
    highPtrPid = -1;
  }

  static const uint8_t NODE_TYPE_INDEX = 0;
  static const uint8_t NODE_TYPE_LEAF = 1;

  int32_t pid;
  int8_t type;
  int32_t linkPtrPid;
  int32_t highPtrPid; // Pointer to child node with range (k[n-1], infinity]. 
                      // If -1, it doesn't exist. 
  int32_t const numEntries;
  IndexEntry entries[1]; // In non-leaf node, a node pointed by entries[i].pid is 
                         // responsible for keys in interval (entries[i-1].key, entries[i].key]

 private: 
};

template<typename Key, class Comparator>
typename BTree<Key, Comparator>::Node*
BTree<Key, Comparator>::NewNode(int numEntries, int8_t type) {
  char* mem = allocator_->AllocateAligned(sizeof(Node) + sizeof(IndexEntry) * std::max(0, numEntries - 1));
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
  return node_ != nullptr && offset_ != -1 && offset_ < node_->numEntries;
  // TODO return fase if we are pointing to rightmost index entry of the rightmost node
}

template<typename Key, class Comparator>
inline const Key& BTree<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->entries[offset_].obj;
}

template<typename Key, class Comparator>
inline void BTree<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  if (offset_ == (int)(node_->numEntries - 1)) {
    if (node_->linkPtrPid == -1) {
      node_ = nullptr;
      offset_ = -1;
      return;
    }
    node_ = tree_->mappingTable_.getNode(node_->linkPtrPid);
    offset_ = 0;
    assert(node_ != nullptr);
  } else {
    offset_++;
  }
}

template<typename Key, class Comparator>
inline void BTree<Key, Comparator>::Iterator::Prev() {
  // We search for the last node that falls before key.
  assert(Valid());
  if (offset_ == 0) {
    Key currentKey = node_->entries[0].key;
    auto tuple = tree_->FindLessThan(currentKey);
    node_ = std::get<0>(tuple);
    offset_ = std::get<1>(tuple);
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
  offset_ = std::get<1>(tuple);
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
  offset_ = node_->numEntries - 1;
}

template<typename Key, class Comparator>
std::tuple<typename BTree<Key, Comparator>::Node*, int, std::unique_ptr<class std::stack<typename BTree<Key, Comparator>::Node*> > > 
BTree<Key, Comparator>::FindGreaterOrEqual(const Key& key) const {
  Node* x = mappingTable_.getNode(rootPid_);
  auto stack = std::unique_ptr<class std::stack<Node*> >(new std::stack<Node*>());

  while (true) {
    assert(x != nullptr);
    // In case parent routed us to wrong(being splitted) node,
    // We might need to look at the next linked node
    if (x->numEntries == 0 || (x->highPtrPid == -1 && compare_(key, x->entries[x->numEntries - 1].key) > 0)) {
      // node is root or searching key is greater than highest key in this node
      if (x->linkPtrPid == -1) {
        // Reached the end of the tree
        return std::make_tuple(nullptr, -1, std::move(stack));
      }
      // Move to next node on the same level
      x = mappingTable_.getNode(x->linkPtrPid);
      continue;
    }
    stack->push(x);

    int left = 0;
    int right = x->numEntries;
    int middle = -1;
    // Binary search
    while (left < right) {
      middle = (left + right) / 2;
      int compared = compare_(key, x->entries[middle].key);
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
    if (left == right) {
      middle = left;
    }
    
    if (middle == x->numEntries) {
      assert(x->highPtrPid != -1 && x->type == Node::NODE_TYPE_INDEX); // This node must have highPtr
      x = mappingTable_.getNode(x->highPtrPid);
    } else {
      if (x->type == Node::NODE_TYPE_INDEX) {
        x = mappingTable_.getNode(x->entries[middle].pid);
      } else { // Leaf node
        return std::make_tuple(x, middle, std::move(stack)); 
      }
    }
  }
}

template<typename Key, class Comparator>
typename std::tuple<typename BTree<Key, Comparator>::Node*, int>
BTree<Key, Comparator>::FindLessThan(const Key& key) const {
  // Find the node where this key would be inserted
  Node* node;
  int entryIndex;
  std::unique_ptr<std::stack<Node*> > stack;
  std::tie(node, entryIndex, stack) = FindGreaterOrEqual(key);
  
  if (node == nullptr) {// If every key in B-Tree is less than key? node == nullptr
      std::tie(node, entryIndex, std::ignore) = FindLast();
      return std::make_tuple(node, entryIndex);
  }

  Key lastEntry;
  
  assert(entryIndex >= 0);
  if (entryIndex != 0) { // Regular case where maximum entry less than key is in the same node
    return std::make_tuple(node, entryIndex - 1);
  }

  while (entryIndex == 0 && node->pid != rootPid_) {
    entryIndex = -1;
    lastEntry = node->entries[node->numEntries - 1].key;
    stack->pop();
    node = stack->top();

    for (int i=0; i<node->numEntries; i++) {
      if (compare_(lastEntry, node->entries[i].key) == 0) {
        entryIndex = i;
      }
    }
  }

  if (entryIndex == 0 && node->pid == rootPid_) {
    return std::make_tuple(nullptr, -1);
  }

  if (entryIndex == -1) {
    entryIndex = node->numEntries;
  }

  assert(entryIndex > 0);
  Key greatestLesserKey = node->entries[entryIndex-1].key;
  assert(compare_(greatestLesserKey, key) < 0);
  // We assert that an entry with above key exists in B-Tree
  Node* foundNode;
  int foundEntryIndex;
  // Find the entry that matches exactly with this given key; It may be inefficient but correct
  std::tie(foundNode, foundEntryIndex, std::ignore) = FindGreaterOrEqual(greatestLesserKey);
  assert(compare_(greatestLesserKey, foundNode->entries[foundEntryIndex].key) == 0);
  assert(foundNode->pid != node->pid);
  assert(foundNode->numEntries - 1 == foundEntryIndex);
  return std::make_tuple(foundNode, foundEntryIndex);
}

template<typename Key, class Comparator>
typename std::tuple<typename BTree<Key, Comparator>::Node*, int>
BTree<Key, Comparator>::FindFirst() const {
  Node* x = mappingTable_.getNode(rootPid_);
  while (true) {
    assert(x != nullptr);
    if (x->type == Node::NODE_TYPE_INDEX) {
      x = mappingTable_.getNode(x->entries[0].pid);
    } else { // Leaf node
      return std::make_tuple(x, 0); 
    }
  }
}

template<typename Key, class Comparator>
typename std::tuple<typename BTree<Key, Comparator>::Node*, int, std::unique_ptr<class std::stack<typename BTree<Key, Comparator>::Node*> > >
BTree<Key, Comparator>::FindLast() const {
  Node* x = mappingTable_.getNode(rootPid_);
  auto stack = std::unique_ptr<class std::stack<Node*> >(new std::stack<Node*>());
  
  while (true) {
    assert(x != nullptr);
    if (x->linkPtrPid != -1) {
      x = mappingTable_.getNode(x->linkPtrPid);
      continue;
    }
    stack->push(x);
    
    if (x->type == Node::NODE_TYPE_INDEX) {
      assert(x->highPtrPid != -1); // On the very right nodes, highPtrPid must not be -1
      x = mappingTable_.getNode(x->highPtrPid);
    } else { // Leaf node
      return std::make_tuple(x, x->numEntries - 1, std::move(stack)); 
    }
  }
}

template<typename Key, class Comparator>
BTree<Key, Comparator>::BTree(const Comparator cmp, Allocator* allocator, int32_t maxNodeSize)
    : kMaxNodeSize_(maxNodeSize),
      compare_(cmp),
      allocator_(allocator),
      mappingTable_(allocator) {
  assert(kMaxNodeSize_ > 0);
  Node* root = NewNode(0, Node::NODE_TYPE_LEAF);
  rootPid_ = mappingTable_.addNode(root);
  root->pid = rootPid_;
}

template<typename Key, class Comparator>
bool BTree<Key, Comparator>::splitIsNecessary(Node* node) {
  assert(node != nullptr);
  return node->numEntries >= kMaxNodeSize_;
}

// Create two new Nodes 
template<typename Key, class Comparator>
typename std::tuple<typename BTree<Key, Comparator>::Node*, typename BTree<Key, Comparator>::Node*> 
BTree<Key, Comparator>::splitWithAddedEntry(Node* node, const IndexEntry & newEntry, bool shouldUpdateEntryKey, const Key updateEntryKey) {
  // Split function must consolidate the original and new node, so that calculating numEntries
  // would return correct value.
  int newSize = 1 + node->numEntries; // number of total elements in node including the element to be added
  
  Node* left = NewNode(newSize / 2, node->type);
  Node* right = NewNode(newSize - left->numEntries, node->type);
  right->pid = mappingTable_.addNode(right);
  right->highPtrPid = node->highPtrPid;
  right->linkPtrPid = node->linkPtrPid;

  left->pid = node->pid;
  left->linkPtrPid = right->pid;

  bool newEntryAdded = false;
  bool prevEntryUpdated = !shouldUpdateEntryKey;
  Node* cur;
  int nodeIndex = 0;
  cur = left;
  int i = 0;
  while (cur != right || nodeIndex < right->numEntries) {
    if (nodeIndex == left->numEntries && cur == left) {
      cur = right;
      nodeIndex = 0;
    }
    
    if (!newEntryAdded &&
        i < node->numEntries && 
        compare_(newEntry.key, node->entries[i].key) < 0) {
      // Normal case(insert in the middle of a node, or at leaf); Insert entry into current index
      cur->entries[nodeIndex] = newEntry;
      newEntryAdded = true;
    } else if (!newEntryAdded &&
        i == node->numEntries) {
      // Fact that it reached here means new key is greater than anything in the node
      if (node->type == Node::NODE_TYPE_INDEX && compare_(newEntry.key, node->entries[node->numEntries - 1].key) > 0) {
        // Second condition distinguishes the children being splitted was to the rightmost of their level
        assert(node->linkPtrPid == -1);
        cur->entries[nodeIndex].key = updateEntryKey;
        cur->entries[nodeIndex].pid = node->highPtrPid;
        cur->highPtrPid = newEntry.pid;  
      } else { // Leaf or children being splitted was second entry from the rightmost
        cur->entries[nodeIndex] = newEntry;
      }
      newEntryAdded = true;
    } else {
      cur->entries[nodeIndex] = node->entries[i];
      if (!prevEntryUpdated && compare_(newEntry.key, node->entries[i].key) == 0) {
        cur->entries[nodeIndex].key = updateEntryKey;
        prevEntryUpdated = true;
      }
      i++;
    }
    nodeIndex++;
  }

  mappingTable_.updateNode(left->pid, left);
  return std::make_tuple(left, right);
}

template<typename Key, class Comparator>
void BTree<Key, Comparator>::Insert(const Key& key) {
  Node* node;
  int entryIndex;
  std::unique_ptr<std::stack<Node*> > stack;
  std::tie(node, entryIndex, stack) = FindGreaterOrEqual(key);

  // Our data structure does not allow duplicate insertion
  assert(node == nullptr || !Equal(key, node->entries[entryIndex].key));

  if (node == nullptr) {
    // Need to insert at the end of the tree
    std::tie(node, entryIndex, stack) = FindLast();
  }
  
  IndexEntry newEntry;
  bool shouldUpdateEntryKey = false;
  Key updateEntryKey;
  newEntry.key = key;
  newEntry.obj = key;
  while (!stack->empty()) {
    node = stack->top();
    stack->pop();
    if (splitIsNecessary(node)) {
      Node* left;
      Node* right;
      std::tie(left, right) = splitWithAddedEntry(node, newEntry, shouldUpdateEntryKey, updateEntryKey);
      // left->right->(old left's next) are all connected at this point.
      // IndexEntry to insert on the parent level
      newEntry.key = right->entries[right->numEntries - 1].key; // highest key contained in the right node
      newEntry.pid = right->pid;
      // When parent is modified, the old parent's indexEntry that pointed to left must be updated with the new high key
      shouldUpdateEntryKey = true;
      updateEntryKey = left->entries[left->numEntries - 1].key; // highest key contained in the left node

      if (stack->empty()) {
        // We are at the top of the tree, but need to split.(We need to create a new root)
        Node* newRoot = NewNode(1, Node::NODE_TYPE_INDEX);
        newRoot->entries[0].key = left->entries[left->numEntries - 1].key;
        newRoot->entries[0].pid = left->pid;
        newRoot->highPtrPid = right->pid;
        newRoot->pid = mappingTable_.addNode(newRoot);
        // TODO Potentially Memory barrier
        rootPid_ = newRoot->pid;
      }
    } else { // Split is unnecessary; Copy current node and replace it after inserting a new IndexEntry
      int newSize = 1 + node->numEntries;
      Node* newNode = NewNode(newSize, node->type);
      newNode->pid = node->pid;
      newNode->highPtrPid = node->highPtrPid;
      newNode->linkPtrPid = node->linkPtrPid;
      int nodeIndex = 0;
      int i = 0;
      bool newEntryAdded = false;
      bool prevEntryUpdated = !shouldUpdateEntryKey;
      while (nodeIndex < newSize) {
        if (!newEntryAdded &&
            i < node->numEntries && 
            compare_(newEntry.key, node->entries[i].key) < 0) {
          // Normal case(insert in the middle of a node, or at leaf); Insert entry into current index
          newNode->entries[nodeIndex] = newEntry;
          newEntryAdded = true;
        } else if (!newEntryAdded &&
            i == node->numEntries) {
          // Fact that it reached here means new key is greater than anything in the node
          if (node->type == Node::NODE_TYPE_INDEX && compare_(newEntry.key, node->entries[node->numEntries - 1].key) > 0) {
            // Second condition distinguishes the children being splitted was to the rightmost of their level
            assert(node->linkPtrPid == -1);
            newNode->entries[nodeIndex].key = updateEntryKey;
            newNode->entries[nodeIndex].pid = node->highPtrPid;
            newNode->highPtrPid = newEntry.pid;  
          } else { // Leaf or children being splitted was second entry from the rightmost
            newNode->entries[nodeIndex] = newEntry;
          }
          newEntryAdded = true;
        } else {
          newNode->entries[nodeIndex] = node->entries[i];
          if (!prevEntryUpdated && compare_(newEntry.key, node->entries[i].key) == 0) {
            newNode->entries[nodeIndex].key = updateEntryKey;
            prevEntryUpdated = true;
          }
          i++;
        }
        nodeIndex++;
      }
      mappingTable_.updateNode(newNode->pid, newNode);
      break;
    }
  }
}

template<typename Key, class Comparator>
bool BTree<Key, Comparator>::Contains(const Key& key) const {
  Node* node;
  int entryIndex;
  std::tie(node, entryIndex, std::ignore) = FindGreaterOrEqual(key);
  if (node != nullptr && Equal(key, node->entries[entryIndex].key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace rocksdb
