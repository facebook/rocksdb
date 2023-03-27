/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=2:softtabstop=2:
#ident "$Id$"
/*======
This file is part of PerconaFT.


Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved.

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License, version 2,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License, version 3,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
======= */

#ident \
    "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#pragma once

#include <string.h>

#include "../ft/comparator.h"
#include "../portability/memory.h"
#include "../portability/toku_pthread.h"
// PORT: we need LTM_STATUS
#include "../ft/ft-status.h"
#include "../portability/txn_subst.h"
#include "keyrange.h"

namespace toku {

// a node in a tree with its own mutex
// - range is the "key" of this node
// - txnid is the single txnid associated with this node
// - left and right children may be null
//
// to build a tree on top of this abstraction, the user:
// - provides memory for a root node, initializes it via create_root()
// - performs tree operations on the root node. memory management
//   below the root node is handled by the abstraction, not the user.
// this pattern:
// - guaruntees a root node always exists.
// - does not allow for rebalances on the root node

class treenode {
 public:
  // every treenode function has some common requirements:
  // - node is locked and children are never locked
  // - node may be unlocked if no other thread has visibility

  // effect: create the root node
  void create_root(const comparator *cmp);

  // effect: destroys the root node
  void destroy_root(void);

  // effect: sets the txnid and copies the given range for this node
  void set_range_and_txnid(const keyrange &range, TXNID txnid, bool is_shared);

  // returns: true iff this node is marked as empty
  bool is_empty(void);

  // returns: true if this is the root node, denoted by a null parent
  bool is_root(void);

  // returns: true if the given range overlaps with this node's range
  bool range_overlaps(const keyrange &range);

  // effect: locks the node
  void mutex_lock(void);

  // effect: unlocks the node
  void mutex_unlock(void);

  // return: node whose child overlaps, or a child that is empty
  //         and would contain range if it existed
  // given: if cmp_hint is non-null, then it is a precomputed
  //        comparison of this node's range to the given range.
  treenode *find_node_with_overlapping_child(
      const keyrange &range, const keyrange::comparison *cmp_hint);

  // effect: performs an in-order traversal of the ranges that overlap the
  //         given range, calling function->fn() on each node that does
  // requires: function signature is: bool fn(const keyrange &range, TXNID
  // txnid) requires: fn returns true to keep iterating, false to stop iterating
  // requires: fn does not attempt to use any ranges read out by value
  //           after removing a node with an overlapping range from the tree.
  template <class F>
  void traverse_overlaps(const keyrange &range, F *function) {
    keyrange::comparison c = range.compare(*m_cmp, m_range);
    if (c == keyrange::comparison::EQUALS) {
      // Doesn't matter if fn wants to keep going, there
      // is nothing left, so return.
      function->fn(m_range, m_txnid, m_is_shared, m_owners);
      return;
    }

    treenode *left = m_left_child.get_locked();
    if (left) {
      if (c != keyrange::comparison::GREATER_THAN) {
        // Target range is less than this node, or it overlaps this
        // node.  There may be something on the left.
        left->traverse_overlaps(range, function);
      }
      left->mutex_unlock();
    }

    if (c == keyrange::comparison::OVERLAPS) {
      bool keep_going = function->fn(m_range, m_txnid, m_is_shared, m_owners);
      if (!keep_going) {
        return;
      }
    }

    treenode *right = m_right_child.get_locked();
    if (right) {
      if (c != keyrange::comparison::LESS_THAN) {
        // Target range is greater than this node, or it overlaps this
        // node.  There may be something on the right.
        right->traverse_overlaps(range, function);
      }
      right->mutex_unlock();
    }
  }

  // effect: inserts the given range and txnid into a subtree, recursively
  // requires: range does not overlap with any node below the subtree
  bool insert(const keyrange &range, TXNID txnid, bool is_shared);

  // effect: removes the given range from the subtree
  // requires: range exists in the subtree
  // returns: the root of the resulting subtree
  treenode *remove(const keyrange &range, TXNID txnid);

  // effect: removes this node and all of its children, recursively
  // requires: every node at and below this node is unlocked
  void recursive_remove(void);

 private:
  // the child_ptr is a light abstraction for the locking of
  // a child and the maintenence of its depth estimate.

  struct child_ptr {
    // set the child pointer
    void set(treenode *node);

    // get and lock this child if it exists
    treenode *get_locked(void);

    treenode *ptr;
    uint32_t depth_est;
  };

  // the balance factor at which a node is considered imbalanced
  static const int32_t IMBALANCE_THRESHOLD = 2;

  // node-level mutex
  toku_mutex_t m_mutex;

  // the range and txnid for this node. the range contains a copy
  // of the keys originally inserted into the tree. nodes may
  // swap ranges. but at the end of the day, when a node is
  // destroyed, it frees the memory associated with whatever range
  // it has at the time of destruction.
  keyrange m_range;

  void remove_shared_owner(TXNID txnid);

  bool has_multiple_owners() { return (m_txnid == TXNID_SHARED); }

 private:
  // Owner transaction id.
  // A value of TXNID_SHARED means this node has multiple owners
  TXNID m_txnid;

  // If true, this lock is a non-exclusive lock, and it can have either
  // one or several owners.
  bool m_is_shared;

  // List of the owners, or nullptr if there's just one owner.
  TxnidVector *m_owners;

  // two child pointers
  child_ptr m_left_child;
  child_ptr m_right_child;

  // comparator for ranges
  // psergey-todo: Is there any sense to store the comparator in each tree
  // node?
  const comparator *m_cmp;

  // marked for the root node. the root node is never free()'d
  // when removed, but instead marked as empty.
  bool m_is_root;

  // marked for an empty node. only valid for the root.
  bool m_is_empty;

  // effect: initializes an empty node with the given comparator
  void init(const comparator *cmp);

  // requires: this is a shared node (m_is_shared==true)
  // effect: another transaction is added as an owner.
  // returns: true <=> added another owner
  //          false <=> this transaction is already an owner
  bool add_shared_owner(TXNID txnid);

  // requires: *parent is initialized to something meaningful.
  // requires: subtree is non-empty
  // returns: the leftmost child of the given subtree
  // returns: a pointer to the parent of said child in *parent, only
  //          if this function recurred, otherwise it is untouched.
  treenode *find_leftmost_child(treenode **parent);

  // requires: *parent is initialized to something meaningful.
  // requires: subtree is non-empty
  // returns: the rightmost child of the given subtree
  // returns: a pointer to the parent of said child in *parent, only
  //          if this function recurred, otherwise it is untouched.
  treenode *find_rightmost_child(treenode **parent);

  // effect: remove the root of this subtree, destroying the old root
  // returns: the new root of the subtree
  treenode *remove_root_of_subtree(void);

  // requires: subtree is non-empty, direction is not 0
  // returns: the child of the subtree at either the left or rightmost extreme
  treenode *find_child_at_extreme(int direction, treenode **parent);

  // effect: retrieves and possibly rebalances the left child
  // returns: a locked left child, if it exists
  treenode *lock_and_rebalance_left(void);

  // effect: retrieves and possibly rebalances the right child
  // returns: a locked right child, if it exists
  treenode *lock_and_rebalance_right(void);

  // returns: the estimated depth of this subtree
  uint32_t get_depth_estimate(void) const;

  // returns: true iff left subtree depth is sufficiently less than the right
  bool left_imbalanced(int threshold) const;

  // returns: true iff right subtree depth is sufficiently greater than the left
  bool right_imbalanced(int threshold) const;

  // effect: performs an O(1) rebalance, which will "heal" an imbalance by at
  // most 1. effect: if the new root is not this node, then this node is
  // unlocked. returns: locked node representing the new root of the rebalanced
  // subtree
  treenode *maybe_rebalance(void);

  // returns: allocated treenode populated with a copy of the range and txnid
  static treenode *alloc(const comparator *cmp, const keyrange &range,
                         TXNID txnid, bool is_shared);

  // requires: node is a locked root node, or an unlocked non-root node
  static void free(treenode *node);

  // effect: swaps the range/txnid pairs for node1 and node2.
  static void swap_in_place(treenode *node1, treenode *node2);

  friend class concurrent_tree_unit_test;
};

} /* namespace toku */
