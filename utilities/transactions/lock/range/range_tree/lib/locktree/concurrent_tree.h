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

#include "../ft/comparator.h"
#include "keyrange.h"
#include "treenode.h"

namespace toku {

// A concurrent_tree stores non-overlapping ranges.
// Access to disjoint parts of the tree usually occurs concurrently.

class concurrent_tree {
 public:
  // A locked_keyrange gives you exclusive access to read and write
  // operations that occur on any keys in that range. You only have
  // the right to operate on keys in that range or keys that were read
  // from the keyrange using iterate()
  //
  // Access model:
  // - user prepares a locked keyrange. all threads serialize behind prepare().
  // - user breaks the serialzation point by acquiring a range, or releasing.
  // - one thread operates on a certain locked_keyrange object at a time.
  // - when the thread is finished, it releases

  class locked_keyrange {
   public:
    // effect: prepare to acquire a locked keyrange over the given
    //         concurrent_tree, preventing other threads from preparing
    //         until this thread either does acquire() or release().
    // note: operations performed on a prepared keyrange are equivalent
    //         to ones performed on an acquired keyrange over -inf, +inf.
    // rationale: this provides the user with a serialization point for
    // descending
    //            or modifying the the tree. it also proives a convenient way of
    //            doing serializable operations on the tree.
    // There are two valid sequences of calls:
    //  - prepare, acquire, [operations], release
    //  - prepare, [operations],release
    void prepare(concurrent_tree *tree);

    // requires: the locked keyrange was prepare()'d
    // effect: acquire a locked keyrange over the given concurrent_tree.
    //         the locked keyrange represents the range of keys overlapped
    //         by the given range
    void acquire(const keyrange &range);

    // effect: releases a locked keyrange and the mutex it holds
    void release(void);

    // effect: iterate over each range this locked_keyrange represents,
    //         calling function->fn() on each node's keyrange and txnid
    //         until there are no more or the function returns false
    template <class F>
    void iterate(F *function) const {
      // if the subtree is non-empty, traverse it by calling the given
      // function on each range, txnid pair found that overlaps.
      if (!m_subtree->is_empty()) {
        m_subtree->traverse_overlaps(m_range, function);
      }
    }

    // Adds another owner to the lock on the specified keyrange.
    // requires: the keyrange contains one treenode whose bounds are
    //           exactly equal to the specifed range (no sub/supersets)
    bool add_shared_owner(const keyrange &range, TXNID new_owner);

    // inserts the given range into the tree, with an associated txnid.
    // requires: range does not overlap with anything in this locked_keyrange
    // rationale: caller is responsible for only inserting unique ranges
    void insert(const keyrange &range, TXNID txnid, bool is_shared);

    // effect: removes the given range from the tree.
    //         - txnid=TXNID_ANY means remove the range no matter what its
    //           owners are
    //         - Other value means remove the specified txnid from
    //           ownership (if the range has other owners, it will remain
    //           in the tree)
    // requires: range exists exactly in this locked_keyrange
    // rationale: caller is responsible for only removing existing ranges
    void remove(const keyrange &range, TXNID txnid);

    // effect: removes all of the keys represented by this locked keyrange
    // rationale: we'd like a fast way to empty out a tree
    void remove_all(void);

   private:
    // the concurrent tree this locked keyrange is for
    concurrent_tree *m_tree;

    // the range of keys this locked keyrange represents
    keyrange m_range;

    // the subtree under which all overlapping ranges exist
    treenode *m_subtree;

    friend class concurrent_tree_unit_test;
  };

  // effect: initialize the tree to an empty state
  void create(const comparator *cmp);

  // effect: destroy the tree.
  // requires: tree is empty
  void destroy(void);

  // returns: true iff the tree is empty
  bool is_empty(void);

  // returns: the memory overhead of a single insertion into the tree
  static uint64_t get_insertion_memory_overhead(void);

 private:
  // the root needs to always exist so there's a lock to grab
  // even if the tree is empty. that's why we store a treenode
  // here and not a pointer to one.
  treenode m_root;

  friend class concurrent_tree_unit_test;
};

} /* namespace toku */
