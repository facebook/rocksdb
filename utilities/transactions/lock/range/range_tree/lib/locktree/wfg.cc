/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ifndef ROCKSDB_LITE
#ifndef OS_WIN
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

#include "../db.h"
#include "../portability/memory.h"
// PORT #include <toku_assert.h>
#include <memory.h>
#include <string.h>

#include "txnid_set.h"
#include "wfg.h"

namespace toku {

// Create a lock request graph
void wfg::create(void) { m_nodes.create(); }

// Destroy the internals of the lock request graph
void wfg::destroy(void) {
  uint32_t n_nodes = m_nodes.size();
  for (uint32_t i = 0; i < n_nodes; i++) {
    node *n;
    int r = m_nodes.fetch(i, &n);
    invariant_zero(r);
    invariant_notnull(n);
    if (r) continue;  // Get rid of "may be used uninitialized" warning
    node::free(n);
  }
  m_nodes.destroy();
}

// Add an edge (a_id, b_id) to the graph
void wfg::add_edge(TXNID a_txnid, TXNID b_txnid) {
  node *a_node = find_create_node(a_txnid);
  node *b_node = find_create_node(b_txnid);
  a_node->edges.add(b_node->txnid);
}

// Return true if a node with the given transaction id exists in the graph.
// Return false otherwise.
bool wfg::node_exists(TXNID txnid) {
  node *n = find_node(txnid);
  return n != NULL;
}

bool wfg::cycle_exists_from_node(node *target, node *head,
                                 std::function<void(TXNID)> reporter) {
  bool cycle_found = false;
  head->visited = true;
  uint32_t n_edges = head->edges.size();
  for (uint32_t i = 0; i < n_edges && !cycle_found; i++) {
    TXNID edge_id = head->edges.get(i);
    if (target->txnid == edge_id) {
      cycle_found = true;
      if (reporter) reporter(edge_id);
    } else {
      node *new_head = find_node(edge_id);
      if (new_head && !new_head->visited) {
        cycle_found = cycle_exists_from_node(target, new_head, reporter);
        if (cycle_found && reporter) reporter(edge_id);
      }
    }
  }
  head->visited = false;
  return cycle_found;
}

// Return true if there exists a cycle from a given transaction id in the graph.
// Return false otherwise.
bool wfg::cycle_exists_from_txnid(TXNID txnid,
                                  std::function<void(TXNID)> reporter) {
  node *a_node = find_node(txnid);
  bool cycles_found = false;
  if (a_node) {
    cycles_found = cycle_exists_from_node(a_node, a_node, reporter);
  }
  return cycles_found;
}

// Apply a given function f to all of the nodes in the graph.  The apply
// function returns when the function f is called for all of the nodes in the
// graph, or the function f returns non-zero.
void wfg::apply_nodes(int (*fn)(TXNID id, void *extra), void *extra) {
  int r = 0;
  uint32_t n_nodes = m_nodes.size();
  for (uint32_t i = 0; i < n_nodes && r == 0; i++) {
    node *n;
    r = m_nodes.fetch(i, &n);
    invariant_zero(r);
    if (r) continue;  // Get rid of "may be used uninitialized" warning
    r = fn(n->txnid, extra);
  }
}

// Apply a given function f to all of the edges whose origin is a given node id.
// The apply function returns when the function f is called for all edges in the
// graph rooted at node id, or the function f returns non-zero.
void wfg::apply_edges(TXNID txnid,
                      int (*fn)(TXNID txnid, TXNID edge_txnid, void *extra),
                      void *extra) {
  node *n = find_node(txnid);
  if (n) {
    int r = 0;
    uint32_t n_edges = n->edges.size();
    for (uint32_t i = 0; i < n_edges && r == 0; i++) {
      r = fn(txnid, n->edges.get(i), extra);
    }
  }
}

// find node by id
wfg::node *wfg::find_node(TXNID txnid) {
  node *n = nullptr;
  int r = m_nodes.find_zero<TXNID, find_by_txnid>(txnid, &n, nullptr);
  invariant(r == 0 || r == DB_NOTFOUND);
  return n;
}

// this is the omt comparison function
// nodes are compared by their txnid.
int wfg::find_by_txnid(node *const &node_a, const TXNID &txnid_b) {
  TXNID txnid_a = node_a->txnid;
  if (txnid_a < txnid_b) {
    return -1;
  } else if (txnid_a == txnid_b) {
    return 0;
  } else {
    return 1;
  }
}

// insert a new node
wfg::node *wfg::find_create_node(TXNID txnid) {
  node *n;
  uint32_t idx;
  int r = m_nodes.find_zero<TXNID, find_by_txnid>(txnid, &n, &idx);
  if (r == DB_NOTFOUND) {
    n = node::alloc(txnid);
    r = m_nodes.insert_at(n, idx);
    invariant_zero(r);
  }
  invariant_notnull(n);
  return n;
}

wfg::node *wfg::node::alloc(TXNID txnid) {
  node *XCALLOC(n);
  n->txnid = txnid;
  n->visited = false;
  n->edges.create();
  return n;
}

void wfg::node::free(wfg::node *n) {
  n->edges.destroy();
  toku_free(n);
}

} /* namespace toku */
#endif  // OS_WIN
#endif  // ROCKSDB_LITE
