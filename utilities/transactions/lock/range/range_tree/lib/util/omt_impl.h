/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
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

#include <string.h>

#include "../db.h"
#include "../portability/memory.h"

namespace toku {

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::create(void) {
  this->create_internal(2);
  if (supports_marks) {
    this->convert_to_tree();
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::create_no_array(void) {
  if (!supports_marks) {
    this->create_internal_no_array(0);
  } else {
    this->is_array = false;
    this->capacity = 0;
    this->d.t.nodes = nullptr;
    this->d.t.root.set_to_null();
    this->d.t.free_idx = 0;
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::create_from_sorted_array(
    const omtdata_t *const values, const uint32_t numvalues) {
  this->create_internal(numvalues);
  memcpy(this->d.a.values, values, numvalues * (sizeof values[0]));
  this->d.a.num_values = numvalues;
  if (supports_marks) {
    this->convert_to_tree();
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::create_steal_sorted_array(
    omtdata_t **const values, const uint32_t numvalues,
    const uint32_t new_capacity) {
  paranoid_invariant_notnull(values);
  this->create_internal_no_array(new_capacity);
  this->d.a.num_values = numvalues;
  this->d.a.values = *values;
  *values = nullptr;
  if (supports_marks) {
    this->convert_to_tree();
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
int omt<omtdata_t, omtdataout_t, supports_marks>::split_at(omt *const newomt,
                                                           const uint32_t idx) {
  barf_if_marked(*this);
  paranoid_invariant_notnull(newomt);
  if (idx > this->size()) {
    return EINVAL;
  }
  this->convert_to_array();
  const uint32_t newsize = this->size() - idx;
  newomt->create_from_sorted_array(&this->d.a.values[this->d.a.start_idx + idx],
                                   newsize);
  this->d.a.num_values = idx;
  this->maybe_resize_array(idx);
  if (supports_marks) {
    this->convert_to_tree();
  }
  return 0;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::merge(omt *const leftomt,
                                                         omt *const rightomt) {
  barf_if_marked(*this);
  paranoid_invariant_notnull(leftomt);
  paranoid_invariant_notnull(rightomt);
  const uint32_t leftsize = leftomt->size();
  const uint32_t rightsize = rightomt->size();
  const uint32_t newsize = leftsize + rightsize;

  if (leftomt->is_array) {
    if (leftomt->capacity -
            (leftomt->d.a.start_idx + leftomt->d.a.num_values) >=
        rightsize) {
      this->create_steal_sorted_array(
          &leftomt->d.a.values, leftomt->d.a.num_values, leftomt->capacity);
      this->d.a.start_idx = leftomt->d.a.start_idx;
    } else {
      this->create_internal(newsize);
      memcpy(&this->d.a.values[0], &leftomt->d.a.values[leftomt->d.a.start_idx],
             leftomt->d.a.num_values * (sizeof this->d.a.values[0]));
    }
  } else {
    this->create_internal(newsize);
    leftomt->fill_array_with_subtree_values(&this->d.a.values[0],
                                            leftomt->d.t.root);
  }
  leftomt->destroy();
  this->d.a.num_values = leftsize;

  if (rightomt->is_array) {
    memcpy(&this->d.a.values[this->d.a.start_idx + this->d.a.num_values],
           &rightomt->d.a.values[rightomt->d.a.start_idx],
           rightomt->d.a.num_values * (sizeof this->d.a.values[0]));
  } else {
    rightomt->fill_array_with_subtree_values(
        &this->d.a.values[this->d.a.start_idx + this->d.a.num_values],
        rightomt->d.t.root);
  }
  rightomt->destroy();
  this->d.a.num_values += rightsize;
  paranoid_invariant(this->size() == newsize);
  if (supports_marks) {
    this->convert_to_tree();
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::clone(const omt &src) {
  barf_if_marked(*this);
  this->create_internal(src.size());
  if (src.is_array) {
    memcpy(&this->d.a.values[0], &src.d.a.values[src.d.a.start_idx],
           src.d.a.num_values * (sizeof this->d.a.values[0]));
  } else {
    src.fill_array_with_subtree_values(&this->d.a.values[0], src.d.t.root);
  }
  this->d.a.num_values = src.size();
  if (supports_marks) {
    this->convert_to_tree();
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::clear(void) {
  if (this->is_array) {
    this->d.a.start_idx = 0;
    this->d.a.num_values = 0;
  } else {
    this->d.t.root.set_to_null();
    this->d.t.free_idx = 0;
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::destroy(void) {
  this->clear();
  this->capacity = 0;
  if (this->is_array) {
    if (this->d.a.values != nullptr) {
      toku_free(this->d.a.values);
    }
    this->d.a.values = nullptr;
  } else {
    if (this->d.t.nodes != nullptr) {
      toku_free(this->d.t.nodes);
    }
    this->d.t.nodes = nullptr;
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
uint32_t omt<omtdata_t, omtdataout_t, supports_marks>::size(void) const {
  if (this->is_array) {
    return this->d.a.num_values;
  } else {
    return this->nweight(this->d.t.root);
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
int omt<omtdata_t, omtdataout_t, supports_marks>::insert(const omtdata_t &value,
                                                         const omtcmp_t &v,
                                                         uint32_t *const idx) {
  int r;
  uint32_t insert_idx;

  r = this->find_zero<omtcmp_t, h>(v, nullptr, &insert_idx);
  if (r == 0) {
    if (idx) *idx = insert_idx;
    return DB_KEYEXIST;
  }
  if (r != DB_NOTFOUND) return r;

  if ((r = this->insert_at(value, insert_idx))) return r;
  if (idx) *idx = insert_idx;

  return 0;
}

// The following 3 functions implement a static if for us.
template <typename omtdata_t, typename omtdataout_t>
static void barf_if_marked(const omt<omtdata_t, omtdataout_t, false> &UU(omt)) {
}

template <typename omtdata_t, typename omtdataout_t>
static void barf_if_marked(const omt<omtdata_t, omtdataout_t, true> &omt) {
  invariant(!omt.has_marks());
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
bool omt<omtdata_t, omtdataout_t, supports_marks>::has_marks(void) const {
  static_assert(supports_marks, "Does not support marks");
  if (this->d.t.root.is_null()) {
    return false;
  }
  const omt_node &node = this->d.t.nodes[this->d.t.root.get_index()];
  return node.get_marks_below() || node.get_marked();
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
int omt<omtdata_t, omtdataout_t, supports_marks>::insert_at(
    const omtdata_t &value, const uint32_t idx) {
  barf_if_marked(*this);
  if (idx > this->size()) {
    return EINVAL;
  }

  this->maybe_resize_or_convert(this->size() + 1);
  if (this->is_array && idx != this->d.a.num_values &&
      (idx != 0 || this->d.a.start_idx == 0)) {
    this->convert_to_tree();
  }
  if (this->is_array) {
    if (idx == this->d.a.num_values) {
      this->d.a.values[this->d.a.start_idx + this->d.a.num_values] = value;
    } else {
      this->d.a.values[--this->d.a.start_idx] = value;
    }
    this->d.a.num_values++;
  } else {
    subtree *rebalance_subtree = nullptr;
    this->insert_internal(&this->d.t.root, value, idx, &rebalance_subtree);
    if (rebalance_subtree != nullptr) {
      this->rebalance(rebalance_subtree);
    }
  }
  return 0;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
int omt<omtdata_t, omtdataout_t, supports_marks>::set_at(const omtdata_t &value,
                                                         const uint32_t idx) {
  barf_if_marked(*this);
  if (idx >= this->size()) {
    return EINVAL;
  }

  if (this->is_array) {
    this->set_at_internal_array(value, idx);
  } else {
    this->set_at_internal(this->d.t.root, value, idx);
  }
  return 0;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
int omt<omtdata_t, omtdataout_t, supports_marks>::delete_at(
    const uint32_t idx) {
  barf_if_marked(*this);
  if (idx >= this->size()) {
    return EINVAL;
  }

  this->maybe_resize_or_convert(this->size() - 1);
  if (this->is_array && idx != 0 && idx != this->d.a.num_values - 1) {
    this->convert_to_tree();
  }
  if (this->is_array) {
    // Testing for 0 does not rule out it being the last entry.
    // Test explicitly for num_values-1
    if (idx != this->d.a.num_values - 1) {
      this->d.a.start_idx++;
    }
    this->d.a.num_values--;
  } else {
    subtree *rebalance_subtree = nullptr;
    this->delete_internal(&this->d.t.root, idx, nullptr, &rebalance_subtree);
    if (rebalance_subtree != nullptr) {
      this->rebalance(rebalance_subtree);
    }
  }
  return 0;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename iterate_extra_t,
          int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
int omt<omtdata_t, omtdataout_t, supports_marks>::iterate(
    iterate_extra_t *const iterate_extra) const {
  return this->iterate_on_range<iterate_extra_t, f>(0, this->size(),
                                                    iterate_extra);
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename iterate_extra_t,
          int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
int omt<omtdata_t, omtdataout_t, supports_marks>::iterate_on_range(
    const uint32_t left, const uint32_t right,
    iterate_extra_t *const iterate_extra) const {
  if (right > this->size()) {
    return EINVAL;
  }
  if (left == right) {
    return 0;
  }
  if (this->is_array) {
    return this->iterate_internal_array<iterate_extra_t, f>(left, right,
                                                            iterate_extra);
  }
  return this->iterate_internal<iterate_extra_t, f>(left, right, this->d.t.root,
                                                    0, iterate_extra);
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename iterate_extra_t,
          int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
int omt<omtdata_t, omtdataout_t, supports_marks>::iterate_and_mark_range(
    const uint32_t left, const uint32_t right,
    iterate_extra_t *const iterate_extra) {
  static_assert(supports_marks, "does not support marks");
  if (right > this->size()) {
    return EINVAL;
  }
  if (left == right) {
    return 0;
  }
  paranoid_invariant(!this->is_array);
  return this->iterate_and_mark_range_internal<iterate_extra_t, f>(
      left, right, this->d.t.root, 0, iterate_extra);
}

// TODO: We can optimize this if we steal 3 bits.  1 bit: this node is
// marked.  1 bit: left subtree has marks. 1 bit: right subtree has marks.
template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename iterate_extra_t,
          int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
int omt<omtdata_t, omtdataout_t, supports_marks>::iterate_over_marked(
    iterate_extra_t *const iterate_extra) const {
  static_assert(supports_marks, "does not support marks");
  paranoid_invariant(!this->is_array);
  return this->iterate_over_marked_internal<iterate_extra_t, f>(
      this->d.t.root, 0, iterate_extra);
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::unmark(
    const subtree &st, const uint32_t index,
    GrowableArray<node_idx> *const indexes) {
  if (st.is_null()) {
    return;
  }
  omt_node &n = this->d.t.nodes[st.get_index()];
  const uint32_t index_root = index + this->nweight(n.left);

  const bool below = n.get_marks_below();
  if (below) {
    this->unmark(n.left, index, indexes);
  }
  if (n.get_marked()) {
    indexes->push(index_root);
  }
  n.clear_stolen_bits();
  if (below) {
    this->unmark(n.right, index_root + 1, indexes);
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::delete_all_marked(void) {
  static_assert(supports_marks, "does not support marks");
  if (!this->has_marks()) {
    return;
  }
  paranoid_invariant(!this->is_array);
  GrowableArray<node_idx> marked_indexes;
  marked_indexes.init();

  // Remove all marks.
  // We need to delete all the stolen bits before calling delete_at to
  // prevent barfing.
  this->unmark(this->d.t.root, 0, &marked_indexes);

  for (uint32_t i = 0; i < marked_indexes.get_size(); i++) {
    // Delete from left to right, shift by number already deleted.
    // Alternative is delete from right to left.
    int r = this->delete_at(marked_indexes.fetch_unchecked(i) - i);
    lazy_assert_zero(r);
  }
  marked_indexes.deinit();
  barf_if_marked(*this);
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
uint32_t
omt<omtdata_t, omtdataout_t, supports_marks>::verify_marks_consistent_internal(
    const subtree &st, const bool UU(allow_marks)) const {
  if (st.is_null()) {
    return 0;
  }
  const omt_node &node = this->d.t.nodes[st.get_index()];
  uint32_t num_marks =
      verify_marks_consistent_internal(node.left, node.get_marks_below());
  num_marks +=
      verify_marks_consistent_internal(node.right, node.get_marks_below());
  if (node.get_marks_below()) {
    paranoid_invariant(allow_marks);
    paranoid_invariant(num_marks > 0);
  } else {
    // redundant with invariant below, but nice to have explicitly
    paranoid_invariant(num_marks == 0);
  }
  if (node.get_marked()) {
    paranoid_invariant(allow_marks);
    ++num_marks;
  }
  return num_marks;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::verify_marks_consistent(
    void) const {
  static_assert(supports_marks, "does not support marks");
  paranoid_invariant(!this->is_array);
  this->verify_marks_consistent_internal(this->d.t.root, true);
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename iterate_extra_t,
          int (*f)(omtdata_t *, const uint32_t, iterate_extra_t *const)>
void omt<omtdata_t, omtdataout_t, supports_marks>::iterate_ptr(
    iterate_extra_t *const iterate_extra) {
  if (this->is_array) {
    this->iterate_ptr_internal_array<iterate_extra_t, f>(0, this->size(),
                                                         iterate_extra);
  } else {
    this->iterate_ptr_internal<iterate_extra_t, f>(
        0, this->size(), this->d.t.root, 0, iterate_extra);
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
int omt<omtdata_t, omtdataout_t, supports_marks>::fetch(
    const uint32_t idx, omtdataout_t *const value) const {
  if (idx >= this->size()) {
    return EINVAL;
  }
  if (this->is_array) {
    this->fetch_internal_array(idx, value);
  } else {
    this->fetch_internal(this->d.t.root, idx, value);
  }
  return 0;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
int omt<omtdata_t, omtdataout_t, supports_marks>::find_zero(
    const omtcmp_t &extra, omtdataout_t *const value,
    uint32_t *const idxp) const {
  uint32_t tmp_index;
  uint32_t *const child_idxp = (idxp != nullptr) ? idxp : &tmp_index;
  int r;
  if (this->is_array) {
    r = this->find_internal_zero_array<omtcmp_t, h>(extra, value, child_idxp);
  } else {
    r = this->find_internal_zero<omtcmp_t, h>(this->d.t.root, extra, value,
                                              child_idxp);
  }
  return r;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
int omt<omtdata_t, omtdataout_t, supports_marks>::find(
    const omtcmp_t &extra, int direction, omtdataout_t *const value,
    uint32_t *const idxp) const {
  uint32_t tmp_index;
  uint32_t *const child_idxp = (idxp != nullptr) ? idxp : &tmp_index;
  paranoid_invariant(direction != 0);
  if (direction < 0) {
    if (this->is_array) {
      return this->find_internal_minus_array<omtcmp_t, h>(extra, value,
                                                          child_idxp);
    } else {
      return this->find_internal_minus<omtcmp_t, h>(this->d.t.root, extra,
                                                    value, child_idxp);
    }
  } else {
    if (this->is_array) {
      return this->find_internal_plus_array<omtcmp_t, h>(extra, value,
                                                         child_idxp);
    } else {
      return this->find_internal_plus<omtcmp_t, h>(this->d.t.root, extra, value,
                                                   child_idxp);
    }
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
size_t omt<omtdata_t, omtdataout_t, supports_marks>::memory_size(void) {
  if (this->is_array) {
    return (sizeof *this) + this->capacity * (sizeof this->d.a.values[0]);
  }
  return (sizeof *this) + this->capacity * (sizeof this->d.t.nodes[0]);
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::create_internal_no_array(
    const uint32_t new_capacity) {
  this->is_array = true;
  this->d.a.start_idx = 0;
  this->d.a.num_values = 0;
  this->d.a.values = nullptr;
  this->capacity = new_capacity;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::create_internal(
    const uint32_t new_capacity) {
  this->create_internal_no_array(new_capacity);
  XMALLOC_N(this->capacity, this->d.a.values);
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
uint32_t omt<omtdata_t, omtdataout_t, supports_marks>::nweight(
    const subtree &st) const {
  if (st.is_null()) {
    return 0;
  } else {
    return this->d.t.nodes[st.get_index()].weight;
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
typename omt<omtdata_t, omtdataout_t, supports_marks>::node_idx
omt<omtdata_t, omtdataout_t, supports_marks>::node_malloc(void) {
  paranoid_invariant(this->d.t.free_idx < this->capacity);
  omt_node &n = this->d.t.nodes[this->d.t.free_idx];
  n.clear_stolen_bits();
  return this->d.t.free_idx++;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::node_free(
    const node_idx UU(idx)) {
  paranoid_invariant(idx < this->capacity);
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::maybe_resize_array(
    const uint32_t n) {
  const uint32_t new_size = n <= 2 ? 4 : 2 * n;
  const uint32_t room = this->capacity - this->d.a.start_idx;

  if (room < n || this->capacity / 2 >= new_size) {
    omtdata_t *XMALLOC_N(new_size, tmp_values);
    if (this->d.a.num_values) {
      memcpy(tmp_values, &this->d.a.values[this->d.a.start_idx],
             this->d.a.num_values * (sizeof tmp_values[0]));
    }
    this->d.a.start_idx = 0;
    this->capacity = new_size;
    toku_free(this->d.a.values);
    this->d.a.values = tmp_values;
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t,
         supports_marks>::fill_array_with_subtree_values(omtdata_t *const array,
                                                         const subtree &st)
    const {
  if (st.is_null()) return;
  const omt_node &tree = this->d.t.nodes[st.get_index()];
  this->fill_array_with_subtree_values(&array[0], tree.left);
  array[this->nweight(tree.left)] = tree.value;
  this->fill_array_with_subtree_values(&array[this->nweight(tree.left) + 1],
                                       tree.right);
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::convert_to_array(void) {
  if (!this->is_array) {
    const uint32_t num_values = this->size();
    uint32_t new_size = 2 * num_values;
    new_size = new_size < 4 ? 4 : new_size;

    omtdata_t *XMALLOC_N(new_size, tmp_values);
    this->fill_array_with_subtree_values(tmp_values, this->d.t.root);
    toku_free(this->d.t.nodes);
    this->is_array = true;
    this->capacity = new_size;
    this->d.a.num_values = num_values;
    this->d.a.values = tmp_values;
    this->d.a.start_idx = 0;
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::rebuild_from_sorted_array(
    subtree *const st, const omtdata_t *const values,
    const uint32_t numvalues) {
  if (numvalues == 0) {
    st->set_to_null();
  } else {
    const uint32_t halfway = numvalues / 2;
    const node_idx newidx = this->node_malloc();
    omt_node *const newnode = &this->d.t.nodes[newidx];
    newnode->weight = numvalues;
    newnode->value = values[halfway];
    st->set_index(newidx);
    // update everything before the recursive calls so the second call
    // can be a tail call.
    this->rebuild_from_sorted_array(&newnode->left, &values[0], halfway);
    this->rebuild_from_sorted_array(&newnode->right, &values[halfway + 1],
                                    numvalues - (halfway + 1));
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::convert_to_tree(void) {
  if (this->is_array) {
    const uint32_t num_nodes = this->size();
    uint32_t new_size = num_nodes * 2;
    new_size = new_size < 4 ? 4 : new_size;

    omt_node *XMALLOC_N(new_size, new_nodes);
    omtdata_t *const values = this->d.a.values;
    omtdata_t *const tmp_values = &values[this->d.a.start_idx];
    this->is_array = false;
    this->d.t.nodes = new_nodes;
    this->capacity = new_size;
    this->d.t.free_idx = 0;
    this->d.t.root.set_to_null();
    this->rebuild_from_sorted_array(&this->d.t.root, tmp_values, num_nodes);
    toku_free(values);
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::maybe_resize_or_convert(
    const uint32_t n) {
  if (this->is_array) {
    this->maybe_resize_array(n);
  } else {
    const uint32_t new_size = n <= 2 ? 4 : 2 * n;
    const uint32_t num_nodes = this->nweight(this->d.t.root);
    if ((this->capacity / 2 >= new_size) ||
        (this->d.t.free_idx >= this->capacity && num_nodes < n) ||
        (this->capacity < n)) {
      this->convert_to_array();
      // if we had a free list, the "supports_marks" version could
      // just resize, as it is now, we have to convert to and back
      // from an array.
      if (supports_marks) {
        this->convert_to_tree();
      }
    }
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
bool omt<omtdata_t, omtdataout_t, supports_marks>::will_need_rebalance(
    const subtree &st, const int leftmod, const int rightmod) const {
  if (st.is_null()) {
    return false;
  }
  const omt_node &n = this->d.t.nodes[st.get_index()];
  // one of the 1's is for the root.
  // the other is to take ceil(n/2)
  const uint32_t weight_left = this->nweight(n.left) + leftmod;
  const uint32_t weight_right = this->nweight(n.right) + rightmod;
  return ((1 + weight_left < (1 + 1 + weight_right) / 2) ||
          (1 + weight_right < (1 + 1 + weight_left) / 2));
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::insert_internal(
    subtree *const subtreep, const omtdata_t &value, const uint32_t idx,
    subtree **const rebalance_subtree) {
  if (subtreep->is_null()) {
    paranoid_invariant_zero(idx);
    const node_idx newidx = this->node_malloc();
    omt_node *const newnode = &this->d.t.nodes[newidx];
    newnode->weight = 1;
    newnode->left.set_to_null();
    newnode->right.set_to_null();
    newnode->value = value;
    subtreep->set_index(newidx);
  } else {
    omt_node &n = this->d.t.nodes[subtreep->get_index()];
    n.weight++;
    if (idx <= this->nweight(n.left)) {
      if (*rebalance_subtree == nullptr &&
          this->will_need_rebalance(*subtreep, 1, 0)) {
        *rebalance_subtree = subtreep;
      }
      this->insert_internal(&n.left, value, idx, rebalance_subtree);
    } else {
      if (*rebalance_subtree == nullptr &&
          this->will_need_rebalance(*subtreep, 0, 1)) {
        *rebalance_subtree = subtreep;
      }
      const uint32_t sub_index = idx - this->nweight(n.left) - 1;
      this->insert_internal(&n.right, value, sub_index, rebalance_subtree);
    }
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::set_at_internal_array(
    const omtdata_t &value, const uint32_t idx) {
  this->d.a.values[this->d.a.start_idx + idx] = value;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::set_at_internal(
    const subtree &st, const omtdata_t &value, const uint32_t idx) {
  paranoid_invariant(!st.is_null());
  omt_node &n = this->d.t.nodes[st.get_index()];
  const uint32_t leftweight = this->nweight(n.left);
  if (idx < leftweight) {
    this->set_at_internal(n.left, value, idx);
  } else if (idx == leftweight) {
    n.value = value;
  } else {
    this->set_at_internal(n.right, value, idx - leftweight - 1);
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::delete_internal(
    subtree *const subtreep, const uint32_t idx, omt_node *const copyn,
    subtree **const rebalance_subtree) {
  paranoid_invariant_notnull(subtreep);
  paranoid_invariant_notnull(rebalance_subtree);
  paranoid_invariant(!subtreep->is_null());
  omt_node &n = this->d.t.nodes[subtreep->get_index()];
  const uint32_t leftweight = this->nweight(n.left);
  if (idx < leftweight) {
    n.weight--;
    if (*rebalance_subtree == nullptr &&
        this->will_need_rebalance(*subtreep, -1, 0)) {
      *rebalance_subtree = subtreep;
    }
    this->delete_internal(&n.left, idx, copyn, rebalance_subtree);
  } else if (idx == leftweight) {
    if (n.left.is_null()) {
      const uint32_t oldidx = subtreep->get_index();
      *subtreep = n.right;
      if (copyn != nullptr) {
        copyn->value = n.value;
      }
      this->node_free(oldidx);
    } else if (n.right.is_null()) {
      const uint32_t oldidx = subtreep->get_index();
      *subtreep = n.left;
      if (copyn != nullptr) {
        copyn->value = n.value;
      }
      this->node_free(oldidx);
    } else {
      if (*rebalance_subtree == nullptr &&
          this->will_need_rebalance(*subtreep, 0, -1)) {
        *rebalance_subtree = subtreep;
      }
      // don't need to copy up value, it's only used by this
      // next call, and when that gets to the bottom there
      // won't be any more recursion
      n.weight--;
      this->delete_internal(&n.right, 0, &n, rebalance_subtree);
    }
  } else {
    n.weight--;
    if (*rebalance_subtree == nullptr &&
        this->will_need_rebalance(*subtreep, 0, -1)) {
      *rebalance_subtree = subtreep;
    }
    this->delete_internal(&n.right, idx - leftweight - 1, copyn,
                          rebalance_subtree);
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename iterate_extra_t,
          int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
int omt<omtdata_t, omtdataout_t, supports_marks>::iterate_internal_array(
    const uint32_t left, const uint32_t right,
    iterate_extra_t *const iterate_extra) const {
  int r;
  for (uint32_t i = left; i < right; ++i) {
    r = f(this->d.a.values[this->d.a.start_idx + i], i, iterate_extra);
    if (r != 0) {
      return r;
    }
  }
  return 0;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename iterate_extra_t,
          int (*f)(omtdata_t *, const uint32_t, iterate_extra_t *const)>
void omt<omtdata_t, omtdataout_t, supports_marks>::iterate_ptr_internal(
    const uint32_t left, const uint32_t right, const subtree &st,
    const uint32_t idx, iterate_extra_t *const iterate_extra) {
  if (!st.is_null()) {
    omt_node &n = this->d.t.nodes[st.get_index()];
    const uint32_t idx_root = idx + this->nweight(n.left);
    if (left < idx_root) {
      this->iterate_ptr_internal<iterate_extra_t, f>(left, right, n.left, idx,
                                                     iterate_extra);
    }
    if (left <= idx_root && idx_root < right) {
      int r = f(&n.value, idx_root, iterate_extra);
      lazy_assert_zero(r);
    }
    if (idx_root + 1 < right) {
      this->iterate_ptr_internal<iterate_extra_t, f>(
          left, right, n.right, idx_root + 1, iterate_extra);
    }
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename iterate_extra_t,
          int (*f)(omtdata_t *, const uint32_t, iterate_extra_t *const)>
void omt<omtdata_t, omtdataout_t, supports_marks>::iterate_ptr_internal_array(
    const uint32_t left, const uint32_t right,
    iterate_extra_t *const iterate_extra) {
  for (uint32_t i = left; i < right; ++i) {
    int r = f(&this->d.a.values[this->d.a.start_idx + i], i, iterate_extra);
    lazy_assert_zero(r);
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename iterate_extra_t,
          int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
int omt<omtdata_t, omtdataout_t, supports_marks>::iterate_internal(
    const uint32_t left, const uint32_t right, const subtree &st,
    const uint32_t idx, iterate_extra_t *const iterate_extra) const {
  if (st.is_null()) {
    return 0;
  }
  int r;
  const omt_node &n = this->d.t.nodes[st.get_index()];
  const uint32_t idx_root = idx + this->nweight(n.left);
  if (left < idx_root) {
    r = this->iterate_internal<iterate_extra_t, f>(left, right, n.left, idx,
                                                   iterate_extra);
    if (r != 0) {
      return r;
    }
  }
  if (left <= idx_root && idx_root < right) {
    r = f(n.value, idx_root, iterate_extra);
    if (r != 0) {
      return r;
    }
  }
  if (idx_root + 1 < right) {
    return this->iterate_internal<iterate_extra_t, f>(
        left, right, n.right, idx_root + 1, iterate_extra);
  }
  return 0;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename iterate_extra_t,
          int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
int omt<omtdata_t, omtdataout_t, supports_marks>::
    iterate_and_mark_range_internal(const uint32_t left, const uint32_t right,
                                    const subtree &st, const uint32_t idx,
                                    iterate_extra_t *const iterate_extra) {
  paranoid_invariant(!st.is_null());
  int r;
  omt_node &n = this->d.t.nodes[st.get_index()];
  const uint32_t idx_root = idx + this->nweight(n.left);
  if (left < idx_root && !n.left.is_null()) {
    n.set_marks_below_bit();
    r = this->iterate_and_mark_range_internal<iterate_extra_t, f>(
        left, right, n.left, idx, iterate_extra);
    if (r != 0) {
      return r;
    }
  }
  if (left <= idx_root && idx_root < right) {
    n.set_marked_bit();
    r = f(n.value, idx_root, iterate_extra);
    if (r != 0) {
      return r;
    }
  }
  if (idx_root + 1 < right && !n.right.is_null()) {
    n.set_marks_below_bit();
    return this->iterate_and_mark_range_internal<iterate_extra_t, f>(
        left, right, n.right, idx_root + 1, iterate_extra);
  }
  return 0;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename iterate_extra_t,
          int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
int omt<omtdata_t, omtdataout_t, supports_marks>::iterate_over_marked_internal(
    const subtree &st, const uint32_t idx,
    iterate_extra_t *const iterate_extra) const {
  if (st.is_null()) {
    return 0;
  }
  int r;
  const omt_node &n = this->d.t.nodes[st.get_index()];
  const uint32_t idx_root = idx + this->nweight(n.left);
  if (n.get_marks_below()) {
    r = this->iterate_over_marked_internal<iterate_extra_t, f>(n.left, idx,
                                                               iterate_extra);
    if (r != 0) {
      return r;
    }
  }
  if (n.get_marked()) {
    r = f(n.value, idx_root, iterate_extra);
    if (r != 0) {
      return r;
    }
  }
  if (n.get_marks_below()) {
    return this->iterate_over_marked_internal<iterate_extra_t, f>(
        n.right, idx_root + 1, iterate_extra);
  }
  return 0;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::fetch_internal_array(
    const uint32_t i, omtdataout_t *const value) const {
  if (value != nullptr) {
    copyout(value, &this->d.a.values[this->d.a.start_idx + i]);
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::fetch_internal(
    const subtree &st, const uint32_t i, omtdataout_t *const value) const {
  omt_node &n = this->d.t.nodes[st.get_index()];
  const uint32_t leftweight = this->nweight(n.left);
  if (i < leftweight) {
    this->fetch_internal(n.left, i, value);
  } else if (i == leftweight) {
    if (value != nullptr) {
      copyout(value, &n);
    }
  } else {
    this->fetch_internal(n.right, i - leftweight - 1, value);
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::fill_array_with_subtree_idxs(
    node_idx *const array, const subtree &st) const {
  if (!st.is_null()) {
    const omt_node &tree = this->d.t.nodes[st.get_index()];
    this->fill_array_with_subtree_idxs(&array[0], tree.left);
    array[this->nweight(tree.left)] = st.get_index();
    this->fill_array_with_subtree_idxs(&array[this->nweight(tree.left) + 1],
                                       tree.right);
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::rebuild_subtree_from_idxs(
    subtree *const st, const node_idx *const idxs, const uint32_t numvalues) {
  if (numvalues == 0) {
    st->set_to_null();
  } else {
    uint32_t halfway = numvalues / 2;
    st->set_index(idxs[halfway]);
    // node_idx newidx = idxs[halfway];
    omt_node &newnode = this->d.t.nodes[st->get_index()];
    newnode.weight = numvalues;
    // value is already in there.
    this->rebuild_subtree_from_idxs(&newnode.left, &idxs[0], halfway);
    this->rebuild_subtree_from_idxs(&newnode.right, &idxs[halfway + 1],
                                    numvalues - (halfway + 1));
    // n_idx = newidx;
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::rebalance(
    subtree *const st) {
  node_idx idx = st->get_index();
  if (idx == this->d.t.root.get_index()) {
    // Try to convert to an array.
    // If this fails, (malloc) nothing will have changed.
    // In the failure case we continue on to the standard rebalance
    // algorithm.
    this->convert_to_array();
    if (supports_marks) {
      this->convert_to_tree();
    }
  } else {
    const omt_node &n = this->d.t.nodes[idx];
    node_idx *tmp_array;
    size_t mem_needed = n.weight * (sizeof tmp_array[0]);
    size_t mem_free =
        (this->capacity - this->d.t.free_idx) * (sizeof this->d.t.nodes[0]);
    bool malloced;
    if (mem_needed <= mem_free) {
      // There is sufficient free space at the end of the nodes array
      // to hold enough node indexes to rebalance.
      malloced = false;
      tmp_array =
          reinterpret_cast<node_idx *>(&this->d.t.nodes[this->d.t.free_idx]);
    } else {
      malloced = true;
      XMALLOC_N(n.weight, tmp_array);
    }
    this->fill_array_with_subtree_idxs(tmp_array, *st);
    this->rebuild_subtree_from_idxs(st, tmp_array, n.weight);
    if (malloced) toku_free(tmp_array);
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::copyout(
    omtdata_t *const out, const omt_node *const n) {
  *out = n->value;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::copyout(
    omtdata_t **const out, omt_node *const n) {
  *out = &n->value;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::copyout(
    omtdata_t *const out, const omtdata_t *const stored_value_ptr) {
  *out = *stored_value_ptr;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
void omt<omtdata_t, omtdataout_t, supports_marks>::copyout(
    omtdata_t **const out, omtdata_t *const stored_value_ptr) {
  *out = stored_value_ptr;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
int omt<omtdata_t, omtdataout_t, supports_marks>::find_internal_zero_array(
    const omtcmp_t &extra, omtdataout_t *const value,
    uint32_t *const idxp) const {
  paranoid_invariant_notnull(idxp);
  uint32_t min = this->d.a.start_idx;
  uint32_t limit = this->d.a.start_idx + this->d.a.num_values;
  uint32_t best_pos = subtree::NODE_NULL;
  uint32_t best_zero = subtree::NODE_NULL;

  while (min != limit) {
    uint32_t mid = (min + limit) / 2;
    int hv = h(this->d.a.values[mid], extra);
    if (hv < 0) {
      min = mid + 1;
    } else if (hv > 0) {
      best_pos = mid;
      limit = mid;
    } else {
      best_zero = mid;
      limit = mid;
    }
  }
  if (best_zero != subtree::NODE_NULL) {
    // Found a zero
    if (value != nullptr) {
      copyout(value, &this->d.a.values[best_zero]);
    }
    *idxp = best_zero - this->d.a.start_idx;
    return 0;
  }
  if (best_pos != subtree::NODE_NULL)
    *idxp = best_pos - this->d.a.start_idx;
  else
    *idxp = this->d.a.num_values;
  return DB_NOTFOUND;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
int omt<omtdata_t, omtdataout_t, supports_marks>::find_internal_zero(
    const subtree &st, const omtcmp_t &extra, omtdataout_t *const value,
    uint32_t *const idxp) const {
  paranoid_invariant_notnull(idxp);
  if (st.is_null()) {
    *idxp = 0;
    return DB_NOTFOUND;
  }
  omt_node &n = this->d.t.nodes[st.get_index()];
  int hv = h(n.value, extra);
  if (hv < 0) {
    int r = this->find_internal_zero<omtcmp_t, h>(n.right, extra, value, idxp);
    *idxp += this->nweight(n.left) + 1;
    return r;
  } else if (hv > 0) {
    return this->find_internal_zero<omtcmp_t, h>(n.left, extra, value, idxp);
  } else {
    int r = this->find_internal_zero<omtcmp_t, h>(n.left, extra, value, idxp);
    if (r == DB_NOTFOUND) {
      *idxp = this->nweight(n.left);
      if (value != nullptr) {
        copyout(value, &n);
      }
      r = 0;
    }
    return r;
  }
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
int omt<omtdata_t, omtdataout_t, supports_marks>::find_internal_plus_array(
    const omtcmp_t &extra, omtdataout_t *const value,
    uint32_t *const idxp) const {
  paranoid_invariant_notnull(idxp);
  uint32_t min = this->d.a.start_idx;
  uint32_t limit = this->d.a.start_idx + this->d.a.num_values;
  uint32_t best = subtree::NODE_NULL;

  while (min != limit) {
    const uint32_t mid = (min + limit) / 2;
    const int hv = h(this->d.a.values[mid], extra);
    if (hv > 0) {
      best = mid;
      limit = mid;
    } else {
      min = mid + 1;
    }
  }
  if (best == subtree::NODE_NULL) {
    return DB_NOTFOUND;
  }
  if (value != nullptr) {
    copyout(value, &this->d.a.values[best]);
  }
  *idxp = best - this->d.a.start_idx;
  return 0;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
int omt<omtdata_t, omtdataout_t, supports_marks>::find_internal_plus(
    const subtree &st, const omtcmp_t &extra, omtdataout_t *const value,
    uint32_t *const idxp) const {
  paranoid_invariant_notnull(idxp);
  if (st.is_null()) {
    return DB_NOTFOUND;
  }
  omt_node *const n = &this->d.t.nodes[st.get_index()];
  int hv = h(n->value, extra);
  int r;
  if (hv > 0) {
    r = this->find_internal_plus<omtcmp_t, h>(n->left, extra, value, idxp);
    if (r == DB_NOTFOUND) {
      *idxp = this->nweight(n->left);
      if (value != nullptr) {
        copyout(value, n);
      }
      r = 0;
    }
  } else {
    r = this->find_internal_plus<omtcmp_t, h>(n->right, extra, value, idxp);
    if (r == 0) {
      *idxp += this->nweight(n->left) + 1;
    }
  }
  return r;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
int omt<omtdata_t, omtdataout_t, supports_marks>::find_internal_minus_array(
    const omtcmp_t &extra, omtdataout_t *const value,
    uint32_t *const idxp) const {
  paranoid_invariant_notnull(idxp);
  uint32_t min = this->d.a.start_idx;
  uint32_t limit = this->d.a.start_idx + this->d.a.num_values;
  uint32_t best = subtree::NODE_NULL;

  while (min != limit) {
    const uint32_t mid = (min + limit) / 2;
    const int hv = h(this->d.a.values[mid], extra);
    if (hv < 0) {
      best = mid;
      min = mid + 1;
    } else {
      limit = mid;
    }
  }
  if (best == subtree::NODE_NULL) {
    return DB_NOTFOUND;
  }
  if (value != nullptr) {
    copyout(value, &this->d.a.values[best]);
  }
  *idxp = best - this->d.a.start_idx;
  return 0;
}

template <typename omtdata_t, typename omtdataout_t, bool supports_marks>
template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
int omt<omtdata_t, omtdataout_t, supports_marks>::find_internal_minus(
    const subtree &st, const omtcmp_t &extra, omtdataout_t *const value,
    uint32_t *const idxp) const {
  paranoid_invariant_notnull(idxp);
  if (st.is_null()) {
    return DB_NOTFOUND;
  }
  omt_node *const n = &this->d.t.nodes[st.get_index()];
  int hv = h(n->value, extra);
  if (hv < 0) {
    int r =
        this->find_internal_minus<omtcmp_t, h>(n->right, extra, value, idxp);
    if (r == 0) {
      *idxp += this->nweight(n->left) + 1;
    } else if (r == DB_NOTFOUND) {
      *idxp = this->nweight(n->left);
      if (value != nullptr) {
        copyout(value, n);
      }
      r = 0;
    }
    return r;
  } else {
    return this->find_internal_minus<omtcmp_t, h>(n->left, extra, value, idxp);
  }
}
}  // namespace toku
