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

#include "keyrange.h"

#include "../util/dbt.h"

namespace toku {

// create a keyrange by borrowing the left and right dbt
// pointers. no memory is copied. no checks for infinity needed.
void keyrange::create(const DBT *left, const DBT *right) {
  init_empty();
  m_left_key = left;
  m_right_key = right;
}

// destroy the key copies. if they were never set, then destroy does nothing.
void keyrange::destroy(void) {
  toku_destroy_dbt(&m_left_key_copy);
  toku_destroy_dbt(&m_right_key_copy);
}

// create a keyrange by copying the keys from the given range.
void keyrange::create_copy(const keyrange &range) {
  // start with an initialized, empty range
  init_empty();

  // optimize the case where the left and right keys are the same.
  // we'd like to only have one copy of the data.
  if (toku_dbt_equals(range.get_left_key(), range.get_right_key())) {
    set_both_keys(range.get_left_key());
  } else {
    // replace our empty left and right keys with
    // copies of the range's left and right keys
    replace_left_key(range.get_left_key());
    replace_right_key(range.get_right_key());
  }
}

// extend this keyrange by choosing the leftmost and rightmost
// endpoints between this range and the given. replaced keys
// in this range are freed and inherited keys are copied.
void keyrange::extend(const comparator &cmp, const keyrange &range) {
  const DBT *range_left = range.get_left_key();
  const DBT *range_right = range.get_right_key();
  if (cmp(range_left, get_left_key()) < 0) {
    replace_left_key(range_left);
  }
  if (cmp(range_right, get_right_key()) > 0) {
    replace_right_key(range_right);
  }
}

// how much memory does this keyrange take?
// - the size of the left and right keys
// --- ignore the fact that we may have optimized the point case.
//     it complicates things for little gain.
// - the size of the keyrange class itself
uint64_t keyrange::get_memory_size(void) const {
  const DBT *left_key = get_left_key();
  const DBT *right_key = get_right_key();
  return left_key->size + right_key->size + sizeof(keyrange);
}

// compare ranges.
keyrange::comparison keyrange::compare(const comparator &cmp,
                                       const keyrange &range) const {
  if (cmp(get_right_key(), range.get_left_key()) < 0) {
    return comparison::LESS_THAN;
  } else if (cmp(get_left_key(), range.get_right_key()) > 0) {
    return comparison::GREATER_THAN;
  } else if (cmp(get_left_key(), range.get_left_key()) == 0 &&
             cmp(get_right_key(), range.get_right_key()) == 0) {
    return comparison::EQUALS;
  } else {
    return comparison::OVERLAPS;
  }
}

bool keyrange::overlaps(const comparator &cmp, const keyrange &range) const {
  // equality is a stronger form of overlapping.
  // so two ranges "overlap" if they're either equal or just overlapping.
  comparison c = compare(cmp, range);
  return c == comparison::EQUALS || c == comparison::OVERLAPS;
}

keyrange keyrange::get_infinite_range(void) {
  keyrange range;
  range.create(toku_dbt_negative_infinity(), toku_dbt_positive_infinity());
  return range;
}

void keyrange::init_empty(void) {
  m_left_key = nullptr;
  m_right_key = nullptr;
  toku_init_dbt(&m_left_key_copy);
  toku_init_dbt(&m_right_key_copy);
  m_point_range = false;
}

const DBT *keyrange::get_left_key(void) const {
  if (m_left_key) {
    return m_left_key;
  } else {
    return &m_left_key_copy;
  }
}

const DBT *keyrange::get_right_key(void) const {
  if (m_right_key) {
    return m_right_key;
  } else {
    return &m_right_key_copy;
  }
}

// copy the given once and set both the left and right pointers.
// optimization for point ranges, so the left and right ranges
// are not copied twice.
void keyrange::set_both_keys(const DBT *key) {
  if (toku_dbt_is_infinite(key)) {
    m_left_key = key;
    m_right_key = key;
  } else {
    toku_clone_dbt(&m_left_key_copy, *key);
    toku_copyref_dbt(&m_right_key_copy, m_left_key_copy);
  }
  m_point_range = true;
}

// destroy the current left key. set and possibly copy the new one
void keyrange::replace_left_key(const DBT *key) {
  // a little magic:
  //
  // if this is a point range, then the left and right keys share
  // one copy of the data, and it lives in the left key copy. so
  // if we're replacing the left key, move the real data to the
  // right key copy instead of destroying it. now, the memory is
  // owned by the right key and the left key may be replaced.
  if (m_point_range) {
    m_right_key_copy = m_left_key_copy;
  } else {
    toku_destroy_dbt(&m_left_key_copy);
  }

  if (toku_dbt_is_infinite(key)) {
    m_left_key = key;
  } else {
    toku_clone_dbt(&m_left_key_copy, *key);
    m_left_key = nullptr;
  }
  m_point_range = false;
}

// destroy the current right key. set and possibly copy the new one
void keyrange::replace_right_key(const DBT *key) {
  toku_destroy_dbt(&m_right_key_copy);
  if (toku_dbt_is_infinite(key)) {
    m_right_key = key;
  } else {
    toku_clone_dbt(&m_right_key_copy, *key);
    m_right_key = nullptr;
  }
  m_point_range = false;
}

} /* namespace toku */
#endif  // OS_WIN
#endif  // ROCKSDB_LITE
