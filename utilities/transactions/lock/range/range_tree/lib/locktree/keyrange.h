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

#pragma once

#include "../ft/comparator.h"

namespace toku {

// A keyrange has a left and right key as endpoints.
//
// When a keyrange is created it owns no memory, but when it copies
// or extends another keyrange, it copies memory as necessary. This
// means it is cheap in the common case.

class keyrange {
 public:
  // effect: constructor that borrows left and right key pointers.
  //         no memory is allocated or copied.
  void create(const DBT *left_key, const DBT *right_key);

  // effect: constructor that allocates and copies another keyrange's points.
  void create_copy(const keyrange &range);

  // effect: destroys the keyrange, freeing any allocated memory
  void destroy(void);

  // effect: extends the keyrange by choosing the leftmost and rightmost
  //         endpoints from this range and the given range.
  //         replaced keys in this range are freed, new keys are copied.
  void extend(const comparator &cmp, const keyrange &range);

  // returns: the amount of memory this keyrange takes. does not account
  //          for point optimizations or malloc overhead.
  uint64_t get_memory_size(void) const;

  // returns: pointer to the left key of this range
  const DBT *get_left_key(void) const;

  // returns: pointer to the right key of this range
  const DBT *get_right_key(void) const;

  // two ranges are either equal, lt, gt, or overlapping
  enum comparison { EQUALS, LESS_THAN, GREATER_THAN, OVERLAPS };

  // effect: compares this range to the given range
  // returns: LESS_THAN    if given range is strictly to the left
  //          GREATER_THAN if given range is strictly to the right
  //          EQUALS       if given range has the same left and right endpoints
  //          OVERLAPS     if at least one of the given range's endpoints falls
  //                       between this range's endpoints
  comparison compare(const comparator &cmp, const keyrange &range) const;

  // returns: true if the range and the given range are equal or overlapping
  bool overlaps(const comparator &cmp, const keyrange &range) const;

  // returns: a keyrange representing -inf, +inf
  static keyrange get_infinite_range(void);

 private:
  // some keys should be copied, some keys should not be.
  //
  // to support both, we use two DBTs for copies and two pointers
  // for temporaries. the access rule is:
  // - if a pointer is non-null, then it reprsents the key.
  // - otherwise the pointer is null, and the key is in the copy.
  DBT m_left_key_copy;
  DBT m_right_key_copy;
  const DBT *m_left_key;
  const DBT *m_right_key;

  // if this range is a point range, then m_left_key == m_right_key
  // and the actual data is stored exactly once in m_left_key_copy.
  bool m_point_range;

  // effect: initializes a keyrange to be empty
  void init_empty(void);

  // effect: copies the given key once into the left key copy
  //         and sets the right key copy to share the left.
  // rationale: optimization for point ranges to only do one malloc
  void set_both_keys(const DBT *key);

  // effect: destroys the current left key. sets and copies the new one.
  void replace_left_key(const DBT *key);

  // effect: destroys the current right key. sets and copies the new one.
  void replace_right_key(const DBT *key);
};

} /* namespace toku */
