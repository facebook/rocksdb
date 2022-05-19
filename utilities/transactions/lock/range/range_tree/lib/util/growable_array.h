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

#include <memory.h>

//******************************************************************************
//
// Overview: A growable array is a little bit like std::vector except that
//  it doesn't have constructors (hence can be used in static constructs, since
//  the google style guide says no constructors), and it's a little simpler.
// Operations:
//   init and deinit (we don't have constructors and destructors).
//   fetch_unchecked to get values out.
//   store_unchecked to put values in.
//   push to add an element at the end
//   get_size to find out the size
//   get_memory_size to find out how much memory the data stucture is using.
//
//******************************************************************************

namespace toku {

template <typename T>
class GrowableArray {
 public:
  void init(void)
  // Effect: Initialize the array to contain no elements.
  {
    m_array = NULL;
    m_size = 0;
    m_size_limit = 0;
  }

  void deinit(void)
  // Effect: Deinitialize the array (freeing any memory it uses, for example).
  {
    toku_free(m_array);
    m_array = NULL;
    m_size = 0;
    m_size_limit = 0;
  }

  T fetch_unchecked(size_t i) const
  // Effect: Fetch the ith element.  If i is out of range, the system asserts.
  {
    return m_array[i];
  }

  void store_unchecked(size_t i, T v)
  // Effect: Store v in the ith element.  If i is out of range, the system
  // asserts.
  {
    paranoid_invariant(i < m_size);
    m_array[i] = v;
  }

  void push(T v)
  // Effect: Add v to the end of the array (increasing the size).  The amortized
  // cost of this operation is constant. Implementation hint:  Double the size
  // of the array when it gets too big so that the amortized cost stays
  // constant.
  {
    if (m_size >= m_size_limit) {
      if (m_array == NULL) {
        m_size_limit = 1;
      } else {
        m_size_limit *= 2;
      }
      XREALLOC_N(m_size_limit, m_array);
    }
    m_array[m_size++] = v;
  }

  size_t get_size(void) const
  // Effect: Return the number of elements in the array.
  {
    return m_size;
  }
  size_t memory_size(void) const
  // Effect: Return the size (in bytes) that the array occupies in memory.  This
  // is really only an estimate.
  {
    return sizeof(*this) + sizeof(T) * m_size_limit;
  }

 private:
  T *m_array;
  size_t m_size;
  size_t m_size_limit;  // How much space is allocated in array.
};

}  // namespace toku
