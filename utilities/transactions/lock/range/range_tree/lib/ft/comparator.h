/* -*- mode: C; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
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

#include "../db.h"
#include "../portability/memory.h"
#include "../util/dbt.h"

typedef int (*ft_compare_func)(void *arg, const DBT *a, const DBT *b);

int toku_keycompare(const void *key1, size_t key1len, const void *key2,
                    size_t key2len);

int toku_builtin_compare_fun(const DBT *, const DBT *)
    __attribute__((__visibility__("default")));

namespace toku {

// a comparator object encapsulates the data necessary for
// comparing two keys in a fractal tree. it further understands
// that points may be positive or negative infinity.

class comparator {
  void init(ft_compare_func cmp, void *cmp_arg, uint8_t memcmp_magic) {
    _cmp = cmp;
    _cmp_arg = cmp_arg;
    _memcmp_magic = memcmp_magic;
  }

 public:
  // This magic value is reserved to mean that the magic has not been set.
  static const uint8_t MEMCMP_MAGIC_NONE = 0;

  void create(ft_compare_func cmp, void *cmp_arg,
              uint8_t memcmp_magic = MEMCMP_MAGIC_NONE) {
    init(cmp, cmp_arg, memcmp_magic);
  }

  // inherit the attributes of another comparator, but keep our own
  // copy of fake_db that is owned separately from the one given.
  void inherit(const comparator &cmp) {
    invariant_notnull(cmp._cmp);
    init(cmp._cmp, cmp._cmp_arg, cmp._memcmp_magic);
  }

  // like inherit, but doesn't require that the this comparator
  // was already created
  void create_from(const comparator &cmp) { inherit(cmp); }

  void destroy() {}

  ft_compare_func get_compare_func() const { return _cmp; }

  uint8_t get_memcmp_magic() const { return _memcmp_magic; }

  bool valid() const { return _cmp != nullptr; }

  inline bool dbt_has_memcmp_magic(const DBT *dbt) const {
    return *reinterpret_cast<const char *>(dbt->data) == _memcmp_magic;
  }

  int operator()(const DBT *a, const DBT *b) const {
    if (__builtin_expect(toku_dbt_is_infinite(a) || toku_dbt_is_infinite(b),
                         0)) {
      return toku_dbt_infinite_compare(a, b);
    } else if (_memcmp_magic != MEMCMP_MAGIC_NONE
               // If `a' has the memcmp magic..
               && dbt_has_memcmp_magic(a)
               // ..then we expect `b' to also have the memcmp magic
               && __builtin_expect(dbt_has_memcmp_magic(b), 1)) {
      assert(0);  // psergey: this branch should not be taken.
      return toku_builtin_compare_fun(a, b);
    } else {
      // yikes, const sadness here
      return _cmp(_cmp_arg, a, b);
    }
  }

 private:
  ft_compare_func _cmp;
  void *_cmp_arg;

  uint8_t _memcmp_magic;
};

} /* namespace toku */
