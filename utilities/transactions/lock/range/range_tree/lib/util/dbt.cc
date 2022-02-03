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
======= */

#ident \
    "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#include "dbt.h"

#include <string.h>

#include "../db.h"
#include "../portability/memory.h"

DBT *toku_init_dbt(DBT *dbt) {
  memset(dbt, 0, sizeof(*dbt));
  return dbt;
}

DBT toku_empty_dbt(void) {
  static const DBT empty_dbt = {.data = 0, .size = 0, .ulen = 0, .flags = 0};
  return empty_dbt;
}

DBT *toku_init_dbt_flags(DBT *dbt, uint32_t flags) {
  toku_init_dbt(dbt);
  dbt->flags = flags;
  return dbt;
}

void toku_destroy_dbt(DBT *dbt) {
  switch (dbt->flags) {
    case DB_DBT_MALLOC:
    case DB_DBT_REALLOC:
      toku_free(dbt->data);
      toku_init_dbt(dbt);
      break;
  }
}

DBT *toku_fill_dbt(DBT *dbt, const void *k, size_t len) {
  toku_init_dbt(dbt);
  dbt->size = len;
  dbt->data = (char *)k;
  return dbt;
}

DBT *toku_memdup_dbt(DBT *dbt, const void *k, size_t len) {
  toku_init_dbt_flags(dbt, DB_DBT_MALLOC);
  dbt->size = len;
  dbt->data = toku_xmemdup(k, len);
  return dbt;
}

DBT *toku_copyref_dbt(DBT *dst, const DBT src) {
  dst->flags = 0;
  dst->ulen = 0;
  dst->size = src.size;
  dst->data = src.data;
  return dst;
}

DBT *toku_clone_dbt(DBT *dst, const DBT &src) {
  return toku_memdup_dbt(dst, src.data, src.size);
}

void toku_sdbt_cleanup(struct simple_dbt *sdbt) {
  if (sdbt->data) toku_free(sdbt->data);
  memset(sdbt, 0, sizeof(*sdbt));
}

const DBT *toku_dbt_positive_infinity(void) {
  static DBT positive_infinity_dbt = {
      .data = 0, .size = 0, .ulen = 0, .flags = 0};  // port
  return &positive_infinity_dbt;
}

const DBT *toku_dbt_negative_infinity(void) {
  static DBT negative_infinity_dbt = {
      .data = 0, .size = 0, .ulen = 0, .flags = 0};  // port
  return &negative_infinity_dbt;
}

bool toku_dbt_is_infinite(const DBT *dbt) {
  return dbt == toku_dbt_positive_infinity() ||
         dbt == toku_dbt_negative_infinity();
}

bool toku_dbt_is_empty(const DBT *dbt) {
  // can't have a null data field with a non-zero size
  paranoid_invariant(dbt->data != nullptr || dbt->size == 0);
  return dbt->data == nullptr;
}

int toku_dbt_infinite_compare(const DBT *a, const DBT *b) {
  if (a == b) {
    return 0;
  } else if (a == toku_dbt_positive_infinity()) {
    return 1;
  } else if (b == toku_dbt_positive_infinity()) {
    return -1;
  } else if (a == toku_dbt_negative_infinity()) {
    return -1;
  } else {
    invariant(b == toku_dbt_negative_infinity());
    return 1;
  }
}

bool toku_dbt_equals(const DBT *a, const DBT *b) {
  if (!toku_dbt_is_infinite(a) && !toku_dbt_is_infinite(b)) {
    return a->data == b->data && a->size == b->size;
  } else {
    // a or b is infinite, so they're equal if they are the same infinite
    return a == b ? true : false;
  }
}
#endif  // OS_WIN
#endif  // ROCKSDB_LITE
