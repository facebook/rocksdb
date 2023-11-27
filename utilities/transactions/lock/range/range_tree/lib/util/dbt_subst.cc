//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// A substitute for util/dbt.cc
//
#ifndef OS_WIN

#include "dbt.h"

#include <string.h>

#include "../db.h"
#include "../portability/memory.h"

DBT *toku_init_dbt(DBT *dbt) {
  memset(dbt, 0, sizeof(*dbt));
  return dbt;
}

DBT *toku_init_dbt_flags(DBT *dbt, uint32_t flags) {
  toku_init_dbt(dbt);
  dbt->flags = flags;
  return dbt;
}

void toku_destroy_dbt(DBT *dbt) {
  if (dbt->flags == DB_DBT_MALLOC || dbt->flags == DB_DBT_REALLOC) {
    toku_free(dbt->data);
    toku_init_dbt(dbt);
  }
}

DBT *toku_fill_dbt(DBT *dbt, const void *k, size_t len) {
  toku_init_dbt(dbt);
  dbt->size = len;
  dbt->data = const_cast<void*>(k);
  return dbt;
}

DBT *toku_memdup_dbt(DBT *dbt, const void *k, size_t len) {
  toku_init_dbt_flags(dbt, DB_DBT_MALLOC);
  dbt->size = len;
  dbt->data = toku_xmemdup(k, len);
  return dbt;
}

DBT *toku_copyref_dbt(DBT *dst, const DBT src) {
  toku_init_dbt(dst);
  dst->size = src.size;
  dst->data = src.data;
  return dst;
}

DBT *toku_clone_dbt(DBT *dst, const DBT &src) {
  return toku_memdup_dbt(dst, src.data, src.size);
}

const DBT *toku_dbt_positive_infinity(void) {
  static const DBT positive_infinity_dbt = {};
  return &positive_infinity_dbt;
}

const DBT *toku_dbt_negative_infinity(void) {
  static const DBT negative_infinity_dbt = {};
  return &negative_infinity_dbt;
}

bool toku_dbt_is_infinite(const DBT *dbt) {
  return dbt == toku_dbt_positive_infinity() ||
         dbt == toku_dbt_negative_infinity();
}

int toku_dbt_infinite_compare(const DBT *a, const DBT *b) {
  if (a == toku_dbt_positive_infinity()) {
    return (b == toku_dbt_positive_infinity()) ? 0 : 1;
  } else if (a == toku_dbt_negative_infinity()) {
    return (b == toku_dbt_negative_infinity()) ? 0 : -1;
  } else {
    if (b == toku_dbt_positive_infinity()) {
      return -1;
    } else if (b == toku_dbt_negative_infinity()) {
      return 1;
    } else {
      invariant(!"At least one argument should be infinity");
      return 0;
    }
  }
}

bool toku_dbt_equals(const DBT *a, const DBT *b) {
  if (a == b) {
    return true;
  } else if (toku_dbt_is_infinite(a) || toku_dbt_is_infinite(b)) {
    return false;
  } else {
    return a->data == b->data && a->size == b->size;
  }
}

#endif  // OS_WIN
