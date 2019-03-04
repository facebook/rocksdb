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
======= */

#ident "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#pragma once

#include <db.h>

// TODO: John
// Document this API a little better so that DBT
// memory management can be morm widely understood.

DBT *toku_init_dbt(DBT *);

// returns: an initialized but empty dbt (for which toku_dbt_is_empty() is true)
DBT toku_empty_dbt(void);

DBT *toku_init_dbt_flags(DBT *, uint32_t flags);

void toku_destroy_dbt(DBT *);

DBT *toku_fill_dbt(DBT *dbt, const void *k, uint32_t len);

DBT *toku_memdup_dbt(DBT *dbt, const void *k, size_t len);

DBT *toku_copyref_dbt(DBT *dst, const DBT src);

DBT *toku_clone_dbt(DBT *dst, const DBT &src);

int toku_dbt_set(uint32_t len, const void *val, DBT *d, struct simple_dbt *sdbt);

int toku_dbt_set_value(DBT *, const void **val, uint32_t vallen, void **staticptrp, bool dbt1_disposable);

void toku_sdbt_cleanup(struct simple_dbt *sdbt);

// returns: special DBT pointer representing positive infinity
const DBT *toku_dbt_positive_infinity(void);

// returns: special DBT pointer representing negative infinity
const DBT *toku_dbt_negative_infinity(void);

// returns: true if the given dbt is either positive or negative infinity
bool toku_dbt_is_infinite(const DBT *dbt);

// returns: true if the given dbt has no data (ie: dbt->data == nullptr)
bool toku_dbt_is_empty(const DBT *dbt);

// effect: compares two potentially infinity-valued dbts
// requires: at least one is infinite (assert otherwise)
int toku_dbt_infinite_compare(const DBT *a, const DBT *b);

// returns: true if the given dbts have the same data pointer and size
bool toku_dbt_equals(const DBT *a, const DBT *b);
