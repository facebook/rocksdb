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

#include <db.h>
#include <string.h>

#include "portability/memory.h"

#include "util/dbt.h"

DBT *toku_init_dbt(DBT *dbt) {
    memset(dbt, 0, sizeof(*dbt));
    return dbt;
}

DBT toku_empty_dbt(void) {
    static const DBT empty_dbt = { .data = 0, .size = 0, .ulen = 0, .flags = 0 };
    return empty_dbt;
}

DBT *toku_init_dbt_flags(DBT *dbt, uint32_t flags) {
    toku_init_dbt(dbt);
    dbt->flags = flags;
    return dbt;
}

DBT_ARRAY *toku_dbt_array_init(DBT_ARRAY *dbts, uint32_t size) {
    uint32_t capacity = 1;
    while (capacity < size) { capacity *= 2; }

    XMALLOC_N(capacity, dbts->dbts);
    for (uint32_t i = 0; i < capacity; i++) {
        toku_init_dbt_flags(&dbts->dbts[i], DB_DBT_REALLOC);
    }
    dbts->size = size;
    dbts->capacity = capacity;
    return dbts;
}

void toku_dbt_array_resize(DBT_ARRAY *dbts, uint32_t size) {
    if (size != dbts->size) {
        if (size > dbts->capacity) {
            const uint32_t old_capacity = dbts->capacity;
            uint32_t new_capacity = dbts->capacity;
            while (new_capacity < size) {
                new_capacity *= 2;
            }
            dbts->capacity = new_capacity;
            XREALLOC_N(new_capacity, dbts->dbts);
            for (uint32_t i = old_capacity; i < new_capacity; i++) {
                toku_init_dbt_flags(&dbts->dbts[i], DB_DBT_REALLOC);
            }
        } else if (size < dbts->size) {
            if (dbts->capacity >= 8 && size < dbts->capacity / 4) {
                const int old_capacity = dbts->capacity;
                const int new_capacity = dbts->capacity / 2;
                for (int i = new_capacity; i < old_capacity; i++) {
                    toku_destroy_dbt(&dbts->dbts[i]);
                }
                XREALLOC_N(new_capacity, dbts->dbts);
                dbts->capacity = new_capacity;
            }
        }
        dbts->size = size;
    }
}

void toku_dbt_array_destroy_shallow(DBT_ARRAY *dbts) {
    toku_free(dbts->dbts);
    ZERO_STRUCT(*dbts);
}

void toku_dbt_array_destroy(DBT_ARRAY *dbts) {
    for (uint32_t i = 0; i < dbts->capacity; i++) {
        toku_destroy_dbt(&dbts->dbts[i]);
    }
    toku_dbt_array_destroy_shallow(dbts);
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

DBT *toku_fill_dbt(DBT *dbt, const void *k, uint32_t len) {
    toku_init_dbt(dbt);
    dbt->size=len;
    dbt->data=(char*)k;
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

void
toku_sdbt_cleanup(struct simple_dbt *sdbt) {
    if (sdbt->data) toku_free(sdbt->data);
    memset(sdbt, 0, sizeof(*sdbt));
}

static inline int sdbt_realloc(struct simple_dbt *sdbt) {
    void *new_data = toku_realloc(sdbt->data, sdbt->len);
    int r;
    if (new_data == NULL) {
        r = get_error_errno();
    } else {
        sdbt->data = new_data;
        r = 0;
    }
    return r;
}

static inline int dbt_realloc(DBT *dbt) {
    void *new_data = toku_realloc(dbt->data, dbt->ulen);
    int r;
    if (new_data == NULL) {
        r = get_error_errno();
    } else {
        dbt->data = new_data;
        r = 0;
    }
    return r;
}

// sdbt is the static value used when flags==0
// Otherwise malloc or use the user-supplied memory, as according to the flags in d->flags.
int toku_dbt_set(uint32_t len, const void *val, DBT *d, struct simple_dbt *sdbt) {
    int r;
    if (d == nullptr) {
        r = 0;
    } else {
        switch (d->flags) {
        case (DB_DBT_USERMEM):
            d->size = len;
            if (d->ulen<len) r = DB_BUFFER_SMALL;
            else {
                memcpy(d->data, val, len);
                r = 0;
            }
            break;
        case (DB_DBT_MALLOC):
            d->data = NULL;
            d->ulen = 0;
            // fallthrough
            // to DB_DBT_REALLOC
        case (DB_DBT_REALLOC):
            if (d->ulen < len) {
                d->ulen = len*2;
                r = dbt_realloc(d);
            }
            else if (d->ulen > 16 && d->ulen > len*4) {
                d->ulen = len*2 < 16 ? 16 : len*2;
                r = dbt_realloc(d);
            }
            else if (d->data==NULL) {
                d->ulen = len;
                r = dbt_realloc(d);
            }
            else r=0;

            if (r==0) {
                memcpy(d->data, val, len);
                d->size = len;
            }
            break;
        case (0):
            if (sdbt->len < len) {
                sdbt->len = len*2;
                r = sdbt_realloc(sdbt);
            }
            else if (sdbt->len > 16 && sdbt->len > len*4) {
                sdbt->len = len*2 < 16 ? 16 : len*2;
                r = sdbt_realloc(sdbt);
            }
            else r=0;

            if (r==0) {
                memcpy(sdbt->data, val, len);
                d->data = sdbt->data;
                d->size = len;
            }
            break;
        default:
            r = EINVAL;
            break;
        }
    }
    return r;
}

const DBT *toku_dbt_positive_infinity(void) {
    static DBT positive_infinity_dbt = { .data = 0, .size = 0, .ulen = 0, .flags = 0 }; // port
    return &positive_infinity_dbt;
}

const DBT *toku_dbt_negative_infinity(void) {
    static DBT negative_infinity_dbt = { .data = 0, .size = 0, .ulen = 0, .flags = 0 }; // port
    return &negative_infinity_dbt;
}

bool toku_dbt_is_infinite(const DBT *dbt) {
    return dbt == toku_dbt_positive_infinity() || dbt == toku_dbt_negative_infinity();
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
