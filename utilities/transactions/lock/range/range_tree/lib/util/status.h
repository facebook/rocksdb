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

#include "partitioned_counter.h"
// PORT2: #include <util/constexpr.h>

#define TOKUFT_STATUS_INIT(array, k, c, t, l, inc)                    \
  do {                                                                \
    array.status[k].keyname = #k;                                     \
    array.status[k].columnname = #c;                                  \
    array.status[k].type = t;                                         \
    array.status[k].legend = l;                                       \
    constexpr_static_assert(                                          \
        strcmp(#c, "NULL") && strcmp(#c, "0"),                        \
        "Use nullptr for no column name instead of NULL, 0, etc..."); \
    constexpr_static_assert(                                          \
        (inc) == TOKU_ENGINE_STATUS || strcmp(#c, "nullptr"),         \
        "Missing column name.");                                      \
    array.status[k].include =                                         \
        static_cast<toku_engine_status_include_type>(inc);            \
    if (t == STATUS_PARCOUNT) {                                       \
      array.status[k].value.parcount = create_partitioned_counter();  \
    }                                                                 \
  } while (0)
