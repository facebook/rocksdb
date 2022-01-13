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

#include "../db.h"
#include "../portability/toku_race_tools.h"
#include "../util/status.h"

//
// Lock Tree Manager statistics
//
class LTM_STATUS_S {
 public:
  enum {
    LTM_SIZE_CURRENT = 0,
    LTM_SIZE_LIMIT,
    LTM_ESCALATION_COUNT,
    LTM_ESCALATION_TIME,
    LTM_ESCALATION_LATEST_RESULT,
    LTM_NUM_LOCKTREES,
    LTM_LOCK_REQUESTS_PENDING,
    LTM_STO_NUM_ELIGIBLE,
    LTM_STO_END_EARLY_COUNT,
    LTM_STO_END_EARLY_TIME,
    LTM_WAIT_COUNT,
    LTM_WAIT_TIME,
    LTM_LONG_WAIT_COUNT,
    LTM_LONG_WAIT_TIME,
    LTM_TIMEOUT_COUNT,
    LTM_WAIT_ESCALATION_COUNT,
    LTM_WAIT_ESCALATION_TIME,
    LTM_LONG_WAIT_ESCALATION_COUNT,
    LTM_LONG_WAIT_ESCALATION_TIME,
    LTM_STATUS_NUM_ROWS  // must be last
  };

  void init(void);
  void destroy(void);

  TOKU_ENGINE_STATUS_ROW_S status[LTM_STATUS_NUM_ROWS];

 private:
  bool m_initialized = false;
};
typedef LTM_STATUS_S* LTM_STATUS;
extern LTM_STATUS_S ltm_status;

#define LTM_STATUS_VAL(x) ltm_status.status[LTM_STATUS_S::x].value.num

void toku_status_init(void);     // just call ltm_status.init();
void toku_status_destroy(void);  // just call ltm_status.destroy();
