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

#include "txnid_set.h"

#include "../db.h"

namespace toku {

int find_by_txnid(const TXNID &txnid_a, const TXNID &txnid_b);
int find_by_txnid(const TXNID &txnid_a, const TXNID &txnid_b) {
  if (txnid_a < txnid_b) {
    return -1;
  } else if (txnid_a == txnid_b) {
    return 0;
  } else {
    return 1;
  }
}

void txnid_set::create(void) {
  // lazily allocate the underlying omt, since it is common
  // to create a txnid set and never put anything in it.
  m_txnids.create_no_array();
}

void txnid_set::destroy(void) { m_txnids.destroy(); }

// Return true if the given transaction id is a member of the set.
// Otherwise, return false.
bool txnid_set::contains(TXNID txnid) const {
  TXNID find_txnid;
  int r = m_txnids.find_zero<TXNID, find_by_txnid>(txnid, &find_txnid, nullptr);
  return r == 0 ? true : false;
}

// Add a given txnid to the set
void txnid_set::add(TXNID txnid) {
  int r = m_txnids.insert<TXNID, find_by_txnid>(txnid, txnid, nullptr);
  invariant(r == 0 || r == DB_KEYEXIST);
}

// Delete a given txnid from the set.
void txnid_set::remove(TXNID txnid) {
  uint32_t idx;
  int r = m_txnids.find_zero<TXNID, find_by_txnid>(txnid, nullptr, &idx);
  if (r == 0) {
    r = m_txnids.delete_at(idx);
    invariant_zero(r);
  }
}

// Return the size of the set
uint32_t txnid_set::size(void) const { return m_txnids.size(); }

// Get the ith id in the set, assuming that the set is sorted.
TXNID txnid_set::get(uint32_t i) const {
  TXNID txnid;
  int r = m_txnids.fetch(i, &txnid);
  if (r == EINVAL) /* Shouldn't happen, avoid compiler warning */
    return TXNID_NONE;
  invariant_zero(r);
  return txnid;
}

} /* namespace toku */
#endif  // OS_WIN
#endif  // ROCKSDB_LITE
