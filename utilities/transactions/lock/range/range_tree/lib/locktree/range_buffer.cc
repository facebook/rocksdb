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

#include "range_buffer.h"

#include <string.h>

#include "../portability/memory.h"
#include "../util/dbt.h"

namespace toku {

bool range_buffer::record_header::left_is_infinite(void) const {
  return left_neg_inf || left_pos_inf;
}

bool range_buffer::record_header::right_is_infinite(void) const {
  return right_neg_inf || right_pos_inf;
}

void range_buffer::record_header::init(const DBT *left_key,
                                       const DBT *right_key,
                                       bool is_exclusive) {
  is_exclusive_lock = is_exclusive;
  left_neg_inf = left_key == toku_dbt_negative_infinity();
  left_pos_inf = left_key == toku_dbt_positive_infinity();
  left_key_size = toku_dbt_is_infinite(left_key) ? 0 : left_key->size;
  if (right_key) {
    right_neg_inf = right_key == toku_dbt_negative_infinity();
    right_pos_inf = right_key == toku_dbt_positive_infinity();
    right_key_size = toku_dbt_is_infinite(right_key) ? 0 : right_key->size;
  } else {
    right_neg_inf = left_neg_inf;
    right_pos_inf = left_pos_inf;
    right_key_size = 0;
  }
}

const DBT *range_buffer::iterator::record::get_left_key(void) const {
  if (_header.left_neg_inf) {
    return toku_dbt_negative_infinity();
  } else if (_header.left_pos_inf) {
    return toku_dbt_positive_infinity();
  } else {
    return &_left_key;
  }
}

const DBT *range_buffer::iterator::record::get_right_key(void) const {
  if (_header.right_neg_inf) {
    return toku_dbt_negative_infinity();
  } else if (_header.right_pos_inf) {
    return toku_dbt_positive_infinity();
  } else {
    return &_right_key;
  }
}

size_t range_buffer::iterator::record::size(void) const {
  return sizeof(record_header) + _header.left_key_size + _header.right_key_size;
}

void range_buffer::iterator::record::deserialize(const char *buf) {
  size_t current = 0;

  // deserialize the header
  memcpy(&_header, buf, sizeof(record_header));
  current += sizeof(record_header);

  // deserialize the left key if necessary
  if (!_header.left_is_infinite()) {
    // point the left DBT's buffer into ours
    toku_fill_dbt(&_left_key, buf + current, _header.left_key_size);
    current += _header.left_key_size;
  }

  // deserialize the right key if necessary
  if (!_header.right_is_infinite()) {
    if (_header.right_key_size == 0) {
      toku_copyref_dbt(&_right_key, _left_key);
    } else {
      toku_fill_dbt(&_right_key, buf + current, _header.right_key_size);
    }
  }
}

toku::range_buffer::iterator::iterator()
    : _ma_chunk_iterator(nullptr),
      _current_chunk_base(nullptr),
      _current_chunk_offset(0),
      _current_chunk_max(0),
      _current_rec_size(0) {}

toku::range_buffer::iterator::iterator(const range_buffer *buffer)
    : _ma_chunk_iterator(&buffer->_arena),
      _current_chunk_base(nullptr),
      _current_chunk_offset(0),
      _current_chunk_max(0),
      _current_rec_size(0) {
  reset_current_chunk();
}

void range_buffer::iterator::reset_current_chunk() {
  _current_chunk_base = _ma_chunk_iterator.current(&_current_chunk_max);
  _current_chunk_offset = 0;
}

bool range_buffer::iterator::current(record *rec) {
  if (_current_chunk_offset < _current_chunk_max) {
    const char *buf = reinterpret_cast<const char *>(_current_chunk_base);
    rec->deserialize(buf + _current_chunk_offset);
    _current_rec_size = rec->size();
    return true;
  } else {
    return false;
  }
}

// move the iterator to the next record in the buffer
void range_buffer::iterator::next(void) {
  invariant(_current_chunk_offset < _current_chunk_max);
  invariant(_current_rec_size > 0);

  // the next record is _current_rec_size bytes forward
  _current_chunk_offset += _current_rec_size;
  // now, we don't know how big the current is, set it to 0.
  _current_rec_size = 0;

  if (_current_chunk_offset >= _current_chunk_max) {
    // current chunk is exhausted, try moving to the next one
    if (_ma_chunk_iterator.more()) {
      _ma_chunk_iterator.next();
      reset_current_chunk();
    }
  }
}

void range_buffer::create(void) {
  // allocate buffer space lazily instead of on creation. this way,
  // no malloc/free is done if the transaction ends up taking no locks.
  _arena.create(0);
  _num_ranges = 0;
}

void range_buffer::append(const DBT *left_key, const DBT *right_key,
                          bool is_write_request) {
  // if the keys are equal, then only one copy is stored.
  if (toku_dbt_equals(left_key, right_key)) {
    invariant(left_key->size <= MAX_KEY_SIZE);
    append_point(left_key, is_write_request);
  } else {
    invariant(left_key->size <= MAX_KEY_SIZE);
    invariant(right_key->size <= MAX_KEY_SIZE);
    append_range(left_key, right_key, is_write_request);
  }
  _num_ranges++;
}

bool range_buffer::is_empty(void) const { return total_memory_size() == 0; }

uint64_t range_buffer::total_memory_size(void) const {
  return _arena.total_size_in_use();
}

int range_buffer::get_num_ranges(void) const { return _num_ranges; }

void range_buffer::destroy(void) { _arena.destroy(); }

void range_buffer::append_range(const DBT *left_key, const DBT *right_key,
                                bool is_exclusive) {
  size_t record_length =
      sizeof(record_header) + left_key->size + right_key->size;
  char *buf = reinterpret_cast<char *>(_arena.malloc_from_arena(record_length));

  record_header h;
  h.init(left_key, right_key, is_exclusive);

  // serialize the header
  memcpy(buf, &h, sizeof(record_header));
  buf += sizeof(record_header);

  // serialize the left key if necessary
  if (!h.left_is_infinite()) {
    memcpy(buf, left_key->data, left_key->size);
    buf += left_key->size;
  }

  // serialize the right key if necessary
  if (!h.right_is_infinite()) {
    memcpy(buf, right_key->data, right_key->size);
  }
}

void range_buffer::append_point(const DBT *key, bool is_exclusive) {
  size_t record_length = sizeof(record_header) + key->size;
  char *buf = reinterpret_cast<char *>(_arena.malloc_from_arena(record_length));

  record_header h;
  h.init(key, nullptr, is_exclusive);

  // serialize the header
  memcpy(buf, &h, sizeof(record_header));
  buf += sizeof(record_header);

  // serialize the key if necessary
  if (!h.left_is_infinite()) {
    memcpy(buf, key->data, key->size);
  }
}

} /* namespace toku */
#endif  // OS_WIN
#endif  // ROCKSDB_LITE
