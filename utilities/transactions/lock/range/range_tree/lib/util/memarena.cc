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

#include "memarena.h"

#include <string.h>

#include <algorithm>

#include "../portability/memory.h"

void memarena::create(size_t initial_size) {
  _current_chunk = arena_chunk();
  _other_chunks = nullptr;
  _size_of_other_chunks = 0;
  _footprint_of_other_chunks = 0;
  _n_other_chunks = 0;

  _current_chunk.size = initial_size;
  if (_current_chunk.size > 0) {
    XMALLOC_N(_current_chunk.size, _current_chunk.buf);
  }
}

void memarena::destroy(void) {
  if (_current_chunk.buf) {
    toku_free(_current_chunk.buf);
  }
  for (int i = 0; i < _n_other_chunks; i++) {
    toku_free(_other_chunks[i].buf);
  }
  if (_other_chunks) {
    toku_free(_other_chunks);
  }
  _current_chunk = arena_chunk();
  _other_chunks = nullptr;
  _n_other_chunks = 0;
}

static size_t round_to_page(size_t size) {
  const size_t page_size = 4096;
  const size_t r = page_size + ((size - 1) & ~(page_size - 1));
  assert((r & (page_size - 1)) == 0);  // make sure it's aligned
  assert(r >= size);                   // make sure it's not too small
  assert(r <
         size + page_size);  // make sure we didn't grow by more than a page.
  return r;
}

static const size_t MEMARENA_MAX_CHUNK_SIZE = 64 * 1024 * 1024;

void *memarena::malloc_from_arena(size_t size) {
  if (_current_chunk.buf == nullptr ||
      _current_chunk.size < _current_chunk.used + size) {
    // The existing block isn't big enough.
    // Add the block to the vector of blocks.
    if (_current_chunk.buf) {
      invariant(_current_chunk.size > 0);
      int old_n = _n_other_chunks;
      XREALLOC_N(old_n + 1, _other_chunks);
      _other_chunks[old_n] = _current_chunk;
      _n_other_chunks = old_n + 1;
      _size_of_other_chunks += _current_chunk.size;
      _footprint_of_other_chunks +=
          toku_memory_footprint(_current_chunk.buf, _current_chunk.used);
    }

    // Make a new one. Grow the buffer size exponentially until we hit
    // the max chunk size, but make it at least `size' bytes so the
    // current allocation always fit.
    size_t new_size =
        std::min(MEMARENA_MAX_CHUNK_SIZE, 2 * _current_chunk.size);
    if (new_size < size) {
      new_size = size;
    }
    new_size = round_to_page(
        new_size);  // at least size, but round to the next page size
    XMALLOC_N(new_size, _current_chunk.buf);
    _current_chunk.used = 0;
    _current_chunk.size = new_size;
  }
  invariant(_current_chunk.buf != nullptr);

  // allocate in the existing block.
  char *p = _current_chunk.buf + _current_chunk.used;
  _current_chunk.used += size;
  return p;
}

void memarena::move_memory(memarena *dest) {
  // Move memory to dest
  XREALLOC_N(dest->_n_other_chunks + _n_other_chunks + 1, dest->_other_chunks);
  dest->_size_of_other_chunks += _size_of_other_chunks + _current_chunk.size;
  dest->_footprint_of_other_chunks +=
      _footprint_of_other_chunks +
      toku_memory_footprint(_current_chunk.buf, _current_chunk.used);
  for (int i = 0; i < _n_other_chunks; i++) {
    dest->_other_chunks[dest->_n_other_chunks++] = _other_chunks[i];
  }
  dest->_other_chunks[dest->_n_other_chunks++] = _current_chunk;

  // Clear out this memarena's memory
  toku_free(_other_chunks);
  _current_chunk = arena_chunk();
  _other_chunks = nullptr;
  _size_of_other_chunks = 0;
  _footprint_of_other_chunks = 0;
  _n_other_chunks = 0;
}

size_t memarena::total_memory_size(void) const {
  return sizeof(*this) + total_size_in_use() +
         _n_other_chunks * sizeof(*_other_chunks);
}

size_t memarena::total_size_in_use(void) const {
  return _size_of_other_chunks + _current_chunk.used;
}

size_t memarena::total_footprint(void) const {
  return sizeof(*this) + _footprint_of_other_chunks +
         toku_memory_footprint(_current_chunk.buf, _current_chunk.used) +
         _n_other_chunks * sizeof(*_other_chunks);
}

////////////////////////////////////////////////////////////////////////////////

const void *memarena::chunk_iterator::current(size_t *used) const {
  if (_chunk_idx < 0) {
    *used = _ma->_current_chunk.used;
    return _ma->_current_chunk.buf;
  } else if (_chunk_idx < _ma->_n_other_chunks) {
    *used = _ma->_other_chunks[_chunk_idx].used;
    return _ma->_other_chunks[_chunk_idx].buf;
  }
  *used = 0;
  return nullptr;
}

void memarena::chunk_iterator::next() { _chunk_idx++; }

bool memarena::chunk_iterator::more() const {
  if (_chunk_idx < 0) {
    return _ma->_current_chunk.buf != nullptr;
  }
  return _chunk_idx < _ma->_n_other_chunks;
}
#endif  // OS_WIN
#endif  // ROCKSDB_LITE
