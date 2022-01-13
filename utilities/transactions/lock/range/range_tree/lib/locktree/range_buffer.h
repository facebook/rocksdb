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

#include <inttypes.h>
#include <stdint.h>

#include "../util/dbt.h"
#include "../util/memarena.h"

namespace toku {

// a key range buffer represents a set of key ranges that can
// be stored, iterated over, and then destroyed all at once.
class range_buffer {
 private:
  // the key range buffer is a bunch of records in a row.
  // each record has the following header, followed by the
  // left key and right key data payload, if applicable.
  // we limit keys to be 2^16, since we store lengths as 2 bytes.
  static const size_t MAX_KEY_SIZE = 1 << 16;

  struct record_header {
    bool left_neg_inf;
    bool left_pos_inf;
    bool right_pos_inf;
    bool right_neg_inf;
    uint16_t left_key_size;
    uint16_t right_key_size;
    bool is_exclusive_lock;

    bool left_is_infinite(void) const;

    bool right_is_infinite(void) const;

    void init(const DBT *left_key, const DBT *right_key, bool is_exclusive);
  };
  // PORT static_assert(sizeof(record_header) == 8, "record header format is
  // off");

 public:
  // the iterator abstracts reading over a buffer of variable length
  // records one by one until there are no more left.
  class iterator {
   public:
    iterator();
    iterator(const range_buffer *buffer);

    // a record represents the user-view of a serialized key range.
    // it handles positive and negative infinity and the optimized
    // point range case, where left and right points share memory.
    class record {
     public:
      // get a read-only pointer to the left key of this record's range
      const DBT *get_left_key(void) const;

      // get a read-only pointer to the right key of this record's range
      const DBT *get_right_key(void) const;

      // how big is this record? this tells us where the next record is
      size_t size(void) const;

      bool get_exclusive_flag() const { return _header.is_exclusive_lock; }

      // populate a record header and point our DBT's
      // buffers into ours if they are not infinite.
      void deserialize(const char *buf);

     private:
      record_header _header;
      DBT _left_key;
      DBT _right_key;
    };

    // populate the given record object with the current
    // the memory referred to by record is valid for only
    // as long as the record exists.
    bool current(record *rec);

    // move the iterator to the next record in the buffer
    void next(void);

   private:
    void reset_current_chunk();

    // the key range buffer we are iterating over, the current
    // offset in that buffer, and the size of the current record.
    memarena::chunk_iterator _ma_chunk_iterator;
    const void *_current_chunk_base;
    size_t _current_chunk_offset;
    size_t _current_chunk_max;
    size_t _current_rec_size;
  };

  // allocate buffer space lazily instead of on creation. this way,
  // no malloc/free is done if the transaction ends up taking no locks.
  void create(void);

  // append a left/right key range to the buffer.
  // if the keys are equal, then only one copy is stored.
  void append(const DBT *left_key, const DBT *right_key,
              bool is_write_request = false);

  // is this range buffer empty?
  bool is_empty(void) const;

  // how much memory is being used by this range buffer?
  uint64_t total_memory_size(void) const;

  // how many ranges are stored in this range buffer?
  int get_num_ranges(void) const;

  void destroy(void);

 private:
  memarena _arena;
  int _num_ranges;

  void append_range(const DBT *left_key, const DBT *right_key,
                    bool is_write_request);

  // append a point to the buffer. this is the space/time saving
  // optimization for key ranges where left == right.
  void append_point(const DBT *key, bool is_write_request);
};

} /* namespace toku */
