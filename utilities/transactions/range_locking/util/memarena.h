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

/*
 * A memarena is used to efficiently store a collection of objects that never move
 * The pattern is allocate more and more stuff and free all of the items at once.
 * The underlying memory will store 1 or more objects per chunk. Each chunk is 
 * contiguously laid out in memory but chunks are not necessarily contiguous with
 * each other.
 */
class memarena {
public:
    memarena() :
        _current_chunk(arena_chunk()),
        _other_chunks(nullptr),
        _n_other_chunks(0),
        _size_of_other_chunks(0),
        _footprint_of_other_chunks(0) {
    }

    // Effect: Create a memarena with the specified initial size
    void create(size_t initial_size);

    void destroy(void);

    // Effect: Allocate some memory.  The returned value remains valid until the memarena is cleared or closed.
    //  In case of ENOMEM, aborts.
    void *malloc_from_arena(size_t size);

    // Effect: Move all the memory from this memarena into DEST. 
    //         When SOURCE is closed the memory won't be freed. 
    //         When DEST is closed, the memory will be freed, unless DEST moves its memory to another memarena...
    void move_memory(memarena *dest);

    // Effect: Calculate the amount of memory used by a memory arena.
    size_t total_memory_size(void) const;

    // Effect: Calculate the used space of the memory arena (ie: excludes unused space)
    size_t total_size_in_use(void) const;

    // Effect: Calculate the amount of memory used, according to toku_memory_footprint(),
    //         which is a more expensive but more accurate count of memory used.
    size_t total_footprint(void) const;

    // iterator over the underlying chunks that store objects in the memarena.
    // a chunk is represented by a pointer to const memory and a usable byte count.
    class chunk_iterator {
    public:
        chunk_iterator(const memarena *ma) :
            _ma(ma), _chunk_idx(-1) {
        }

        // returns: base pointer to the current chunk
        //          *used set to the number of usable bytes
        //          if more() is false, returns nullptr and *used = 0
        const void *current(size_t *used) const;

        // requires: more() is true
        void next();

        bool more() const;

    private:
        // -1 represents the 'initial' chunk in a memarena, ie: ma->_current_chunk
        // >= 0 represents the i'th chunk in the ma->_other_chunks array
        const memarena *_ma;
        int _chunk_idx;
    };

private:
    struct arena_chunk {
        arena_chunk() : buf(nullptr), used(0), size(0) { }
        char *buf;
        size_t used;
        size_t size;
    };

    struct arena_chunk _current_chunk;
    struct arena_chunk *_other_chunks;
    int _n_other_chunks;
    size_t _size_of_other_chunks; // the buf_size of all the other chunks.
    size_t _footprint_of_other_chunks; // the footprint of all the other chunks.

    friend class memarena_unit_test;
};
