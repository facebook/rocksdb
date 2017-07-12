//  Portions Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Large parts of this file is borrowed from the public domain code below.
// from https://github.com/mstump/queues

// C++ implementation of Dmitry Vyukov's non-intrusive
// lock free unbound MPSC queue
// http://www.1024cores.net/home/
// lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue

// License from mstump/queues
// This is free and unencumbered software released into the public domain.
//
// Anyone is free to copy, modify, publish, use, compile, sell, or
// distribute this software, either in source code form or as a compiled
// binary, for any purpose, commercial or non-commercial, and by any
// means.
//
// In jurisdictions that recognize copyright laws, the author or authors
// of this software dedicate any and all copyright interest in the
// software to the public domain. We make this dedication for the benefit
// of the public at large and to the detriment of our heirs and
// successors. We intend this dedication to be an overt act of
// relinquishment in perpetuity of all present and future rights to this
// software under copyright law.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
//
// For more information, please refer to <http://unlicense.org>

// License from http://www.1024cores.net/home/
// lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
// Copyright (c) 2010-2011 Dmitry Vyukov. All rights reserved.
// Redistribution and use in source and binary forms, with or
// without modification, are permitted provided that the following
// conditions are met:
// 1. Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY DMITRY VYUKOV "AS IS" AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
// EVENT SHALL DMITRY VYUKOV OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
// USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// The views and conclusions contained in the software and documentation
// are those of the authors and should not be interpreted as representing
// official policies, either expressed or implied, of Dmitry Vyukov.
//

#ifndef UTIL_MPSC_H_
#define UTIL_MPSC_H_

#include <atomic>
#include <cassert>
#include <type_traits>

/**
 * Multiple Producer Single Consumer Lockless Q
 */
template <typename T>
class mpsc_queue_t {
 public:
  struct buffer_node_t {
    T data;
    std::atomic<buffer_node_t*> next;
  };

  mpsc_queue_t() {
    buffer_node_aligned_t* al_st = new buffer_node_aligned_t;
    buffer_node_t* node = new (al_st) buffer_node_t();
    _head.store(node);
    _tail.store(node);

    node->next.store(nullptr, std::memory_order_relaxed);
  }

  ~mpsc_queue_t() {
    T output;
    while (this->dequeue(&output)) {
    }
    buffer_node_t* front = _head.load(std::memory_order_relaxed);
    front->~buffer_node_t();

    ::operator delete(front);
  }

  void enqueue(const T& input) {
    buffer_node_aligned_t* al_st = new buffer_node_aligned_t;
    buffer_node_t* node = new (al_st) buffer_node_t();

    node->data = input;
    node->next.store(nullptr, std::memory_order_relaxed);

    buffer_node_t* prev_head = _head.exchange(node, std::memory_order_acq_rel);
    prev_head->next.store(node, std::memory_order_release);
  }

  bool dequeue(T* output) {
    buffer_node_t* tail = _tail.load(std::memory_order_relaxed);
    buffer_node_t* next = tail->next.load(std::memory_order_acquire);

    if (next == nullptr) {
      return false;
    }

    *output = next->data;
    _tail.store(next, std::memory_order_release);

    tail->~buffer_node_t();

    ::operator delete(tail);
    return true;
  }

  // you can only use pop_all if the queue is SPSC
  buffer_node_t* pop_all() {
    // nobody else can move the tail pointer.
    buffer_node_t* tptr = _tail.load(std::memory_order_relaxed);
    buffer_node_t* next =
        tptr->next.exchange(nullptr, std::memory_order_acquire);
    _head.exchange(tptr, std::memory_order_acquire);

    // there is a race condition here
    return next;
  }

 private:
  typedef typename std::aligned_storage<
      sizeof(buffer_node_t), std::alignment_of<buffer_node_t>::value>::type
      buffer_node_aligned_t;

  std::atomic<buffer_node_t*> _head;
  std::atomic<buffer_node_t*> _tail;

  mpsc_queue_t(const mpsc_queue_t&) = delete;
  mpsc_queue_t& operator=(const mpsc_queue_t&) = delete;
};

#endif  // UTIL_MPSC_H_
