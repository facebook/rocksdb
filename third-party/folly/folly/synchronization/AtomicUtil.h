//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstdint>

namespace folly {

/**
 * Sets a bit at the given index in the binary representation of the integer
 * to 1.  Returns the previous value of the bit, so true if the bit was not
 * changed, false otherwise
 *
 * On some architectures, using this is more efficient than the corresponding
 * std::atomic::fetch_or() with a mask.  For example to set the first (least
 * significant) bit of an integer, you could do atomic.fetch_or(0b1)
 *
 * The efficiency win is only visible in x86 (yet) and comes from the
 * implementation using the x86 bts instruction when possible.
 *
 * When something other than std::atomic is passed, the implementation assumed
 * incompatibility with this interface and calls Atomic::fetch_or()
 */
template <typename Atomic>
bool atomic_fetch_set(
    Atomic& atomic,
    std::size_t bit,
    std::memory_order order = std::memory_order_seq_cst);

/**
 * Resets a bit at the given index in the binary representation of the integer
 * to 0.  Returns the previous value of the bit, so true if the bit was
 * changed, false otherwise
 *
 * This follows the same underlying principle and implementation as
 * fetch_set().  Using the optimized implementation when possible and falling
 * back to std::atomic::fetch_and() when in debug mode or in an architecture
 * where an optimization is not possible
 */
template <typename Atomic>
bool atomic_fetch_reset(
    Atomic& atomic,
    std::size_t bit,
    std::memory_order order = std::memory_order_seq_cst);

} // namespace folly

#include <folly/synchronization/AtomicUtil-inl.h>
