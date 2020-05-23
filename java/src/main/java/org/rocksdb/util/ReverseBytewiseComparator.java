// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import org.rocksdb.AbstractComparator;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Slice;

import java.nio.ByteBuffer;

/**
 * This is a Java Native implementation of the C++
 * equivalent ReverseBytewiseComparatorImpl using {@link Slice}
 *
 * The performance of Comparators implemented in Java is always
 * less than their C++ counterparts due to the bridging overhead,
 * as such you likely don't want to use this apart from benchmarking
 * and you most likely instead wanted
 * {@link BuiltinComparator#REVERSE_BYTEWISE_COMPARATOR}
 */
public final class ReverseBytewiseComparator extends AbstractComparator {

  public ReverseBytewiseComparator(final ComparatorOptions copt) {
    super(copt);
  }

  @Override
  public String name() {
    return "rocksdb.java.ReverseBytewiseComparator";
  }

  @Override
  public int compare(final ByteBuffer a, final ByteBuffer b) {
    return -BytewiseComparator._compare(a, b);
  }

  @Override
  public void findShortestSeparator(final ByteBuffer start,
      final ByteBuffer limit) {
    // Find length of common prefix
    final int minLength = Math.min(start.remaining(), limit.remaining());
    int diffIndex = 0;
    while (diffIndex < minLength &&
        start.get(diffIndex) == limit.get(diffIndex)) {
      diffIndex++;
    }

    assert(diffIndex <= minLength);
    if (diffIndex == minLength) {
      // Do not shorten if one string is a prefix of the other
      //
      // We could handle cases like:
      //     V
      // A A 2 X Y
      // A A 2
      // in a similar way as BytewiseComparator::FindShortestSeparator().
      // We keep it simple by not implementing it. We can come back to it
      // later when needed.
    } else {
      final int startByte = start.get(diffIndex) & 0xff;
      final int limitByte = limit.get(diffIndex) & 0xff;
      if (startByte > limitByte && diffIndex < start.remaining() - 1) {
        // Case like
        //     V
        // A A 3 A A
        // A A 1 B B
        //
        // or
        //     v
        // A A 2 A A
        // A A 1 B B
        // In this case "AA2" will be good.
//#ifndef NDEBUG
//        std::string old_start = *start;
//#endif
        start.limit(diffIndex + 1);
//#ifndef NDEBUG
//        assert(old_start >= *start);
//#endif
        assert(BytewiseComparator._compare(start.duplicate(), limit.duplicate()) > 0);
      }
    }
  }
}
