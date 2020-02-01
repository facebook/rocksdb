// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;

import java.nio.ByteBuffer;

/**
 * This is a Java implementation of a Comparator for Java int
 * keys.
 *
 * This comparator assumes keys are (at least) four bytes, so
 * the caller must guarantee that in accessing other APIs in
 * combination with this comparator.
 *
 * The performance of Comparators implemented in Java is always
 * less than their C++ counterparts due to the bridging overhead,
 * as such you likely don't want to use this apart from benchmarking
 * or testing.
 */
public final class IntComparator extends AbstractComparator {

  public IntComparator(final ComparatorOptions copt) {
    super(copt);
  }

  @Override
  public String name() {
    return "rocksdb.java.IntComparator";
  }

  @Override
  public int compare(final ByteBuffer a, final ByteBuffer b) {
    return compareIntKeys(a, b);
  }

  /**
   * Compares integer keys
   * so that they are in ascending order
   *
   * @param a 4-bytes representing an integer key
   * @param b 4-bytes representing an integer key
   *
   * @return negative if a &lt; b, 0 if a == b, positive otherwise
   */
  private final int compareIntKeys(final ByteBuffer a, final ByteBuffer b) {
    final int iA = a.getInt();
    final int iB = b.getInt();

    // protect against int key calculation overflow
    final long diff = (long)iA - iB;
    final int result;
    if (diff < Integer.MIN_VALUE) {
      result = Integer.MIN_VALUE;
    } else if(diff > Integer.MAX_VALUE) {
      result = Integer.MAX_VALUE;
    } else {
      result = (int)diff;
    }
    return result;
  }
}
