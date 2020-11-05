// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import org.rocksdb.*;

import java.nio.ByteBuffer;

import static org.rocksdb.util.ByteUtil.memcmp;

/**
 * This is a Java Native implementation of the C++
 * equivalent BytewiseComparatorImpl using {@link Slice}
 *
 * The performance of Comparators implemented in Java is always
 * less than their C++ counterparts due to the bridging overhead,
 * as such you likely don't want to use this apart from benchmarking
 * and you most likely instead wanted
 * {@link org.rocksdb.BuiltinComparator#BYTEWISE_COMPARATOR}
 */
public final class BytewiseComparator extends AbstractComparator {

  public BytewiseComparator(final ComparatorOptions copt) {
    super(copt);
  }

  @Override
  public String name() {
    return "rocksdb.java.BytewiseComparator";
  }

  @Override
  public int compare(final ByteBuffer a, final ByteBuffer b) {
    return _compare(a, b);
  }

  static int _compare(final ByteBuffer a, final ByteBuffer b) {
    assert(a != null && b != null);
    final int minLen = a.remaining() < b.remaining() ?
        a.remaining() : b.remaining();
    int r = memcmp(a, b, minLen);
    if (r == 0) {
      if (a.remaining() < b.remaining()) {
        r = -1;
      } else if (a.remaining() > b.remaining()) {
        r = +1;
      }
    }
    return r;
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

    if (diffIndex >= minLength) {
      // Do not shorten if one string is a prefix of the other
    } else {
      final int startByte = start.get(diffIndex) & 0xff;
      final int limitByte = limit.get(diffIndex) & 0xff;
      if (startByte >= limitByte) {
        // Cannot shorten since limit is smaller than start or start is
        // already the shortest possible.
        return;
      }
      assert(startByte < limitByte);

      if (diffIndex < limit.remaining() - 1 || startByte + 1 < limitByte) {
        start.put(diffIndex, (byte)((start.get(diffIndex) & 0xff) + 1));
        start.limit(diffIndex + 1);
      } else {
        //     v
        // A A 1 A A A
        // A A 2
        //
        // Incrementing the current byte will make start bigger than limit, we
        // will skip this byte, and find the first non 0xFF byte in start and
        // increment it.
        diffIndex++;

        while (diffIndex < start.remaining()) {
          // Keep moving until we find the first non 0xFF byte to
          // increment it
          if ((start.get(diffIndex) & 0xff) <
              0xff) {
            start.put(diffIndex, (byte)((start.get(diffIndex) & 0xff) + 1));
            start.limit(diffIndex + 1);
            break;
          }
          diffIndex++;
        }
      }
      assert(compare(start.duplicate(), limit.duplicate()) < 0);
    }
  }

  @Override
  public void findShortSuccessor(final ByteBuffer key) {
    // Find first character that can be incremented
    final int n = key.remaining();
    for (int i = 0; i < n; i++) {
      final int byt = key.get(i) & 0xff;
      if (byt != 0xff) {
        key.put(i, (byte)(byt + 1));
        key.limit(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
}
