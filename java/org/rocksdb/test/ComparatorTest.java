// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.*;

import java.io.IOException;
import java.nio.file.FileSystems;

public class ComparatorTest {
  private static final String db_path = "/tmp/comparator_db";

  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args) throws IOException {

    final AbstractComparatorTest comparatorTest = new AbstractComparatorTest() {
      @Override
      public AbstractComparator getAscendingIntKeyComparator() {
        return new Comparator(new ComparatorOptions()) {

          @Override
          public String name() {
            return "test.AscendingIntKeyComparator";
          }

          @Override
          public int compare(final Slice a, final Slice b) {
            return compareIntKeys(a.data(), b.data());
          }
        };
      }
    };

    // test the round-tripability of keys written and read with the Comparator
    comparatorTest.testRoundtrip(FileSystems.getDefault().getPath(db_path));

    System.out.println("Passed ComparatorTest");
  }
}
