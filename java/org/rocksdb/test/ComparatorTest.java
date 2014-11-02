// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.*;

import java.io.IOException;
import java.nio.file.FileSystems;

public class ComparatorTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void shouldTestComparator() throws IOException {

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
    comparatorTest.testRoundtrip(FileSystems.getDefault().getPath(
        dbFolder.getRoot().getAbsolutePath()));

    System.out.println("Passed ComparatorTest");
  }
}
