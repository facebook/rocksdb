// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.*;

import java.io.IOException;
import java.nio.file.FileSystems;

import static org.assertj.core.api.Assertions.assertThat;

public class ComparatorTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @AfterClass
  public static void printMessage(){
    System.out.println("Passed ComparatorTest.");
  }

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
  }

  @Test
  public void shouldTestBuiltinForwardComparator()
      throws RocksDBException {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
    RocksDB rocksDB = RocksDB.open(options,
        dbFolder.getRoot().getAbsolutePath());

    rocksDB.put("abc1".getBytes(), "abc1".getBytes());
    rocksDB.put("abc2".getBytes(), "abc2".getBytes());
    rocksDB.put("abc3".getBytes(), "abc3".getBytes());

    RocksIterator rocksIterator = rocksDB.newIterator();
    // Iterate over keys using a iterator
    rocksIterator.seekToFirst();
    assertThat(rocksIterator.isValid()).isTrue();
    assertThat(rocksIterator.key()).isEqualTo(
        "abc1".getBytes());
    assertThat(rocksIterator.value()).isEqualTo(
        "abc1".getBytes());
    rocksIterator.next();
    assertThat(rocksIterator.isValid()).isTrue();
    assertThat(rocksIterator.key()).isEqualTo(
        "abc2".getBytes());
    assertThat(rocksIterator.value()).isEqualTo(
        "abc2".getBytes());
    rocksIterator.next();
    assertThat(rocksIterator.isValid()).isTrue();
    assertThat(rocksIterator.key()).isEqualTo(
        "abc3".getBytes());
    assertThat(rocksIterator.value()).isEqualTo(
        "abc3".getBytes());
    rocksIterator.next();
    assertThat(rocksIterator.isValid()).isFalse();
    // Get last one
    rocksIterator.seekToLast();
    assertThat(rocksIterator.isValid()).isTrue();
    assertThat(rocksIterator.key()).isEqualTo(
        "abc3".getBytes());
    assertThat(rocksIterator.value()).isEqualTo(
        "abc3".getBytes());
    // Seek for abc
    rocksIterator.seek("abc".getBytes());
    assertThat(rocksIterator.isValid()).isTrue();
    assertThat(rocksIterator.key()).isEqualTo(
        "abc1".getBytes());
    assertThat(rocksIterator.value()).isEqualTo(
        "abc1".getBytes());
    rocksIterator.dispose();
    rocksDB.close();
    options.dispose();
  }

  @Test
  public void shouldTestBuiltinReverseComparator()
      throws RocksDBException {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setComparator(
        BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR);
    RocksDB rocksDB = RocksDB.open(options,
        dbFolder.getRoot().getAbsolutePath());

    rocksDB.put("abc1".getBytes(), "abc1".getBytes());
    rocksDB.put("abc2".getBytes(), "abc2".getBytes());
    rocksDB.put("abc3".getBytes(), "abc3".getBytes());

    RocksIterator rocksIterator = rocksDB.newIterator();
    // Iterate over keys using a iterator
    rocksIterator.seekToFirst();
    assertThat(rocksIterator.isValid()).isTrue();
    assertThat(rocksIterator.key()).isEqualTo(
        "abc3".getBytes());
    assertThat(rocksIterator.value()).isEqualTo(
        "abc3".getBytes());
    rocksIterator.next();
    assertThat(rocksIterator.isValid()).isTrue();
    assertThat(rocksIterator.key()).isEqualTo(
        "abc2".getBytes());
    assertThat(rocksIterator.value()).isEqualTo(
        "abc2".getBytes());
    rocksIterator.next();
    assertThat(rocksIterator.isValid()).isTrue();
    assertThat(rocksIterator.key()).isEqualTo(
        "abc1".getBytes());
    assertThat(rocksIterator.value()).isEqualTo(
        "abc1".getBytes());
    rocksIterator.next();
    assertThat(rocksIterator.isValid()).isFalse();
    // Get last one
    rocksIterator.seekToLast();
    assertThat(rocksIterator.isValid()).isTrue();
    assertThat(rocksIterator.key()).isEqualTo(
        "abc1".getBytes());
    assertThat(rocksIterator.value()).isEqualTo(
        "abc1".getBytes());
    // Will be invalid because abc is after abc1
    rocksIterator.seek("abc".getBytes());
    assertThat(rocksIterator.isValid()).isFalse();
    // Will be abc3 because the next one after abc999
    // is abc3
    rocksIterator.seek("abc999".getBytes());
    assertThat(rocksIterator.key()).isEqualTo(
        "abc3".getBytes());
    assertThat(rocksIterator.value()).isEqualTo(
        "abc3".getBytes());
    rocksIterator.dispose();
    rocksDB.close();
    options.dispose();
  }

  @Test
  public void shouldTestBuiltinComparatorEnum(){
    assertThat(BuiltinComparator.BYTEWISE_COMPARATOR.ordinal())
        .isEqualTo(0);
    assertThat(
        BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR.ordinal())
        .isEqualTo(1);
  }
}
