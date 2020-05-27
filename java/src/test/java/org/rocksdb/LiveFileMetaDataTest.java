// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.util.Random;
import org.junit.ClassRule;
import org.junit.Test;

public class LiveFileMetaDataTest {
  static {
    RocksDB.loadLibrary();
  }

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  public static final Random rand = PlatformRandomHelper.getPlatformSpecificRandomFactory();

  @Test
  public void createLiveFileMetaDataWithoutParameters() {
    try (final LiveFileMetaData metadata = new LiveFileMetaData()) {
      assertThat(metadata).isNotNull();
    }
  }

  @Test
  public void createLiveFileMetaDataWithParameters() throws UnsupportedEncodingException {
    final byte[] columnFamilyName = "testColumnFamilyName".getBytes("UTF-8");
    final int level = rand.nextInt();
    final String fileName = "testFileName";
    final String path = "testPath";
    final long size = rand.nextInt();
    final long smallestSeqno = rand.nextLong();
    final long largestSeqno = rand.nextLong();
    final byte[] smallestKey = "testSmallestKey".getBytes("UTF-8");
    final byte[] largestKey = "testLargestKey".getBytes("UTF-8");
    final long numReadsSampled = rand.nextLong();
    final boolean beingCompacted = rand.nextBoolean();
    final long numEntries = rand.nextLong();
    final long numDeletions = rand.nextLong();

    try (final LiveFileMetaData metadata = new LiveFileMetaData(columnFamilyName, level, fileName,
             path, size, smallestSeqno, largestSeqno, smallestKey, largestKey, numReadsSampled,
             beingCompacted, numEntries, numDeletions)) {
      assertThat(metadata).isNotNull();
      assertThat(new String(metadata.columnFamilyName(), "UTF-8"))
          .isEqualTo(new String(columnFamilyName, "UTF-8"));
      assertThat(metadata.level()).isEqualTo(level);
      assertThat(metadata.fileName()).isEqualTo(fileName);
      assertThat(metadata.path()).isEqualTo(path);
      assertThat(metadata.size()).isEqualTo(size);
      assertThat(metadata.smallestSeqno()).isEqualTo(smallestSeqno);
      assertThat(metadata.largestSeqno()).isEqualTo(largestSeqno);
      assertThat(new String(metadata.smallestKey(), "UTF-8"))
          .isEqualTo(new String(smallestKey, "UTF-8"));
      assertThat(new String(metadata.largestKey(), "UTF-8"))
          .isEqualTo(new String(largestKey, "UTF-8"));
      assertThat(metadata.numReadsSampled()).isEqualTo(numReadsSampled);
      assertThat(metadata.beingCompacted()).isEqualTo(beingCompacted);
      assertThat(metadata.numEntries()).isEqualTo(numEntries);
      assertThat(metadata.numDeletions()).isEqualTo(numDeletions);
    }
  }
}
