// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.ClassRule;
import org.junit.Test;

public class ExportImportFilesMetaDataTest {
  static {
    RocksDB.loadLibrary();
  }

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  public static final Random rand = PlatformRandomHelper.getPlatformSpecificRandomFactory();

  @Test
  public void createExportImportFilesMetaDataWithoutParameters() {
    try (final ExportImportFilesMetaData metadata = new ExportImportFilesMetaData()) {
      assertThat(metadata).isNotNull();
    }
  }

  @Test
  public void createExportImportFilesMetaDataWithParameters() throws UnsupportedEncodingException {
    final String dbComparatorName = "testDbComparatorName";
    final List<LiveFileMetaData> files = new ArrayList<>();

    final String columnFamilyName = "testColumnFamilyName";
    final int level = rand.nextInt();
    final String fileName = "testFileName";
    final String path = "testPath";
    final long size = rand.nextInt();
    final long smallestSeqno = rand.nextLong();
    final long largestSeqno = rand.nextLong();
    final String smallestKey = "testSmallestKey";
    final String largestKey = "testLargestKey";
    final long numReadsSampled = rand.nextLong();
    final boolean beingCompacted = rand.nextBoolean();
    final long numEntries = rand.nextLong();
    final long numDeletions = rand.nextLong();

    for (int i = 0; i < 2; i++) {
      LiveFileMetaData file = new LiveFileMetaData((columnFamilyName + i).getBytes("UTF-8"),
          level + i, fileName + i, path + i, size + i, smallestSeqno + i, largestSeqno + i,
          (smallestKey + i).getBytes("UTF-8"), (largestKey + i).getBytes("UTF-8"),
          numReadsSampled + i, beingCompacted, numEntries + i, numDeletions + i);

      files.add(file);
    }

    try (final ExportImportFilesMetaData metadata =
             new ExportImportFilesMetaData(dbComparatorName, files)) {
      assertThat(metadata).isNotNull();
      assertThat(metadata.dbComparatorName()).isEqualTo(dbComparatorName);
      assertThat(metadata.files().size()).isEqualTo(files.size());

      for (int i = 0; i < metadata.files().size(); i++) {
        LiveFileMetaData file = metadata.files().get(i);

        assertThat(new String(file.columnFamilyName(), "UTF-8")).isEqualTo(columnFamilyName + i);
        assertThat(file.level()).isEqualTo(level + i);
        assertThat(file.fileName()).isEqualTo(fileName + i);
        assertThat(file.path()).isEqualTo(path + i);
        assertThat(file.size()).isEqualTo(size + i);
        assertThat(file.smallestSeqno()).isEqualTo(smallestSeqno + i);
        assertThat(file.largestSeqno()).isEqualTo(largestSeqno + i);
        assertThat(new String(file.smallestKey(), "UTF-8")).isEqualTo(smallestKey + i);
        assertThat(new String(file.largestKey(), "UTF-8")).isEqualTo(largestKey + i);
        assertThat(file.numReadsSampled()).isEqualTo(numReadsSampled + i);
        assertThat(file.beingCompacted()).isEqualTo(beingCompacted);
        assertThat(file.numEntries()).isEqualTo(numEntries + i);
        assertThat(file.numDeletions()).isEqualTo(numDeletions + i);
      }
    }
  }
}
