//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;
import java.util.List;

/**
 * The metadata that describes a column family.
 */
public class ExportImportFilesMetaData {
  private final byte[] dbComparatorName;
  private final LiveFileMetaData[] files;

  /**
   * Called from JNI C++
   */
  public ExportImportFilesMetaData(final byte[] dbComparatorName, final LiveFileMetaData[] files) {
    this.dbComparatorName = dbComparatorName;
    this.files = files;
  }

  /**
   * The name of the db comparator.
   *
   * @return the dbComparatorName
   */
  public byte[] dbComparatorName() {
    return dbComparatorName;
  }

  /**
   * The metadata of all files in this column family.
   *
   * @return the levels files
   */
  public List<LiveFileMetaData> files() {
    return Arrays.asList(files);
  }

  public long newExportImportFilesMetaDataHandle() {
    final long[] liveFileMetaDataHandles = new long[files.length];
    for (int i = 0; i < files.length; i++) {
      liveFileMetaDataHandles[i] = files[i].newLiveFileMetaDataHandle();
    }
    return newExportImportFilesMetaDataHandle(
        dbComparatorName, dbComparatorName.length, liveFileMetaDataHandles);
  }

  private native long newExportImportFilesMetaDataHandle(final byte[] dbComparatorName,
      final int dbComparatorNameLen, final long[] liveFileMetaDataHandles);
}
