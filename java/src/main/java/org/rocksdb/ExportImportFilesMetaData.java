// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;
import java.util.List;

/**
 * Metadata returned as output from ExportColumnFamily() and used as input to
 * CreateColumnFamiliesWithImport().
 */
public class ExportImportFilesMetaData extends RocksObject {
  static {
    RocksDB.loadLibrary();
  }

  public ExportImportFilesMetaData() {
    super(newExportImportFilesMetaData());
  }

  public ExportImportFilesMetaData(
      final String dbComparatorName, final List<LiveFileMetaData> files) {
    super(newExportImportFilesMetaData(dbComparatorName, getFileHandles(files)));
  }

  public ExportImportFilesMetaData(final long nativeHandle) {
    super(nativeHandle);
  }

  private static long[] getFileHandles(final List<LiveFileMetaData> files) {
    long fileHandles[] = new long[files.size()];
    for (int i = 0; i < files.size(); i++) {
      fileHandles[i] = files.get(i).nativeHandle_;
    }
    return fileHandles;
  }

  /**
   * Get the name of the db comparator.
   *
   * @return the name of the db comparator
   */
  public String dbComparatorName() {
    return dbComparatorName(nativeHandle_);
  }

  /**
   * Get the list of LiveFileMetaData.
   *
   * @return the list of LiveFileMetaData.
   */
  public List<LiveFileMetaData> files() {
    return Arrays.asList(files(nativeHandle_));
  }

  private static native long newExportImportFilesMetaData();
  private static native long newExportImportFilesMetaData(
      final String dbComparatorName, final long[] fileHandles);
  @Override protected native void disposeInternal(final long handle);

  private native String dbComparatorName(final long handle);
  private native LiveFileMetaData[] files(final long handle);
}
