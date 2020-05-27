// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * ImportColumnFamilyOptions are used in
 * {@link RocksDB#createColumnFamilyWithImport(ColumnFamilyOptions, String,
 * ImportColumnFamilyOptions, ExportImportFilesMetaData)} calls.
 */
public class ImportColumnFamilyOptions extends RocksObject {
  public ImportColumnFamilyOptions() {
    super(newImportColumnFamilyOptions());
  }

  /**
   * @param moveFiles Whether move the files or copying them.
   */
  public ImportColumnFamilyOptions(final boolean moveFiles) {
    super(newImportColumnFamilyOptions(moveFiles));
  }

  /**
   * Whether move the files or copying them.
   *
   * @return true if files will be moved, false if files will be copied
   */
  public boolean moveFiles() {
    return moveFiles(nativeHandle_);
  }

  private static native long newImportColumnFamilyOptions();
  private static native long newImportColumnFamilyOptions(final boolean move_files);
  @Override protected native void disposeInternal(final long handle);

  private native boolean moveFiles(final long handle);
}
