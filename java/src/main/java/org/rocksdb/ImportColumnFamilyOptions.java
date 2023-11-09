//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * ImportColumnFamilyOptions is used by
 * {@link RocksDB#createColumnFamilyWithImport(ColumnFamilyDescriptor, ImportColumnFamilyOptions,
 * ExportImportFilesMetaData)}.
 */
public class ImportColumnFamilyOptions extends RocksObject {
  public ImportColumnFamilyOptions() {
    super(newImportColumnFamilyOptions());
  }

  /**
   * Can be set to true to move the files instead of copying them.
   *
   * @return true if files will be moved
   */
  public boolean moveFiles() {
    return moveFiles(nativeHandle_);
  }

  /**
   * Can be set to true to move the files instead of copying them.
   *
   * @param moveFiles true if files should be moved instead of copied
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public ImportColumnFamilyOptions setMoveFiles(final boolean moveFiles) {
    setMoveFiles(nativeHandle_, moveFiles);
    return this;
  }

  private static native long newImportColumnFamilyOptions();
  private native boolean moveFiles(final long handle);
  private native void setMoveFiles(final long handle, final boolean move_files);
  @Override protected final native void disposeInternal(final long handle);
}
