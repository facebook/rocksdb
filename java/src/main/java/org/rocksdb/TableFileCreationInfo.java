// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Objects;

public class TableFileCreationInfo extends TableFileCreationBriefInfo {
  private final long fileSize;
  private final TableProperties tableProperties;
  private final Status status;

  /**
   * Access is protected as this will only be constructed from
   * C++ via JNI.
   *
   * @param fileSize the size of the table file
   * @param tableProperties the properties of the table file
   * @param status the status of the creation operation
   * @param dbName the database name
   * @param columnFamilyName the column family name
   * @param filePath the path to the table file
   * @param jobId the job identifier
   * @param tableFileCreationReasonValue the reason for creation of the table file
   */
  protected TableFileCreationInfo(final long fileSize, final TableProperties tableProperties,
      final Status status, final String dbName, final String columnFamilyName,
      final String filePath, final int jobId, final byte tableFileCreationReasonValue) {
    super(dbName, columnFamilyName, filePath, jobId, tableFileCreationReasonValue);
    this.fileSize = fileSize;
    this.tableProperties = tableProperties;
    this.status = status;
  }

  /**
   * Get the size of the file.
   *
   * @return the size.
   */
  public long getFileSize() {
    return fileSize;
  }

  /**
   * Get the detailed properties of the created file.
   *
   * @return the properties.
   */
  public TableProperties getTableProperties() {
    return tableProperties;
  }

  /**
   * Get the status indicating whether the creation was successful or not.
   *
   * @return the status.
   */
  public Status getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    TableFileCreationInfo that = (TableFileCreationInfo) o;
    return fileSize == that.fileSize && Objects.equals(tableProperties, that.tableProperties)
        && Objects.equals(status, that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileSize, tableProperties, status);
  }

  @Override
  public String toString() {
    return "TableFileCreationInfo{"
        + "fileSize=" + fileSize + ", tableProperties=" + tableProperties + ", status=" + status
        + '}';
  }
}
