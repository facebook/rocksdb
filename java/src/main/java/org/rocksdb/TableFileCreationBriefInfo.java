// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Objects;

public class TableFileCreationBriefInfo {
  private final String dbName;
  private final String columnFamilyName;
  private final String filePath;
  private final int jobId;
  private final TableFileCreationReason reason;

  /**
   * Access is private as this will only be constructed from
   * C++ via JNI, either directly of via
   * {@link TableFileCreationInfo#TableFileCreationInfo(long, TableProperties, Status, String,
   * String, String, int, byte)}.
   *
   * @param dbName the database name
   * @param columnFamilyName the column family name
   * @param filePath the path to the table file
   * @param jobId the job identifier
   * @param tableFileCreationReasonValue the reason for creation of the table file
   */
  protected TableFileCreationBriefInfo(final String dbName, final String columnFamilyName,
      final String filePath, final int jobId, final byte tableFileCreationReasonValue) {
    this.dbName = dbName;
    this.columnFamilyName = columnFamilyName;
    this.filePath = filePath;
    this.jobId = jobId;
    this.reason = TableFileCreationReason.fromValue(tableFileCreationReasonValue);
  }

  /**
   * Get the name of the database where the file was created.
   *
   * @return the name of the database.
   */
  public String getDbName() {
    return dbName;
  }

  /**
   * Get the name of the column family where the file was created.
   *
   * @return the name of the column family.
   */
  public String getColumnFamilyName() {
    return columnFamilyName;
  }

  /**
   * Get the path to the created file.
   *
   * @return the path.
   */
  public String getFilePath() {
    return filePath;
  }

  /**
   * Get the id of the job (which could be flush or compaction) that
   * created the file.
   *
   * @return the id of the job.
   */
  public int getJobId() {
    return jobId;
  }

  /**
   * Get the reason for creating the table.
   *
   * @return the reason for creating the table.
   */
  public TableFileCreationReason getReason() {
    return reason;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    TableFileCreationBriefInfo that = (TableFileCreationBriefInfo) o;
    return jobId == that.jobId && Objects.equals(dbName, that.dbName)
        && Objects.equals(columnFamilyName, that.columnFamilyName)
        && Objects.equals(filePath, that.filePath) && reason == that.reason;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dbName, columnFamilyName, filePath, jobId, reason);
  }

  @Override
  public String toString() {
    return "TableFileCreationBriefInfo{"
        + "dbName='" + dbName + '\'' + ", columnFamilyName='" + columnFamilyName + '\''
        + ", filePath='" + filePath + '\'' + ", jobId=" + jobId + ", reason=" + reason + '}';
  }
}
