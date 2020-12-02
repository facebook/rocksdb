// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Objects;

public class ExternalFileIngestionInfo {
  private final String columnFamilyName;
  private final String externalFilePath;
  private final String internalFilePath;
  private final long globalSeqno;
  private final TableProperties tableProperties;

  /**
   * Access is package private as this will only be constructed from
   * C++ via JNI and for testing.
   */
  ExternalFileIngestionInfo(final String columnFamilyName, final String externalFilePath,
      final String internalFilePath, final long globalSeqno,
      final TableProperties tableProperties) {
    this.columnFamilyName = columnFamilyName;
    this.externalFilePath = externalFilePath;
    this.internalFilePath = internalFilePath;
    this.globalSeqno = globalSeqno;
    this.tableProperties = tableProperties;
  }

  /**
   * Get the name of the column family.
   *
   * @return the name of the column family.
   */
  public String getColumnFamilyName() {
    return columnFamilyName;
  }

  /**
   * Get the path of the file outside the DB.
   *
   * @return the path of the file outside the DB.
   */
  public String getExternalFilePath() {
    return externalFilePath;
  }

  /**
   * Get the path of the file inside the DB.
   *
   * @return the path of the file inside the DB.
   */
  public String getInternalFilePath() {
    return internalFilePath;
  }

  /**
   * Get the global sequence number assigned to keys in this file.
   *
   * @return the global sequence number.
   */
  public long getGlobalSeqno() {
    return globalSeqno;
  }

  /**
   * Get the Table properties of the table being flushed.
   *
   * @return the table properties.
   */
  public TableProperties getTableProperties() {
    return tableProperties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ExternalFileIngestionInfo that = (ExternalFileIngestionInfo) o;
    return globalSeqno == that.globalSeqno
        && Objects.equals(columnFamilyName, that.columnFamilyName)
        && Objects.equals(externalFilePath, that.externalFilePath)
        && Objects.equals(internalFilePath, that.internalFilePath)
        && Objects.equals(tableProperties, that.tableProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        columnFamilyName, externalFilePath, internalFilePath, globalSeqno, tableProperties);
  }

  @Override
  public String toString() {
    return "ExternalFileIngestionInfo{"
        + "columnFamilyName='" + columnFamilyName + '\'' + ", externalFilePath='" + externalFilePath
        + '\'' + ", internalFilePath='" + internalFilePath + '\'' + ", globalSeqno=" + globalSeqno
        + ", tableProperties=" + tableProperties + '}';
  }
}
