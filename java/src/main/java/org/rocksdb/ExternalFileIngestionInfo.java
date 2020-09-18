// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class ExternalFileIngestionInfo {
  private final String columnFamilyName;
  private final String externalFilePath;
  private final String internalFilePath;
  private final long globalSeqno;
  private final TableProperties tableProperties;

  /**
   * Access is private as this will only be constructed from
   * C++ via JNI.
   */
  private ExternalFileIngestionInfo(
      final String columnFamilyName, final String externalFilePath,
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
}