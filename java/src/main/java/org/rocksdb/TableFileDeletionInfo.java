// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Objects;

public class TableFileDeletionInfo {
  private final String dbName;
  private final String filePath;
  private final int jobId;
  private final Status status;

  /**
   * Access is package private as this will only be constructed from
   * C++ via JNI and for testing.
   */
  TableFileDeletionInfo(
      final String dbName, final String filePath, final int jobId, final Status status) {
    this.dbName = dbName;
    this.filePath = filePath;
    this.jobId = jobId;
    this.status = status;
  }

  /**
   * Get the name of the database where the file was deleted.
   *
   * @return the name of the database.
   */
  public String getDbName() {
    return dbName;
  }

  /**
   * Get the path to the deleted file.
   *
   * @return the path.
   */
  public String getFilePath() {
    return filePath;
  }

  /**
   * Get the id of the job which deleted the file.
   *
   * @return the id of the job.
   */
  public int getJobId() {
    return jobId;
  }

  /**
   * Get the status indicating whether the deletion was successful or not.
   *
   * @return the status
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
    TableFileDeletionInfo that = (TableFileDeletionInfo) o;
    return jobId == that.jobId && Objects.equals(dbName, that.dbName)
        && Objects.equals(filePath, that.filePath) && Objects.equals(status, that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dbName, filePath, jobId, status);
  }

  @Override
  public String toString() {
    return "TableFileDeletionInfo{"
        + "dbName='" + dbName + '\'' + ", filePath='" + filePath + '\'' + ", jobId=" + jobId
        + ", status=" + status + '}';
  }
}
