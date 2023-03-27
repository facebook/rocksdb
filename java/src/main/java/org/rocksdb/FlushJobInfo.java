// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Objects;

public class FlushJobInfo {
  private final long columnFamilyId;
  private final String columnFamilyName;
  private final String filePath;
  private final long threadId;
  private final int jobId;
  private final boolean triggeredWritesSlowdown;
  private final boolean triggeredWritesStop;
  private final long smallestSeqno;
  private final long largestSeqno;
  private final TableProperties tableProperties;
  private final FlushReason flushReason;

  /**
   * Access is package private as this will only be constructed from
   * C++ via JNI and for testing.
   */
  FlushJobInfo(final long columnFamilyId, final String columnFamilyName, final String filePath,
      final long threadId, final int jobId, final boolean triggeredWritesSlowdown,
      final boolean triggeredWritesStop, final long smallestSeqno, final long largestSeqno,
      final TableProperties tableProperties, final byte flushReasonValue) {
    this.columnFamilyId = columnFamilyId;
    this.columnFamilyName = columnFamilyName;
    this.filePath = filePath;
    this.threadId = threadId;
    this.jobId = jobId;
    this.triggeredWritesSlowdown = triggeredWritesSlowdown;
    this.triggeredWritesStop = triggeredWritesStop;
    this.smallestSeqno = smallestSeqno;
    this.largestSeqno = largestSeqno;
    this.tableProperties = tableProperties;
    this.flushReason = FlushReason.fromValue(flushReasonValue);
  }

  /**
   * Get the id of the column family.
   *
   * @return the id of the column family
   */
  public long getColumnFamilyId() {
    return columnFamilyId;
  }

  /**
   * Get the name of the column family.
   *
   * @return the name of the column family
   */
  public String getColumnFamilyName() {
    return columnFamilyName;
  }

  /**
   * Get the path to the newly created file.
   *
   * @return the path to the newly created file
   */
  public String getFilePath() {
    return filePath;
  }

  /**
   * Get the id of the thread that completed this flush job.
   *
   * @return the id of the thread that completed this flush job
   */
  public long getThreadId() {
    return threadId;
  }

  /**
   * Get the job id, which is unique in the same thread.
   *
   * @return the job id
   */
  public int getJobId() {
    return jobId;
  }

  /**
   * Determine if rocksdb is currently slowing-down all writes to prevent
   * creating too many Level 0 files as compaction seems not able to
   * catch up the write request speed.
   *
   * This indicates that there are too many files in Level 0.
   *
   * @return true if rocksdb is currently slowing-down all writes,
   *     false otherwise
   */
  public boolean isTriggeredWritesSlowdown() {
    return triggeredWritesSlowdown;
  }

  /**
   * Determine if rocksdb is currently blocking any writes to prevent
   * creating more L0 files.
   *
   * This indicates that there are too many files in level 0.
   * Compactions should try to compact L0 files down to lower levels as soon
   * as possible.
   *
   * @return true  if rocksdb is currently blocking any writes, false otherwise
   */
  public boolean isTriggeredWritesStop() {
    return triggeredWritesStop;
  }

  /**
   * Get the smallest sequence number in the newly created file.
   *
   * @return the smallest sequence number
   */
  public long getSmallestSeqno() {
    return smallestSeqno;
  }

  /**
   * Get the largest sequence number in the newly created file.
   *
   * @return the largest sequence number
   */
  public long getLargestSeqno() {
    return largestSeqno;
  }

  /**
   * Get the Table properties of the table being flushed.
   *
   * @return the Table properties of the table being flushed
   */
  public TableProperties getTableProperties() {
    return tableProperties;
  }

  /**
   * Get the reason for initiating the flush.
   *
   * @return the reason for initiating the flush.
   */
  public FlushReason getFlushReason() {
    return flushReason;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    FlushJobInfo that = (FlushJobInfo) o;
    return columnFamilyId == that.columnFamilyId && threadId == that.threadId && jobId == that.jobId
        && triggeredWritesSlowdown == that.triggeredWritesSlowdown
        && triggeredWritesStop == that.triggeredWritesStop && smallestSeqno == that.smallestSeqno
        && largestSeqno == that.largestSeqno
        && Objects.equals(columnFamilyName, that.columnFamilyName)
        && Objects.equals(filePath, that.filePath)
        && Objects.equals(tableProperties, that.tableProperties) && flushReason == that.flushReason;
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnFamilyId, columnFamilyName, filePath, threadId, jobId,
        triggeredWritesSlowdown, triggeredWritesStop, smallestSeqno, largestSeqno, tableProperties,
        flushReason);
  }

  @Override
  public String toString() {
    return "FlushJobInfo{"
        + "columnFamilyId=" + columnFamilyId + ", columnFamilyName='" + columnFamilyName + '\''
        + ", filePath='" + filePath + '\'' + ", threadId=" + threadId + ", jobId=" + jobId
        + ", triggeredWritesSlowdown=" + triggeredWritesSlowdown
        + ", triggeredWritesStop=" + triggeredWritesStop + ", smallestSeqno=" + smallestSeqno
        + ", largestSeqno=" + largestSeqno + ", tableProperties=" + tableProperties
        + ", flushReason=" + flushReason + '}';
  }
}
