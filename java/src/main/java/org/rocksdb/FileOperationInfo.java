// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class FileOperationInfo {
  private final String path;
  private final long offset;
  private final long length;
  private final long startTimestamp;
  private final long finishTimestamp;
  private final Status status;

  /**
   * Access is private as this will only be constructed from
   * C++ via JNI.
   */
  private FileOperationInfo(final String path, final long offset,
      final long length, final long startTimestamp, final long finishTimestamp,
      final Status status) {
    this.path = path;
    this.offset = offset;
    this.length = length;
    this.startTimestamp = startTimestamp;
    this.finishTimestamp = finishTimestamp;
    this.status = status;
  }

  /**
   * Get the file path.
   *
   * @return the file path.
   */
  public String getPath() {
    return path;
  }

  /**
   * Get the offset.
   *
   * @return the offset.
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Get the length.
   *
   * @return the length.
   */
  public long getLength() {
    return length;
  }

  /**
   * Get the start timestamp.
   *
   * @return the start timestamp.
   */
  public long getStartTimestamp() {
    return startTimestamp;
  }

  /**
   * Get the finish timestamp.
   *
   * @return the finish timestamp.
   */
  public long getFinishTimestamp() {
    return finishTimestamp;
  }

  /**
   * Get the status.
   *
   * @return the status.
   */
  public Status getStatus() {
    return status;
  }
}