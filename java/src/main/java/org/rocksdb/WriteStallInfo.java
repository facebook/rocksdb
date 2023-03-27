// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Objects;

public class WriteStallInfo {
  private final String columnFamilyName;
  private final WriteStallCondition currentCondition;
  private final WriteStallCondition previousCondition;

  /**
   * Access is package private as this will only be constructed from
   * C++ via JNI and for testing.
   */
  WriteStallInfo(final String columnFamilyName, final byte currentConditionValue,
      final byte previousConditionValue) {
    this.columnFamilyName = columnFamilyName;
    this.currentCondition = WriteStallCondition.fromValue(currentConditionValue);
    this.previousCondition = WriteStallCondition.fromValue(previousConditionValue);
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
   * Get the current state of the write controller.
   *
   * @return the current state.
   */
  public WriteStallCondition getCurrentCondition() {
    return currentCondition;
  }

  /**
   * Get the previous state of the write controller.
   *
   * @return the previous state.
   */
  public WriteStallCondition getPreviousCondition() {
    return previousCondition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    WriteStallInfo that = (WriteStallInfo) o;
    return Objects.equals(columnFamilyName, that.columnFamilyName)
        && currentCondition == that.currentCondition && previousCondition == that.previousCondition;
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnFamilyName, currentCondition, previousCondition);
  }

  @Override
  public String toString() {
    return "WriteStallInfo{"
        + "columnFamilyName='" + columnFamilyName + '\'' + ", currentCondition=" + currentCondition
        + ", previousCondition=" + previousCondition + '}';
  }
}
