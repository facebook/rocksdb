// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class WriteStallInfo {
  private final byte[] columnFamilyName;
  private final WriteStallCondition currentCondition;
  private final WriteStallCondition previousCondition;

  /**
   * Access is private as this will only be constructed from
   * C++ via JNI.
   */
  private WriteStallInfo(final byte[] columnFamilyName,
      final byte currentConditionValue,
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
  public byte[] getColumnFamilyName() {
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
}