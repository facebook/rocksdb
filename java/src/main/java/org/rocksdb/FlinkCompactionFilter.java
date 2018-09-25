//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Just a Java wrapper around FlinkCompactionFilter implemented in C++
 */
public class FlinkCompactionFilter
    extends AbstractCompactionFilter<Slice> {
  public enum StateType {
    // WARNING!!! Do not change the order of enum entries as it is important for jni translation
    Value,
    List,
    Map,
    Disabled
  }

  public FlinkCompactionFilter() {
    super(createNewFlinkCompactionFilter0(InfoLogLevel.ERROR_LEVEL.getValue()));
  }

  public FlinkCompactionFilter(InfoLogLevel logLevel) {
    super(createNewFlinkCompactionFilter0(logLevel.getValue()));
  }

  public void configure(StateType stateType, int timestampOffset, long ttl, boolean useSystemTime) {
    configureFlinkCompactionFilter(nativeHandle_, stateType.ordinal(), timestampOffset, ttl, useSystemTime);
  }

  public void setCurrentTimestamp(long currentTimestamp) {
    setCurrentTimestamp(nativeHandle_, currentTimestamp);
  }

  private native static long createNewFlinkCompactionFilter0(byte logLevel);
  private native static long configureFlinkCompactionFilter(
          long filterHandle, int stateType, int timestampOffset, long ttl, boolean useSystemTime);
  private native static long setCurrentTimestamp(
          long filterHandle, long currentTimestamp);
}
