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
    Map
  }

  public interface TimeProvider {
    long currentTimestamp();
  }

  private final TimeProvider timeProvider;

  public FlinkCompactionFilter(StateType stateType, long ttl, TimeProvider timeProvider) {
    super(createNewFlinkCompactionFilter0(timeProvider, stateType.ordinal(), ttl, timeProvider == null));
    this.timeProvider = timeProvider;
  }

  @SuppressWarnings("unused")
  public long currentTimestamp() {
    return timeProvider.currentTimestamp();
  }

  private native static long createNewFlinkCompactionFilter0(
          Object filter, int stateType, long ttl, boolean useSystemTime);
}
