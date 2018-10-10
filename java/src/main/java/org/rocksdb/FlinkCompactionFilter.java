//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Just a Java wrapper around FlinkCompactionFilter implemented in C++.
 *
 * Note: this compaction filter is a special implementation, designed for usage only in Apache Flink project.
 */
public class FlinkCompactionFilter
    extends AbstractCompactionFilter<Slice> {
  public enum StateType {
    // WARNING!!! Do not change the order of enum entries as it is important for jni translation
    Disabled,
    Value,
    List
  }

  private final Logger logger;

  public FlinkCompactionFilter() {
    this(null);
  }

  public FlinkCompactionFilter(Logger logger) {
    super(createNewFlinkCompactionFilter0(logger == null ? 0 : logger.nativeHandle_));
    this.logger = logger;
  }

  @Override
  public void close() {
    super.close();
    if (logger != null) {
      logger.close();
    }
  }

  public void configure(Config config) {
    configureFlinkCompactionFilter(nativeHandle_, config.stateType.ordinal(), config.timestampOffset,
            config.ttl, config.useSystemTime,
            config.fixedElementLength, config.listElementIter);
  }

  public void setCurrentTimestamp(long currentTimestamp) {
    setCurrentTimestamp(nativeHandle_, currentTimestamp);
  }

  private native static long createNewFlinkCompactionFilter0(long loggerHandle);
  private native static long configureFlinkCompactionFilter(
          long filterHandle, int stateType, int timestampOffset, long ttl, boolean useSystemTime,
          int fixedElementLength, ListElementIter listElementIter);
  private native static long setCurrentTimestamp(
          long filterHandle, long currentTimestamp);

  public interface ListElementIter {
    void setListBytes(byte[] list);
    int nextOffset(int currentOffset);
  }

  public static abstract class AbstractListElementIter implements ListElementIter {
    protected byte[] list;

    @Override
    public void setListBytes(byte[] list) {
      assert list != null;
      this.list = list;
    }
  }

  public static class Config {
    final StateType stateType;
    final int timestampOffset;
    final long ttl;
    final boolean useSystemTime;
    final int fixedElementLength;
    final ListElementIter listElementIter;

    private Config(
            StateType stateType, int timestampOffset, long ttl, boolean useSystemTime,
            int fixedElementLength, ListElementIter listElementIter) {
      this.stateType = stateType;
      this.timestampOffset = timestampOffset;
      this.ttl = ttl;
      this.useSystemTime = useSystemTime;
      this.fixedElementLength = fixedElementLength;
      this.listElementIter = listElementIter;
    }

    public static Config create(StateType stateType, int timestampOffset, long ttl, boolean useSystemTime) {
      return new Config(stateType, timestampOffset, ttl, useSystemTime, -1, null);
    }

    public static Config createForFixedElementList(int timestampOffset, long ttl, boolean useSystemTime, int fixedElementLength) {
      return new Config(StateType.List, timestampOffset, ttl, useSystemTime, fixedElementLength, null);
    }

    public static Config createForList(int timestampOffset, long ttl, boolean useSystemTime, ListElementIter listElementIter) {
      return new Config(StateType.List, timestampOffset, ttl, useSystemTime, -1, listElementIter);
    }
  }
}
