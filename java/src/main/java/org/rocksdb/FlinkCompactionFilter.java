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

  public FlinkCompactionFilter(ConfigHolder configHolder) {
    this(configHolder, null);
  }

  public FlinkCompactionFilter(ConfigHolder configHolder, Logger logger) {
    super(createNewFlinkCompactionFilter0(configHolder.nativeHandle_, logger == null ? 0 : logger.nativeHandle_));
  }

  private native static long createNewFlinkCompactionFilter0(long configHolderHandle, long loggerHandle);
  private native static long createNewFlinkCompactionFilterConfigHolder();
  private native static void disposeFlinkCompactionFilterConfigHolder(long configHolderHandle);
  private native static long configureFlinkCompactionFilter(
          long configHolderHandle, int stateType, int timestampOffset, long ttl, boolean useSystemTime,
          int fixedElementLength, ListElementIterFactory listElementIterFactory);
  private native static long setCurrentTimestamp(
          long configHolderHandle, long currentTimestamp);

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

  public interface ListElementIterFactory {
    ListElementIter createListElementIter();
  }

  public static class Config {
    final StateType stateType;
    final int timestampOffset;
    final long ttl;
    final boolean useSystemTime;
    final int fixedElementLength;
    final ListElementIterFactory listElementIterFactory;

    private Config(
            StateType stateType, int timestampOffset, long ttl, boolean useSystemTime,
            int fixedElementLength, ListElementIterFactory listElementIterFactory) {
      this.stateType = stateType;
      this.timestampOffset = timestampOffset;
      this.ttl = ttl;
      this.useSystemTime = useSystemTime;
      this.fixedElementLength = fixedElementLength;
      this.listElementIterFactory = listElementIterFactory;
    }

    public static Config create(StateType stateType, int timestampOffset, long ttl, boolean useSystemTime) {
      return new Config(stateType, timestampOffset, ttl, useSystemTime, -1, null);
    }

    public static Config createForFixedElementList(int timestampOffset, long ttl, boolean useSystemTime, int fixedElementLength) {
      return new Config(StateType.List, timestampOffset, ttl, useSystemTime, fixedElementLength, null);
    }

    public static Config createForList(int timestampOffset, long ttl, boolean useSystemTime, ListElementIterFactory listElementIterFactory) {
      return new Config(StateType.List, timestampOffset, ttl, useSystemTime, -1, listElementIterFactory);
    }
  }

  private static class ConfigHolder extends RocksObject {
    protected ConfigHolder() {
      super(createNewFlinkCompactionFilterConfigHolder());
    }

    @Override
    protected void disposeInternal(long handle) {
      disposeFlinkCompactionFilterConfigHolder(handle);
    }
  }

  public static class FlinkCompactionFilterFactory extends AbstractCompactionFilterFactory<FlinkCompactionFilter> {
    private final Logger logger;
    private final ConfigHolder configHolder = new ConfigHolder();

    public FlinkCompactionFilterFactory() {
      this(null);
    }

    public FlinkCompactionFilterFactory(Logger logger) {
      this.logger = logger;
    }

    @Override
    public void close() {
      super.close();
      configHolder.close();
      if (logger != null) {
        logger.close();
      }
    }

    @Override
    public FlinkCompactionFilter createCompactionFilter(Context context) {
      return new FlinkCompactionFilter(configHolder, logger);
    }

    @Override
    public String name() {
      return "FlinkCompactionFilterFactory";
    }

    public void configure(Config config) {
      configureFlinkCompactionFilter(configHolder.nativeHandle_, config.stateType.ordinal(), config.timestampOffset,
              config.ttl, config.useSystemTime,
              config.fixedElementLength, config.listElementIterFactory);
    }

    public void setCurrentTimestamp(long currentTimestamp) {
      FlinkCompactionFilter.setCurrentTimestamp(configHolder.nativeHandle_, currentTimestamp);
    }
  }
}
