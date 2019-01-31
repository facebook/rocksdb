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

  public FlinkCompactionFilter(ConfigHolder configHolder, TimeProvider timeProvider) {
    this(configHolder, timeProvider,null);
  }

  public FlinkCompactionFilter(ConfigHolder configHolder, TimeProvider timeProvider, Logger logger) {
    super(createNewFlinkCompactionFilter0(configHolder.nativeHandle_, timeProvider, logger == null ? 0 : logger.nativeHandle_));
  }

  private native static long createNewFlinkCompactionFilter0(long configHolderHandle, TimeProvider timeProvider, long loggerHandle);
  private native static long createNewFlinkCompactionFilterConfigHolder();
  private native static void disposeFlinkCompactionFilterConfigHolder(long configHolderHandle);
  private native static boolean configureFlinkCompactionFilter(
          long configHolderHandle, int stateType, int timestampOffset, long ttl, long queryTimeAfterNumEntries,
          int fixedElementLength, ListElementFilterFactory listElementFilterFactory);

  public interface ListElementFilter {
    /**
     * Gets offset of the first unexpired element in the list.
     *
     * <p>Native code wraps this java object and calls it for list state
     * for which element byte length is unknown and Flink custom type serializer has to be used
     * to compute offset of the next element in serialized form.
     *
     * @param list serialised list of elements with timestamp
     * @param ttl time-to-live of the list elements
     * @param currentTimestamp current timestamp to check expiration against
     * @return offset of the first unexpired element in the list
     */
    @SuppressWarnings("unused")
    int nextUnexpiredOffset(byte[] list, long ttl, long currentTimestamp);
  }

  public interface ListElementFilterFactory {
    @SuppressWarnings("unused")
    ListElementFilter createListElementFilter();
  }

  public static class Config {
    final StateType stateType;
    final int timestampOffset;
    final long ttl;
    /** Number of state entries to process by compaction filter before updating current timestamp. */
    final long queryTimeAfterNumEntries;
    final int fixedElementLength;
    final ListElementFilterFactory listElementFilterFactory;

    private Config(
            StateType stateType, int timestampOffset, long ttl, long queryTimeAfterNumEntries,
            int fixedElementLength, ListElementFilterFactory listElementFilterFactory) {
      this.stateType = stateType;
      this.timestampOffset = timestampOffset;
      this.ttl = ttl;
      this.queryTimeAfterNumEntries = queryTimeAfterNumEntries;
      this.fixedElementLength = fixedElementLength;
      this.listElementFilterFactory = listElementFilterFactory;
    }

    @SuppressWarnings("WeakerAccess")
    public static Config createNotList(StateType stateType, int timestampOffset, long ttl, long queryTimeAfterNumEntries) {
      return new Config(stateType, timestampOffset, ttl, queryTimeAfterNumEntries, -1, null);
    }

    @SuppressWarnings("unused")
    public static Config createForValue(long ttl, long queryTimeAfterNumEntries) {
      return createNotList(StateType.Value, 0, ttl, queryTimeAfterNumEntries);
    }

    @SuppressWarnings("unused")
    public static Config createForMap(long ttl, long queryTimeAfterNumEntries) {
      return createNotList(StateType.Value, 1, ttl, queryTimeAfterNumEntries);
    }

    @SuppressWarnings("WeakerAccess")
    public static Config createForFixedElementList(long ttl, long queryTimeAfterNumEntries, int fixedElementLength) {
      return new Config(StateType.List, 0, ttl, queryTimeAfterNumEntries, fixedElementLength, null);
    }

    @SuppressWarnings("WeakerAccess")
    public static Config createForList(long ttl, long queryTimeAfterNumEntries, ListElementFilterFactory listElementFilterFactory) {
      return new Config(StateType.List, 0, ttl, queryTimeAfterNumEntries, -1, listElementFilterFactory);
    }
  }

  private static class ConfigHolder extends RocksObject {
    ConfigHolder() {
      super(createNewFlinkCompactionFilterConfigHolder());
    }

    @Override
    protected void disposeInternal(long handle) {
      disposeFlinkCompactionFilterConfigHolder(handle);
    }
  }

  /** Provides current timestamp to check expiration, it must be thread safe. */
  public interface TimeProvider {
    long currentTimestamp();
  }

  public static class FlinkCompactionFilterFactory extends AbstractCompactionFilterFactory<FlinkCompactionFilter> {
    private final ConfigHolder configHolder;
    private final TimeProvider timeProvider;
    private final Logger logger;

    @SuppressWarnings("unused")
    public FlinkCompactionFilterFactory(TimeProvider timeProvider) {
      this(timeProvider, null);
    }

    @SuppressWarnings("WeakerAccess")
    public FlinkCompactionFilterFactory(TimeProvider timeProvider, Logger logger) {
      this.configHolder = new ConfigHolder();
      this.timeProvider = timeProvider;
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
      return new FlinkCompactionFilter(configHolder, timeProvider, logger);
    }

    @Override
    public String name() {
      return "FlinkCompactionFilterFactory";
    }

    @SuppressWarnings("WeakerAccess")
    public void configure(Config config) {
      boolean already_configured = !configureFlinkCompactionFilter(
              configHolder.nativeHandle_, config.stateType.ordinal(), config.timestampOffset,
              config.ttl, config.queryTimeAfterNumEntries, config.fixedElementLength, config.listElementFilterFactory);
      if (already_configured) {
        throw new IllegalStateException("Compaction filter is already configured");
      }
    }
  }
}
