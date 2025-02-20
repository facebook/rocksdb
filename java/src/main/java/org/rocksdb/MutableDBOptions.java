// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MutableDBOptions extends AbstractMutableOptions {
  /**
   * User must use builder pattern, or parser.
   *
   * @param keys the keys
   * @param values the values
   * <p>
   * See {@link #builder()} and {@link #parse(String)}.
   */
  private MutableDBOptions(final String[] keys, final String[] values) {
    super(keys, values);
  }

  /**
   * Creates a builder which allows you
   * to set MutableDBOptions in a fluent
   * manner
   *
   * @return A builder for MutableDBOptions
   */
  public static MutableDBOptionsBuilder builder() {
    return new MutableDBOptionsBuilder();
  }

  /**
   * Parses a String representation of MutableDBOptions
   * <p>
   * The format is: key1=value1;key2=value2;key3=value3 etc
   * <p>
   * For int[] values, each int should be separated by a comma, e.g.
   * <p>
   * key1=value1;intArrayKey1=1:2:3
   *
   * @param str The string representation of the mutable db options
   * @param ignoreUnknown what to do if the key is not one of the keys we expect
   *
   * @return A builder for the mutable db options
   */
  public static MutableDBOptionsBuilder parse(final String str, final boolean ignoreUnknown) {
    Objects.requireNonNull(str);

    final List<OptionString.Entry> parsedOptions = OptionString.Parser.parse(str);
    return new MutableDBOptions.MutableDBOptionsBuilder().fromParsed(parsedOptions, ignoreUnknown);
  }

  public static MutableDBOptionsBuilder parse(final String str) {
    return parse(str, false);
  }

  private interface MutableDBOptionKey extends MutableOptionKey {}

  public enum DBOption implements MutableDBOptionKey {
    max_background_jobs(ValueType.INT),
    max_background_compactions(ValueType.INT),
    avoid_flush_during_shutdown(ValueType.BOOLEAN),
    writable_file_max_buffer_size(ValueType.LONG),
    delayed_write_rate(ValueType.LONG),
    max_total_wal_size(ValueType.LONG),
    delete_obsolete_files_period_micros(ValueType.LONG),
    stats_dump_period_sec(ValueType.INT),
    stats_persist_period_sec(ValueType.INT),
    stats_history_buffer_size(ValueType.LONG),
    max_open_files(ValueType.INT),
    bytes_per_sync(ValueType.LONG),
    wal_bytes_per_sync(ValueType.LONG),
    strict_bytes_per_sync(ValueType.BOOLEAN),
    compaction_readahead_size(ValueType.LONG),

    daily_offpeak_time_utc(ValueType.STRING);

    private final ValueType valueType;
    DBOption(final ValueType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ValueType getValueType() {
      return valueType;
    }
  }

  public static class MutableDBOptionsBuilder
      extends AbstractMutableOptionsBuilder<MutableDBOptions, MutableDBOptionsBuilder, MutableDBOptionKey>
      implements MutableDBOptionsInterface<MutableDBOptionsBuilder> {
    private static final Map<String, MutableDBOptionKey> ALL_KEYS_LOOKUP = new HashMap<>();
    static {
      for(final MutableDBOptionKey key : DBOption.values()) {
        ALL_KEYS_LOOKUP.put(key.name(), key);
      }
    }

    private MutableDBOptionsBuilder() {
      super();
    }

    @Override
    protected MutableDBOptionsBuilder self() {
      return this;
    }

    @Override
    protected Map<String, MutableDBOptionKey> allKeys() {
      return ALL_KEYS_LOOKUP;
    }

    @Override
    protected MutableDBOptions build(final String[] keys,
        final String[] values) {
      return new MutableDBOptions(keys, values);
    }

    @Override
    public MutableDBOptionsBuilder setMaxBackgroundJobs(
        final int maxBackgroundJobs) {
      return setInt(DBOption.max_background_jobs, maxBackgroundJobs);
    }

    @Override
    public int maxBackgroundJobs() {
      return getInt(DBOption.max_background_jobs);
    }

    @Override
    @Deprecated
    public MutableDBOptionsBuilder setMaxBackgroundCompactions(
        final int maxBackgroundCompactions) {
      return setInt(DBOption.max_background_compactions,
          maxBackgroundCompactions);
    }

    @Override
    @Deprecated
    public int maxBackgroundCompactions() {
      return getInt(DBOption.max_background_compactions);
    }

    @Override
    public MutableDBOptionsBuilder setAvoidFlushDuringShutdown(
        final boolean avoidFlushDuringShutdown) {
      return setBoolean(DBOption.avoid_flush_during_shutdown,
          avoidFlushDuringShutdown);
    }

    @Override
    public boolean avoidFlushDuringShutdown() {
      return getBoolean(DBOption.avoid_flush_during_shutdown);
    }

    @Override
    public MutableDBOptionsBuilder setWritableFileMaxBufferSize(
        final long writableFileMaxBufferSize) {
      return setLong(DBOption.writable_file_max_buffer_size,
          writableFileMaxBufferSize);
    }

    @Override
    public long writableFileMaxBufferSize() {
      return getLong(DBOption.writable_file_max_buffer_size);
    }

    @Override
    public MutableDBOptionsBuilder setDelayedWriteRate(
        final long delayedWriteRate) {
      return setLong(DBOption.delayed_write_rate,
          delayedWriteRate);
    }

    @Override
    public long delayedWriteRate() {
      return getLong(DBOption.delayed_write_rate);
    }

    @Override
    public MutableDBOptionsBuilder setMaxTotalWalSize(
        final long maxTotalWalSize) {
      return setLong(DBOption.max_total_wal_size, maxTotalWalSize);
    }

    @Override
    public long maxTotalWalSize() {
      return getLong(DBOption.max_total_wal_size);
    }

    @Override
    public MutableDBOptionsBuilder setDeleteObsoleteFilesPeriodMicros(
        final long micros) {
      return setLong(DBOption.delete_obsolete_files_period_micros, micros);
    }

    @Override
    public long deleteObsoleteFilesPeriodMicros() {
      return getLong(DBOption.delete_obsolete_files_period_micros);
    }

    @Override
    public MutableDBOptionsBuilder setStatsDumpPeriodSec(
        final int statsDumpPeriodSec) {
      return setInt(DBOption.stats_dump_period_sec, statsDumpPeriodSec);
    }

    @Override
    public int statsDumpPeriodSec() {
      return getInt(DBOption.stats_dump_period_sec);
    }

    @Override
    public MutableDBOptionsBuilder setStatsPersistPeriodSec(
        final int statsPersistPeriodSec) {
      return setInt(DBOption.stats_persist_period_sec, statsPersistPeriodSec);
    }

    @Override
    public int statsPersistPeriodSec() {
      return getInt(DBOption.stats_persist_period_sec);
    }

    @Override
    public MutableDBOptionsBuilder setStatsHistoryBufferSize(
        final long statsHistoryBufferSize) {
      return setLong(DBOption.stats_history_buffer_size, statsHistoryBufferSize);
    }

    @Override
    public long statsHistoryBufferSize() {
      return getLong(DBOption.stats_history_buffer_size);
    }

    @Override
    public MutableDBOptionsBuilder setMaxOpenFiles(final int maxOpenFiles) {
      return setInt(DBOption.max_open_files, maxOpenFiles);
    }

    @Override
    public int maxOpenFiles() {
      return getInt(DBOption.max_open_files);
    }

    @Override
    public MutableDBOptionsBuilder setBytesPerSync(final long bytesPerSync) {
      return setLong(DBOption.bytes_per_sync, bytesPerSync);
    }

    @Override
    public long bytesPerSync() {
      return getLong(DBOption.bytes_per_sync);
    }

    @Override
    public MutableDBOptionsBuilder setWalBytesPerSync(
        final long walBytesPerSync) {
      return setLong(DBOption.wal_bytes_per_sync, walBytesPerSync);
    }

    @Override
    public long walBytesPerSync() {
      return getLong(DBOption.wal_bytes_per_sync);
    }

    @Override
    public MutableDBOptionsBuilder setStrictBytesPerSync(
        final boolean strictBytesPerSync) {
      return setBoolean(DBOption.strict_bytes_per_sync, strictBytesPerSync);
    }

    @Override
    public boolean strictBytesPerSync() {
      return getBoolean(DBOption.strict_bytes_per_sync);
    }

    @Override
    public MutableDBOptionsBuilder setCompactionReadaheadSize(
        final long compactionReadaheadSize) {
      return setLong(DBOption.compaction_readahead_size,
          compactionReadaheadSize);
    }

    @Override
    public long compactionReadaheadSize() {
      return getLong(DBOption.compaction_readahead_size);
    }

    @Override
    public MutableDBOptionsBuilder setDailyOffpeakTimeUTC(final String offpeakTimeUTC) {
      return setString(DBOption.daily_offpeak_time_utc, offpeakTimeUTC);
    }

    @Override
    public String dailyOffpeakTimeUTC() {
      return getString(DBOption.daily_offpeak_time_utc);
    }
  }
}
