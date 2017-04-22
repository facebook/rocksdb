// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.*;

public class MutableColumnFamilyOptions {
  private final static String KEY_VALUE_PAIR_SEPARATOR = ";";
  private final static char KEY_VALUE_SEPARATOR = '=';
  private final static String INT_ARRAY_INT_SEPARATOR = ",";

  private final String[] keys;
  private final String[] values;

  // user must use builder pattern, or parser
  private MutableColumnFamilyOptions(final String keys[],
      final String values[]) {
    this.keys = keys;
    this.values = values;
  }

  String[] getKeys() {
    return keys;
  }

  String[] getValues() {
    return values;
  }

  /**
   * Creates a builder which allows you
   * to set MutableColumnFamilyOptions in a fluent
   * manner
   *
   * @return A builder for MutableColumnFamilyOptions
   */
  public static MutableColumnFamilyOptionsBuilder builder() {
    return new MutableColumnFamilyOptionsBuilder();
  }

  /**
   * Parses a String representation of MutableColumnFamilyOptions
   *
   * The format is: key1=value1;key2=value2;key3=value3 etc
   *
   * For int[] values, each int should be separated by a comma, e.g.
   *
   * key1=value1;intArrayKey1=1,2,3
   *
   * @param str The string representation of the mutable column family options
   *
   * @return A builder for the mutable column family options
   */
  public static MutableColumnFamilyOptionsBuilder parse(final String str) {
    Objects.requireNonNull(str);

    final MutableColumnFamilyOptionsBuilder builder =
        new MutableColumnFamilyOptionsBuilder();

    final String options[] = str.trim().split(KEY_VALUE_PAIR_SEPARATOR);
    for(final String option : options) {
      final int equalsOffset = option.indexOf(KEY_VALUE_SEPARATOR);
      if(equalsOffset <= 0) {
        throw new IllegalArgumentException(
            "options string has an invalid key=value pair");
      }

      final String key = option.substring(0, equalsOffset);
      if(key == null || key.isEmpty()) {
        throw new IllegalArgumentException("options string is invalid");
      }

      final String value = option.substring(equalsOffset + 1);
      if(value == null || value.isEmpty()) {
        throw new IllegalArgumentException("options string is invalid");
      }

      builder.fromString(key, value);
    }

    return builder;
  }

  /**
   * Returns a string representation
   * of MutableColumnFamilyOptions which is
   * suitable for consumption by {@link #parse(String)}
   *
   * @return String representation of MutableColumnFamilyOptions
   */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    for(int i = 0; i < keys.length; i++) {
      buffer
          .append(keys[i])
          .append(KEY_VALUE_SEPARATOR)
          .append(values[i]);

      if(i + 1 < keys.length) {
        buffer.append(KEY_VALUE_PAIR_SEPARATOR);
      }
    }
    return buffer.toString();
  }

  public enum ValueType {
    DOUBLE,
    LONG,
    INT,
    BOOLEAN,
    INT_ARRAY,
    ENUM
  }

  public enum MemtableOption implements MutableColumnFamilyOptionKey {
    write_buffer_size(ValueType.LONG),
    arena_block_size(ValueType.LONG),
    memtable_prefix_bloom_size_ratio(ValueType.DOUBLE),
    @Deprecated memtable_prefix_bloom_bits(ValueType.INT),
    @Deprecated memtable_prefix_bloom_probes(ValueType.INT),
    memtable_huge_page_size(ValueType.LONG),
    max_successive_merges(ValueType.LONG),
    @Deprecated filter_deletes(ValueType.BOOLEAN),
    max_write_buffer_number(ValueType.INT),
    inplace_update_num_locks(ValueType.LONG);

    private final ValueType valueType;
    MemtableOption(final ValueType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ValueType getValueType() {
      return valueType;
    }
  }

  public enum CompactionOption implements MutableColumnFamilyOptionKey {
    disable_auto_compactions(ValueType.BOOLEAN),
    @Deprecated soft_rate_limit(ValueType.DOUBLE),
    soft_pending_compaction_bytes_limit(ValueType.LONG),
    @Deprecated hard_rate_limit(ValueType.DOUBLE),
    hard_pending_compaction_bytes_limit(ValueType.LONG),
    level0_file_num_compaction_trigger(ValueType.INT),
    level0_slowdown_writes_trigger(ValueType.INT),
    level0_stop_writes_trigger(ValueType.INT),
    max_compaction_bytes(ValueType.LONG),
    target_file_size_base(ValueType.LONG),
    target_file_size_multiplier(ValueType.INT),
    max_bytes_for_level_base(ValueType.LONG),
    max_bytes_for_level_multiplier(ValueType.INT),
    max_bytes_for_level_multiplier_additional(ValueType.INT_ARRAY);

    private final ValueType valueType;
    CompactionOption(final ValueType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ValueType getValueType() {
      return valueType;
    }
  }

  public enum MiscOption implements MutableColumnFamilyOptionKey {
    max_sequential_skip_in_iterations(ValueType.LONG),
    paranoid_file_checks(ValueType.BOOLEAN),
    report_bg_io_stats(ValueType.BOOLEAN),
    compression_type(ValueType.ENUM);

    private final ValueType valueType;
    MiscOption(final ValueType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ValueType getValueType() {
      return valueType;
    }
  }

  private interface MutableColumnFamilyOptionKey {
    String name();
    ValueType getValueType();
  }

  private static abstract class MutableColumnFamilyOptionValue<T> {
    protected final T value;

    MutableColumnFamilyOptionValue(final T value) {
      this.value = value;
    }

    abstract double asDouble() throws NumberFormatException;
    abstract long asLong() throws NumberFormatException;
    abstract int asInt() throws NumberFormatException;
    abstract boolean asBoolean() throws IllegalStateException;
    abstract int[] asIntArray() throws IllegalStateException;
    abstract String asString();
    abstract T asObject();
  }

  private static class MutableColumnFamilyOptionStringValue
      extends MutableColumnFamilyOptionValue<String> {
    MutableColumnFamilyOptionStringValue(final String value) {
      super(value);
    }

    @Override
    double asDouble() throws NumberFormatException {
      return Double.parseDouble(value);
    }

    @Override
    long asLong() throws NumberFormatException {
      return Long.parseLong(value);
    }

    @Override
    int asInt() throws NumberFormatException {
      return Integer.parseInt(value);
    }

    @Override
    boolean asBoolean() throws IllegalStateException {
      return Boolean.parseBoolean(value);
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      throw new IllegalStateException("String is not applicable as int[]");
    }

    @Override
    String asString() {
      return value;
    }

    @Override
    String asObject() {
      return value;
    }
  }

  private static class MutableColumnFamilyOptionDoubleValue
      extends MutableColumnFamilyOptionValue<Double> {
    MutableColumnFamilyOptionDoubleValue(final double value) {
      super(value);
    }

    @Override
    double asDouble() {
      return value;
    }

    @Override
    long asLong() throws NumberFormatException {
      return value.longValue();
    }

    @Override
    int asInt() throws NumberFormatException {
      if(value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
        throw new NumberFormatException(
            "double value lies outside the bounds of int");
      }
      return value.intValue();
    }

    @Override
    boolean asBoolean() throws IllegalStateException {
      throw new IllegalStateException(
          "double is not applicable as boolean");
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      if(value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
        throw new NumberFormatException(
            "double value lies outside the bounds of int");
      }
      return new int[] { value.intValue() };
    }

    @Override
    String asString() {
      return Double.toString(value);
    }

    @Override
    Double asObject() {
      return value;
    }
  }

  private static class MutableColumnFamilyOptionLongValue
      extends MutableColumnFamilyOptionValue<Long> {
    MutableColumnFamilyOptionLongValue(final long value) {
      super(value);
    }

    @Override
    double asDouble() {
      if(value > Double.MAX_VALUE || value < Double.MIN_VALUE) {
        throw new NumberFormatException(
            "long value lies outside the bounds of int");
      }
      return value.doubleValue();
    }

    @Override
    long asLong() throws NumberFormatException {
      return value;
    }

    @Override
    int asInt() throws NumberFormatException {
      if(value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
        throw new NumberFormatException(
            "long value lies outside the bounds of int");
      }
      return value.intValue();
    }

    @Override
    boolean asBoolean() throws IllegalStateException {
      throw new IllegalStateException(
          "long is not applicable as boolean");
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      if(value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
        throw new NumberFormatException(
            "long value lies outside the bounds of int");
      }
      return new int[] { value.intValue() };
    }

    @Override
    String asString() {
      return Long.toString(value);
    }

    @Override
    Long asObject() {
      return value;
    }
  }

  private static class MutableColumnFamilyOptionIntValue
      extends MutableColumnFamilyOptionValue<Integer> {
    MutableColumnFamilyOptionIntValue(final int value) {
      super(value);
    }

    @Override
    double asDouble() {
      if(value > Double.MAX_VALUE || value < Double.MIN_VALUE) {
        throw new NumberFormatException("int value lies outside the bounds of int");
      }
      return value.doubleValue();
    }

    @Override
    long asLong() throws NumberFormatException {
      return value;
    }

    @Override
    int asInt() throws NumberFormatException {
      return value;
    }

    @Override
    boolean asBoolean() throws IllegalStateException {
      throw new IllegalStateException("int is not applicable as boolean");
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      return new int[] { value };
    }

    @Override
    String asString() {
      return Integer.toString(value);
    }

    @Override
    Integer asObject() {
      return value;
    }
  }

  private static class MutableColumnFamilyOptionBooleanValue
      extends MutableColumnFamilyOptionValue<Boolean> {
    MutableColumnFamilyOptionBooleanValue(final boolean value) {
      super(value);
    }

    @Override
    double asDouble() {
      throw new NumberFormatException("boolean is not applicable as double");
    }

    @Override
    long asLong() throws NumberFormatException {
      throw new NumberFormatException("boolean is not applicable as Long");
    }

    @Override
    int asInt() throws NumberFormatException {
      throw new NumberFormatException("boolean is not applicable as int");
    }

    @Override
    boolean asBoolean() {
      return value;
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      throw new IllegalStateException("boolean is not applicable as int[]");
    }

    @Override
    String asString() {
      return Boolean.toString(value);
    }

    @Override
    Boolean asObject() {
      return value;
    }
  }

  private static class MutableColumnFamilyOptionIntArrayValue
      extends MutableColumnFamilyOptionValue<int[]> {
    MutableColumnFamilyOptionIntArrayValue(final int[] value) {
      super(value);
    }

    @Override
    double asDouble() {
      throw new NumberFormatException("int[] is not applicable as double");
    }

    @Override
    long asLong() throws NumberFormatException {
      throw new NumberFormatException("int[] is not applicable as Long");
    }

    @Override
    int asInt() throws NumberFormatException {
      throw new NumberFormatException("int[] is not applicable as int");
    }

    @Override
    boolean asBoolean() {
      throw new NumberFormatException("int[] is not applicable as boolean");
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      return value;
    }

    @Override
    String asString() {
      final StringBuilder builder = new StringBuilder();
      for(int i = 0; i < value.length; i++) {
        builder.append(Integer.toString(i));
        if(i + 1 < value.length) {
          builder.append(INT_ARRAY_INT_SEPARATOR);
        }
      }
      return builder.toString();
    }

    @Override
    int[] asObject() {
      return value;
    }
  }

  private static class MutableColumnFamilyOptionEnumValue<T extends Enum<T>>
      extends MutableColumnFamilyOptionValue<T> {

    MutableColumnFamilyOptionEnumValue(final T value) {
      super(value);
    }

    @Override
    double asDouble() throws NumberFormatException {
      throw new NumberFormatException("Enum is not applicable as double");
    }

    @Override
    long asLong() throws NumberFormatException {
      throw new NumberFormatException("Enum is not applicable as long");
    }

    @Override
    int asInt() throws NumberFormatException {
      throw new NumberFormatException("Enum is not applicable as int");
    }

    @Override
    boolean asBoolean() throws IllegalStateException {
      throw new NumberFormatException("Enum is not applicable as boolean");
    }

    @Override
    int[] asIntArray() throws IllegalStateException {
      throw new NumberFormatException("Enum is not applicable as int[]");
    }

    @Override
    String asString() {
      return value.name();
    }

    @Override
    T asObject() {
      return value;
    }
  }

  public static class MutableColumnFamilyOptionsBuilder
      implements MutableColumnFamilyOptionsInterface {

    private final static Map<String, MutableColumnFamilyOptionKey> ALL_KEYS_LOOKUP = new HashMap<>();
    static {
      for(final MutableColumnFamilyOptionKey key : MemtableOption.values()) {
        ALL_KEYS_LOOKUP.put(key.name(), key);
      }

      for(final MutableColumnFamilyOptionKey key : CompactionOption.values()) {
        ALL_KEYS_LOOKUP.put(key.name(), key);
      }

      for(final MutableColumnFamilyOptionKey key : MiscOption.values()) {
        ALL_KEYS_LOOKUP.put(key.name(), key);
      }
    }

    private final Map<MutableColumnFamilyOptionKey, MutableColumnFamilyOptionValue<?>> options = new LinkedHashMap<>();

    public MutableColumnFamilyOptions build() {
      final String keys[] = new String[options.size()];
      final String values[] = new String[options.size()];

      int i = 0;
      for(final Map.Entry<MutableColumnFamilyOptionKey, MutableColumnFamilyOptionValue<?>> option : options.entrySet()) {
        keys[i] = option.getKey().name();
        values[i] = option.getValue().asString();
        i++;
      }

      return new MutableColumnFamilyOptions(keys, values);
    }

    private MutableColumnFamilyOptionsBuilder setDouble(
        final MutableColumnFamilyOptionKey key, final double value) {
      if(key.getValueType() != ValueType.DOUBLE) {
        throw new IllegalArgumentException(
            key + " does not accept a double value");
      }
      options.put(key, new MutableColumnFamilyOptionDoubleValue(value));
      return this;
    }

    private double getDouble(final MutableColumnFamilyOptionKey key)
        throws NoSuchElementException, NumberFormatException {
      final MutableColumnFamilyOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + " has not been set");
      }
      return value.asDouble();
    }

    private MutableColumnFamilyOptionsBuilder setLong(
        final MutableColumnFamilyOptionKey key, final long value) {
      if(key.getValueType() != ValueType.LONG) {
        throw new IllegalArgumentException(
            key + " does not accept a long value");
      }
      options.put(key, new MutableColumnFamilyOptionLongValue(value));
      return this;
    }

    private long getLong(final MutableColumnFamilyOptionKey key)
        throws NoSuchElementException, NumberFormatException {
      final MutableColumnFamilyOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + " has not been set");
      }
      return value.asLong();
    }

    private MutableColumnFamilyOptionsBuilder setInt(
        final MutableColumnFamilyOptionKey key, final int value) {
      if(key.getValueType() != ValueType.INT) {
        throw new IllegalArgumentException(
            key + " does not accept an integer value");
      }
      options.put(key, new MutableColumnFamilyOptionIntValue(value));
      return this;
    }

    private int getInt(final MutableColumnFamilyOptionKey key)
        throws NoSuchElementException, NumberFormatException {
      final MutableColumnFamilyOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + " has not been set");
      }
      return value.asInt();
    }

    private MutableColumnFamilyOptionsBuilder setBoolean(
        final MutableColumnFamilyOptionKey key, final boolean value) {
      if(key.getValueType() != ValueType.BOOLEAN) {
        throw new IllegalArgumentException(
            key + " does not accept a boolean value");
      }
      options.put(key, new MutableColumnFamilyOptionBooleanValue(value));
      return this;
    }

    private boolean getBoolean(final MutableColumnFamilyOptionKey key)
        throws NoSuchElementException, NumberFormatException {
      final MutableColumnFamilyOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + " has not been set");
      }
      return value.asBoolean();
    }

    private MutableColumnFamilyOptionsBuilder setIntArray(
        final MutableColumnFamilyOptionKey key, final int[] value) {
      if(key.getValueType() != ValueType.INT_ARRAY) {
        throw new IllegalArgumentException(
            key + " does not accept an int array value");
      }
      options.put(key, new MutableColumnFamilyOptionIntArrayValue(value));
      return this;
    }

    private int[] getIntArray(final MutableColumnFamilyOptionKey key)
        throws NoSuchElementException, NumberFormatException {
      final MutableColumnFamilyOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + " has not been set");
      }
      return value.asIntArray();
    }

    private <T extends Enum<T>> MutableColumnFamilyOptionsBuilder setEnum(
        final MutableColumnFamilyOptionKey key, final T value) {
      if(key.getValueType() != ValueType.ENUM) {
        throw new IllegalArgumentException(
            key + " does not accept a Enum value");
      }
      options.put(key, new MutableColumnFamilyOptionEnumValue<T>(value));
      return this;

    }

    private <T extends Enum<T>> T getEnum(final MutableColumnFamilyOptionKey key)
        throws NoSuchElementException, NumberFormatException {
      final MutableColumnFamilyOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(key.name() + " has not been set");
      }

      if(!(value instanceof MutableColumnFamilyOptionEnumValue)) {
        throw new NoSuchElementException(key.name() + " is not of Enum type");
      }

      return ((MutableColumnFamilyOptionEnumValue<T>)value).asObject();
    }

    public MutableColumnFamilyOptionsBuilder fromString(final String keyStr,
        final String valueStr) throws IllegalArgumentException {
      Objects.requireNonNull(keyStr);
      Objects.requireNonNull(valueStr);

      final MutableColumnFamilyOptionKey key = ALL_KEYS_LOOKUP.get(keyStr);
      switch(key.getValueType()) {
        case DOUBLE:
          return setDouble(key, Double.parseDouble(valueStr));

        case LONG:
          return setLong(key, Long.parseLong(valueStr));

        case INT:
          return setInt(key, Integer.parseInt(valueStr));

        case BOOLEAN:
          return setBoolean(key, Boolean.parseBoolean(valueStr));

        case INT_ARRAY:
          final String[] strInts = valueStr
              .trim().split(INT_ARRAY_INT_SEPARATOR);
          if(strInts == null || strInts.length == 0) {
            throw new IllegalArgumentException(
                "int array value is not correctly formatted");
          }

          final int value[] = new int[strInts.length];
          int i = 0;
          for(final String strInt : strInts) {
            value[i++] = Integer.parseInt(strInt);
          }
          return setIntArray(key, value);
      }

      throw new IllegalStateException(
          key + " has unknown value type: " + key.getValueType());
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setWriteBufferSize(
        final long writeBufferSize) {
      return setLong(MemtableOption.write_buffer_size, writeBufferSize);
    }

    @Override
    public long writeBufferSize() {
      return getLong(MemtableOption.write_buffer_size);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setArenaBlockSize(
        final long arenaBlockSize) {
      return setLong(MemtableOption.arena_block_size, arenaBlockSize);
    }

    @Override
    public long arenaBlockSize() {
      return getLong(MemtableOption.arena_block_size);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMemtablePrefixBloomSizeRatio(
        final double memtablePrefixBloomSizeRatio) {
      return setDouble(MemtableOption.memtable_prefix_bloom_size_ratio,
          memtablePrefixBloomSizeRatio);
    }

    @Override
    public double memtablePrefixBloomSizeRatio() {
      return getDouble(MemtableOption.memtable_prefix_bloom_size_ratio);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMemtableHugePageSize(
        final long memtableHugePageSize) {
      return setLong(MemtableOption.memtable_huge_page_size,
          memtableHugePageSize);
    }

    @Override
    public long memtableHugePageSize() {
      return getLong(MemtableOption.memtable_huge_page_size);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxSuccessiveMerges(
        final long maxSuccessiveMerges) {
      return setLong(MemtableOption.max_successive_merges, maxSuccessiveMerges);
    }

    @Override
    public long maxSuccessiveMerges() {
      return getLong(MemtableOption.max_successive_merges);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxWriteBufferNumber(
        final int maxWriteBufferNumber) {
      return setInt(MemtableOption.max_write_buffer_number,
          maxWriteBufferNumber);
    }

    @Override
    public int maxWriteBufferNumber() {
      return getInt(MemtableOption.max_write_buffer_number);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setInplaceUpdateNumLocks(
        final long inplaceUpdateNumLocks) {
      return setLong(MemtableOption.inplace_update_num_locks,
          inplaceUpdateNumLocks);
    }

    @Override
    public long inplaceUpdateNumLocks() {
      return getLong(MemtableOption.inplace_update_num_locks);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setDisableAutoCompactions(
        final boolean disableAutoCompactions) {
      return setBoolean(CompactionOption.disable_auto_compactions,
          disableAutoCompactions);
    }

    @Override
    public boolean disableAutoCompactions() {
      return getBoolean(CompactionOption.disable_auto_compactions);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setSoftPendingCompactionBytesLimit(
        final long softPendingCompactionBytesLimit) {
      return setLong(CompactionOption.soft_pending_compaction_bytes_limit,
          softPendingCompactionBytesLimit);
    }

    @Override
    public long softPendingCompactionBytesLimit() {
      return getLong(CompactionOption.soft_pending_compaction_bytes_limit);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setHardPendingCompactionBytesLimit(
        final long hardPendingCompactionBytesLimit) {
      return setLong(CompactionOption.hard_pending_compaction_bytes_limit,
          hardPendingCompactionBytesLimit);
    }

    @Override
    public long hardPendingCompactionBytesLimit() {
      return getLong(CompactionOption.hard_pending_compaction_bytes_limit);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setLevel0FileNumCompactionTrigger(
        final int level0FileNumCompactionTrigger) {
      return setInt(CompactionOption.level0_file_num_compaction_trigger,
          level0FileNumCompactionTrigger);
    }

    @Override
    public int level0FileNumCompactionTrigger() {
      return getInt(CompactionOption.level0_file_num_compaction_trigger);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setLevel0SlowdownWritesTrigger(
        final int level0SlowdownWritesTrigger) {
      return setInt(CompactionOption.level0_slowdown_writes_trigger,
          level0SlowdownWritesTrigger);
    }

    @Override
    public int level0SlowdownWritesTrigger() {
      return getInt(CompactionOption.level0_slowdown_writes_trigger);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setLevel0StopWritesTrigger(
        final int level0StopWritesTrigger) {
      return setInt(CompactionOption.level0_stop_writes_trigger,
          level0StopWritesTrigger);
    }

    @Override
    public int level0StopWritesTrigger() {
      return getInt(CompactionOption.level0_stop_writes_trigger);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxCompactionBytes(final long maxCompactionBytes) {
      return setLong(CompactionOption.max_compaction_bytes, maxCompactionBytes);
    }

    @Override
    public long maxCompactionBytes() {
      return getLong(CompactionOption.max_compaction_bytes);
    }


    @Override
    public MutableColumnFamilyOptionsBuilder setTargetFileSizeBase(
        final long targetFileSizeBase) {
      return setLong(CompactionOption.target_file_size_base,
          targetFileSizeBase);
    }

    @Override
    public long targetFileSizeBase() {
      return getLong(CompactionOption.target_file_size_base);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setTargetFileSizeMultiplier(
        final int targetFileSizeMultiplier) {
      return setInt(CompactionOption.target_file_size_multiplier,
          targetFileSizeMultiplier);
    }

    @Override
    public int targetFileSizeMultiplier() {
      return getInt(CompactionOption.target_file_size_multiplier);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxBytesForLevelBase(
        final long maxBytesForLevelBase) {
      return setLong(CompactionOption.max_bytes_for_level_base,
          maxBytesForLevelBase);
    }

    @Override
    public long maxBytesForLevelBase() {
      return getLong(CompactionOption.max_bytes_for_level_base);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxBytesForLevelMultiplier(
        final double maxBytesForLevelMultiplier) {
      return setDouble(CompactionOption.max_bytes_for_level_multiplier, maxBytesForLevelMultiplier);
    }

    @Override
    public double maxBytesForLevelMultiplier() {
      return getDouble(CompactionOption.max_bytes_for_level_multiplier);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxBytesForLevelMultiplierAdditional(
        final int[] maxBytesForLevelMultiplierAdditional) {
      return setIntArray(
          CompactionOption.max_bytes_for_level_multiplier_additional,
          maxBytesForLevelMultiplierAdditional);
    }

    @Override
    public int[] maxBytesForLevelMultiplierAdditional() {
      return getIntArray(
          CompactionOption.max_bytes_for_level_multiplier_additional);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxSequentialSkipInIterations(
        final long maxSequentialSkipInIterations) {
      return setLong(MiscOption.max_sequential_skip_in_iterations,
          maxSequentialSkipInIterations);
    }

    @Override
    public long maxSequentialSkipInIterations() {
      return getLong(MiscOption.max_sequential_skip_in_iterations);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setParanoidFileChecks(
        final boolean paranoidFileChecks) {
      return setBoolean(MiscOption.paranoid_file_checks, paranoidFileChecks);
    }

    @Override
    public boolean paranoidFileChecks() {
      return getBoolean(MiscOption.paranoid_file_checks);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setCompressionType(
        final CompressionType compressionType) {
      return setEnum(MiscOption.compression_type, compressionType);
    }

    @Override
    public CompressionType compressionType() {
      return (CompressionType)getEnum(MiscOption.compression_type);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setReportBgIoStats(
        final boolean reportBgIoStats) {
      return setBoolean(MiscOption.report_bg_io_stats, reportBgIoStats);
    }

    @Override
    public boolean reportBgIoStats() {
      return getBoolean(MiscOption.report_bg_io_stats);
    }
  }
}
