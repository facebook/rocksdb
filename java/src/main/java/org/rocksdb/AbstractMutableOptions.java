// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

@SuppressWarnings("AbstractClassWithoutAbstractMethods")
public abstract class AbstractMutableOptions {
  private static final String KEY_VALUE_PAIR_SEPARATOR = ";";
  private static final char KEY_VALUE_SEPARATOR = '=';
  static final String INT_ARRAY_INT_SEPARATOR = ":";

  private static final String MESSAGE_KEY_HAS_NOT_BEEN_SET = "Key {0} has not been set";
  private static final String MESSAGE_VALUE_IS_NOT_ENUM = "{0} is not of Enum type";
  private static final String MESSAGE_UNABLE_TO_PARSE_OR_ROUND =
      "Unable to parse or round {0} to {1}";

  protected final String[] keys;
  private final String[] values;

  /**
   * User must use builder pattern, or parser.
   *
   * @param keys the keys
   * @param values the values
   */
  protected AbstractMutableOptions(final String[] keys, final String[] values) {
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
   * Returns a string representation of MutableOptions which
   * is suitable for consumption by {@code #parse(String)}.
   *
   * @return String representation of MutableOptions
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

  public abstract static class AbstractMutableOptionsBuilder<
      T extends AbstractMutableOptions, U extends AbstractMutableOptionsBuilder<T, U, K>, K
          extends MutableOptionKey> {
    private final Map<K, MutableOptionValue<?>> options = new LinkedHashMap<>();
    private final List<OptionString.Entry> unknown = new ArrayList<>();

    protected abstract U self();

    /**
     * Get all of the possible keys
     *
     * @return A map of all keys, indexed by name.
     */
    protected abstract Map<String, K> allKeys();

    /**
     * Construct a sub-class instance of {@link AbstractMutableOptions}.
     *
     * @param keys the keys
     * @param values the values
     *
     * @return an instance of the options.
     */
    protected abstract T build(final String[] keys, final String[] values);

    public final T build() {
      final String[] keys = new String[options.size()];
      final String[] values = new String[options.size()];

      int i = 0;
      for (final Map.Entry<K, MutableOptionValue<?>> option : options.entrySet()) {
        keys[i] = option.getKey().name();
        values[i] = option.getValue().asString();
        i++;
      }

      return build(keys, values);
    }

    protected U setDouble(final K key, final double value) {
      if (key.getValueType() != MutableOptionKey.ValueType.DOUBLE) {
        throw new IllegalArgumentException(
            key + " does not accept a double value");
      }
      options.put(key, MutableOptionValue.fromDouble(value));
      return self();
    }

    /**
     * @param key of the option
     * @return the value stored for the key, interpreted as a double
     * @throws NoSuchElementException if the key does not exist in the options
     * @throws NumberFormatException if the value does not parse to a double
     */
    protected double getDouble(final K key) {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(MessageFormat.format("{0} has not been set", key.name()));
      }
      return value.asDouble();
    }

    protected U setLong(
        final K key, final long value) {
      if(key.getValueType() != MutableOptionKey.ValueType.LONG) {
        throw new IllegalArgumentException(
            key + " does not accept a long value");
      }
      options.put(key, MutableOptionValue.fromLong(value));
      return self();
    }

    /**
     * @param key of the option
     * @return the value stored for the key, interpreted as a long
     * @throws NoSuchElementException if the key does not exist in the options
     * @throws NumberFormatException if the value does not parse to a long
     */
    protected long getLong(final K key) {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(
            MessageFormat.format(MESSAGE_KEY_HAS_NOT_BEEN_SET, key.name()));
      }
      return value.asLong();
    }

    protected U setInt(
        final K key, final int value) {
      if(key.getValueType() != MutableOptionKey.ValueType.INT) {
        throw new IllegalArgumentException(
            key + " does not accept an integer value");
      }
      options.put(key, MutableOptionValue.fromInt(value));
      return self();
    }

    /**
     * @param key of the option
     * @return the value stored for the key, interpreted as a long
     * @throws NoSuchElementException if the key does not exist in the options
     * @throws NumberFormatException if the value does not parse to a long
     */
    protected int getInt(final K key) {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(
            MessageFormat.format(MESSAGE_KEY_HAS_NOT_BEEN_SET, key.name()));
      }
      return value.asInt();
    }

    protected U setBoolean(
        final K key, final boolean value) {
      if(key.getValueType() != MutableOptionKey.ValueType.BOOLEAN) {
        throw new IllegalArgumentException(
            key + " does not accept a boolean value");
      }
      options.put(key, MutableOptionValue.fromBoolean(value));
      return self();
    }

    /**
     * @param key of the option
     * @return the value stored for the key, interpreted as a boolean
     * @throws NoSuchElementException if the key does not exist in the options
     * @throws NumberFormatException if the value does not parse to a boolean
     */
    protected boolean getBoolean(final K key) {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(
            MessageFormat.format(MESSAGE_KEY_HAS_NOT_BEEN_SET, key.name()));
      }
      return value.asBoolean();
    }

    protected U setIntArray(
        final K key, final int[] value) {
      if(key.getValueType() != MutableOptionKey.ValueType.INT_ARRAY) {
        throw new IllegalArgumentException(
            key + " does not accept an int array value");
      }
      options.put(key, MutableOptionValue.fromIntArray(value));
      return self();
    }

    /**
     * @param key of the option
     * @return the value stored for the key, interpreted as an int array
     * @throws NoSuchElementException if the key does not exist in the options
     * @throws NumberFormatException if the value does not parse to an int array
     */
    protected int[] getIntArray(final K key) {
      final MutableOptionValue<?> value = options.get(key);
      if(value == null) {
        throw new NoSuchElementException(
            MessageFormat.format(MESSAGE_KEY_HAS_NOT_BEEN_SET, key.name()));
      }
      return value.asIntArray();
    }

    protected <N extends Enum<N>> U setEnum(
        final K key, final N value) {
      if(key.getValueType() != MutableOptionKey.ValueType.ENUM) {
        throw new IllegalArgumentException(
            key + " does not accept a Enum value");
      }
      options.put(key, MutableOptionValue.fromEnum(value));
      return self();
    }

    /**
     * @param key of the option
     * @return the value stored for the key, interpreted as an enum
     * @throws NoSuchElementException if the key does not exist in the options
     * @throws NumberFormatException if the value does not parse to an enum
     */
    @SuppressWarnings("unchecked")
    protected <N extends Enum<N>> N getEnum(final K key) {
      final MutableOptionValue<?> value = options.get(key);
      if (value == null) {
        throw new NoSuchElementException(
            MessageFormat.format(MESSAGE_KEY_HAS_NOT_BEEN_SET, key.name()));
      }

      if (!(value instanceof MutableOptionValue.MutableOptionEnumValue)) {
        throw new NoSuchElementException(
            MessageFormat.format(MESSAGE_VALUE_IS_NOT_ENUM, key.name()));
      }

      return ((MutableOptionValue.MutableOptionEnumValue<N>) value).asObject();
    }

    /**
     * Parse a string into a long value, accepting values expressed as a double (such as 9.00) which
     * are meant to be a long, not a double
     *
     * @param value the string containing a value which represents a long
     * @return the long value of the parsed string
     */
    private static long parseAsLong(final String value) {
      try {
        return Long.parseLong(value);
      } catch (final NumberFormatException ignored) {
      }

      final double doubleValue = Double.parseDouble(value);
      if (Double.doubleToLongBits(doubleValue) != Double.doubleToLongBits(Math.round(doubleValue)))
        throw new IllegalArgumentException(
            MessageFormat.format(MESSAGE_UNABLE_TO_PARSE_OR_ROUND, value, " to long"));
      return Math.round(doubleValue);
    }

    /**
     * Parse a string into an int value, accepting values expressed as a double (such as 9.00) which
     * are meant to be an int, not a double
     *
     * @param value the string containing a value which represents an int
     * @return the int value of the parsed string
     */
    private static int parseAsInt(final String value) {
      try {
        return Integer.parseInt(value);
      } catch (final NumberFormatException ignored) {
      }

      final double doubleValue = Double.parseDouble(value);
      if (Double.doubleToLongBits(doubleValue) != Double.doubleToLongBits(Math.round(doubleValue)))
        throw new IllegalArgumentException(
            MessageFormat.format(MESSAGE_UNABLE_TO_PARSE_OR_ROUND, value, " to int"));
      //noinspection NumericCastThatLosesPrecision
      return (int) Math.round(doubleValue);
    }

    /**
     * Constructs a builder for mutable column family options from a hierarchical parsed options
     * string representation. The {@link OptionString.Parser} class output has been used to create a
     * (name,value)-list; each value may be either a simple string or a (name, value)-list in turn.
     *
     * @param options a list of parsed option string objects
     * @param ignoreUnknown what to do if the key is not one of the keys we expect
     *
     * @return a builder with the values from the parsed input set
     *
     * @throws IllegalArgumentException if an option value is of the wrong type, or a key is empty
     */
    protected U fromParsed(final List<OptionString.Entry> options, final boolean ignoreUnknown) {
      Objects.requireNonNull(options);

      for (final OptionString.Entry option : options) {
        try {
          if (option.key.isEmpty()) {
            throw new IllegalArgumentException(
                MessageFormat.format("options string is invalid: {0}", option));
          }
          fromOptionString(option, ignoreUnknown);
        } catch (final NumberFormatException nfe) {
          throw new IllegalArgumentException(
              MessageFormat.format(
                  "{0}={1} - not a valid value for its type", option.key, option.value),
              nfe);
        }
      }

      return self();
    }

    /**
     * Set a value in the builder from the supplied option string
     *
     * @param option the option key/value to add to this builder
     * @param ignoreUnknown if this is not set, throw an exception when a key is not in the known
     *     set
     * @return the same object, after adding options
     * @throws IllegalArgumentException if the key is unkown, or a value has the wrong type/form
     * @throws NumberFormatException if a string is not a number of the expected kind
     */
    @SuppressWarnings("UnusedReturnValue")
    private U fromOptionString(final OptionString.Entry option, final boolean ignoreUnknown) {
      Objects.requireNonNull(option.key);
      Objects.requireNonNull(option.value);

      final K key = allKeys().get(option.key);
      if (key == null && ignoreUnknown) {
        unknown.add(option);
        return self();
      } else if (key == null) {
        throw new IllegalArgumentException(
            MessageFormat.format("Key: {0} is not a known option key", option.key));
      }

      if (!option.value.isList()) {
        throw new IllegalArgumentException(MessageFormat.format(
            "Option: {0}={1} is not a simple value or list, don't know how to parse it", option.key,
            key));
      }

      // Check that simple values are the single item in the array
      if (key.getValueType() != MutableOptionKey.ValueType.INT_ARRAY) {
        {
          if (option.value.list.size() != 1) {
            throw new IllegalArgumentException(MessageFormat.format(
                "Simple value does not have exactly 1 item: {0}", option.value.list));
          }
        }
      }

      final List<String> valueStrs = option.value.list;
      final String valueStr = valueStrs.get(0);

      switch (key.getValueType()) {
        case DOUBLE:
          return setDouble(key, Double.parseDouble(valueStr));

        case LONG:
          return setLong(key, parseAsLong(valueStr));

        case INT:
          return setInt(key, parseAsInt(valueStr));

        case BOOLEAN:
          return setBoolean(key, Boolean.parseBoolean(valueStr));

        case INT_ARRAY:
          final int[] value = new int[valueStrs.size()];
          for (int i = 0; i < valueStrs.size(); i++) {
            value[i] = Integer.parseInt(valueStrs.get(i));
          }
          return setIntArray(key, value);

        case ENUM:
          String optionName = key.name();
          if (optionName.equals("prepopulate_blob_cache")) {
            final PrepopulateBlobCache prepopulateBlobCache =
                PrepopulateBlobCache.getFromInternal(valueStr);
            return setEnum(key, prepopulateBlobCache);
          } else if (optionName.equals("compression")
              || optionName.equals("blob_compression_type")) {
            final CompressionType compressionType = CompressionType.getFromInternal(valueStr);
            return setEnum(key, compressionType);
          } else {
            throw new IllegalArgumentException("Unknown enum type: " + key.name());
          }

        default:
          throw new IllegalStateException(
              MessageFormat.format("{0} has unknown value type: {1}", key, key.getValueType()));
      }
    }

    /**
     *
     * @return the list of keys encountered which were not known to the type being generated
     */
    public List<OptionString.Entry> getUnknown() {
      return new ArrayList<>(unknown);
    }
  }
}
