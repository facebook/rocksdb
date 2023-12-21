// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

/**
 * Mutable Option keys.
 */
public interface MutableOptionKey {
  /**
   * Types of values used for Mutable Options,
   */
  enum ValueType {

    /**
     * Double precision floating point number.
     */
    DOUBLE,

    /**
     * 64 bit signed integer.
     */
    LONG,

    /**
     * 32 bit signed integer.
     */
    INT,

    /**
     * Boolean.
     */
    BOOLEAN,

    /**
     * Array of 32 bit signed integers.
     */
    INT_ARRAY,

    /**
     * Enumeration.
     */
    ENUM,

    /**
     * String.
     */
    STRING,
  }

  /**
   * Get the name of the MutableOption key.
   *
   * @return the name of the key.
   */
  String name();

  /**
   * Get the value type of the MutableOption.
   *
   * @return the value type.
   */
  ValueType getValueType();
}
