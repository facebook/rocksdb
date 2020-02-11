// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Simple instance reference wrapper.
 */
public class Holder<T> {
  private /* @Nullable */ T value;

  /**
   * Constructs a new Holder with null instance.
   */
  public Holder() {
  }

  /**
   * Constructs a new Holder.
   *
   * @param value the instance or null
   */
  public Holder(/* @Nullable */ final T value) {
    this.value = value;
  }

  /**
   * Get the instance reference.
   *
   * @return value the instance reference or null
   */
  public /* @Nullable */ T getValue() {
    return value;
  }

  /**
   * Set the instance reference.
   *
   * @param value the instance reference or null
   */
  public void setValue(/* @Nullable */ final T value) {
    this.value = value;
  }
}
