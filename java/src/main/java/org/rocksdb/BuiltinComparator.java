// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Builtin RocksDB comparators.
 */
public enum BuiltinComparator {
  /**
   * Sorts all keys in ascending byte wise.
   */
  BYTEWISE_COMPARATOR,

  /**
   * Sorts all keys in descending byte wise order.
   */
  REVERSE_BYTEWISE_COMPARATOR
}
