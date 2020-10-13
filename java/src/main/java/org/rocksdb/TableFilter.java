// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Filter for iterating a table.
 */
public interface TableFilter {

  /**
   * A callback to determine whether relevant keys for this scan exist in a
   * given table based on the table's properties. The callback is passed the
   * properties of each table during iteration. If the callback returns false,
   * the table will not be scanned. This option only affects Iterators and has
   * no impact on point lookups.
   *
   * @param tableProperties the table properties.
   *
   * @return true if the table should be scanned, false otherwise.
   */
  boolean filter(final TableProperties tableProperties);
}
