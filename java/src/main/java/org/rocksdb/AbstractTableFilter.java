// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Base class for Table Filters.
 */
public abstract class AbstractTableFilter
    extends RocksCallbackObject implements TableFilter {

  protected AbstractTableFilter() {
    super();
  }

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    return createNewTableFilter();
  }

  private native long createNewTableFilter();
}
