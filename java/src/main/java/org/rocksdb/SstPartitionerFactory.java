// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Handle to factory for SstPartitioner. It is used in {@link ColumnFamilyOptions}
 */
public abstract class SstPartitionerFactory extends RocksObject {
  protected SstPartitionerFactory(final long nativeHandle) {
    super(nativeHandle);
  }
}
