// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * Just a Java wrapper around CassandraCompactionFilter implemented in C++
 */
public class CassandraCompactionFilter
    extends AbstractCompactionFilter<Slice> {
  public CassandraCompactionFilter(boolean purgeTtlOnExpiration) {
      super(createNewCassandraCompactionFilter0(purgeTtlOnExpiration));
  }

  private native static long createNewCassandraCompactionFilter0(boolean purgeTtlOnExpiration);
}
