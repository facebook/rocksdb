//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Just a Java wrapper around CassandraCompactionFilter implemented in C++.
 * <p>
 * Compaction filter for removing expired Cassandra data with ttl.
 * Is also in charge of removing tombstone that has been
 * promoted to kValue type after serials of merging in compaction.
 */
public class CassandraCompactionFilter
    extends AbstractCompactionFilter<Slice> {

  /**
   * Constructs a new CasandraCompactionFilter.
   *
   * @param purgeTtlOnExpiration if set to true, expired data will be directly purged,
   *                             otherwise expired data will be converted to tombstones
   *                             first and then be eventually removed after
   *                             {@code gcGracePeriodInSeconds}. Should only be on in
   *                             the case that all the writes have the same ttl setting,
   *                             otherwise it could bring old data back.
   * @param gcGracePeriodInSeconds the grace period in seconds for gc.
   */
  public CassandraCompactionFilter(
      final boolean purgeTtlOnExpiration, final int gcGracePeriodInSeconds) {
    super(createNewCassandraCompactionFilter0(purgeTtlOnExpiration, gcGracePeriodInSeconds));
  }

  private static native long createNewCassandraCompactionFilter0(
      boolean purgeTtlOnExpiration, int gcGracePeriodInSeconds);
}
