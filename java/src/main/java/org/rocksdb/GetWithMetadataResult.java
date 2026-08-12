//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/** Result from an experimental metadata read. */
public final class GetWithMetadataResult {
  public final byte[] value;
  public final byte[] timestamp;
  public final boolean newerVersionPresent;

  @SuppressWarnings("PMD.ArrayIsStoredDirectly")
  GetWithMetadataResult(
      final byte[] value, final byte[] timestamp, final boolean newerVersionPresent) {
    this.value = value;
    this.timestamp = timestamp;
    this.newerVersionPresent = newerVersionPresent;
  }
}
