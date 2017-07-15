// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RateLimiterTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void setBytesPerSecond() {
    try(final RateLimiter rateLimiter =
            new RateLimiter(1000, 100 * 1000, 1)) {
      rateLimiter.setBytesPerSecond(2000);
    }
  }

  @Test
  public void getSingleBurstBytes() {
    try(final RateLimiter rateLimiter =
            new RateLimiter(1000, 100 * 1000, 1)) {
      assertThat(rateLimiter.getSingleBurstBytes()).isEqualTo(100);
    }
  }

  @Test
  public void getTotalBytesThrough() {
    try(final RateLimiter rateLimiter =
            new RateLimiter(1000, 100 * 1000, 1)) {
      assertThat(rateLimiter.getTotalBytesThrough()).isEqualTo(0);
    }
  }

  @Test
  public void getTotalRequests() {
    try(final RateLimiter rateLimiter =
            new RateLimiter(1000, 100 * 1000, 1)) {
      assertThat(rateLimiter.getTotalRequests()).isEqualTo(0);
    }
  }
}
