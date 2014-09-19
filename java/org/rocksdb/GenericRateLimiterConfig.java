// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
package org.rocksdb;

/**
 * Config for rate limiter, which is used to control write rate of flush and
 * compaction.
 */
public class GenericRateLimiterConfig extends RateLimiterConfig {
  private static final long DEFAULT_REFILL_PERIOD_MICROS = (100 * 1000);
  private static final int DEFAULT_FAIRNESS = 10;
    
  public GenericRateLimiterConfig(long rateBytesPerSecond,
      long refillPeriodMicros, int fairness) {
    rateBytesPerSecond_ = rateBytesPerSecond;
    refillPeriodMicros_ = refillPeriodMicros;
    fairness_ = fairness;
  }
  
  public GenericRateLimiterConfig(long rateBytesPerSecond) {
    this(rateBytesPerSecond, DEFAULT_REFILL_PERIOD_MICROS, DEFAULT_FAIRNESS);
  }
  
  @Override protected long newRateLimiterHandle() {
    return newRateLimiterHandle(rateBytesPerSecond_, refillPeriodMicros_,
        fairness_);
  }
    
  private native long newRateLimiterHandle(long rateBytesPerSecond,
      long refillPeriodMicros, int fairness);
  private final long rateBytesPerSecond_;
  private final long refillPeriodMicros_;
  private final int fairness_;
}
