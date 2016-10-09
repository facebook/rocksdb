// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * RateLimiter, which is used to control write rate of flush and
 * compaction.
 *
 * @since 3.10.0
 */
public class RateLimiter extends RocksObject {
  private static final long DEFAULT_REFILL_PERIOD_MICROS = (100 * 1000);
  private static final int DEFAULT_FAIRNESS = 10;

  /**
   * RateLimiter constructor
   *
   * @param rateBytesPerSecond this is the only parameter you want to set
   *     most of the time. It controls the total write rate of compaction
   *     and flush in bytes per second. Currently, RocksDB does not enforce
   *     rate limit for anything other than flush and compaction, e.g. write to WAL.
   * @param refillPeriodMicros this controls how often tokens are refilled. For example,
   *     when rate_bytes_per_sec is set to 10MB/s and refill_period_us is set to
   *     100ms, then 1MB is refilled every 100ms internally. Larger value can lead to
   *     burstier writes while smaller value introduces more CPU overhead.
   *     The default should work for most cases.
   * @param fairness RateLimiter accepts high-pri requests and low-pri requests.
   *     A low-pri request is usually blocked in favor of hi-pri request. Currently,
   *     RocksDB assigns low-pri to request from compaction and high-pri to request
   *     from flush. Low-pri requests can get blocked if flush requests come in
   *     continuously. This fairness parameter grants low-pri requests permission by
   *     fairness chance even though high-pri requests exist to avoid starvation.
   *     You should be good by leaving it at default 10.
   */
  public RateLimiter(final long rateBytesPerSecond,
      final long refillPeriodMicros, final int fairness) {
    super(newRateLimiterHandle(rateBytesPerSecond,
        refillPeriodMicros, fairness));
  }

  /**
   * RateLimiter constructor
   *
   * @param rateBytesPerSecond this is the only parameter you want to set
   *     most of the time. It controls the total write rate of compaction
   *     and flush in bytes per second. Currently, RocksDB does not enforce
   *     rate limit for anything other than flush and compaction, e.g. write to WAL.
   */
  public RateLimiter(final long rateBytesPerSecond) {
    this(rateBytesPerSecond, DEFAULT_REFILL_PERIOD_MICROS, DEFAULT_FAIRNESS);
  }

  /**
   * <p>This API allows user to dynamically change rate limiter's bytes per second.
   * REQUIRED: bytes_per_second &gt; 0</p>
   *
   * @param bytesPerSecond bytes per second.
   */
  public void setBytesPerSecond(final long bytesPerSecond) {
    assert(isOwningHandle());
    setBytesPerSecond(nativeHandle_, bytesPerSecond);
  }

  /**
   * <p>Request for token to write bytes. If this request can not be satisfied,
   * the call is blocked. Caller is responsible to make sure
   * {@code bytes &lt; GetSingleBurstBytes()}.</p>
   *
   * @param bytes requested bytes.
   */
  public void request(final long bytes) {
    assert(isOwningHandle());
    request(nativeHandle_, bytes);
  }

  /**
   * <p>Max bytes can be granted in a single burst.</p>
   *
   * @return max bytes can be granted in a single burst.
   */
  public long getSingleBurstBytes() {
    assert(isOwningHandle());
    return getSingleBurstBytes(nativeHandle_);
  }

  /**
   * <p>Total bytes that go though rate limiter.</p>
   *
   * @return total bytes that go though rate limiter.
   */
  public long getTotalBytesThrough() {
    assert(isOwningHandle());
    return getTotalBytesThrough(nativeHandle_);
  }

  /**
   * <p>Total # of requests that go though rate limiter.</p>
   *
   * @return total # of requests that go though rate limiter.
   */
  public long getTotalRequests() {
    assert(isOwningHandle());
    return getTotalRequests(nativeHandle_);
  }

  private static native long newRateLimiterHandle(final long rateBytesPerSecond,
      final long refillPeriodMicros, final int fairness);
  @Override protected final native void disposeInternal(final long handle);

  private native void setBytesPerSecond(final long handle,
      final long bytesPerSecond);
  private native void request(final long handle, final long bytes);
  private native long getSingleBurstBytes(final long handle);
  private native long getTotalBytesThrough(final long handle);
  private native long getTotalRequests(final long handle);
}
