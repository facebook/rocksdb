// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * Statistics to analyze the performance of a db. Pointer for statistics object
 * is managed by Options class.
 */
public class Statistics {

  //  TODO(AR) fix the ownership semantics of this class

  private final long statsHandle_;

  public Statistics(final long statsHandle) {
    statsHandle_ = statsHandle;
  }

  public StatsLevel statsLevel() {
    return StatsLevel.getStatsLevel(statsLevel(statsHandle_));
  }

  public void setStatsLevel(final StatsLevel statsLevel) {
    setStatsLevel(statsHandle_, statsLevel.getValue());
  }

  public long getTickerCount(final TickerType tickerType) {
    assert(isInitialized());
    return getTickerCount0(statsHandle_, tickerType.getValue());
  }

  public HistogramData getHistogramData(final HistogramType histogramType) {
    assert(isInitialized());
    return getHistogramData0(statsHandle_, histogramType.getValue());
  }

  // TODO(AR) add missing methods!

  private boolean isInitialized() {
    return (statsHandle_ != 0);
  }

  private native byte statsLevel(final long handle);
  private native void setStatsLevel(final long handle, final byte statsLevel);
  private native long getTickerCount0(final long handle, final byte tickerType);
  private native HistogramData getHistogramData0(final long handle, final byte histogramType);
}
