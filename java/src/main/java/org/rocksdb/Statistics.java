// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Statistics to analyze the performance of a db. Pointer for statistics object
 * is managed by Options class.
 */
public class Statistics {

  private final long statsHandle_;

  public Statistics(final long statsHandle) {
    statsHandle_ = statsHandle;
  }

  public long getTickerCount(TickerType tickerType) {
    assert(isInitialized());
    return getTickerCount0(tickerType.getValue(), statsHandle_);
  }

  public HistogramData getHistogramData(final HistogramType histogramType) {
    assert(isInitialized());
    return getHistogramData0(
        histogramType.getValue(), statsHandle_);
  }

  private boolean isInitialized() {
    return (statsHandle_ != 0);
  }

  private native long getTickerCount0(int tickerType, long handle);
  private native HistogramData getHistogramData0(int histogramType, long handle);
}
