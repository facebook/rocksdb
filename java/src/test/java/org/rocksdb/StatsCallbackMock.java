// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class StatsCallbackMock implements StatisticsCollectorCallback {
  public int tickerCallbackCount = 0;
  public int histCallbackCount = 0;

  public void tickerCallback(final TickerType tickerType, final long tickerCount) {
    tickerCallbackCount++;
  }

  public void histogramCallback(final HistogramType histType, final HistogramData histData) {
    histCallbackCount++;
  }
}
