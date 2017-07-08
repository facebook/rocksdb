// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.EnumSet;

/**
 * Statistics to analyze the performance of a db. Pointer for statistics object
 * is managed by Options class.
 */
public class Statistics extends RocksObject {

  public Statistics() {
    super(newStatistics());
  }

  public Statistics(final Statistics otherStatistics) {
    super(newStatistics(otherStatistics.nativeHandle_));
  }

  public Statistics(final EnumSet<HistogramType> ignoreHistograms) {
    super(newStatistics(toArrayValues(ignoreHistograms)));
  }

  public Statistics(final EnumSet<HistogramType> ignoreHistograms, final Statistics otherStatistics) {
    super(newStatistics(toArrayValues(ignoreHistograms), otherStatistics.nativeHandle_));
  }

  /**
   * Intentionally package-private.
   *
   * Used from {@link DBOptions#statistics()}
   *
   * @param existingStatisticsHandle The C++ pointer to an existing statistics object
   */
  Statistics(final long existingStatisticsHandle) {
    super(existingStatisticsHandle);
  }

  private static byte[] toArrayValues(final EnumSet<HistogramType> histogramTypes) {
    final byte[] values = new byte[histogramTypes.size()];
    int i = 0;
    for(final HistogramType histogramType : histogramTypes) {
      values[i++] = histogramType.getValue();
    }
    return values;
  }

  public StatsLevel statsLevel() {
    return StatsLevel.getStatsLevel(statsLevel(nativeHandle_));
  }

  public void setStatsLevel(final StatsLevel statsLevel) {
    setStatsLevel(nativeHandle_, statsLevel.getValue());
  }

  public long getTickerCount(final TickerType tickerType) {
    assert(isOwningHandle());
    return getTickerCount0(nativeHandle_, tickerType.getValue());
  }

  public HistogramData getHistogramData(final HistogramType histogramType) {
    assert(isOwningHandle());
    return getHistogramData0(nativeHandle_, histogramType.getValue());
  }

  private native static long newStatistics();
  private native static long newStatistics(final long otherStatisticsHandle);
  private native static long newStatistics(final byte[] ignoreHistograms);
  private native static long newStatistics(final byte[] ignoreHistograms, final long otherStatisticsHandle);

  @Override protected final native void disposeInternal(final long handle);

  // TODO(AR) add missing methods!

  private native byte statsLevel(final long handle);
  private native void setStatsLevel(final long handle, final byte statsLevel);
  private native long getTickerCount0(final long handle, final byte tickerType);
  private native HistogramData getHistogramData0(final long handle, final byte histogramType);
}
