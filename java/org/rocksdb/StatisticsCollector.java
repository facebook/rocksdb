// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Helper class to collect DB statistics periodically at a period specified in
 * constructor. Callback function (provided in constructor) is called with
 * every statistics collection.
 *
 * Caller should call start() to start statistics collection. Shutdown() should
 * be called to stop stats collection and should be called before statistics (
 * provided in constructor) reference has been disposed.
 */
public class StatisticsCollector {
  private final Statistics _statistics;
  private final ThreadPoolExecutor _threadPoolExecutor;
  private final int _statsCollectionInterval;
  private final StatisticsCollectorCallback _statsCallback;
  private volatile boolean _isRunning = true;

  /**
   * Constructor for statistics collector.
   * @param statistics Reference of DB statistics.
   * @param statsCollectionIntervalInMilliSeconds Statistics collection time 
   *        period (specified in milliseconds) 
   * @param statsCallback Reference of statistics callback interface.
   */
  public StatisticsCollector(Statistics statistics,
      int statsCollectionIntervalInMilliSeconds,
      StatisticsCollectorCallback statsCallback) {
    _statistics = statistics;
    _statsCollectionInterval = statsCollectionIntervalInMilliSeconds;
    _statsCallback = statsCallback;

    _threadPoolExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(1));
  }

  public void start() {
    _threadPoolExecutor.submit(collectStatistics());
  }

  public void shutDown() throws InterruptedException {
    _isRunning = false;

    _threadPoolExecutor.shutdown();
    // Wait for collectStatistics runnable to finish so that disposal of
    // statistics does not cause any exceptions to be thrown.
    _threadPoolExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
  }

  private Runnable collectStatistics() {
    return new Runnable() {

      @Override
      public void run() {
        while (_isRunning) {
          try {
            // Collect ticker data
            for(TickerType ticker : TickerType.values()) {
              long tickerValue = _statistics.getTickerCount(ticker);
              _statsCallback.tickerCallback(ticker, tickerValue);
            }

            // Collect histogram data
            for(HistogramType histogramType : HistogramType.values()) {
              HistogramData histogramData =
                  _statistics.geHistogramData(histogramType);
              _statsCallback.histogramCallback(histogramType, histogramData);
            }

            Thread.sleep(_statsCollectionInterval);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread got interrupted!", e);
          }
          catch (Exception e) {
            throw new RuntimeException("Error while calculating statistics", e);
          }
        }
      }
    };
  }
}
