// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Histogram Data.
 */
public class HistogramData {
  private final double median_;
  private final double percentile95_;
  private final double percentile99_;
  private final double average_;
  private final double standardDeviation_;
  private final double max_;
  private final long count_;
  private final long sum_;
  private final double min_;

  /**
   * Constructs a HistogramData.
   *
   * @param median the median value.
   * @param percentile95 the 95th percentile value.
   * @param percentile99 the 99th percentile value.
   * @param average the average value.
   * @param standardDeviation the value of the standard deviation.
   */
  public HistogramData(final double median, final double percentile95,
                       final double percentile99, final double average,
                       final double standardDeviation) {
    this(median, percentile95, percentile99, average, standardDeviation, 0.0, 0, 0, 0.0);
  }

  /**
   * Constructs a HistogramData.
   *
   * @param median the median value.
   * @param percentile95 the 95th percentile value.
   * @param percentile99 the 99th percentile value.
   * @param average the average value.
   * @param standardDeviation the value of the standard deviation.
   * @param max the maximum value.
   * @param count the number of values.
   * @param sum the sum of the values.
   * @param min the minimum value.
   */
  public HistogramData(final double median, final double percentile95,
      final double percentile99, final double average,
      final double standardDeviation, final double max, final long count,
      final long sum, final double min) {
    median_ = median;
    percentile95_ = percentile95;
    percentile99_ = percentile99;
    average_ = average;
    standardDeviation_ = standardDeviation;
    min_ = min;
    max_ = max;
    count_ = count;
    sum_ = sum;
  }

  /**
   * Get the median value.
   *
   * @return the median value.
   */
  public double getMedian() {
    return median_;
  }

  /**
   * Get the 95th percentile value.
   *
   * @return the 95th percentile value.
   */
  public double getPercentile95() {
    return percentile95_;
  }

  /**
   * Get the 99th percentile value.
   *
   * @return the 99th percentile value.
   */
  public double getPercentile99() {
    return percentile99_;
  }

  /**
   * Get the average value.
   *
   * @return the average value.
   */
  public double getAverage() {
    return average_;
  }

  /**
   * Get the value of the standard deviation.
   *
   * @return the value of the standard deviation.
   */
  public double getStandardDeviation() {
    return standardDeviation_;
  }

  /**
   * Get the maximum value.
   *
   * @return the maximum value.
   */
  public double getMax() {
    return max_;
  }

  /**
   * Get the number of values.
   *
   * @return the number of values.
   */
  public long getCount() {
    return count_;
  }

  /**
   * Get the sum of the values.
   *
   * @return the sum of the values.
   */
  public long getSum() {
    return sum_;
  }

  /**
   * Get the minimum value.
   *
   * @return the minimum value.
   */
  public double getMin() {
    return min_;
  }
}
