//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <cmath>

#include "monitoring/histogram.h"
#include "monitoring/histogram_windowing.h"
#include "util/testharness.h"

namespace rocksdb {

class HistogramTest : public testing::Test {};

namespace {
  const double kIota = 0.1;
  const HistogramBucketMapper bucketMapper;
  Env* env = Env::Default();
}

void PopulateHistogram(Histogram& histogram,
             uint64_t low, uint64_t high, uint64_t loop = 1) {
  for (; loop > 0; loop--) {
    for (uint64_t i = low; i <= high; i++) {
      histogram.Add(i);
    }
  }
}

void BasicOperation(Histogram& histogram) {
  PopulateHistogram(histogram, 1, 100, 10);

  HistogramData data;
  histogram.Data(&data);

  ASSERT_LE(fabs(histogram.Percentile(100.0) - 100.0), kIota);
  ASSERT_LE(fabs(data.percentile99 - 99.0), kIota);
  ASSERT_LE(fabs(data.percentile95 - 95.0), kIota);
  ASSERT_LE(fabs(data.median - 50.0), kIota);
  ASSERT_EQ(data.average, 50.5);               // avg is acurately calculated.
  ASSERT_LT(fabs(data.standard_deviation- 28.86), kIota); //sd is ~= 28.86
}

void MergeHistogram(Histogram& histogram, Histogram& other) {
  PopulateHistogram(histogram, 1, 100);
  PopulateHistogram(other, 101, 200);
  histogram.Merge(other);

  HistogramData data;
  histogram.Data(&data);

  ASSERT_LE(fabs(histogram.Percentile(100.0) - 200.0), kIota);
  ASSERT_LE(fabs(data.percentile99 - 198.0), kIota);
  ASSERT_LE(fabs(data.percentile95 - 190.0), kIota);
  ASSERT_LE(fabs(data.median - 100.0), kIota);
  ASSERT_EQ(data.average, 100.5);                // avg is acurately calculated.
  ASSERT_LT(fabs(data.standard_deviation - 57.73), kIota); //sd is ~= 57.73
}

void EmptyHistogram(Histogram& histogram) {
  ASSERT_EQ(histogram.min(), bucketMapper.LastValue());
  ASSERT_EQ(histogram.max(), 0);
  ASSERT_EQ(histogram.num(), 0);
  ASSERT_EQ(histogram.Median(), 0.0);
  ASSERT_EQ(histogram.Percentile(85.0), 0.0);
  ASSERT_EQ(histogram.Average(), 0.0);
  ASSERT_EQ(histogram.StandardDeviation(), 0.0);
}

void ClearHistogram(Histogram& histogram) {
  for (uint64_t i = 1; i <= 100; i++) {
    histogram.Add(i);
  }
  histogram.Clear();
  ASSERT_TRUE(histogram.Empty());
  ASSERT_EQ(histogram.Median(), 0);
  ASSERT_EQ(histogram.Percentile(85.0), 0);
  ASSERT_EQ(histogram.Average(), 0);
}

TEST_F(HistogramTest, BasicOperation) {
  HistogramImpl histogram;
  BasicOperation(histogram);

  HistogramWindowingImpl histogramWindowing;
  BasicOperation(histogramWindowing);
}

TEST_F(HistogramTest, MergeHistogram) {
  HistogramImpl histogram;
  HistogramImpl other;
  MergeHistogram(histogram, other);

  HistogramWindowingImpl histogramWindowing;
  HistogramWindowingImpl otherWindowing;
  MergeHistogram(histogramWindowing, otherWindowing);
}

TEST_F(HistogramTest, EmptyHistogram) {
  HistogramImpl histogram;
  EmptyHistogram(histogram);

  HistogramWindowingImpl histogramWindowing;
  EmptyHistogram(histogramWindowing);
}

TEST_F(HistogramTest, ClearHistogram) {
  HistogramImpl histogram;
  ClearHistogram(histogram);

  HistogramWindowingImpl histogramWindowing;
  ClearHistogram(histogramWindowing);
}

TEST_F(HistogramTest, HistogramWindowingExpire) {
  uint64_t num_windows = 3;
  int micros_per_window = 1000000;
  uint64_t min_num_per_window = 0;

  HistogramWindowingImpl
      histogramWindowing(num_windows, micros_per_window, min_num_per_window);

  PopulateHistogram(histogramWindowing, 1, 1, 100);
  env->SleepForMicroseconds(micros_per_window);
  ASSERT_EQ(histogramWindowing.num(), 100);
  ASSERT_EQ(histogramWindowing.min(), 1);
  ASSERT_EQ(histogramWindowing.max(), 1);
  ASSERT_EQ(histogramWindowing.Average(), 1);

  PopulateHistogram(histogramWindowing, 2, 2, 100);
  env->SleepForMicroseconds(micros_per_window);
  ASSERT_EQ(histogramWindowing.num(), 200);
  ASSERT_EQ(histogramWindowing.min(), 1);
  ASSERT_EQ(histogramWindowing.max(), 2);
  ASSERT_EQ(histogramWindowing.Average(), 1.5);

  PopulateHistogram(histogramWindowing, 3, 3, 100);
  env->SleepForMicroseconds(micros_per_window);
  ASSERT_EQ(histogramWindowing.num(), 300);
  ASSERT_EQ(histogramWindowing.min(), 1);
  ASSERT_EQ(histogramWindowing.max(), 3);
  ASSERT_EQ(histogramWindowing.Average(), 2.0);

  // dropping oldest window with value 1, remaining 2 ~ 4
  PopulateHistogram(histogramWindowing, 4, 4, 100);
  env->SleepForMicroseconds(micros_per_window);
  ASSERT_EQ(histogramWindowing.num(), 300);
  ASSERT_EQ(histogramWindowing.min(), 2);
  ASSERT_EQ(histogramWindowing.max(), 4);
  ASSERT_EQ(histogramWindowing.Average(), 3.0);

  // dropping oldest window with value 2, remaining 3 ~ 5
  PopulateHistogram(histogramWindowing, 5, 5, 100);
  env->SleepForMicroseconds(micros_per_window);
  ASSERT_EQ(histogramWindowing.num(), 300);
  ASSERT_EQ(histogramWindowing.min(), 3);
  ASSERT_EQ(histogramWindowing.max(), 5);
  ASSERT_EQ(histogramWindowing.Average(), 4.0);
}

TEST_F(HistogramTest, HistogramWindowingMerge) {
  uint64_t num_windows = 3;
  int micros_per_window = 1000000;
  uint64_t min_num_per_window = 0;

  HistogramWindowingImpl
      histogramWindowing(num_windows, micros_per_window, min_num_per_window);
  HistogramWindowingImpl
      otherWindowing(num_windows, micros_per_window, min_num_per_window);

  PopulateHistogram(histogramWindowing, 1, 1, 100);
  PopulateHistogram(otherWindowing, 1, 1, 100);
  env->SleepForMicroseconds(micros_per_window);

  PopulateHistogram(histogramWindowing, 2, 2, 100);
  PopulateHistogram(otherWindowing, 2, 2, 100);
  env->SleepForMicroseconds(micros_per_window);

  PopulateHistogram(histogramWindowing, 3, 3, 100);
  PopulateHistogram(otherWindowing, 3, 3, 100);
  env->SleepForMicroseconds(micros_per_window);

  histogramWindowing.Merge(otherWindowing);
  ASSERT_EQ(histogramWindowing.num(), 600);
  ASSERT_EQ(histogramWindowing.min(), 1);
  ASSERT_EQ(histogramWindowing.max(), 3);
  ASSERT_EQ(histogramWindowing.Average(), 2.0);

  // dropping oldest window with value 1, remaining 2 ~ 4
  PopulateHistogram(histogramWindowing, 4, 4, 100);
  env->SleepForMicroseconds(micros_per_window);
  ASSERT_EQ(histogramWindowing.num(), 500);
  ASSERT_EQ(histogramWindowing.min(), 2);
  ASSERT_EQ(histogramWindowing.max(), 4);

  // dropping oldest window with value 2, remaining 3 ~ 5
  PopulateHistogram(histogramWindowing, 5, 5, 100);
  env->SleepForMicroseconds(micros_per_window);
  ASSERT_EQ(histogramWindowing.num(), 400);
  ASSERT_EQ(histogramWindowing.min(), 3);
  ASSERT_EQ(histogramWindowing.max(), 5);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
