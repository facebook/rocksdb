/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef THRIFT_TEST_LOADGEN_RNG_H_
#define THRIFT_TEST_LOADGEN_RNG_H_ 1

#include <boost/random/mersenne_twister.hpp>

namespace apache { namespace thrift { namespace loadgen {

/**
 * A random number generator to use for load tests.
 *
 * We keep one RNG per thread, to avoid having to perform locking when getting
 * random numbers.
 *
 * RNG also implements the boost random number generator interface, so it can
 * be used as an engine for boost random distributions.
 *
 *
 * This class is essentially just a wrapper around boost::mt19937.
 * We have to wrap it because some of the boost code copy-constructs the
 * generators, causing the state of the original generator to never be updated.
 * (In particular uniform_01 copy constructs its generator argument, and
 * variate_generator may end up using uniform_01.)
 *
 * It's rather annoying that we have to define our own wrapper class for this.
 */
class RNG {
 public:
  // Use boost::mt19937 as the underlying RNG
  typedef boost::mt19937 BoostRNG;

  // result_type and has_fixed_range are required for the boost interfaces
  typedef BoostRNG::result_type result_type;
  static const bool has_fixed_range = false;

  RNG(BoostRNG* rng) : rng_(rng) {}

  /**
   * Get the thread-local RNG.
   */
  static RNG& getRNG();

  /**
   * Set the RNG seed.
   *
   * This value is used to pick seeds for new thread-local RNGs.
   * It won't affect thread-local RNGs that have already been created.
   *
   * Note that that the seed value used for each thread-local RNG depends on
   * the order in which the thread-local RNGs are created.  You may not get
   * consistent results across runs if your threads are not initialized in a
   * consistent order.
   */
  static void setGlobalSeed(result_type s);

  /**
   * Re-seed this RNG
   */
  void seed(result_type s) {
    rng_->seed(s);
  }

  /**
   * Get a random number.
   *
   * Part of the boost random generator interface.
   */
  result_type operator()() {
    return (*rng_)();
  }

  /**
   * Get the minimum value that can be returned.
   *
   * Part of the boost random generator interface.
   */
  result_type min() const {
    return rng_->min();
  }

  /**
   * Get the maximum value that can be returned.
   *
   * Part of the boost random generator interface.
   */
  result_type max() const {
    return rng_->max();
  }

  /*
   * Helper functions to pick random uint32_t values
   */
  static uint32_t getU32();
  static uint32_t getU32(uint32_t max);
  static uint32_t getU32(uint32_t min, uint32_t max);

  /**
   * Helper function to pick random double values in the range [0.0, 1.0)
   */
  static double getReal();

  /**
   * Helper function to pick random double values in the range [min, max)
   */
  static double getReal(double min, double max);

  /**
   * Helper function to pick random values in a log-normal distribution.
   *
   * @param mean The mean value for the log-normal distribution.
   * @param sigma The sigma value for the log-normal distribution.
   *              (This controls how spread-out the distribution will be.)
   *              If negative, defaults to half the mean.
   */
  static double getLogNormal(double mean, double sigma = -1.0);

 private:
  BoostRNG* rng_;
};

}}} // apache::thrift::test

#endif // THRIFT_TEST_LOADGEN_RNG_H_
