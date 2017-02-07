//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "db/write_controller.h"

#include "rocksdb/env.h"
#include "util/testharness.h"

namespace rocksdb {

class WriteControllerTest : public testing::Test {};

class TimeSetEnv : public EnvWrapper {
 public:
  explicit TimeSetEnv() : EnvWrapper(nullptr) {}
  uint64_t now_nanos_ = 6666000;
  virtual uint64_t NowNanos() override { return now_nanos_; }
};

TEST_F(WriteControllerTest, ChangeDelayRateTest) {
  TimeSetEnv env;
  WriteController controller(40000000u);  // also set max delayed rate
  controller.set_delayed_write_rate(10000000u);
  auto delay_token_0 =
      controller.GetDelayToken(controller.delayed_write_rate());
  ASSERT_EQ(static_cast<uint64_t>(2000000000),
            controller.GetDelay(&env, 20000000u));
  auto delay_token_1 = controller.GetDelayToken(2000000u);
  ASSERT_EQ(static_cast<uint64_t>(10000000000),
            controller.GetDelay(&env, 20000000u));
  auto delay_token_2 = controller.GetDelayToken(1000000u);
  ASSERT_EQ(static_cast<uint64_t>(20000000000),
            controller.GetDelay(&env, 20000000u));
  auto delay_token_3 = controller.GetDelayToken(20000000u);
  ASSERT_EQ(static_cast<uint64_t>(1000000000),
            controller.GetDelay(&env, 20000000u));
  // This is more than max rate. Max delayed rate will be used.
  auto delay_token_4 =
      controller.GetDelayToken(controller.delayed_write_rate() * 3);
  ASSERT_EQ(static_cast<uint64_t>(500000000),
            controller.GetDelay(&env, 20000000u));
}

TEST_F(WriteControllerTest, SanityTest) {
  WriteController controller(10000000u);
  auto stop_token_1 = controller.GetStopToken();
  auto stop_token_2 = controller.GetStopToken();

  ASSERT_TRUE(controller.IsStopped());
  stop_token_1.reset();
  ASSERT_TRUE(controller.IsStopped());
  stop_token_2.reset();
  ASSERT_FALSE(controller.IsStopped());

  TimeSetEnv env;

  auto delay_token_1 = controller.GetDelayToken(10000000u);
  ASSERT_EQ(static_cast<uint64_t>(2000000000),
            controller.GetDelay(&env, 20000000u));

  env.now_nanos_ += 1999900000u;  // sleep debt 100000

  auto delay_token_2 = controller.GetDelayToken(10000000u);
  // Rate reset after changing the token.
  ASSERT_EQ(static_cast<uint64_t>(2000000000),
            controller.GetDelay(&env, 20000000u));

  env.now_nanos_ += 1999900000u;  // sleep debt 100000

  // One refill: 10000 bytes allowed, 1000 used, 9000 left
  ASSERT_EQ(static_cast<uint64_t>(1100000), controller.GetDelay(&env, 1000u));
  env.now_nanos_ += 1100000u;  // sleep debt 0

  delay_token_2.reset();
  // 1000 used, 8000 left
  ASSERT_EQ(static_cast<uint64_t>(0), controller.GetDelay(&env, 1000u));

  env.now_nanos_ += 100000u;  // sleep credit 100000
  // 1000 used, 7000 left
  ASSERT_EQ(static_cast<uint64_t>(0), controller.GetDelay(&env, 1000u));

  env.now_nanos_ += 100000u;  // sleep credit 200000
  // One refill: 10000 filed, sleep credit generates 2000. 8000 used
  //             7000 + 10000 + 2000 - 8000 = 11000 left
  ASSERT_EQ(static_cast<uint64_t>(1000000u), controller.GetDelay(&env, 8000u));

  env.now_nanos_ += 200000u;  // sleep debt 800000
  // 1000 used, 10000 left.
  ASSERT_EQ(static_cast<uint64_t>(0), controller.GetDelay(&env, 1000u));

  env.now_nanos_ += 200000u;  // sleep debt 600000
  // Out of bound sleep, still 10480 left
  ASSERT_EQ(static_cast<uint64_t>(3000600000u),
            controller.GetDelay(&env, 30000000u));

  env.now_nanos_ += 3000700000u;  // sleep credit 100000
  // 6000 used, 4000 left.
  ASSERT_EQ(static_cast<uint64_t>(0), controller.GetDelay(&env, 6000u));

  env.now_nanos_ += 200000u;  // sleep credit 300000
  // One refill, credit 4000 balance + 3000 credit + 10000 refill
  // Use 8000, 9000 left
  ASSERT_EQ(static_cast<uint64_t>(1000000u), controller.GetDelay(&env, 8000u));

  env.now_nanos_ += 3000000u;  // sleep credit 2000000

  // 1000 left
  ASSERT_EQ(static_cast<uint64_t>(0u), controller.GetDelay(&env, 8000u));

  // 1000 balance + 20000 credit = 21000 left
  // Use 8000, 13000 left
  ASSERT_EQ(static_cast<uint64_t>(0u), controller.GetDelay(&env, 8000u));

  // 5000 left
  ASSERT_EQ(static_cast<uint64_t>(0u), controller.GetDelay(&env, 8000u));

  // Need a refill
  ASSERT_EQ(static_cast<uint64_t>(1000000u), controller.GetDelay(&env, 9000u));

  delay_token_1.reset();
  ASSERT_EQ(static_cast<uint64_t>(0), controller.GetDelay(&env, 30000000u));
  delay_token_1.reset();
  ASSERT_FALSE(controller.IsStopped());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
