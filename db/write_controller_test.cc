//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/write_controller.h"

#include <ratio>

#include "rocksdb/system_clock.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
namespace {
class TimeSetClock : public SystemClockWrapper {
 public:
  explicit TimeSetClock() : SystemClockWrapper(nullptr) {}
  const char* Name() const override { return "TimeSetClock"; }
  uint64_t now_micros_ = 6666;
  uint64_t NowNanos() override { return now_micros_ * std::milli::den; }
};
}  // namespace
class WriteControllerTest : public testing::Test {
 public:
  WriteControllerTest() { clock_ = std::make_shared<TimeSetClock>(); }
  std::shared_ptr<TimeSetClock> clock_;
};

TEST_F(WriteControllerTest, ChangeDelayRateTest) {
  WriteController controller(40000000u);  // also set max delayed rate
  controller.set_delayed_write_rate(10000000u);
  auto delay_token_0 =
      controller.GetDelayToken(controller.delayed_write_rate());
  ASSERT_EQ(static_cast<uint64_t>(2000000),
            controller.GetDelay(clock_.get(), 20000000u));
  auto delay_token_1 = controller.GetDelayToken(2000000u);
  ASSERT_EQ(static_cast<uint64_t>(10000000),
            controller.GetDelay(clock_.get(), 20000000u));
  auto delay_token_2 = controller.GetDelayToken(1000000u);
  ASSERT_EQ(static_cast<uint64_t>(20000000),
            controller.GetDelay(clock_.get(), 20000000u));
  auto delay_token_3 = controller.GetDelayToken(20000000u);
  ASSERT_EQ(static_cast<uint64_t>(1000000),
            controller.GetDelay(clock_.get(), 20000000u));
  // This is more than max rate. Max delayed rate will be used.
  auto delay_token_4 =
      controller.GetDelayToken(controller.delayed_write_rate() * 3);
  ASSERT_EQ(static_cast<uint64_t>(500000),
            controller.GetDelay(clock_.get(), 20000000u));
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

  auto delay_token_1 = controller.GetDelayToken(10000000u);
  ASSERT_EQ(static_cast<uint64_t>(2000000),
            controller.GetDelay(clock_.get(), 20000000u));

  clock_->now_micros_ += 1999900u;  // sleep debt 1000

  auto delay_token_2 = controller.GetDelayToken(10000000u);
  // Rate reset after changing the token.
  ASSERT_EQ(static_cast<uint64_t>(2000000),
            controller.GetDelay(clock_.get(), 20000000u));

  clock_->now_micros_ += 1999900u;  // sleep debt 1000

  // One refill: 10240 bytes allowed, 1000 used, 9240 left
  ASSERT_EQ(static_cast<uint64_t>(1124),
            controller.GetDelay(clock_.get(), 1000u));
  clock_->now_micros_ += 1124u;  // sleep debt 0

  delay_token_2.reset();
  // 1000 used, 8240 left
  ASSERT_EQ(static_cast<uint64_t>(0), controller.GetDelay(clock_.get(), 1000u));

  clock_->now_micros_ += 100u;  // sleep credit 100
  // 1000 used, 7240 left
  ASSERT_EQ(static_cast<uint64_t>(0), controller.GetDelay(clock_.get(), 1000u));

  clock_->now_micros_ += 100u;  // sleep credit 200
  // One refill: 10240 fileed, sleep credit generates 2000. 8000 used
  //             7240 + 10240 + 2000 - 8000 = 11480 left
  ASSERT_EQ(static_cast<uint64_t>(1024u),
            controller.GetDelay(clock_.get(), 8000u));

  clock_->now_micros_ += 200u;  // sleep debt 824
  // 1000 used, 10480 left.
  ASSERT_EQ(static_cast<uint64_t>(0), controller.GetDelay(clock_.get(), 1000u));

  clock_->now_micros_ += 200u;  // sleep debt 624
  // Out of bound sleep, still 10480 left
  ASSERT_EQ(static_cast<uint64_t>(3000624u),
            controller.GetDelay(clock_.get(), 30000000u));

  clock_->now_micros_ += 3000724u;  // sleep credit 100
  // 6000 used, 4480 left.
  ASSERT_EQ(static_cast<uint64_t>(0), controller.GetDelay(clock_.get(), 6000u));

  clock_->now_micros_ += 200u;  // sleep credit 300
  // One refill, credit 4480 balance + 3000 credit + 10240 refill
  // Use 8000, 9720 left
  ASSERT_EQ(static_cast<uint64_t>(1024u),
            controller.GetDelay(clock_.get(), 8000u));

  clock_->now_micros_ += 3024u;  // sleep credit 2000

  // 1720 left
  ASSERT_EQ(static_cast<uint64_t>(0u),
            controller.GetDelay(clock_.get(), 8000u));

  // 1720 balance + 20000 credit = 20170 left
  // Use 8000, 12170 left
  ASSERT_EQ(static_cast<uint64_t>(0u),
            controller.GetDelay(clock_.get(), 8000u));

  // 4170 left
  ASSERT_EQ(static_cast<uint64_t>(0u),
            controller.GetDelay(clock_.get(), 8000u));

  // Need a refill
  ASSERT_EQ(static_cast<uint64_t>(1024u),
            controller.GetDelay(clock_.get(), 9000u));

  delay_token_1.reset();
  ASSERT_EQ(static_cast<uint64_t>(0),
            controller.GetDelay(clock_.get(), 30000000u));
  delay_token_1.reset();
  ASSERT_FALSE(controller.IsStopped());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
