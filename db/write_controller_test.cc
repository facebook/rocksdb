//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/write_controller.h"

#include <array>
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
}  // anonymous namespace
class WriteControllerTest : public testing::Test {
 public:
  WriteControllerTest() { clock_ = std::make_shared<TimeSetClock>(); }
  std::shared_ptr<TimeSetClock> clock_;
};

// Make tests easier to read
#define MILLION *1000000u
#define MB MILLION
#define MBPS MILLION
#define SECS MILLION  // in microseconds

TEST_F(WriteControllerTest, BasicAPI) {
  WriteController controller(40 MBPS);  // also set max delayed rate
  EXPECT_EQ(controller.delayed_write_rate(), 40 MBPS);
  EXPECT_FALSE(controller.IsStopped());
  EXPECT_FALSE(controller.NeedsDelay());
  EXPECT_EQ(0, controller.GetDelay(clock_.get(), 100 MB));

  // set, get
  controller.set_delayed_write_rate(20 MBPS);
  EXPECT_EQ(controller.delayed_write_rate(), 20 MBPS);
  EXPECT_FALSE(controller.IsStopped());
  EXPECT_FALSE(controller.NeedsDelay());
  EXPECT_EQ(0, controller.GetDelay(clock_.get(), 100 MB));

  {
    // set with token, get
    auto delay_token_0 = controller.GetDelayToken(10 MBPS);
    EXPECT_EQ(controller.delayed_write_rate(), 10 MBPS);
    EXPECT_FALSE(controller.IsStopped());
    EXPECT_TRUE(controller.NeedsDelay());
    // test with delay
    EXPECT_EQ(2 SECS, controller.GetDelay(clock_.get(), 20 MB));
    clock_->now_micros_ += 2 SECS;  // pay the "debt"

    auto delay_token_1 = controller.GetDelayToken(2 MBPS);
    EXPECT_EQ(10 SECS, controller.GetDelay(clock_.get(), 20 MB));
    clock_->now_micros_ += 10 SECS;  // pay the "debt"

    auto delay_token_2 = controller.GetDelayToken(1 MBPS);
    EXPECT_EQ(20 SECS, controller.GetDelay(clock_.get(), 20 MB));
    clock_->now_micros_ += 20 SECS;  // pay the "debt"

    auto delay_token_3 = controller.GetDelayToken(20 MBPS);
    EXPECT_EQ(1 SECS, controller.GetDelay(clock_.get(), 20 MB));
    clock_->now_micros_ += 1 SECS;  // pay the "debt"

    // 60M is more than the max rate of 40M. Max rate will be used.
    EXPECT_EQ(controller.delayed_write_rate(), 20 MBPS);
    auto delay_token_4 =
        controller.GetDelayToken(controller.delayed_write_rate() * 3);
    EXPECT_EQ(controller.delayed_write_rate(), 40 MBPS);
    EXPECT_EQ(static_cast<uint64_t>(0.5 SECS),
              controller.GetDelay(clock_.get(), 20 MB));

    EXPECT_FALSE(controller.IsStopped());
    EXPECT_TRUE(controller.NeedsDelay());

    // Test stop tokens
    {
      auto stop_token_1 = controller.GetStopToken();
      EXPECT_TRUE(controller.IsStopped());
      EXPECT_EQ(0, controller.GetDelay(clock_.get(), 100 MB));
      {
        auto stop_token_2 = controller.GetStopToken();
        EXPECT_TRUE(controller.IsStopped());
        EXPECT_EQ(0, controller.GetDelay(clock_.get(), 100 MB));
      }
      EXPECT_TRUE(controller.IsStopped());
      EXPECT_EQ(0, controller.GetDelay(clock_.get(), 100 MB));
    }
    // Stop tokens released
    EXPECT_FALSE(controller.IsStopped());
    EXPECT_TRUE(controller.NeedsDelay());
    EXPECT_EQ(controller.delayed_write_rate(), 40 MBPS);
    // pay the previous "debt"
    clock_->now_micros_ += static_cast<uint64_t>(0.5 SECS);
    EXPECT_EQ(1 SECS, controller.GetDelay(clock_.get(), 40 MB));
  }

  // Delay tokens released
  EXPECT_FALSE(controller.NeedsDelay());
}

TEST_F(WriteControllerTest, StartFilled) {
  WriteController controller(10 MBPS);

  // Attempt to write two things that combined would be allowed within
  // a single refill interval
  auto delay_token_0 =
      controller.GetDelayToken(controller.delayed_write_rate());

  // Verify no delay because write rate has not been exceeded within
  // refill interval.
  EXPECT_EQ(0U, controller.GetDelay(clock_.get(), 2000u /*bytes*/));
  EXPECT_EQ(0U, controller.GetDelay(clock_.get(), 2000u /*bytes*/));

  // Allow refill (kMicrosPerRefill)
  clock_->now_micros_ += 1000;

  // Again
  EXPECT_EQ(0U, controller.GetDelay(clock_.get(), 2000u /*bytes*/));
  EXPECT_EQ(0U, controller.GetDelay(clock_.get(), 2000u /*bytes*/));

  // Control: something bigger that would exceed write rate within interval
  uint64_t delay = controller.GetDelay(clock_.get(), 10 MB);
  EXPECT_GT(1.0 * delay, 0.999 SECS);
  EXPECT_LT(1.0 * delay, 1.001 SECS);
}

TEST_F(WriteControllerTest, DebtAccumulation) {
  WriteController controller(10 MBPS);

  std::array<std::unique_ptr<WriteControllerToken>, 10> tokens;

  // Accumulate a time delay debt with no passage of time, like many column
  // families delaying writes simultaneously. (Old versions of WriteController
  // would reset the debt on every GetDelayToken.)
  uint64_t debt = 0;
  for (unsigned i = 0; i < tokens.size(); ++i) {
    tokens[i] = controller.GetDelayToken((i + 1u) MBPS);
    uint64_t delay = controller.GetDelay(clock_.get(), 63 MB);
    ASSERT_GT(delay, debt);
    uint64_t incremental = delay - debt;
    ASSERT_EQ(incremental, (63 SECS) / (i + 1u));
    debt += incremental;
  }

  // Pay down the debt
  clock_->now_micros_ += debt;
  debt = 0;

  // Now accumulate debt with some passage of time.
  for (unsigned i = 0; i < tokens.size(); ++i) {
    // Debt is accumulated in time, not in bytes, so this new write
    // limit is not applied to prior requested delays, even it they are
    // in progress.
    tokens[i] = controller.GetDelayToken((i + 1u) MBPS);
    uint64_t delay = controller.GetDelay(clock_.get(), 63 MB);
    ASSERT_GT(delay, debt);
    uint64_t incremental = delay - debt;
    ASSERT_EQ(incremental, (63 SECS) / (i + 1u));
    debt += incremental;
    uint64_t credit = debt / 2;
    clock_->now_micros_ += credit;
    debt -= credit;
  }

  // Pay down the debt
  clock_->now_micros_ += debt;
  debt = 0;    // consistent state
  (void)debt;  // appease clang-analyze

  // Verify paid down
  EXPECT_EQ(0U, controller.GetDelay(clock_.get(), 100u /*small bytes*/));

  // Accumulate another debt, without accounting, and releasing tokens
  for (unsigned i = 0; i < tokens.size(); ++i) {
    // Big and small are delayed
    ASSERT_LT(0U, controller.GetDelay(clock_.get(), 63 MB));
    ASSERT_LT(0U, controller.GetDelay(clock_.get(), 100u /*small bytes*/));
    tokens[i].reset();
  }
  // All tokens released.
  // Verify that releasing all tokens pays down debt, even with no time passage.
  tokens[0] = controller.GetDelayToken(1 MBPS);
  ASSERT_EQ(0U, controller.GetDelay(clock_.get(), 100u /*small bytes*/));
}

// This may or may not be a "good" feature, but it's an old feature
TEST_F(WriteControllerTest, CreditAccumulation) {
  WriteController controller(10 MBPS);

  std::array<std::unique_ptr<WriteControllerToken>, 10> tokens;

  // Ensure started
  tokens[0] = controller.GetDelayToken(1 MBPS);
  ASSERT_EQ(10 SECS, controller.GetDelay(clock_.get(), 10 MB));
  clock_->now_micros_ += 10 SECS;

  // Accumulate a credit
  uint64_t credit = 1000 SECS /* see below: * 1 MB / 1 SEC */;
  clock_->now_micros_ += credit;

  // Spend some credit (burst of I/O)
  for (unsigned i = 0; i < tokens.size(); ++i) {
    tokens[i] = controller.GetDelayToken((i + 1u) MBPS);
    ASSERT_EQ(0U, controller.GetDelay(clock_.get(), 63 MB));
    // In WriteController, credit is accumulated in bytes, not in time.
    // After an "unnecessary" delay, all of our time credit will be
    // translated to bytes on the next operation, in this case with
    // setting 1 MBPS. So regardless of the rate at delay time, we just
    // account for the bytes.
    credit -= 63 MB;
  }
  // Spend remaining credit
  tokens[0] = controller.GetDelayToken(1 MBPS);
  ASSERT_EQ(0U, controller.GetDelay(clock_.get(), credit));
  // Verify
  ASSERT_EQ(10 SECS, controller.GetDelay(clock_.get(), 10 MB));
  clock_->now_micros_ += 10 SECS;

  // Accumulate a credit, no accounting
  clock_->now_micros_ += 1000 SECS;

  // Spend a small amount, releasing tokens
  for (unsigned i = 0; i < tokens.size(); ++i) {
    ASSERT_EQ(0U, controller.GetDelay(clock_.get(), 3 MB));
    tokens[i].reset();
  }

  // All tokens released.
  // Verify credit is wiped away on new delay.
  tokens[0] = controller.GetDelayToken(1 MBPS);
  ASSERT_EQ(10 SECS, controller.GetDelay(clock_.get(), 10 MB));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
