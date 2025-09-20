//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Because there are a small set of tests for Slice and there's a cost in having
// extra test binaries for each component, this test file has evolved into a
// "grab bag" of small tests for various reusable components, mostly in  util/.

#include "rocksdb/slice.h"

#include <gtest/gtest.h>

#include <semaphore>

#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/data_structure.h"
#include "rocksdb/types.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/bit_fields.h"
#include "util/cast_util.h"
#include "util/semaphore.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

TEST(SliceTest, StringView) {
  std::string s = "foo";
  std::string_view sv = s;
  ASSERT_EQ(Slice(s), Slice(sv));
  ASSERT_EQ(Slice(s), Slice(std::move(sv)));
}

// Use this to keep track of the cleanups that were actually performed
void Multiplier(void* arg1, void* arg2) {
  int* res = static_cast<int*>(arg1);
  int* num = static_cast<int*>(arg2);
  *res *= *num;
}

class PinnableSliceTest : public testing::Test {
 public:
  void AssertSameData(const std::string& expected, const PinnableSlice& slice) {
    std::string got;
    got.assign(slice.data(), slice.size());
    ASSERT_EQ(expected, got);
  }
};

// Test that the external buffer is moved instead of being copied.
TEST_F(PinnableSliceTest, MoveExternalBuffer) {
  Slice s("123");
  std::string buf;
  PinnableSlice v1(&buf);
  v1.PinSelf(s);

  PinnableSlice v2(std::move(v1));
  ASSERT_EQ(buf.data(), v2.data());
  ASSERT_EQ(&buf, v2.GetSelf());

  PinnableSlice v3;
  v3 = std::move(v2);
  ASSERT_EQ(buf.data(), v3.data());
  ASSERT_EQ(&buf, v3.GetSelf());
}

TEST_F(PinnableSliceTest, Move) {
  int n2 = 2;
  int res = 1;
  const std::string const_str1 = "123";
  const std::string const_str2 = "ABC";
  Slice slice1(const_str1);
  Slice slice2(const_str2);

  {
    // Test move constructor on a pinned slice.
    res = 1;
    PinnableSlice v1;
    v1.PinSlice(slice1, Multiplier, &res, &n2);
    PinnableSlice v2(std::move(v1));

    // Since v1's Cleanable has been moved to v2,
    // no cleanup should happen in Reset.
    v1.Reset();
    ASSERT_EQ(1, res);

    AssertSameData(const_str1, v2);
  }
  // v2 is cleaned up.
  ASSERT_EQ(2, res);

  {
    // Test move constructor on an unpinned slice.
    PinnableSlice v1;
    v1.PinSelf(slice1);
    PinnableSlice v2(std::move(v1));

    AssertSameData(const_str1, v2);
  }

  {
    // Test move assignment from a pinned slice to
    // another pinned slice.
    res = 1;
    PinnableSlice v1;
    v1.PinSlice(slice1, Multiplier, &res, &n2);
    PinnableSlice v2;
    v2.PinSlice(slice2, Multiplier, &res, &n2);
    v2 = std::move(v1);

    // v2's Cleanable will be Reset before moving
    // anything from v1.
    ASSERT_EQ(2, res);
    // Since v1's Cleanable has been moved to v2,
    // no cleanup should happen in Reset.
    v1.Reset();
    ASSERT_EQ(2, res);

    AssertSameData(const_str1, v2);
  }
  // The Cleanable moved from v1 to v2 will be Reset.
  ASSERT_EQ(4, res);

  {
    // Test move assignment from a pinned slice to
    // an unpinned slice.
    res = 1;
    PinnableSlice v1;
    v1.PinSlice(slice1, Multiplier, &res, &n2);
    PinnableSlice v2;
    v2.PinSelf(slice2);
    v2 = std::move(v1);

    // Since v1's Cleanable has been moved to v2,
    // no cleanup should happen in Reset.
    v1.Reset();
    ASSERT_EQ(1, res);

    AssertSameData(const_str1, v2);
  }
  // The Cleanable moved from v1 to v2 will be Reset.
  ASSERT_EQ(2, res);

  {
    // Test move assignment from an upinned slice to
    // another unpinned slice.
    PinnableSlice v1;
    v1.PinSelf(slice1);
    PinnableSlice v2;
    v2.PinSelf(slice2);
    v2 = std::move(v1);

    AssertSameData(const_str1, v2);
  }

  {
    // Test move assignment from an upinned slice to
    // a pinned slice.
    res = 1;
    PinnableSlice v1;
    v1.PinSelf(slice1);
    PinnableSlice v2;
    v2.PinSlice(slice2, Multiplier, &res, &n2);
    v2 = std::move(v1);

    // v2's Cleanable will be Reset before moving
    // anything from v1.
    ASSERT_EQ(2, res);

    AssertSameData(const_str1, v2);
  }
  // No Cleanable is moved from v1 to v2, so no more cleanup.
  ASSERT_EQ(2, res);
}

// ***************************************************************** //
// Unit test for SmallEnumSet
class SmallEnumSetTest : public testing::Test {
 public:
  SmallEnumSetTest() = default;
  ~SmallEnumSetTest() = default;
};

TEST_F(SmallEnumSetTest, SmallEnumSetTest1) {
  FileTypeSet fs;  // based on a legacy enum type
  ASSERT_TRUE(fs.empty());
  ASSERT_EQ(fs.count(), 0U);
  ASSERT_TRUE(fs.Add(FileType::kIdentityFile));
  ASSERT_FALSE(fs.empty());
  ASSERT_EQ(fs.count(), 1U);
  ASSERT_FALSE(fs.Add(FileType::kIdentityFile));
  ASSERT_TRUE(fs.Add(FileType::kInfoLogFile));
  ASSERT_TRUE(fs.Contains(FileType::kIdentityFile));
  ASSERT_FALSE(fs.Contains(FileType::kDBLockFile));
  ASSERT_FALSE(fs.empty());
  ASSERT_EQ(fs.count(), 2U);
  ASSERT_FALSE(fs.Remove(FileType::kDBLockFile));
  ASSERT_TRUE(fs.Remove(FileType::kIdentityFile));
  ASSERT_FALSE(fs.empty());
  ASSERT_EQ(fs.count(), 1U);
  ASSERT_TRUE(fs.Remove(FileType::kInfoLogFile));
  ASSERT_TRUE(fs.empty());
  ASSERT_EQ(fs.count(), 0U);
}

namespace {
enum class MyEnumClass { A, B, C };
}  // namespace

using MyEnumClassSet = SmallEnumSet<MyEnumClass, MyEnumClass::C>;

TEST_F(SmallEnumSetTest, SmallEnumSetTest2) {
  MyEnumClassSet s;  // based on an enum class type
  ASSERT_TRUE(s.Add(MyEnumClass::A));
  ASSERT_TRUE(s.Contains(MyEnumClass::A));
  ASSERT_FALSE(s.Contains(MyEnumClass::B));
  ASSERT_TRUE(s.With(MyEnumClass::B).Contains(MyEnumClass::B));
  ASSERT_TRUE(s.With(MyEnumClass::A).Contains(MyEnumClass::A));
  ASSERT_FALSE(s.Contains(MyEnumClass::B));
  ASSERT_FALSE(s.Without(MyEnumClass::A).Contains(MyEnumClass::A));
  ASSERT_FALSE(
      s.With(MyEnumClass::B).Without(MyEnumClass::B).Contains(MyEnumClass::B));
  ASSERT_TRUE(
      s.Without(MyEnumClass::B).With(MyEnumClass::B).Contains(MyEnumClass::B));
  ASSERT_TRUE(s.Contains(MyEnumClass::A));

  const MyEnumClassSet cs = s;
  ASSERT_TRUE(cs.Contains(MyEnumClass::A));
  ASSERT_EQ(cs, MyEnumClassSet{MyEnumClass::A});
  ASSERT_EQ(cs.Without(MyEnumClass::A), MyEnumClassSet{});
  ASSERT_EQ(cs, MyEnumClassSet::All().Without(MyEnumClass::B, MyEnumClass::C));
  ASSERT_EQ(cs.With(MyEnumClass::B, MyEnumClass::C), MyEnumClassSet::All());
  ASSERT_EQ(
      MyEnumClassSet::All(),
      MyEnumClassSet{}.With(MyEnumClass::A, MyEnumClass::B, MyEnumClass::C));
  ASSERT_NE(cs, MyEnumClassSet{MyEnumClass::B});
  ASSERT_NE(cs, MyEnumClassSet::All());

  ASSERT_EQ(MyEnumClassSet{}.count(), 0U);
  ASSERT_EQ(MyEnumClassSet::All().count(), 3U);

  int count = 0;
  for (MyEnumClass e : cs) {
    ASSERT_EQ(e, MyEnumClass::A);
    ++count;
  }
  ASSERT_EQ(count, 1);
  ASSERT_EQ(cs.count(), 1U);

  count = 0;
  for (MyEnumClass e : MyEnumClassSet::All().Without(MyEnumClass::B)) {
    ASSERT_NE(e, MyEnumClass::B);
    ++count;
  }
  ASSERT_EQ(count, 2);

  for (MyEnumClass e : MyEnumClassSet{}) {
    (void)e;
    assert(false);
  }
}

template <typename ENUM_TYPE, ENUM_TYPE MAX_ENUMERATOR>
void TestBiggerEnumSet() {
  using MySet = SmallEnumSet<ENUM_TYPE, MAX_ENUMERATOR>;
  constexpr int kMaxValue = static_cast<int>(MAX_ENUMERATOR);
  SCOPED_TRACE("kMaxValue = " + std::to_string(kMaxValue));

  ASSERT_EQ(sizeof(MySet), (kMaxValue + 1 + 63) / 64 * 8);

  MySet s;
  ASSERT_TRUE(s.empty());
  ASSERT_EQ(s.count(), 0U);
  ASSERT_TRUE(s.Add(ENUM_TYPE(0)));
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(s.count(), 1U);
  ASSERT_TRUE(s.Add(ENUM_TYPE(kMaxValue - 1)));
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(s.count(), 2U);
  ASSERT_TRUE(s.Add(ENUM_TYPE(kMaxValue)));
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(s.count(), 3U);

  int count = 0;
  for (ENUM_TYPE e : s) {
    ASSERT_TRUE(e == ENUM_TYPE(0) || e == ENUM_TYPE(kMaxValue - 1) ||
                e == ENUM_TYPE(kMaxValue));
    ++count;
  }
  ASSERT_EQ(count, 3);

  ASSERT_TRUE(s.Remove(ENUM_TYPE(0)));
  ASSERT_TRUE(s.Remove(ENUM_TYPE(kMaxValue)));
  ASSERT_FALSE(s.empty());
  ASSERT_EQ(s.count(), 1U);

  count = 0;
  for (ENUM_TYPE e : s) {
    ASSERT_EQ(e, ENUM_TYPE(kMaxValue - 1));
    ++count;
  }
  ASSERT_EQ(count, 1);
}

TEST_F(SmallEnumSetTest, BiggerEnumClasses) {
  enum class BiggerEnumClass63 { A, B, C = 63 };
  enum class BiggerEnumClass64 { A, B, C = 64 };
  enum class BiggerEnumClass65 { A, B, C = 65 };
  enum class BiggerEnumClass127 { A, B, C = 127 };
  enum class BiggerEnumClass128 { A, B, C = 128 };
  enum class BiggerEnumClass129 { A, B, C = 129 };
  enum class BiggerEnumClass150 { A, B, C = 150 };
  enum class BiggerEnumClass255 { A, B, C = 255 };

  TestBiggerEnumSet<BiggerEnumClass63, BiggerEnumClass63::C>();
  TestBiggerEnumSet<BiggerEnumClass64, BiggerEnumClass64::C>();
  TestBiggerEnumSet<BiggerEnumClass65, BiggerEnumClass65::C>();
  TestBiggerEnumSet<BiggerEnumClass127, BiggerEnumClass127::C>();
  TestBiggerEnumSet<BiggerEnumClass128, BiggerEnumClass128::C>();
  TestBiggerEnumSet<BiggerEnumClass129, BiggerEnumClass129::C>();
  TestBiggerEnumSet<BiggerEnumClass150, BiggerEnumClass150::C>();
  TestBiggerEnumSet<BiggerEnumClass255, BiggerEnumClass255::C>();
}

// ***************************************************************** //
// Unit test for Status
TEST(StatusTest, Update) {
  const Status ok = Status::OK();
  const Status inc = Status::Incomplete("blah");
  const Status notf = Status::NotFound("meow");

  Status s = ok;
  ASSERT_TRUE(s.UpdateIfOk(Status::Corruption("bad")).IsCorruption());
  ASSERT_TRUE(s.IsCorruption());

  s = ok;
  ASSERT_TRUE(s.UpdateIfOk(Status::OK()).ok());
  ASSERT_TRUE(s.UpdateIfOk(ok).ok());
  ASSERT_TRUE(s.ok());

  ASSERT_TRUE(s.UpdateIfOk(inc).IsIncomplete());
  ASSERT_TRUE(s.IsIncomplete());

  ASSERT_TRUE(s.UpdateIfOk(notf).IsIncomplete());
  ASSERT_TRUE(s.UpdateIfOk(ok).IsIncomplete());
  ASSERT_TRUE(s.IsIncomplete());

  // Keeps left-most non-OK status
  s = ok;
  ASSERT_TRUE(
      s.UpdateIfOk(Status()).UpdateIfOk(notf).UpdateIfOk(inc).IsNotFound());
  ASSERT_TRUE(s.IsNotFound());
}

// ***************************************************************** //
// Unit test for UnownedPtr
TEST(UnownedPtrTest, Tests) {
  {
    int x = 0;
    UnownedPtr<int> p(&x);
    ASSERT_EQ(p.get(), &x);
    ASSERT_EQ(*p, 0);
    x = 1;
    ASSERT_EQ(*p, 1);
    ASSERT_EQ(p.get(), &x);
    ASSERT_EQ(*p, 1);
    *p = 2;
    ASSERT_EQ(x, 2);
    ASSERT_EQ(*p, 2);
    ASSERT_EQ(p.get(), &x);
    ASSERT_EQ(*p, 2);
  }
  {
    std::unique_ptr<std::pair<int, int>> u =
        std::make_unique<std::pair<int, int>>();
    *u = {1, 2};
    UnownedPtr<std::pair<int, int>> p;
    ASSERT_FALSE(p);
    p = u.get();
    ASSERT_TRUE(p);
    ASSERT_EQ(p->first, 1);
    // These must not compile:
    /*
    u = p;
    u = std::move(p);
    std::unique_ptr<std::pair<int, int>> v{p};
    std::unique_ptr<std::pair<int, int>> v{std::move(p)};
    */
    // END must not compile

    UnownedPtr<std::pair<int, int>> q;
    q = std::move(p);
    ASSERT_EQ(q->first, 1);
    // Not committing to any moved-from semantics (on p here)
  }
  {
    std::shared_ptr<std::pair<int, int>> s =
        std::make_shared<std::pair<int, int>>();
    *s = {1, 2};
    UnownedPtr<std::pair<int, int>> p;
    ASSERT_FALSE(p);
    p = s.get();
    ASSERT_TRUE(p);
    ASSERT_EQ(p->first, 1);
    // These must not compile:
    /*
    s = p;
    s = std::move(p);
    std::unique_ptr<std::pair<int, int>> t{p};
    std::unique_ptr<std::pair<int, int>> t{std::move(p)};
    */
    // END must not compile
    UnownedPtr<std::pair<int, int>> q;
    q = std::move(p);
    ASSERT_EQ(q->first, 1);
    // Not committing to any moved-from semantics (on p here)
  }
}

TEST(ToBaseCharsStringTest, Tests) {
  using ROCKSDB_NAMESPACE::ToBaseCharsString;
  // Base 16
  ASSERT_EQ(ToBaseCharsString<16>(5, 0, true), "00000");
  ASSERT_EQ(ToBaseCharsString<16>(5, 42, true), "0002A");
  ASSERT_EQ(ToBaseCharsString<16>(5, 42, false), "0002a");
  ASSERT_EQ(ToBaseCharsString<16>(2, 255, false), "ff");
  // Base 32
  ASSERT_EQ(ToBaseCharsString<32>(2, 255, false), "7v");
}

TEST(SemaphoreTest, CountingSemaphore) {
  CountingSemaphore sem{0};
  int kCount = 5;
  std::vector<std::thread> threads;
  for (int i = 0; i < kCount; ++i) {
    threads.emplace_back([&sem] { sem.Release(); });
  }
  for (int i = 0; i < kCount; ++i) {
    threads.emplace_back([&sem] { sem.Acquire(); });
  }
  for (auto& t : threads) {
    t.join();
  }
  // Nothing left on the semaphore
  ASSERT_FALSE(sem.TryAcquire());
  // Keep testing
  sem.Release(2);
  ASSERT_TRUE(sem.TryAcquire());
  sem.Acquire();
  ASSERT_FALSE(sem.TryAcquire());
}

TEST(SemaphoreTest, BinarySemaphore) {
  BinarySemaphore sem{0};
  int kCount = 5;
  std::vector<std::thread> threads;
  for (int i = 0; i < kCount; ++i) {
    threads.emplace_back([&sem] {
      sem.Acquire();
      sem.Release();
    });
  }
  threads.emplace_back([&sem] { sem.Release(); });
  for (auto& t : threads) {
    t.join();
  }
  // Only able to acquire one excess release
  ASSERT_TRUE(sem.TryAcquire());
  ASSERT_FALSE(sem.TryAcquire());
}

TEST(BitFieldsTest, BitFields) {
  // Start by verifying example from BitFields comment
  struct MyState : public BitFields<uint32_t, MyState> {
    // Extra helper declarations and/or field type declarations
  };

  using Field1 = UnsignedBitField<MyState, 16, NoPrevBitField>;
  using Field2 = BoolBitField<MyState, Field1>;
  using Field3 = BoolBitField<MyState, Field2>;
  using Field4 = UnsignedBitField<MyState, 5, Field3>;

  // MyState{} is zero-initialized
  auto state = MyState{}.With<Field1>(42U).With<Field2>(true);
  state.Set<Field4>(3U);
  state.Ref<Field1>() += state.Get<Field4>();

  ASSERT_EQ(state.Get<Field1>(), 45U);
  ASSERT_EQ(state.Get<Field2>(), true);
  ASSERT_EQ(state.Get<Field3>(), false);
  ASSERT_EQ(state.Get<Field4>(), 3U);

  // Misc operators
  auto ref = state.Ref<Field3>();
  auto ref2 = std::move(ref);
  ref2 = true;
  ASSERT_EQ(state.Get<Field3>(), true);

  MyState state2;
  // Basic non-concurrent tests for atomic wrappers
  {
    RelaxedBitFieldsAtomic<MyState> relaxed{state};
    ASSERT_EQ(state, relaxed.LoadRelaxed());
    relaxed.StoreRelaxed(state2);
    ASSERT_EQ(state2, relaxed.LoadRelaxed());
    MyState state3 = relaxed.ExchangeRelaxed(state);
    ASSERT_EQ(state2, state3);
    ASSERT_TRUE(relaxed.CasStrongRelaxed(state, state2));
    while (!relaxed.CasWeakRelaxed(state2, state)) {
    }
    ASSERT_EQ(state2, state3);
    ASSERT_EQ(state, relaxed.LoadRelaxed());

    auto transform1 = Field2::ClearTransform() + Field3::ClearTransform();
    MyState before, after;
    relaxed.ApplyRelaxed(transform1, &before, &after);
    ASSERT_EQ(before, state);
    ASSERT_NE(after, state);
    ASSERT_EQ(after.Get<Field2>(), false);
    ASSERT_EQ(after.Get<Field3>(), false);

    auto transform2 = Field2::SetTransform() + Field3::SetTransform();
    relaxed.ApplyRelaxed(transform2, &before, &after);
    ASSERT_NE(before, state);
    ASSERT_EQ(before.Get<Field2>(), false);
    ASSERT_EQ(before.Get<Field3>(), false);
    ASSERT_EQ(after, state);
  }
  {
    AcqRelBitFieldsAtomic<MyState> acqrel{state};
    ASSERT_EQ(state, acqrel.Load());
    acqrel.Store(state2);
    ASSERT_EQ(state2, acqrel.Load());
    MyState state3 = acqrel.Exchange(state);
    ASSERT_EQ(state2, state3);
    ASSERT_TRUE(acqrel.CasStrong(state, state2));
    while (!acqrel.CasWeak(state2, state)) {
    }
    ASSERT_EQ(state2, state3);
    ASSERT_EQ(state, acqrel.Load());

    auto transform1 = Field2::ClearTransform() + Field3::ClearTransform();
    MyState before, after;
    acqrel.Apply(transform1, &before, &after);
    ASSERT_EQ(before, state);
    ASSERT_NE(after, state);
    ASSERT_EQ(after.Get<Field2>(), false);
    ASSERT_EQ(after.Get<Field3>(), false);

    auto transform2 = Field2::SetTransform() + Field3::SetTransform();
    acqrel.Apply(transform2, &before, &after);
    ASSERT_NE(before, state);
    ASSERT_EQ(before.Get<Field2>(), false);
    ASSERT_EQ(before.Get<Field3>(), false);
    ASSERT_EQ(after, state);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
