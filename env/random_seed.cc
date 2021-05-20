//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "env/random_seed.h"

#ifdef OS_WIN
#include <rpc.h>  // for uuid generation
#endif

#include <array>
#include <random>

#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

namespace {

struct RawUuidHelper {
  // All the data we want to go into the initial random seed. For safety in
  // case of API issues, we have two entropy tracks that should each be
  // sufficient for generating unique ids.
  struct Init {
    // Track 1: host and thread identifiers and time
    // TODO? process ID
    uint64_t thread_id = 0;
    std::array<char, 64> hostname_buf{};
    int64_t time = 0;
    uint64_t nano_time = 0;

    // Track 2: random_device
    using RandType = std::random_device::result_type;
    static constexpr size_t kNumRandVals =
        /* generous bits */ 192U / (8U * sizeof(RandType));
    std::array<RandType, kNumRandVals> rand_vals{};
  };

  RawUuidHelper(Env* env) {
    Init init{};

    // Track 1: host and thread identifiers and time
    init.thread_id = env->GetThreadID();
    env->GetHostName(init.hostname_buf.data(), init.hostname_buf.size())
        .PermitUncheckedError();
    env->GetCurrentTime(&init.time).PermitUncheckedError();
    init.nano_time = env->NowNanos();

    // Track 2: random_device
    std::random_device r;
    for (auto& val : init.rand_vals) {
      val = r();
    }

    // Mix it all together (nom nom)
    state = XXH128(&init, sizeof(init), /*seed*/ 123U);
  }

  XXH128_hash_t state;
};

}  // namespace

RawUuid GenerateRawUuid(Env* env) {
  // Track 128-bit state for each thread, initialized with a random seed.
  // Random seeding details in RawUuidHelper.
  thread_local RawUuidHelper helper(env);
  XXH128_hash_t& state = helper.state;

  // Use 128-bit hash function to re-mix from previous state and NowNanos().
  // Even if we get the same NowNanos() many times, we get good pseudorandom
  // results. But in reality, NowNanos() will often vary, injecting some new
  // randomness into our pool for (pseudo)random generation.
  // Note: we aren't too concerned about correlated collisions, where if
  // two machines both generate uuid A then there's a heightened chance
  // (e.g. due to same NowNanos()) that both also generate uuid B. We
  // consider two collisions to be similarly "bad" as one collision, so
  // we focus on quality initial entropy to avert a first collision.
  state = XXH128(&state, sizeof(XXH128_hash_t), /*seed*/ env->NowNanos());
  // Produce result from updated state.
  RawUuid rv;
  rv.lower64 = state.low64;
  rv.upper64 = state.high64;
  // Ensure non-empty
  rv.lower64 += ((rv.lower64 | rv.upper64) == 0);
  assert(!rv.IsEmpty());
  return rv;
}

RfcUuid GenerateRfcUuid(Env* env) {
#ifdef OS_WIN
  // Use Microsoft API
  UUID uuid;
  UuidCreateSequential(&uuid);

  // And convert to ours
  RfcUuid rv;
  rv.upper64 =
      (uint64_t{uuid.Data1} << 16) | uuid.Data2 | (uint64_t{uuid.Data3} << 48);
  // Data4 is big-endian
  rv.lower64 = EndianSwapValue(DecodeFixed64(&uuid.Data4[0]));
  return rv;
#else
  // Note: An old implementation of Env::GenerateUniqueId on posix used
  // /proc/sys/kernel/random/uuid when/where available, but that should not
  // be necessary given this implementation, which is faster.
  return RfcUuid::FromRawLoseData(GenerateRawUuid(env));
#endif  // OS_WIN
}

RocksMuid GenerateMuid(Env *env) {
  return RocksMuid::FromRawLoseData(GenerateRawUuid(env));
}

}  // namespace ROCKSDB_NAMESPACE
