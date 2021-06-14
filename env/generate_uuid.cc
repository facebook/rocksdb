//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "env/generate_uuid.h"

#ifdef OS_WIN
#include <rpc.h>  // for uuid generation
#endif

#include <array>
#include <atomic>
#include <random>

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

RawUuid GenerateRandomRawUuidExpensive(Env* env) {
  // All the data we want to go into the initial random seed. For safety in
  // case of API issues, we have three entropy tracks that should each be
  // sufficient for generating unique ids.
  using RandType = std::random_device::result_type;
  static constexpr size_t kNumRandVals =
      /* generous bits */ 192U / (8U * sizeof(RandType));
  struct Init {
    // Track 1: host and thread identifiers and time
    // TODO? process ID
    uint64_t thread_id = 0;
    std::array<char, 64> hostname_buf{};
    int64_t time = 0;
    uint64_t nano_time = 0;

    // Track 2: random_device
    std::array<RandType, kNumRandVals> rand_vals{};

    // Track 3: platform uuid
    RfcUuid uuid;
  };

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

  // Track 3: platform uuid
  GenerateRfcUuidFromPlatform(&init.uuid).PermitUncheckedError();

  // Mix it all together (nom nom)
  XXH128_hash_t state = XXH128(&init, sizeof(init), /*seed*/ 123U);

  return (Unsigned128{state.high64} << 64) | state.low64;
}

const RawUuid& GetRawUuidForCurrentProcess() {
  // Lazy initialized into static
  // BUG/FIXME: does not change on fork()
  static const RawUuid uuid = GenerateRandomRawUuidExpensive(Env::Default());
  return uuid;
}

RawUuid GenerateRawUuid() {
  static std::atomic<uint64_t> counter{0};

  return AddInCounterA(GetRawUuidForCurrentProcess(), ++counter);
}

Status GenerateRfcUuidFromPlatform(RfcUuid* out) {
#ifdef OS_WIN
  // Use Microsoft API
  UUID uuid;
  UuidCreateSequential(&uuid);

  // Note: Data4 is big-endian
  *out = (Unsigned128{uuid.Data1} << 96) | (Unsigned128{uuid.Data2} << 80) |
         (Unsigned128{uuid.Data3} << 64) |
         EndianSwapValue(DecodeFixed64(&uuid.Data4[0]));
  return Status::OK();
#else
  std::shared_ptr<FileSystem> fs = FileSystem::Default();
  std::string uuid_file = "/proc/sys/kernel/random/uuid";
  std::string uuid_string;
  Status s = ReadFileToString(fs.get(), uuid_file, &uuid_string);
  if (s.ok()) {
    if (uuid_string.size() == 37 && uuid_string.back() == '\n') {
      uuid_string.pop_back();
    }
    s = RfcUuid::Parse(uuid_string, out);
    if (s.ok()) {
      return Status::OK();
    }
  }
  // Failed. Is that expected or unexpected?
  Status exists_status = fs->FileExists(uuid_file, IOOptions(), nullptr);
  if (exists_status.ok()) {
    // Should be supported but failed
    assert(!s.ok());
    return s;
  } else {
    // Seems unsupported
    return Status::NotSupported("No /proc/sys/kernel/random/uuid");
  }
#endif  // OS_WIN
}

RfcUuid GenerateRfcUuid() {
  RfcUuid rv;
  Status s = GenerateRfcUuidFromPlatform(&rv);
  if (s.ok()) {
    return rv;
  } else {
    return RfcUuid::FromRawLoseData(GenerateRawUuid());
  }
}

}  // namespace ROCKSDB_NAMESPACE
