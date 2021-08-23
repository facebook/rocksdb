//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "env/unique_id.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <random>

#include "port/port.h"
#include "rocksdb/env.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

namespace {

struct Opts {
  Env* env = Env::Default();
  bool exclude_port_uuid = false;
  bool exclude_env_details = false;
  bool exclude_random_device = false;
};

struct EntropyTrackPortUuid {
  std::array<char, 36> uuid;

  void Populate(const Opts& opts) {
    if (opts.exclude_port_uuid) {
      return;
    }
    std::string s;
    port::GenerateRfcUuid(&s);
    if (s.size() >= uuid.size()) {
      std::copy_n(s.begin(), uuid.size(), uuid.begin());
    }
  }
};

struct EntropyTrackEnvDetails {
  std::array<char, 64> hostname_buf;
  int64_t process_id;
  uint64_t thread_id;
  int64_t unix_time;
  uint64_t nano_time;
  uintptr_t library_ptr;

  void Populate(const Opts& opts) {
    if (opts.exclude_env_details) {
      return;
    }
    opts.env->GetHostName(hostname_buf.data(), hostname_buf.size())
        .PermitUncheckedError();
    process_id = port::GetProcessID();
    thread_id = opts.env->GetThreadID();
    opts.env->GetCurrentTime(&unix_time).PermitUncheckedError();
    nano_time = opts.env->NowNanos();
    // Use Env::Default() to distinguish among RocksDB versions linked into
    // this process.
    library_ptr = reinterpret_cast<uintptr_t>(Env::Default());
  }
};

struct EntropyTrackRandomDevice {
  using RandType = std::random_device::result_type;
  static constexpr size_t kNumRandVals =
      /* generous bits */ 192U / (8U * sizeof(RandType));
  std::array<RandType, kNumRandVals> rand_vals;

  void Populate(const Opts& opts) {
    if (opts.exclude_random_device) {
      return;
    }
    std::random_device r;
    for (auto& val : rand_vals) {
      val = r();
    }
  }
};

struct Entropy {
  EntropyTrackRandomDevice et1;
  EntropyTrackEnvDetails et2;
  EntropyTrackPortUuid et3;

  void Populate(const Opts& opts) {
    et1.Populate(opts);
    et2.Populate(opts);
    et3.Populate(opts);
  }
};

void GenerateRawUniqueIdImpl(uint64_t* a, uint64_t* b, const Opts& opts) {
  Entropy e;
  std::memset(&e, 0, sizeof(e));
  e.Populate(opts);
  Hash2x64(reinterpret_cast<const char*>(&e), sizeof(e), a, b);
}

}  // namespace

void GenerateRawUniqueId(uint64_t* a, uint64_t* b, bool exclude_port_uuid) {
  Opts opts;
  opts.exclude_port_uuid = exclude_port_uuid;
  assert(!opts.exclude_env_details);
  assert(!opts.exclude_random_device);
  GenerateRawUniqueIdImpl(a, b, opts);
}

void TEST_GenerateRawUniqueId(uint64_t* a, uint64_t* b, bool exclude_port_uuid,
                              bool exclude_env_details,
                              bool exclude_random_device) {
  Opts opts;
  opts.exclude_port_uuid = exclude_port_uuid;
  opts.exclude_env_details = exclude_env_details;
  opts.exclude_random_device = exclude_random_device;
  GenerateRawUniqueIdImpl(a, b, opts);
}

}  // namespace ROCKSDB_NAMESPACE
