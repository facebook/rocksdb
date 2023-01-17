//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is for functions that generate unique identifiers by
// (at least in part) by extracting novel entropy or sources of uniqueness
// from the execution environment. (By contrast, random.h is for algorithmic
// pseudorandomness.)
//
// These functions could eventually migrate to public APIs, such as in Env.

#pragma once

#include <atomic>
#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Generates a new 128-bit identifier that is universally unique
// (with high probability) for each call. The result is split into
// two 64-bit pieces. This function has NOT been validated for use in
// cryptography.
//
// This is used in generating DB session IDs and by Env::GenerateUniqueId
// (used for DB IDENTITY) if the platform does not provide a generator of
// RFC 4122 UUIDs or fails somehow. (Set exclude_port_uuid=true if this
// function is used as a fallback for GenerateRfcUuid, because no need
// trying it again.)
void GenerateRawUniqueId(uint64_t* a, uint64_t* b,
                         bool exclude_port_uuid = false);

#ifndef NDEBUG
// A version of above with options for challenge testing
void TEST_GenerateRawUniqueId(uint64_t* a, uint64_t* b, bool exclude_port_uuid,
                              bool exclude_env_details,
                              bool exclude_random_device);
#endif

// Generates globally unique ids with lower probability of any collisions
// vs. each unique id being independently random (GenerateRawUniqueId).
// We call this "semi-structured" because between different
// SemiStructuredUniqueIdGen objects, the IDs are separated by random
// intervals (unstructured), but within a single SemiStructuredUniqueIdGen
// object, the generated IDs are trivially related (structured). See
// https://github.com/pdillinger/unique_id for how this improves probability
// of no collision. In short, if we have n SemiStructuredUniqueIdGen
// objects each generating m IDs, the first collision is expected at
// around n = sqrt(2^128 / m), equivalently n * sqrt(m) = 2^64,
// rather than n * m = 2^64 for fully random IDs.
class SemiStructuredUniqueIdGen {
 public:
  // Initializes with random starting state (from GenerateRawUniqueId)
  SemiStructuredUniqueIdGen() { Reset(); }
  // Re-initializes, but not thread safe
  void Reset();

  // Assuming no fork(), `lower` is guaranteed unique from one call
  // to the next (thread safe).
  void GenerateNext(uint64_t* upper, uint64_t* lower);

 private:
  uint64_t base_upper_;
  uint64_t base_lower_;
  std::atomic<uint64_t> counter_;
  int64_t saved_process_id_;
};

}  // namespace ROCKSDB_NAMESPACE
