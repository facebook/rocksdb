//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is for functions that extract novel entropy or sources of
// uniqueness from the execution environment. (By contrast, random.h is
// for algorithmic pseudorandomness.)
//
// These functions could eventually migrate to public APIs, such as in Env.

#pragma once

#include "util/uuid.h"

namespace ROCKSDB_NAMESPACE {

class Env;

// Generates a random new 128-bit identifier that is universally unique for
// each call. This implementation is expensive, so GenerateRawUuid is usually
// preferred when uniqueness is required but not full entropy for each call.
//
// May not be strong against an adversary (not validated for cryptography),
// though a best effort is made to gather and combine full entropy from
// several sources.
RawUuid GenerateRandomRawUuidExpensive(Env* env);

// A random 128-bit identifier that is universally unique to this process
// hosting RocksDB. This is mostly intended for use by below APIs (for
// generating any number of uuids).
//
// May not be strong against an adversary (not validated for cryptography).
//
// KNOWN BUG: stays the same after fork()
const RawUuid& GetRawUuidForCurrentProcess();

// Generates a new 128-bit identifier that is universally unique for each
// call. This function attempts to involve all bits from one call to the
// next but each result is linearly related to the previous result.
//
// More detail:
// The period is only guaranteed to 2**64, which is fine in practice (> 100
// years at 4ghz). Only the top n+1 bits are needed to guarantee no collision
// in a sequence of 2**n calls. The functions RfcUuid::FromRawLoseData and
// RocksMuid::FromRawLoseData ensure the full 64 bit implicit counter is
// preserved, thus guaranteeing no collisions.
//
// Probably not be strong against an adversary (not validated for cryptography).
RawUuid GenerateRawUuid();

// Generates an RFC 4122 UUID using an OS API or similar if available.
// Returns NotSupported if not supported or some other non-OK status in
// case of an unexpected failure.
//
// May not be strong against an adversary (not validated for cryptography).
Status GenerateRfcUuidFromPlatform(RfcUuid* out);

// Generates an RFC 4122 UUID, preferring GenerateRfcUuidFromPlatform but
// when that fails falls back on RfcUuid::FromRawLoseData(GenerateRawUuid(env))
//
// Probably not be strong against an adversary (not validated for cryptography).
RfcUuid GenerateRfcUuid();

// Like GenerateRawUuid but for RocksMuid (~103 bits entropy). See the
// called functions for details.
inline RocksMuid GenerateMuid() {
  return RocksMuid::FromRawLoseData(GenerateRawUuid());
}

}  // namespace ROCKSDB_NAMESPACE
