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

#include "rocksdb/env.h"
#include "util/uuid_internal.h"

namespace ROCKSDB_NAMESPACE {

// Generates a random 128-bit identifier that is universally unique. The
// possibility of duplicates is generally lower than the possibility of
// computing the wrong thing due to random hardware errors. May not be
// strong against an adversary (e.g. for cryptography).
RawUuid GenerateRawUuid(Env *env);

// Generates an RFC 4122 UUID. On some platforms this will use an alternate
// generation mechanism than the random generation of GenerateRawUuid(),
// but the typical implementation is this:
//   return RfcUuid::FromRawLoseData(GenerateRawUuid());
// May not be strong against an adversary (e.g. for cryptography).
RfcUuid GenerateRfcUuid(Env *env);

// Like GenerateRawUuid but for RocksMuid (~103 bits entropy)
RocksMuid GenerateMuid(Env *env);

}  // namespace ROCKSDB_NAMESPACE
