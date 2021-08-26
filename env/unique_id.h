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

}  // namespace ROCKSDB_NAMESPACE
