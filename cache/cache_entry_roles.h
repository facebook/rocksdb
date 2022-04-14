//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <type_traits>

#include "rocksdb/cache.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

extern std::array<std::string, kNumCacheEntryRoles>
    kCacheEntryRoleToCamelString;
extern std::array<std::string, kNumCacheEntryRoles>
    kCacheEntryRoleToHyphenString;

// To associate cache entries with their role, we use a hack on the
// existing Cache interface. Because the deleter of an entry can authenticate
// the code origin of an entry, we can elaborate the choice of deleter to
// also encode role information, without inferring false role information
// from entries not choosing to encode a role.
//
// The rest of this file is for handling mappings between deleters and
// roles.

// To infer a role from a deleter, the deleter must be registered. This
// can be done "manually" with this function. This function is thread-safe,
// and the registration mappings go into private but static storage. (Note
// that DeleterFn is a function pointer, not std::function. Registrations
// should not be too many.)
void RegisterCacheDeleterRole(Cache::DeleterFn fn, CacheEntryRole role);

// Gets a copy of the registered deleter -> role mappings. This is the only
// function for reading the mappings made with RegisterCacheDeleterRole.
// Why only this interface for reading?
// * This function has to be thread safe, which could incur substantial
// overhead. We should not pay this overhead for every deleter look-up.
// * This is suitable for preparing for batch operations, like with
// CacheEntryStatsCollector.
// * The number of mappings should be sufficiently small (dozens).
UnorderedMap<Cache::DeleterFn, CacheEntryRole> CopyCacheDeleterRoleMap();

// ************************************************************** //
// An automatic registration infrastructure. This enables code
// to simply ask for a deleter associated with a particular type
// and role, and registration is automatic. In a sense, this is
// a small dependency injection infrastructure, because linking
// in new deleter instantiations is essentially sufficient for
// making stats collection (using CopyCacheDeleterRoleMap) aware
// of them.

namespace cache_entry_roles_detail {

template <typename T, CacheEntryRole R>
struct RegisteredDeleter {
  RegisteredDeleter() { RegisterCacheDeleterRole(Delete, R); }

  // These have global linkage to help ensure compiler optimizations do not
  // break uniqueness for each <T,R>
  static void Delete(const Slice& /* key */, void* value) {
    // Supports T == Something[], unlike delete operator
    std::default_delete<T>()(
        static_cast<typename std::remove_extent<T>::type*>(value));
  }
};

template <CacheEntryRole R>
struct RegisteredNoopDeleter {
  RegisteredNoopDeleter() { RegisterCacheDeleterRole(Delete, R); }

  static void Delete(const Slice& /* key */, void* /* value */) {
    // Here was `assert(value == nullptr);` but we can also put pointers
    // to static data in Cache, for testing at least.
  }
};

}  // namespace cache_entry_roles_detail

// Get an automatically registered deleter for value type T and role R.
// Based on C++ semantics, registration is invoked exactly once in a
// thread-safe way on first call to this function, for each <T, R>.
template <typename T, CacheEntryRole R>
Cache::DeleterFn GetCacheEntryDeleterForRole() {
  static cache_entry_roles_detail::RegisteredDeleter<T, R> reg;
  return reg.Delete;
}

// Get an automatically registered no-op deleter (value should be nullptr)
// and associated with role R. This is used for Cache "reservation" entries
// such as for WriteBufferManager.
template <CacheEntryRole R>
Cache::DeleterFn GetNoopDeleterForRole() {
  static cache_entry_roles_detail::RegisteredNoopDeleter<R> reg;
  return reg.Delete;
}

}  // namespace ROCKSDB_NAMESPACE
