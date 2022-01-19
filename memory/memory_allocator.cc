//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/memory_allocator.h"

#include "memory/jemalloc_nodump_allocator.h"
#include "memory/memkind_kmem_allocator.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "utilities/memory_allocators.h"

namespace ROCKSDB_NAMESPACE {
namespace {
static std::unordered_map<std::string, OptionTypeInfo> ma_wrapper_type_info = {
#ifndef ROCKSDB_LITE
    {"target", OptionTypeInfo::AsCustomSharedPtr<MemoryAllocator>(
                   0, OptionVerificationType::kByName, OptionTypeFlags::kNone)},
#endif  // ROCKSDB_LITE
};

#ifndef ROCKSDB_LITE
static int RegisterBuiltinAllocators(ObjectLibrary& library,
                                     const std::string& /*arg*/) {
  library.AddFactory<MemoryAllocator>(
      DefaultMemoryAllocator::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<MemoryAllocator>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new DefaultMemoryAllocator());
        return guard->get();
      });
  library.AddFactory<MemoryAllocator>(
      CountedMemoryAllocator::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<MemoryAllocator>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new CountedMemoryAllocator(
            std::make_shared<DefaultMemoryAllocator>()));
        return guard->get();
      });
  library.AddFactory<MemoryAllocator>(
      JemallocNodumpAllocator::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<MemoryAllocator>* guard,
         std::string* errmsg) {
        if (JemallocNodumpAllocator::IsSupported(errmsg)) {
          JemallocAllocatorOptions options;
          guard->reset(new JemallocNodumpAllocator(options));
        }
        return guard->get();
      });
  library.AddFactory<MemoryAllocator>(
      MemkindKmemAllocator::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<MemoryAllocator>* guard,
         std::string* errmsg) {
        if (MemkindKmemAllocator::IsSupported(errmsg)) {
          guard->reset(new MemkindKmemAllocator());
        }
        return guard->get();
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}
#endif  // ROCKSDB_LITE
}  // namespace

MemoryAllocatorWrapper::MemoryAllocatorWrapper(
    const std::shared_ptr<MemoryAllocator>& t)
    : target_(t) {
  RegisterOptions("", &target_, &ma_wrapper_type_info);
}

Status MemoryAllocator::CreateFromString(
    const ConfigOptions& options, const std::string& value,
    std::shared_ptr<MemoryAllocator>* result) {
#ifndef ROCKSDB_LITE
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterBuiltinAllocators(*(ObjectLibrary::Default().get()), "");
  });
#else
  if (value == DefaultMemoryAllocator::kClassName()) {
    result->reset(new DefaultMemoryAllocator());
    return Status::OK();
  }
#endif  // ROCKSDB_LITE
  ConfigOptions copy = options;
  copy.invoke_prepare_options = true;
  return LoadManagedObject<MemoryAllocator>(copy, value, result);
}
}  // namespace ROCKSDB_NAMESPACE
