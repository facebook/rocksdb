// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/object_registry.h"

#include "logging/logging.h"
#include "rocksdb/customizable.h"
#include "rocksdb/env.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
#ifndef ROCKSDB_LITE
// Looks through the "type" factories for one that matches "name".
// If found, returns the pointer to the Entry matching this name.
// Otherwise, nullptr is returned
const ObjectLibrary::Entry *ObjectLibrary::FindEntry(
    const std::string &type, const std::string &name) const {
  std::unique_lock<std::mutex> lock(mu_);
  auto entries = entries_.find(type);
  if (entries != entries_.end()) {
    for (const auto &entry : entries->second) {
      if (entry->matches(name)) {
        return entry.get();
      }
    }
  }
  return nullptr;
}

void ObjectLibrary::AddEntry(const std::string &type,
                             std::unique_ptr<Entry> &entry) {
  std::unique_lock<std::mutex> lock(mu_);
  auto &entries = entries_[type];
  entries.emplace_back(std::move(entry));
}

size_t ObjectLibrary::GetFactoryCount(size_t *types) const {
  std::unique_lock<std::mutex> lock(mu_);
  *types = entries_.size();
  size_t factories = 0;
  for (const auto &e : entries_) {
    factories += e.second.size();
  }
  return factories;
}

void ObjectLibrary::Dump(Logger *logger) const {
  std::unique_lock<std::mutex> lock(mu_);
  for (const auto &iter : entries_) {
    ROCKS_LOG_HEADER(logger, "    Registered factories for type[%s] ",
                     iter.first.c_str());
    bool printed_one = false;
    for (const auto &e : iter.second) {
      ROCKS_LOG_HEADER(logger, "%c %s", (printed_one) ? ',' : ':',
                       e->Name().c_str());
      printed_one = true;
    }
  }
  ROCKS_LOG_HEADER(logger, "\n");
}

// Returns the Default singleton instance of the ObjectLibrary
// This instance will contain most of the "standard" registered objects
std::shared_ptr<ObjectLibrary> &ObjectLibrary::Default() {
  static std::shared_ptr<ObjectLibrary> instance =
      std::make_shared<ObjectLibrary>("default");
  return instance;
}

std::shared_ptr<ObjectRegistry> ObjectRegistry::Default() {
  static std::shared_ptr<ObjectRegistry> instance(
      new ObjectRegistry(ObjectLibrary::Default()));
  return instance;
}

std::shared_ptr<ObjectRegistry> ObjectRegistry::NewInstance() {
  return std::make_shared<ObjectRegistry>(Default());
}

std::shared_ptr<ObjectRegistry> ObjectRegistry::NewInstance(
    const std::shared_ptr<ObjectRegistry> &parent) {
  return std::make_shared<ObjectRegistry>(parent);
}

// Searches (from back to front) the libraries looking for the
// an entry that matches this pattern.
// Returns the entry if it is found, and nullptr otherwise
const ObjectLibrary::Entry *ObjectRegistry::FindEntry(
    const std::string &type, const std::string &name) const {
  {
    std::unique_lock<std::mutex> lock(library_mutex_);
    for (auto iter = libraries_.crbegin(); iter != libraries_.crend(); ++iter) {
      const auto *entry = iter->get()->FindEntry(type, name);
      if (entry != nullptr) {
        return entry;
      }
    }
  }
  if (parent_ != nullptr) {
    return parent_->FindEntry(type, name);
  } else {
    return nullptr;
  }
}
Status ObjectRegistry::SetManagedObject(
    const std::string &type, const std::string &id,
    const std::shared_ptr<Customizable> &object) {
  std::string object_key = ToManagedObjectKey(type, id);
  std::shared_ptr<Customizable> curr;
  if (parent_ != nullptr) {
    curr = parent_->GetManagedObject(type, id);
  }
  if (curr == nullptr) {
    // We did not find the object in any parent.  Update in the current
    std::unique_lock<std::mutex> lock(objects_mutex_);
    auto iter = managed_objects_.find(object_key);
    if (iter != managed_objects_.end()) {  // The object exists
      curr = iter->second.lock();
      if (curr != nullptr && curr != object) {
        return Status::InvalidArgument("Object already exists: ", object_key);
      } else {
        iter->second = object;
      }
    } else {
      // The object does not exist.  Add it
      managed_objects_[object_key] = object;
    }
  } else if (curr != object) {
    return Status::InvalidArgument("Object already exists: ", object_key);
  }
  return Status::OK();
}

std::shared_ptr<Customizable> ObjectRegistry::GetManagedObject(
    const std::string &type, const std::string &id) const {
  {
    std::unique_lock<std::mutex> lock(objects_mutex_);
    auto iter = managed_objects_.find(ToManagedObjectKey(type, id));
    if (iter != managed_objects_.end()) {
      return iter->second.lock();
    }
  }
  if (parent_ != nullptr) {
    return parent_->GetManagedObject(type, id);
  } else {
    return nullptr;
  }
}

Status ObjectRegistry::ListManagedObjects(
    const std::string &type, const std::string &name,
    std::vector<std::shared_ptr<Customizable>> *results) const {
  {
    std::string key = ToManagedObjectKey(type, name);
    std::unique_lock<std::mutex> lock(objects_mutex_);
    for (auto iter = managed_objects_.lower_bound(key);
         iter != managed_objects_.end() && StartsWith(iter->first, key);
         ++iter) {
      auto shared = iter->second.lock();
      if (shared != nullptr) {
        if (name.empty() || shared->IsInstanceOf(name)) {
          results->emplace_back(shared);
        }
      }
    }
  }
  if (parent_ != nullptr) {
    return parent_->ListManagedObjects(type, name, results);
  } else {
    return Status::OK();
  }
}

void ObjectRegistry::Dump(Logger *logger) const {
  {
    std::unique_lock<std::mutex> lock(library_mutex_);
    for (auto iter = libraries_.crbegin(); iter != libraries_.crend(); ++iter) {
      iter->get()->Dump(logger);
    }
  }
  if (parent_ != nullptr) {
    parent_->Dump(logger);
  }
}

#endif  // ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE
