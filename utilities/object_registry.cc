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
bool ObjectLibrary::PatternEntry::MatchesPattern(const std::string &name,
                                                 size_t nlen,
                                                 const std::string &target,
                                                 size_t tlen) const {
  if (nlen == tlen) {  // The lengths are the same
    return (name_only_ || patterns_.empty()) && name == target;
  } else if (patterns_.empty()) {
    return nlen == tlen && name == target;
  } else if (tlen < nlen + plength_) {
    // The target is not long enough
    return false;
  } else if (target.compare(0, nlen, name) != 0) {
    return false;  // Target does not start with name
  } else {
    size_t start = nlen;
    auto mode = kMatchExact;
    for (size_t idx = 0; idx < patterns_.size(); ++idx) {
      const auto &pattern = patterns_[idx];
      size_t plen = pattern.first.size();
      if (tlen < start + plen) {
        return false;  // not enough space left
      } else if (mode == kMatchExact) {
        if (target.compare(start, plen, pattern.first) != 0) {
          return false;
        } else {
          start += plen;
        }
      } else {
        auto pos = start + 1;
        if (!pattern.first.empty()) {
          pos = target.find(pattern.first, pos);
        }
        if (pos == std::string::npos) {
          return false;
        } else if (mode == kMatchPattern) {
          start = pos + plen;
        } else if (mode == kMatchNumeric) {
          while (start < pos) {
            if (!std::isdigit(target[start++])) {
              return false;
            }
          }
          start += plen;
        }
      }
      mode = pattern.second;
    }
    if (mode == kMatchExact) {
      return (start == tlen);
    } else if (start >= tlen) {
      return false;
    } else if (mode == kMatchNumeric) {
      while (start < tlen) {
        if (!std::isdigit(target[start++])) {
          return false;
        }
      }
    }
  }
  return true;
}

bool ObjectLibrary::PatternEntry::Matches(const std::string &target) const {
  auto tlen = target.size();
  if (MatchesPattern(name_, nlength_, target, tlen)) {
    return true;
  } else if (!names_.empty()) {
    for (const auto &alt : names_) {
      if (MatchesPattern(alt, alt.size(), target, tlen)) {
        return true;
      }
    }
  }
  return false;
}

size_t ObjectLibrary::GetFactoryCount(size_t *types) const {
  std::unique_lock<std::mutex> lock(mu_);
  *types = factories_.size();
  size_t factories = 0;
  for (const auto &e : factories_) {
    factories += e.second.size();
  }
  return factories;
}

void ObjectLibrary::Dump(Logger *logger) const {
  std::unique_lock<std::mutex> lock(mu_);
  for (const auto &iter : factories_) {
    ROCKS_LOG_HEADER(logger, "    Registered factories for type[%s] ",
                     iter.first.c_str());
    bool printed_one = false;
    for (const auto &e : iter.second) {
      ROCKS_LOG_HEADER(logger, "%c %s", (printed_one) ? ',' : ':', e->Name());
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
