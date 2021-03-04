// Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <functional>
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class DynamicLibrary;
class Logger;
class ObjectLibrary;

// Returns a new T when called with a string. Populates the std::unique_ptr
// argument if granting ownership to caller.
template <typename T>
using FactoryFunc =
    std::function<T*(const std::string&, std::unique_ptr<T>*, std::string*)>;

// The signature of the function for loading factories
// into an object library.  This method is expected to register
// factory functions in the supplied ObjectLibrary.
// @param library   The library to load factories into.
// @param arg       Argument to the library loader
// @return          The number of factories registered by this function
using RegistrarFunc = std::function<int(ObjectLibrary&, const std::string&)>;

class ObjectLibrary {
 public:
  ObjectLibrary(const std::string& id) : id_(id) {}

  const std::string& GetId() const { return id_; }

  // Dump the contents of the library to the logger
  void Dump(Logger* logger) const;
  // Base class for an Entry in the Registry.
  class Entry {
   public:
    virtual ~Entry() {}
    Entry(const std::string& name) : name_(std::move(name)) {}

    // Checks to see if the target matches this entry
    virtual bool matches(const std::string& target) const {
      return name_ == target;
    }
    const std::string& Name() const { return name_; }

   private:
    const std::string name_;  // The name of the Entry
  };                          // End class Entry

  // An Entry containing a FactoryFunc for creating new Objects
  template <typename T>
  class FactoryEntry : public Entry {
   public:
    FactoryEntry(const std::string& name, FactoryFunc<T> f)
        : Entry(name), pattern_(std::move(name)), factory_(std::move(f)) {}
    ~FactoryEntry() override {}
    bool matches(const std::string& target) const override {
      return std::regex_match(target, pattern_);
    }
    // Creates a new T object.
    T* NewFactoryObject(const std::string& target, std::unique_ptr<T>* guard,
                        std::string* msg) const {
      return factory_(target, guard, msg);
    }

   private:
    std::regex pattern_;  // The pattern for this entry
    FactoryFunc<T> factory_;
  };  // End class FactoryEntry
 public:
  // Finds the entry matching the input name and type
  const Entry* FindEntry(const std::string& type,
                         const std::string& name) const;

  // Returns the number of registered types for this library.
  // If specified (not-null), types is updated to include the names of the
  // registered types.
  size_t GetRegisteredTypes(std::unordered_set<std::string>* types) const;

  // Returns the number of registered names for the input type
  // If specified (not-null), names is updated to include the names for the type
  size_t GetNamesForType(const std::string& type,
                         std::vector<std::string>* names) const;

  // Returns the total number of factories registered for this library.
  // This method returns the sum of all factories registered for all types.
  // @param num_types returns how many unique types are registered.
  size_t GetFactoryCount(size_t* num_types) const;

  // Invokes the registrar function with the supplied arg for this library.
  int Register(const RegistrarFunc& registrar, const std::string& arg) {
    return registrar(*this, arg);
  }

  // Registers the factory with the library for the pattern.
  // If the pattern matches, the factory may be used to create a new object.
  template <typename T>
  const FactoryFunc<T>& Register(const std::string& pattern,
                                 const FactoryFunc<T>& factory) {
    std::unique_ptr<Entry> entry(new FactoryEntry<T>(pattern, factory));
    AddEntry(T::Type(), entry);
    return factory;
  }
  // Returns the default ObjectLibrary
  static std::shared_ptr<ObjectLibrary>& Default();

 private:
  // Adds the input entry to the list for the given type
  void AddEntry(const std::string& type, std::unique_ptr<Entry>& entry);

  // The name of this library
  std::string id_;
  // ** FactoryFunctions for this loader, organized by type
  std::unordered_map<std::string, std::vector<std::unique_ptr<Entry>>> entries_;
};

// The ObjectRegistry is used to register objects that can be created by a
// name/pattern at run-time where the specific implementation of the object may
// not be known in advance.
class ObjectRegistry {
 public:
  virtual ~ObjectRegistry() {}

  // Returns the default ObjectRegistry
  static std::shared_ptr<ObjectRegistry>& Default();

  // Returns a new ObjectRegistry with a parent of Default()
  static std::shared_ptr<ObjectRegistry> NewInstance();

  // Returns a new ObjectRegistry with the specified parent
  static std::shared_ptr<ObjectRegistry> NewInstance(
      const std::shared_ptr<ObjectRegistry>& parent);

  // Creates a new T using the factory function that was registered with a
  // pattern that matches the provided "target" string according to
  // std::regex_match.
  //
  // If no registered functions match, returns nullptr. If multiple functions
  // match, the factory function used is unspecified.
  //
  // Populates res_guard with result pointer if caller is granted ownership.
  template <typename T>
  T* NewObject(const std::string& target, std::unique_ptr<T>* guard,
               std::string* errmsg) {
    guard->reset();
    const auto* basic = FindEntry(T::Type(), target);
    if (basic != nullptr) {
      const auto* factory =
          static_cast<const ObjectLibrary::FactoryEntry<T>*>(basic);
      return factory->NewFactoryObject(target, guard, errmsg);
    } else {
      *errmsg = std::string("Could not load ") + T::Type();
      return nullptr;
    }
  }

  // Creates a new unique T using the input factory functions.
  // Returns OK if a new unique T was successfully created
  // Returns NotSupported if the type/target could not be created
  // Returns InvalidArgument if the factory return an unguarded object
  //                      (meaning it cannot be managed by a unique ptr)
  template <typename T>
  Status NewUniqueObject(const std::string& target,
                         std::unique_ptr<T>* result) {
    std::string errmsg;
    T* ptr = NewObject(target, result, &errmsg);
    if (ptr == nullptr) {
      return Status::NotSupported(errmsg, target);
    } else if (*result) {
      return Status::OK();
    } else {
      return Status::InvalidArgument(std::string("Cannot make a unique ") +
                                         T::Type() + " from unguarded one ",
                                     target);
    }
  }

  // Creates a new shared T using the input factory functions.
  // Returns OK if a new shared T was successfully created
  // Returns NotSupported if the type/target could not be created
  // Returns InvalidArgument if the factory return an unguarded object
  //                      (meaning it cannot be managed by a shared ptr)
  template <typename T>
  Status NewSharedObject(const std::string& target,
                         std::shared_ptr<T>* result) {
    std::string errmsg;
    std::unique_ptr<T> guard;
    T* ptr = NewObject(target, &guard, &errmsg);
    if (ptr == nullptr) {
      return Status::NotSupported(errmsg, target);
    } else if (guard) {
      result->reset(guard.release());
      return Status::OK();
    } else {
      return Status::InvalidArgument(std::string("Cannot make a shared ") +
                                         T::Type() + " from unguarded one ",
                                     target);
    }
  }

  // Creates a new static T using the input factory functions.
  // Returns OK if a new static T was successfully created
  // Returns NotSupported if the type/target could not be created
  // Returns InvalidArgument if the factory return a guarded object
  //                      (meaning it is managed by a unique ptr)
  template <typename T>
  Status NewStaticObject(const std::string& target, T** result) {
    std::string errmsg;
    std::unique_ptr<T> guard;
    T* ptr = NewObject(target, &guard, &errmsg);
    if (ptr == nullptr) {
      return Status::NotSupported(errmsg, target);
    } else if (guard.get()) {
      return Status::InvalidArgument(std::string("Cannot make a static ") +
                                         T::Type() + " from a guarded one ",
                                     target);
    } else {
      *result = ptr;
      return Status::OK();
    }
  }

  // Dump the contents of the registry to the logger
  virtual void Dump(Logger* logger) const = 0;

  // Creates a new ObjectLibrary within this ObjectRegistry and invokes
  // registrar(arg) to register the appropriate factories in the new library.
  // @param id The name for this library
  // Returns the number of factories registered.
  virtual std::shared_ptr<ObjectLibrary> AddProgramLibrary(
      const std::string& id, const RegistrarFunc& registrar,
      const std::string& arg) = 0;

  // Returns the number of registered types for this registry.
  // If specified (not-null), types is updated to include the names of the
  // registered types.
  virtual size_t GetRegisteredTypes(
      std::unordered_set<std::string>* types) const = 0;
  // Returns the number of registered names for the input type
  // If specified (not-null), names is updated to include the names for the type
  virtual size_t GetNamesForType(
      const std::string& type,
      std::vector<std::string>* names = nullptr) const = 0;

  // Returns the total number of factories registered for this library.
  // This method returns the sum of all factories registered for all types.
  // @param num_types returns how many unique types are registered.
  virtual size_t GetFactoryCount(size_t* num_types) const = 0;

 protected:
  friend class ObjectRegistryImpl;

  virtual const ObjectLibrary::Entry* FindEntry(
      const std::string& type, const std::string& name) const = 0;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
