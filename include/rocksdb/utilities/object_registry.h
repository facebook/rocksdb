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

#include "rocksdb/customizable.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class DynamicLibrary;
class Env;
class Logger;
class ObjectLibrary;
class ObjectRegistry;
struct ColumnFamilyOptions;
struct DBOptions;

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
using RegistrarFunc = std::function<void(ObjectLibrary&, const std::string&)>;

class ObjectLibrary : public Customizable {
 public:
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

  // Loads the ObjectLibrary specified by the input value into result
  // Libraries can be "default", "local", or "dynamic".
  // - "Default" says to use the ObjectLibrary::Default()
  // - "local" uses methods from the current executable.  Local libraries have
  //        libraries have the following control parameters:
  //      - "method"  specifies the method in the executable to invoke
  //      - "arg"     specifies the argument to pass to that method
  // - "dynamic" uses methods from a dynamically loaded library.  Dynamic
  //        libraries have the following control parameters:
  //      - "library" specifies the library to load
  //      - "method"  specifies the method in that library to invoke
  //      - "arg"     specifies the argument to pass to that method
  // @param config_options Controls how the library is loaded
  // @param value The name and optional properties describing the library
  //      to load.
  // @param result On success, returns the loaded library
  // @return OK if the library  was successfully loaded.
  // @return not-OK if the load failed.
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value,
                                 std::shared_ptr<ObjectLibrary>* result);
  static const char* Type() { return "ObjectLibrary"; }
  virtual ~ObjectLibrary() {}

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
  size_t GetRegisteredNames(const std::string& type,
                            std::vector<std::string>* names) const;

  // Returns the total number of factories registered for this library.
  // This method returns the sum of all factories registered for all types.
  // @param num_types returns how many unique types are registered.
  size_t GetFactoryCount(size_t* num_types) const;

  // Registers the factory with the library for the pattern.
  // If the pattern matches, the factory may be used to create a new object.
  template <typename T>
  const FactoryFunc<T>& Register(const std::string& pattern,
                                 const FactoryFunc<T>& factory) {
    std::unique_ptr<Entry> entry(new FactoryEntry<T>(pattern, factory));
    AddEntry(T::Type(), entry);
    return factory;
  }

  // Invokes the registrar function with the supplied arg for this library.
  void Register(const RegistrarFunc& registrar, const std::string& arg) {
    registrar(*this, arg);
  }

  // Returns the default ObjectLibrary
  static std::shared_ptr<ObjectLibrary>& Default();

 protected:
  friend class ObjectRegistry;
  // ** FactoryFunctions for this loader, organized by type
  std::unordered_map<std::string, std::vector<std::unique_ptr<Entry>>> entries_;
  // Returns the
  std::string AsPrintableOptions(const std::string& name) const;

 private:
  // Adds the input entry to the list for the given type
  void AddEntry(const std::string& type, std::unique_ptr<Entry>& entry);
};

// The ObjectRegistry is used to register objects that can be created by a
// name/pattern at run-time where the specific implementation of the object may
// not be known in advance.
class ObjectRegistry : public Configurable {
 public:
  static std::shared_ptr<ObjectRegistry> NewInstance();
  // Makes a copy of this registry.
  virtual std::shared_ptr<ObjectRegistry> Clone() const = 0;

  // Creates a new local library, registering the factories in registrar
  // @param registrar   The registration function to invoke
  // @param method      The name of the registration function
  // @param arg         Argument to supply to the registration function.
  virtual void AddLocalLibrary(const RegistrarFunc& registrar,
                               const std::string& method,
                               const std::string& arg) = 0;

  // Creates a new local library, registering the factories in method
  // This method must locate the function in the current executable via
  // LoadLibrary
  // @param env         Environment to use to locate the function address
  // @param method      The name of the registration function
  // @param arg         Argument to supply to the registration function.
  // @return            Success if the library could be found and loaded
  virtual Status AddLocalLibrary(Env* env, const std::string& method,
                                 const std::string& arg) = 0;

  // Loads the types from the named library and method into a new dynamic object
  // library.
  // @param env         Environment to use to locate the library
  // @param library     The name of the library to load
  // @param method      The name of the registration function
  // @param arg         Argument to supply to the registration function.
  // @return            Success if the library/methods could be loaded
  virtual Status AddDynamicLibrary(Env* env, const std::string& library,
                                   const std::string& method,
                                   const std::string& arg) = 0;

  // Loads the method from the named library and method into a new dynamic
  // object library.
  // @param library     The dynamic library to use
  // @param method      The name of the registration function
  // @param arg         Argument to supply to the registration function.
  // @return            Success if the library/methods could be loaded
  virtual Status AddDynamicLibrary(
      const std::shared_ptr<DynamicLibrary>& library, const std::string& method,
      const std::string& arg) = 0;

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
  // Returns NoSupported if the type/target could not be created
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

  // Returns the number of registered types for this registry.
  // If specified (not-null), types is updated to include the names of the
  // registered types.
  virtual size_t GetRegisteredTypes(
      std::unordered_set<std::string>* types) const = 0;

  // Returns the number of registered names for the input type
  // If specified (not-null), names is updated to include the names for the type
  virtual size_t GetRegisteredNames(const std::string& type) const = 0;
  virtual size_t GetRegisteredNames(const std::string& type,
                                    std::vector<std::string>* names) const = 0;

  // Returns the total number of factories registered for this library.
  // This method returns the sum of all factories registered for all types.
  // @param num_types returns how many unique types are registered.
  virtual size_t GetFactoryCount(size_t* num_types) const = 0;

 protected:
  virtual const ObjectLibrary::Entry* FindEntry(
      const std::string& type, const std::string& name) const = 0;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
