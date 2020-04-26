// Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/object_registry.h"

#include "rocksdb/convenience.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class EnvRegistryTest : public testing::Test {
 public:
  static int num_a, num_b;
};

int EnvRegistryTest::num_a = 0;
int EnvRegistryTest::num_b = 0;

class CustomEnv : public EnvWrapper {
 public:
  CustomEnv(Env* t) : EnvWrapper(t) {}
  const char* Name() const override { return "CustomEnv"; }
};

static FactoryFunc<Env> test_reg_a = ObjectLibrary::Default()->Register<Env>(
    "a://.*",
    [](const std::string& /*uri*/, std::unique_ptr<Env>* /*env_guard*/,
       std::string* /* errmsg */) {
      ++EnvRegistryTest::num_a;
      return Env::Default();
    });

static FactoryFunc<Env> test_reg_b = ObjectLibrary::Default()->Register<Env>(
    "b://.*", [](const std::string& /*uri*/, std::unique_ptr<Env>* env_guard,
                 std::string* /* errmsg */) {
      ++EnvRegistryTest::num_b;
      // Env::Default() is a singleton so we can't grant ownership directly to
      // the caller - we must wrap it first.
      env_guard->reset(new CustomEnv(Env::Default()));
      return env_guard->get();
    });

extern "C" {
void RegisterTestEnvFactory(ObjectLibrary& library, const std::string& arg) {
  library.Register<Env>(
      arg, [](const std::string& /*uri*/, std::unique_ptr<Env>* /*guard */,
              std::string* /* errmsg */) { return Env::Default(); });
}
}

TEST_F(EnvRegistryTest, Basics) {
  std::string msg;
  std::unique_ptr<Env> env_guard;
  auto registry = ObjectRegistry::NewInstance();
  auto res = registry->NewObject<Env>("a://test", &env_guard, &msg);
  ASSERT_NE(res, nullptr);
  ASSERT_EQ(env_guard, nullptr);
  ASSERT_EQ(1, num_a);
  ASSERT_EQ(0, num_b);

  res = registry->NewObject<Env>("b://test", &env_guard, &msg);
  ASSERT_NE(res, nullptr);
  ASSERT_NE(env_guard, nullptr);
  ASSERT_EQ(1, num_a);
  ASSERT_EQ(1, num_b);

  res = registry->NewObject<Env>("c://test", &env_guard, &msg);
  ASSERT_EQ(res, nullptr);
  ASSERT_EQ(env_guard, nullptr);
  ASSERT_EQ(1, num_a);
  ASSERT_EQ(1, num_b);
}

TEST_F(EnvRegistryTest, LocalRegistry) {
  std::string msg;
  std::unique_ptr<Env> guard;
  auto registry = ObjectRegistry::NewInstance();
  registry->AddLocalLibrary(RegisterTestEnvFactory, "RegistryTestEnvFactory",
                            "test-local");
  ObjectLibrary::Default()->Register(RegisterTestEnvFactory, "test-global");

  ASSERT_EQ(
      ObjectRegistry::NewInstance()->NewObject<Env>("test-local", &guard, &msg),
      nullptr);
  ASSERT_NE(
      ObjectRegistry::NewInstance()->NewObject("test-global", &guard, &msg),
      nullptr);
  ASSERT_NE(registry->NewObject<Env>("test-local", &guard, &msg), nullptr);
  ASSERT_NE(registry->NewObject<Env>("test-global", &guard, &msg), nullptr);
}

TEST_F(EnvRegistryTest, DynamicRegistry) {
  std::shared_ptr<DynamicLibrary> library;
  Status s = Env::Default()->LoadLibrary("object_registry_test", "", &library);
  if (s.ok()) {
    std::string msg;
    std::unique_ptr<Env> guard;
    auto registry = ObjectRegistry::NewInstance();
    ASSERT_EQ(registry->NewObject<Env>("test-dynamic", &guard, &msg), nullptr);
    ASSERT_OK(registry->AddDynamicLibrary(library, "RegisterTestEnvFactory",
                                          "test-dynamic"));
    ASSERT_NE(registry->NewObject("test-dynamic", &guard, &msg), nullptr);
    ASSERT_EQ(
        ObjectRegistry::NewInstance()->NewObject("test-dynamic", &guard, &msg),
        nullptr);
  }
}

extern "C" {
void RegisterTestUnguarded(ObjectLibrary& library, const std::string& /*arg*/) {
  library.Register<Env>(
      "unguarded",
      [](const std::string& /*uri*/, std::unique_ptr<Env>* /*guard */,
         std::string* /* errmsg */) { return Env::Default(); });

  library.Register<Env>(
      "guarded", [](const std::string& /*uri*/, std::unique_ptr<Env>* guard,
                    std::string* /* errmsg */) {
        guard->reset(new CustomEnv(Env::Default()));
        return guard->get();
      });
}
}  // extern "C"

TEST_F(EnvRegistryTest, CheckShared) {
  std::shared_ptr<Env> shared;
  std::shared_ptr<ObjectRegistry> registry = ObjectRegistry::NewInstance();
  registry->AddLocalLibrary(RegisterTestUnguarded, "RegisterTestUnguarded", "");

  ASSERT_OK(registry->NewSharedObject<Env>("guarded", &shared));
  ASSERT_NE(shared, nullptr);
  shared.reset();
  ASSERT_NOK(registry->NewSharedObject<Env>("unguarded", &shared));
  ASSERT_EQ(shared, nullptr);
}

TEST_F(EnvRegistryTest, CheckStatic) {
  Env* env = nullptr;
  std::shared_ptr<ObjectRegistry> registry = ObjectRegistry::NewInstance();
  registry->AddLocalLibrary(RegisterTestUnguarded, "RegisterTestUnguarded", "");
  ASSERT_NOK(registry->NewStaticObject<Env>("guarded", &env));
  ASSERT_EQ(env, nullptr);
  env = nullptr;
  ASSERT_OK(registry->NewStaticObject<Env>("unguarded", &env));
  ASSERT_NE(env, nullptr);
}

TEST_F(EnvRegistryTest, CheckUnique) {
  std::unique_ptr<Env> unique;
  std::shared_ptr<ObjectRegistry> registry = ObjectRegistry::NewInstance();
  registry->AddLocalLibrary(RegisterTestUnguarded, "RegisterTestUnguarded", "");

  ASSERT_OK(registry->NewUniqueObject<Env>("guarded", &unique));
  ASSERT_NE(unique, nullptr);
  unique.reset();
  ASSERT_NOK(registry->NewUniqueObject<Env>("unguarded", &unique));
  ASSERT_EQ(unique, nullptr);
}

class TestDummy {
 public:
  static const char* Type() { return "Dummy"; }
  static void RegisterDummy(ObjectLibrary& library, const std::string& name) {
    library.Register<TestDummy>(
        name,
        [](const std::string& /*uri*/, std::unique_ptr<TestDummy>* /*guard */,
           std::string* /* errmsg */) { return nullptr; });
  }
};

TEST_F(EnvRegistryTest, TestCountObjects) {
  std::string msg;
  std::unique_ptr<Env> guard;
  auto registry = ObjectRegistry::NewInstance();
  size_t base_types, base_count;
  size_t curr_types, curr_count;
  base_count = registry->GetFactoryCount(&base_types);
  ASSERT_EQ(registry->GetRegisteredNames(TestDummy::Type()), 0);
  registry->AddLocalLibrary(TestDummy::RegisterDummy, "RegisterDummy", "noop");
  curr_count = registry->GetFactoryCount(&curr_types);
  ASSERT_EQ(curr_types, base_types + 1);
  ASSERT_EQ(curr_count, base_count + 1);
  std::vector<std::string> names;
  ASSERT_EQ(registry->GetRegisteredNames(TestDummy::Type(), &names), 1);
  ASSERT_EQ(names[0], "noop");
  ASSERT_EQ(registry->GetRegisteredNames(TestDummy::Type(), &names), 1);
  registry->AddLocalLibrary(TestDummy::RegisterDummy, "RegisterDummy", "null");
  registry->AddLocalLibrary(RegisterTestUnguarded, "RegisterTestUnguarded", "");
  curr_count = registry->GetFactoryCount(&curr_types);
  ASSERT_EQ(curr_types, base_types + 1);
  ASSERT_EQ(curr_count, base_count + 4);
  ASSERT_EQ(registry->GetRegisteredNames(TestDummy::Type()), 2);
}

TEST_F(EnvRegistryTest, TestLibrarySerialization) {
  ConfigOptions config_options;
  ConfigOptions other_options;
  Status s = config_options.registry->AddLocalLibrary(
      Env::Default(), "RegisterTestUnguarded", "");
  if (s.ok()) {
    std::string str;
    size_t base_types, base_count;
    size_t copy_types, copy_count;
    base_count = config_options.registry->GetFactoryCount(&base_types);
    copy_count = other_options.registry->GetFactoryCount(&copy_types);
    ASSERT_OK(config_options.registry->GetOptionString(config_options, &str));
    ASSERT_OK(other_options.registry->ConfigureFromString(other_options, str));
    copy_count = other_options.registry->GetFactoryCount(&copy_types);
    ASSERT_EQ(base_count, copy_count);
    ASSERT_EQ(base_types, copy_types);
  }
}

TEST_F(EnvRegistryTest, TestLibraryPaths) {
  ConfigOptions config_options;
  std::shared_ptr<ObjectRegistry> base = ObjectRegistry::NewInstance();
  std::shared_ptr<ObjectRegistry> copy = ObjectRegistry::NewInstance();

  std::string opts_str;
  auto* paths = base->GetOptions<std::string>("LibraryPaths");
  ASSERT_NE(paths, nullptr);
  ASSERT_OK(base->ConfigureFromString(config_options, "paths=a:b:c"));
  ASSERT_EQ(*paths, "a:b:c");
  ASSERT_OK(base->GetOptionString(config_options, &opts_str));
  ASSERT_OK(copy->ConfigureFromString(config_options, opts_str));
  ASSERT_TRUE(base->Matches(config_options, copy.get()));

  ASSERT_OK(base->ConfigureFromString(config_options, "paths={a:b:c}"));
  ASSERT_EQ(*paths, "a:b:c");
  ASSERT_OK(base->GetOptionString(config_options, &opts_str));
  ASSERT_OK(copy->ConfigureFromString(config_options, opts_str));
  ASSERT_TRUE(base->Matches(config_options, copy.get()));

  ASSERT_OK(base->ConfigureFromString(config_options, "paths={a;b;c}"));
  ASSERT_EQ(*paths, "a;b;c");
  ASSERT_OK(base->GetOptionString(config_options, &opts_str));
  ASSERT_OK(copy->ConfigureFromString(config_options, opts_str));
  ASSERT_TRUE(base->Matches(config_options, copy.get()));
}

TEST_F(EnvRegistryTest, InvalidLibrary) {
  ConfigOptions config_options;
  ASSERT_NOK(config_options.registry->ConfigureFromString(config_options,
                                                          "id=dynamic"));
  ASSERT_NOK(config_options.registry->ConfigureFromString(
      config_options, "id=dynamic;library=no such name"));
  ASSERT_NOK(config_options.registry->ConfigureFromString(config_options,
                                                          "id=invalid"));
  ASSERT_NOK(
      config_options.registry->ConfigureFromString(config_options, "id=local"));
  ASSERT_NOK(config_options.registry->ConfigureFromString(
      config_options, "id=local;method=no such method"));
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // ROCKSDB_LITE
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as EnvRegistry is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE
