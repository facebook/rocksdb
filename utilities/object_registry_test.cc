// Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/object_registry.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class EnvRegistryTest : public testing::Test {
 public:
  static int num_a, num_b;
};

int EnvRegistryTest::num_a = 0;
int EnvRegistryTest::num_b = 0;
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
      env_guard->reset(new EnvWrapper(Env::Default()));
      return env_guard->get();
    });

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
  std::shared_ptr<ObjectLibrary> library =
      std::make_shared<ObjectLibrary>("local");
  registry->AddLibrary(library);
  library->Register<Env>(
      "test-local",
      [](const std::string& /*uri*/, std::unique_ptr<Env>* /*guard */,
         std::string* /* errmsg */) { return Env::Default(); });

  ObjectLibrary::Default()->Register<Env>(
      "test-global",
      [](const std::string& /*uri*/, std::unique_ptr<Env>* /*guard */,
         std::string* /* errmsg */) { return Env::Default(); });

  ASSERT_EQ(
      ObjectRegistry::NewInstance()->NewObject<Env>("test-local", &guard, &msg),
      nullptr);
  ASSERT_NE(
      ObjectRegistry::NewInstance()->NewObject("test-global", &guard, &msg),
      nullptr);
  ASSERT_NE(registry->NewObject<Env>("test-local", &guard, &msg), nullptr);
  ASSERT_NE(registry->NewObject<Env>("test-global", &guard, &msg), nullptr);
}

TEST_F(EnvRegistryTest, CheckShared) {
  std::shared_ptr<Env> shared;
  std::shared_ptr<ObjectRegistry> registry = ObjectRegistry::NewInstance();
  std::shared_ptr<ObjectLibrary> library =
      std::make_shared<ObjectLibrary>("shared");
  registry->AddLibrary(library);
  library->Register<Env>(
      "unguarded",
      [](const std::string& /*uri*/, std::unique_ptr<Env>* /*guard */,
         std::string* /* errmsg */) { return Env::Default(); });

  library->Register<Env>(
      "guarded", [](const std::string& /*uri*/, std::unique_ptr<Env>* guard,
                    std::string* /* errmsg */) {
        guard->reset(new EnvWrapper(Env::Default()));
        return guard->get();
      });

  ASSERT_OK(registry->NewSharedObject<Env>("guarded", &shared));
  ASSERT_NE(shared, nullptr);
  shared.reset();
  ASSERT_NOK(registry->NewSharedObject<Env>("unguarded", &shared));
  ASSERT_EQ(shared, nullptr);
}

TEST_F(EnvRegistryTest, CheckStatic) {
  Env* env = nullptr;
  std::shared_ptr<ObjectRegistry> registry = ObjectRegistry::NewInstance();
  std::shared_ptr<ObjectLibrary> library =
      std::make_shared<ObjectLibrary>("static");
  registry->AddLibrary(library);
  library->Register<Env>(
      "unguarded",
      [](const std::string& /*uri*/, std::unique_ptr<Env>* /*guard */,
         std::string* /* errmsg */) { return Env::Default(); });

  library->Register<Env>(
      "guarded", [](const std::string& /*uri*/, std::unique_ptr<Env>* guard,
                    std::string* /* errmsg */) {
        guard->reset(new EnvWrapper(Env::Default()));
        return guard->get();
      });

  ASSERT_NOK(registry->NewStaticObject<Env>("guarded", &env));
  ASSERT_EQ(env, nullptr);
  env = nullptr;
  ASSERT_OK(registry->NewStaticObject<Env>("unguarded", &env));
  ASSERT_NE(env, nullptr);
}

TEST_F(EnvRegistryTest, CheckUnique) {
  std::unique_ptr<Env> unique;
  std::shared_ptr<ObjectRegistry> registry = ObjectRegistry::NewInstance();
  std::shared_ptr<ObjectLibrary> library =
      std::make_shared<ObjectLibrary>("unique");
  registry->AddLibrary(library);
  library->Register<Env>(
      "unguarded",
      [](const std::string& /*uri*/, std::unique_ptr<Env>* /*guard */,
         std::string* /* errmsg */) { return Env::Default(); });

  library->Register<Env>(
      "guarded", [](const std::string& /*uri*/, std::unique_ptr<Env>* guard,
                    std::string* /* errmsg */) {
        guard->reset(new EnvWrapper(Env::Default()));
        return guard->get();
      });

  ASSERT_OK(registry->NewUniqueObject<Env>("guarded", &unique));
  ASSERT_NE(unique, nullptr);
  unique.reset();
  ASSERT_NOK(registry->NewUniqueObject<Env>("unguarded", &unique));
  ASSERT_EQ(unique, nullptr);
}

TEST_F(EnvRegistryTest, TestRegistryParents) {
  auto grand = ObjectRegistry::Default();
  auto parent = ObjectRegistry::NewInstance();  // parent with a grandparent
  auto uncle = ObjectRegistry::NewInstance(grand);
  auto child = ObjectRegistry::NewInstance(parent);
  auto cousin = ObjectRegistry::NewInstance(uncle);

  auto library = parent->AddLibrary("parent");
  library->Register<Env>(
      "parent", [](const std::string& /*uri*/, std::unique_ptr<Env>* guard,
                   std::string* /* errmsg */) {
        guard->reset(new EnvWrapper(Env::Default()));
        return guard->get();
      });
  library = cousin->AddLibrary("cousin");
  library->Register<Env>(
      "cousin", [](const std::string& /*uri*/, std::unique_ptr<Env>* guard,
                   std::string* /* errmsg */) {
        guard->reset(new EnvWrapper(Env::Default()));
        return guard->get();
      });

  std::unique_ptr<Env> guard;
  std::string msg;

  // a:://* is registered in Default, so they should all workd
  ASSERT_NE(parent->NewObject<Env>("a://test", &guard, &msg), nullptr);
  ASSERT_NE(child->NewObject<Env>("a://test", &guard, &msg), nullptr);
  ASSERT_NE(uncle->NewObject<Env>("a://test", &guard, &msg), nullptr);
  ASSERT_NE(cousin->NewObject<Env>("a://test", &guard, &msg), nullptr);

  // The parent env is only registered for parent, not uncle,
  // So parent and child should return success and uncle and cousin should fail
  ASSERT_OK(parent->NewUniqueObject<Env>("parent", &guard));
  ASSERT_OK(child->NewUniqueObject<Env>("parent", &guard));
  ASSERT_NOK(uncle->NewUniqueObject<Env>("parent", &guard));
  ASSERT_NOK(cousin->NewUniqueObject<Env>("parent", &guard));

  // The cousin is only registered in the cousin, so all of the others should
  // fail
  ASSERT_OK(cousin->NewUniqueObject<Env>("cousin", &guard));
  ASSERT_NOK(parent->NewUniqueObject<Env>("cousin", &guard));
  ASSERT_NOK(child->NewUniqueObject<Env>("cousin", &guard));
  ASSERT_NOK(uncle->NewUniqueObject<Env>("cousin", &guard));
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
