// Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/object_registry.h"

#include "rocksdb/customizable.h"
#include "rocksdb/utilities/regex.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class ObjRegistryTest : public testing::Test {
 public:
  static int num_a, num_b;
};

int ObjRegistryTest::num_a = 0;
int ObjRegistryTest::num_b = 0;
static FactoryFunc<Env> test_reg_a = ObjectLibrary::Default()->AddFactory<Env>(
    ObjectLibrary::PatternEntry("a", false).AddSeparator("://"),
    [](const std::string& /*uri*/, std::unique_ptr<Env>* /*env_guard*/,
       std::string* /* errmsg */) {
      ++ObjRegistryTest::num_a;
      return Env::Default();
    });

class WrappedEnv : public EnvWrapper {
 private:
  std::string id_;

 public:
  WrappedEnv(Env* t, const std::string& id) : EnvWrapper(t), id_(id) {}
  const char* Name() const override { return id_.c_str(); }
  std::string GetId() const override { return id_; }
};
static FactoryFunc<Env> test_reg_b = ObjectLibrary::Default()->AddFactory<Env>(
    ObjectLibrary::PatternEntry("b", false).AddSeparator("://"),
    [](const std::string& uri, std::unique_ptr<Env>* env_guard,
       std::string* /* errmsg */) {
      ++ObjRegistryTest::num_b;
      // Env::Default() is a singleton so we can't grant ownership directly to
      // the caller - we must wrap it first.
      env_guard->reset(new WrappedEnv(Env::Default(), uri));
      return env_guard->get();
    });

TEST_F(ObjRegistryTest, Basics) {
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

TEST_F(ObjRegistryTest, LocalRegistry) {
  std::string msg;
  std::unique_ptr<Env> guard;
  auto registry = ObjectRegistry::NewInstance();
  std::shared_ptr<ObjectLibrary> library =
      std::make_shared<ObjectLibrary>("local");
  registry->AddLibrary(library);
  library->AddFactory<Env>(
      "test-local",
      [](const std::string& /*uri*/, std::unique_ptr<Env>* /*guard */,
         std::string* /* errmsg */) { return Env::Default(); });

  ObjectLibrary::Default()->AddFactory<Env>(
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

TEST_F(ObjRegistryTest, CheckShared) {
  std::shared_ptr<Env> shared;
  std::shared_ptr<ObjectRegistry> registry = ObjectRegistry::NewInstance();
  std::shared_ptr<ObjectLibrary> library =
      std::make_shared<ObjectLibrary>("shared");
  registry->AddLibrary(library);
  library->AddFactory<Env>(
      "unguarded",
      [](const std::string& /*uri*/, std::unique_ptr<Env>* /*guard */,
         std::string* /* errmsg */) { return Env::Default(); });

  library->AddFactory<Env>(
      "guarded", [](const std::string& uri, std::unique_ptr<Env>* guard,
                    std::string* /* errmsg */) {
        guard->reset(new WrappedEnv(Env::Default(), uri));
        return guard->get();
      });

  ASSERT_OK(registry->NewSharedObject<Env>("guarded", &shared));
  ASSERT_NE(shared, nullptr);
  shared.reset();
  ASSERT_NOK(registry->NewSharedObject<Env>("unguarded", &shared));
  ASSERT_EQ(shared, nullptr);
}

TEST_F(ObjRegistryTest, CheckStatic) {
  Env* env = nullptr;
  std::shared_ptr<ObjectRegistry> registry = ObjectRegistry::NewInstance();
  std::shared_ptr<ObjectLibrary> library =
      std::make_shared<ObjectLibrary>("static");
  registry->AddLibrary(library);
  library->AddFactory<Env>(
      "unguarded",
      [](const std::string& /*uri*/, std::unique_ptr<Env>* /*guard */,
         std::string* /* errmsg */) { return Env::Default(); });

  library->AddFactory<Env>(
      "guarded", [](const std::string& uri, std::unique_ptr<Env>* guard,
                    std::string* /* errmsg */) {
        guard->reset(new WrappedEnv(Env::Default(), uri));
        return guard->get();
      });

  ASSERT_NOK(registry->NewStaticObject<Env>("guarded", &env));
  ASSERT_EQ(env, nullptr);
  env = nullptr;
  ASSERT_OK(registry->NewStaticObject<Env>("unguarded", &env));
  ASSERT_NE(env, nullptr);
}

TEST_F(ObjRegistryTest, CheckUnique) {
  std::unique_ptr<Env> unique;
  std::shared_ptr<ObjectRegistry> registry = ObjectRegistry::NewInstance();
  std::shared_ptr<ObjectLibrary> library =
      std::make_shared<ObjectLibrary>("unique");
  registry->AddLibrary(library);
  library->AddFactory<Env>(
      "unguarded",
      [](const std::string& /*uri*/, std::unique_ptr<Env>* /*guard */,
         std::string* /* errmsg */) { return Env::Default(); });

  library->AddFactory<Env>(
      "guarded", [](const std::string& uri, std::unique_ptr<Env>* guard,
                    std::string* /* errmsg */) {
        guard->reset(new WrappedEnv(Env::Default(), uri));
        return guard->get();
      });

  ASSERT_OK(registry->NewUniqueObject<Env>("guarded", &unique));
  ASSERT_NE(unique, nullptr);
  unique.reset();
  ASSERT_NOK(registry->NewUniqueObject<Env>("unguarded", &unique));
  ASSERT_EQ(unique, nullptr);
}

TEST_F(ObjRegistryTest, TestRegistryParents) {
  auto grand = ObjectRegistry::Default();
  auto parent = ObjectRegistry::NewInstance();  // parent with a grandparent
  auto uncle = ObjectRegistry::NewInstance(grand);
  auto child = ObjectRegistry::NewInstance(parent);
  auto cousin = ObjectRegistry::NewInstance(uncle);

  auto library = parent->AddLibrary("parent");
  library->AddFactory<Env>(
      "parent", [](const std::string& uri, std::unique_ptr<Env>* guard,
                   std::string* /* errmsg */) {
        guard->reset(new WrappedEnv(Env::Default(), uri));
        return guard->get();
      });
  library = cousin->AddLibrary("cousin");
  library->AddFactory<Env>(
      "cousin", [](const std::string& uri, std::unique_ptr<Env>* guard,
                   std::string* /* errmsg */) {
        guard->reset(new WrappedEnv(Env::Default(), uri));
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
class MyCustomizable : public Customizable {
 public:
  static const char* Type() { return "MyCustomizable"; }
  MyCustomizable(const char* prefix, const std::string& id) : id_(id) {
    name_ = id_.substr(0, strlen(prefix) - 1);
  }
  const char* Name() const override { return name_.c_str(); }
  std::string GetId() const override { return id_; }

 private:
  std::string id_;
  std::string name_;
};

TEST_F(ObjRegistryTest, TestManagedObjects) {
  auto registry = ObjectRegistry::NewInstance();
  auto m_a1 = std::make_shared<MyCustomizable>("", "A");
  auto m_a2 = std::make_shared<MyCustomizable>("", "A");

  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_OK(registry->SetManagedObject<MyCustomizable>(m_a1));
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), m_a1);

  ASSERT_NOK(registry->SetManagedObject<MyCustomizable>(m_a2));
  ASSERT_OK(registry->SetManagedObject<MyCustomizable>(m_a1));
  m_a1.reset();
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_OK(registry->SetManagedObject<MyCustomizable>(m_a2));
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), m_a2);
}

TEST_F(ObjRegistryTest, TestTwoManagedObjects) {
  auto registry = ObjectRegistry::NewInstance();
  auto m_a = std::make_shared<MyCustomizable>("", "A");
  auto m_b = std::make_shared<MyCustomizable>("", "B");
  std::vector<std::shared_ptr<MyCustomizable>> objects;

  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("B"), nullptr);
  ASSERT_OK(registry->ListManagedObjects(&objects));
  ASSERT_EQ(objects.size(), 0U);
  ASSERT_OK(registry->SetManagedObject(m_a));
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("B"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), m_a);
  ASSERT_OK(registry->ListManagedObjects(&objects));
  ASSERT_EQ(objects.size(), 1U);
  ASSERT_EQ(objects.front(), m_a);

  ASSERT_OK(registry->SetManagedObject(m_b));
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), m_a);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("B"), m_b);
  ASSERT_OK(registry->ListManagedObjects(&objects));
  ASSERT_EQ(objects.size(), 2U);
  ASSERT_OK(registry->ListManagedObjects("A", &objects));
  ASSERT_EQ(objects.size(), 1U);
  ASSERT_EQ(objects.front(), m_a);
  ASSERT_OK(registry->ListManagedObjects("B", &objects));
  ASSERT_EQ(objects.size(), 1U);
  ASSERT_EQ(objects.front(), m_b);
  ASSERT_OK(registry->ListManagedObjects("C", &objects));
  ASSERT_EQ(objects.size(), 0U);

  m_a.reset();
  objects.clear();

  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("B"), m_b);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_OK(registry->ListManagedObjects(&objects));
  ASSERT_EQ(objects.size(), 1U);
  ASSERT_EQ(objects.front(), m_b);

  m_b.reset();
  objects.clear();
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("B"), nullptr);
}

TEST_F(ObjRegistryTest, TestAlternateNames) {
  auto registry = ObjectRegistry::NewInstance();
  auto m_a = std::make_shared<MyCustomizable>("", "A");
  auto m_b = std::make_shared<MyCustomizable>("", "B");
  std::vector<std::shared_ptr<MyCustomizable>> objects;
  // Test no objects exist
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("B"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("TheOne"), nullptr);
  ASSERT_OK(registry->ListManagedObjects(&objects));
  ASSERT_EQ(objects.size(), 0U);

  // Mark "TheOne" to be A
  ASSERT_OK(registry->SetManagedObject("TheOne", m_a));
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("B"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("TheOne"), m_a);
  ASSERT_OK(registry->ListManagedObjects(&objects));
  ASSERT_EQ(objects.size(), 1U);
  ASSERT_EQ(objects.front(), m_a);

  // Try to mark "TheOne" again.
  ASSERT_NOK(registry->SetManagedObject("TheOne", m_b));
  ASSERT_OK(registry->SetManagedObject("TheOne", m_a));

  // Add "A" as a managed object.  Registered 2x
  ASSERT_OK(registry->SetManagedObject(m_a));
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("B"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), m_a);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("TheOne"), m_a);
  ASSERT_OK(registry->ListManagedObjects(&objects));
  ASSERT_EQ(objects.size(), 2U);

  // Delete "A".
  m_a.reset();
  objects.clear();

  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("TheOne"), nullptr);
  ASSERT_OK(registry->SetManagedObject("TheOne", m_b));
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("TheOne"), m_b);
  ASSERT_OK(registry->ListManagedObjects(&objects));
  ASSERT_EQ(objects.size(), 1U);
  ASSERT_EQ(objects.front(), m_b);

  m_b.reset();
  objects.clear();
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("TheOne"), nullptr);
  ASSERT_OK(registry->ListManagedObjects(&objects));
  ASSERT_EQ(objects.size(), 0U);
}

TEST_F(ObjRegistryTest, TestTwoManagedClasses) {
  class MyCustomizable2 : public MyCustomizable {
   public:
    static const char* Type() { return "MyCustomizable2"; }
    MyCustomizable2(const char* prefix, const std::string& id)
        : MyCustomizable(prefix, id) {}
  };

  auto registry = ObjectRegistry::NewInstance();
  auto m_a1 = std::make_shared<MyCustomizable>("", "A");
  auto m_a2 = std::make_shared<MyCustomizable2>("", "A");
  std::vector<std::shared_ptr<MyCustomizable>> obj1s;
  std::vector<std::shared_ptr<MyCustomizable2>> obj2s;

  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable2>("A"), nullptr);

  ASSERT_OK(registry->SetManagedObject(m_a1));
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), m_a1);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable2>("A"), nullptr);

  ASSERT_OK(registry->SetManagedObject(m_a2));
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable2>("A"), m_a2);
  ASSERT_OK(registry->ListManagedObjects(&obj1s));
  ASSERT_OK(registry->ListManagedObjects(&obj2s));
  ASSERT_EQ(obj1s.size(), 1U);
  ASSERT_EQ(obj2s.size(), 1U);
  ASSERT_EQ(obj1s.front(), m_a1);
  ASSERT_EQ(obj2s.front(), m_a2);
  m_a1.reset();
  obj1s.clear();
  obj2s.clear();
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable2>("A"), m_a2);

  m_a2.reset();
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable2>("A"), nullptr);
}

TEST_F(ObjRegistryTest, TestManagedObjectsWithParent) {
  auto base = ObjectRegistry::NewInstance();
  auto registry = ObjectRegistry::NewInstance(base);

  auto m_a = std::make_shared<MyCustomizable>("", "A");
  auto m_b = std::make_shared<MyCustomizable>("", "A");

  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_OK(base->SetManagedObject(m_a));
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), m_a);

  ASSERT_NOK(registry->SetManagedObject(m_b));
  ASSERT_OK(registry->SetManagedObject(m_a));

  m_a.reset();
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), nullptr);
  ASSERT_OK(registry->SetManagedObject(m_b));
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("A"), m_b);
}

TEST_F(ObjRegistryTest, TestGetOrCreateManagedObject) {
  auto registry = ObjectRegistry::NewInstance();
  registry->AddLibrary("test")->AddFactory<MyCustomizable>(
      ObjectLibrary::PatternEntry::AsIndividualId("MC"),
      [](const std::string& uri, std::unique_ptr<MyCustomizable>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MyCustomizable("MC", uri));
        return guard->get();
      });
  std::shared_ptr<MyCustomizable> m_a, m_b, obj;
  std::vector<std::shared_ptr<MyCustomizable>> objs;

  std::unordered_map<std::string, std::string> opt_map;

  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("MC@A#1"), nullptr);
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("MC@B#1"), nullptr);
  ASSERT_OK(registry->GetOrCreateManagedObject("MC@A#1", &m_a));
  ASSERT_OK(registry->GetOrCreateManagedObject("MC@B#1", &m_b));
  ASSERT_EQ(registry->GetManagedObject<MyCustomizable>("MC@A#1"), m_a);
  ASSERT_OK(registry->GetOrCreateManagedObject("MC@A#1", &obj));
  ASSERT_EQ(obj, m_a);
  ASSERT_OK(registry->GetOrCreateManagedObject("MC@B#1", &obj));
  ASSERT_EQ(obj, m_b);
  ASSERT_OK(registry->ListManagedObjects(&objs));
  ASSERT_EQ(objs.size(), 2U);

  objs.clear();
  m_a.reset();
  obj.reset();
  ASSERT_OK(registry->GetOrCreateManagedObject("MC@A#1", &m_a));
  ASSERT_EQ(1, m_a.use_count());
  ASSERT_OK(registry->GetOrCreateManagedObject("MC@B#1", &obj));
  ASSERT_EQ(2, obj.use_count());
}

TEST_F(ObjRegistryTest, TestDeprecatedRegex) {
  Regex regex;
  Env* env = nullptr;
  auto registry = ObjectRegistry::NewInstance();
  if (Regex::Parse("XYZ", &regex).ok()) {
    registry->AddLibrary("XYZ")->Register<Env>(
        "XYZ",
        [](const std::string& /*uri*/, std::unique_ptr<Env>* /*env_guard*/,
           std::string* /* errmsg */) { return Env::Default(); });
    ASSERT_NOK(registry->NewStaticObject<Env>("X", &env));
    ASSERT_OK(registry->NewStaticObject<Env>("XYZ", &env));
    ASSERT_EQ(env, Env::Default());
  }
  if (Regex::Parse("ABC://.*", &regex).ok()) {
    registry->AddLibrary("ABC")->Register<Env>(
        "ABC://.*",
        [](const std::string& /*uri*/, std::unique_ptr<Env>* /*env_guard*/,
           std::string* /* errmsg */) { return Env::Default(); });
    ASSERT_NOK(registry->NewStaticObject<Env>("ABC", &env));
    ASSERT_OK(registry->NewStaticObject<Env>("ABC://123", &env));
    ASSERT_EQ(env, Env::Default());
    ASSERT_OK(registry->NewStaticObject<Env>("ABC://", &env));
    ASSERT_EQ(env, Env::Default());
  }
}

class PatternEntryTest : public testing::Test {};

TEST_F(PatternEntryTest, TestSimpleEntry) {
  ObjectLibrary::PatternEntry entry("ABC", true);

  ASSERT_TRUE(entry.Matches("ABC"));
  ASSERT_FALSE(entry.Matches("AABC"));
  ASSERT_FALSE(entry.Matches("ABCA"));
  ASSERT_FALSE(entry.Matches("AABCA"));
  ASSERT_FALSE(entry.Matches("AB"));
  ASSERT_FALSE(entry.Matches("BC"));
  ASSERT_FALSE(entry.Matches("ABD"));
  ASSERT_FALSE(entry.Matches("BCA"));
}

TEST_F(PatternEntryTest, TestPatternEntry) {
  // Matches A:+
  ObjectLibrary::PatternEntry entry("A", false);
  entry.AddSeparator(":");
  ASSERT_FALSE(entry.Matches("A"));
  ASSERT_FALSE(entry.Matches("AA"));
  ASSERT_FALSE(entry.Matches("AB"));
  ASSERT_FALSE(entry.Matches("B"));
  ASSERT_FALSE(entry.Matches("A:"));
  ASSERT_FALSE(entry.Matches("AA:"));
  ASSERT_FALSE(entry.Matches("AA:B"));
  ASSERT_FALSE(entry.Matches("AA:BB"));
  ASSERT_TRUE(entry.Matches("A:B"));
  ASSERT_TRUE(entry.Matches("A:BB"));

  entry.SetOptional(true);  // Now matches "A" or "A:+"
  ASSERT_TRUE(entry.Matches("A"));
  ASSERT_FALSE(entry.Matches("AA"));
  ASSERT_FALSE(entry.Matches("AB"));
  ASSERT_FALSE(entry.Matches("B"));
  ASSERT_FALSE(entry.Matches("A:"));
  ASSERT_FALSE(entry.Matches("AA:"));
  ASSERT_FALSE(entry.Matches("AA:B"));
  ASSERT_FALSE(entry.Matches("AA:BB"));
  ASSERT_TRUE(entry.Matches("A:B"));
  ASSERT_TRUE(entry.Matches("A:BB"));
}

TEST_F(PatternEntryTest, MatchZeroOrMore) {
  // Matches A:*
  ObjectLibrary::PatternEntry entry("A", false);
  entry.AddSeparator(":", false);
  ASSERT_FALSE(entry.Matches("A"));
  ASSERT_FALSE(entry.Matches("AA"));
  ASSERT_FALSE(entry.Matches("AB"));
  ASSERT_FALSE(entry.Matches("B"));
  ASSERT_TRUE(entry.Matches("A:"));
  ASSERT_FALSE(entry.Matches("B:"));
  ASSERT_FALSE(entry.Matches("B:A"));
  ASSERT_FALSE(entry.Matches("AA:"));
  ASSERT_FALSE(entry.Matches("AA:B"));
  ASSERT_FALSE(entry.Matches("AA:BB"));
  ASSERT_TRUE(entry.Matches("A:B"));
  ASSERT_TRUE(entry.Matches("A:BB"));

  entry.SetOptional(true);  // Now matches "A" or "A:*"
  ASSERT_TRUE(entry.Matches("A"));
  ASSERT_FALSE(entry.Matches("AA"));
  ASSERT_FALSE(entry.Matches("AB"));
  ASSERT_FALSE(entry.Matches("B"));
  ASSERT_TRUE(entry.Matches("A:"));
  ASSERT_FALSE(entry.Matches("B:"));
  ASSERT_FALSE(entry.Matches("B:A"));
  ASSERT_FALSE(entry.Matches("AA:"));
  ASSERT_FALSE(entry.Matches("AA:B"));
  ASSERT_FALSE(entry.Matches("AA:BB"));
  ASSERT_TRUE(entry.Matches("A:B"));
  ASSERT_TRUE(entry.Matches("A:BB"));
}

TEST_F(PatternEntryTest, TestSuffixEntry) {
  ObjectLibrary::PatternEntry entry("AA", true);
  entry.AddSuffix("BB");

  ASSERT_TRUE(entry.Matches("AA"));
  ASSERT_TRUE(entry.Matches("AABB"));

  ASSERT_FALSE(entry.Matches("A"));
  ASSERT_FALSE(entry.Matches("AB"));
  ASSERT_FALSE(entry.Matches("B"));
  ASSERT_FALSE(entry.Matches("BB"));
  ASSERT_FALSE(entry.Matches("ABA"));
  ASSERT_FALSE(entry.Matches("BBAA"));
  ASSERT_FALSE(entry.Matches("AABBA"));
  ASSERT_FALSE(entry.Matches("AABBB"));
}

TEST_F(PatternEntryTest, TestNumericEntry) {
  ObjectLibrary::PatternEntry entry("A", false);
  entry.AddNumber(":");
  ASSERT_FALSE(entry.Matches("A"));
  ASSERT_FALSE(entry.Matches("AA"));
  ASSERT_FALSE(entry.Matches("A:"));
  ASSERT_FALSE(entry.Matches("AA:"));
  ASSERT_TRUE(entry.Matches("A:1"));
  ASSERT_TRUE(entry.Matches("A:11"));
  ASSERT_FALSE(entry.Matches("AA:1"));
  ASSERT_FALSE(entry.Matches("AA:11"));
  ASSERT_FALSE(entry.Matches("A:B"));
  ASSERT_FALSE(entry.Matches("A:1B"));
  ASSERT_FALSE(entry.Matches("A:B1"));
}

TEST_F(PatternEntryTest, TestIndividualIdEntry) {
  auto entry = ObjectLibrary::PatternEntry::AsIndividualId("AA");
  ASSERT_TRUE(entry.Matches("AA"));
  ASSERT_TRUE(entry.Matches("AA@123#456"));
  ASSERT_TRUE(entry.Matches("AA@deadbeef#id"));

  ASSERT_FALSE(entry.Matches("A"));
  ASSERT_FALSE(entry.Matches("AAA"));
  ASSERT_FALSE(entry.Matches("AA@123"));
  ASSERT_FALSE(entry.Matches("AA@123#"));
  ASSERT_FALSE(entry.Matches("AA@#123"));
}

TEST_F(PatternEntryTest, TestTwoNameEntry) {
  ObjectLibrary::PatternEntry entry("A");
  entry.AnotherName("B");
  ASSERT_TRUE(entry.Matches("A"));
  ASSERT_TRUE(entry.Matches("B"));
  ASSERT_FALSE(entry.Matches("AA"));
  ASSERT_FALSE(entry.Matches("BB"));
  ASSERT_FALSE(entry.Matches("AA"));
  ASSERT_FALSE(entry.Matches("BA"));
  ASSERT_FALSE(entry.Matches("AB"));
}

TEST_F(PatternEntryTest, TestTwoPatternEntry) {
  ObjectLibrary::PatternEntry entry("AA", false);
  entry.AddSeparator(":");
  entry.AddSeparator(":");
  ASSERT_FALSE(entry.Matches("AA"));
  ASSERT_FALSE(entry.Matches("AA:"));
  ASSERT_FALSE(entry.Matches("AA::"));
  ASSERT_FALSE(entry.Matches("AA::12"));
  ASSERT_TRUE(entry.Matches("AA:1:2"));
  ASSERT_TRUE(entry.Matches("AA:1:2:"));

  ObjectLibrary::PatternEntry entry2("AA", false);
  entry2.AddSeparator("::");
  entry2.AddSeparator("##");
  ASSERT_FALSE(entry2.Matches("AA"));
  ASSERT_FALSE(entry2.Matches("AA:"));
  ASSERT_FALSE(entry2.Matches("AA::"));
  ASSERT_FALSE(entry2.Matches("AA::#"));
  ASSERT_FALSE(entry2.Matches("AA::##"));
  ASSERT_FALSE(entry2.Matches("AA##1::2"));
  ASSERT_FALSE(entry2.Matches("AA::123##"));
  ASSERT_TRUE(entry2.Matches("AA::1##2"));
  ASSERT_TRUE(entry2.Matches("AA::12##34:"));
  ASSERT_TRUE(entry2.Matches("AA::12::34##56"));
  ASSERT_TRUE(entry2.Matches("AA::12##34::56"));
}

TEST_F(PatternEntryTest, TestTwoNumbersEntry) {
  ObjectLibrary::PatternEntry entry("AA", false);
  entry.AddNumber(":");
  entry.AddNumber(":");
  ASSERT_FALSE(entry.Matches("AA"));
  ASSERT_FALSE(entry.Matches("AA:"));
  ASSERT_FALSE(entry.Matches("AA::"));
  ASSERT_FALSE(entry.Matches("AA::12"));
  ASSERT_FALSE(entry.Matches("AA:1:2:"));
  ASSERT_TRUE(entry.Matches("AA:1:2"));
  ASSERT_TRUE(entry.Matches("AA:12:23456"));

  ObjectLibrary::PatternEntry entry2("AA", false);
  entry2.AddNumber(":");
  entry2.AddNumber("#");
  ASSERT_FALSE(entry2.Matches("AA"));
  ASSERT_FALSE(entry2.Matches("AA:"));
  ASSERT_FALSE(entry2.Matches("AA:#"));
  ASSERT_FALSE(entry2.Matches("AA#:"));
  ASSERT_FALSE(entry2.Matches("AA:123#"));
  ASSERT_FALSE(entry2.Matches("AA:123#B"));
  ASSERT_FALSE(entry2.Matches("AA:B#123"));
  ASSERT_TRUE(entry2.Matches("AA:1#2"));
  ASSERT_FALSE(entry2.Matches("AA:123#23:"));
  ASSERT_FALSE(entry2.Matches("AA::12#234"));
}

TEST_F(PatternEntryTest, TestPatternAndSuffix) {
  ObjectLibrary::PatternEntry entry("AA", false);
  entry.AddSeparator("::");
  entry.AddSuffix("##");
  ASSERT_FALSE(entry.Matches("AA"));
  ASSERT_FALSE(entry.Matches("AA::"));
  ASSERT_FALSE(entry.Matches("AA::##"));
  ASSERT_FALSE(entry.Matches("AB::1##"));
  ASSERT_FALSE(entry.Matches("AB::1##2"));
  ASSERT_FALSE(entry.Matches("AA##1::"));
  ASSERT_TRUE(entry.Matches("AA::1##"));
  ASSERT_FALSE(entry.Matches("AA::1###"));

  ObjectLibrary::PatternEntry entry2("AA", false);
  entry2.AddSuffix("::");
  entry2.AddSeparator("##");
  ASSERT_FALSE(entry2.Matches("AA"));
  ASSERT_FALSE(entry2.Matches("AA::"));
  ASSERT_FALSE(entry2.Matches("AA::##"));
  ASSERT_FALSE(entry2.Matches("AB::1##"));
  ASSERT_FALSE(entry2.Matches("AB::1##2"));
  ASSERT_TRUE(entry2.Matches("AA::##12"));
}

TEST_F(PatternEntryTest, TestTwoNamesAndPattern) {
  ObjectLibrary::PatternEntry entry("AA", true);
  entry.AddSeparator("::");
  entry.AnotherName("BBB");
  ASSERT_TRUE(entry.Matches("AA"));
  ASSERT_TRUE(entry.Matches("AA::1"));
  ASSERT_TRUE(entry.Matches("BBB"));
  ASSERT_TRUE(entry.Matches("BBB::2"));

  ASSERT_FALSE(entry.Matches("AA::"));
  ASSERT_FALSE(entry.Matches("AAA::"));
  ASSERT_FALSE(entry.Matches("BBB::"));

  entry.SetOptional(false);
  ASSERT_FALSE(entry.Matches("AA"));
  ASSERT_FALSE(entry.Matches("BBB"));

  ASSERT_FALSE(entry.Matches("AA::"));
  ASSERT_FALSE(entry.Matches("AAA::"));
  ASSERT_FALSE(entry.Matches("BBB::"));

  ASSERT_TRUE(entry.Matches("AA::1"));
  ASSERT_TRUE(entry.Matches("BBB::2"));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // ROCKSDB_LITE
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as ObjRegistry is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE
