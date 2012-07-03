namespace cpp apache.thrift.protocol.neutronium.test

cpp_include "folly/FBString.h"

struct TestFixedSizeStruct1 {
  1: bool a,
  2: bool b,
  3: bool c,
  4: byte d,
}

struct TestFixedSizeStruct2 {
  1: i32 a (neutronium.fixed = 1),
  2: i64 b (neutronium.fixed = 1),
}

struct TestNotFixedSizeStruct2 {
  1: i32 a,
  2: i64 b,
}

struct TestStruct1 {
  1: bool a,
  2: optional bool b,
  3: i32 c (neutronium.fixed = 1),
  4: optional i32 d,
  5: i64 e,
  6: optional i64 f,
  7: string g
}

struct TestStruct2 {
  1: i32 a,
  2: TestStruct1 b,
  3: string c,
  4: list<i32> d,
  5: list<string> e,
  6: map<i32, string> f,
}

struct TestStringEncoding1 {
  1: i32 a,
  2: string b,
  3: string c (neutronium.intern = 1),
  4: string d1 (neutronium.fixed = 10, neutronium.pad = 'X'),
  5: string d2 (neutronium.fixed = 10, neutronium.pad = 'X'),
  6: string e (neutronium.terminator = 'X'),
}

enum Foo {
  HELLO = 1,       // 0
  WORLD = 42,      // 1
  MEOW = 23456,    // 3
  GOODBYE = 12345  // 2
}

struct TestEnumEncoding1 {
  1: Foo a,
  2: Foo b (neutronium.strict = 1),
  3: bool c,
  4: i32 d
}

struct TestEnumEncoding2 {
  1: bool a,
  2: Foo b (neutronium.strict = 1),
  4: optional Foo d (neutronium.strict = 1),
  5: Foo e (neutronium.strict = 1),
}

struct BenchStruct1 {
  1: required i32 a,
}

struct BenchStruct2 {
  1: optional i32 a,
  4: optional string b,
  5: optional list<i32> c
}

