# Java Foreign Function Interface (FFI)

Java19 introduces a new [FFI Preview](https://openjdk.org/jeps/424) which is described as *an API by which Java programs can interoperate with code and data outside of the Java runtime. By efficiently invoking foreign functions (i.e., code outside the JVM), and by safely accessing foreign memory (i.e., memory not managed by the JVM), the API enables Java programs to call native libraries and process native data without the brittleness and danger of JNI*.

If the twin promises of efficiency and safety are realised, then using FFI as a mechanism to support a future RocksDB API may be of significant benefit.

 - Remove the complexity of `JNI` access to `C++ RocksDB`
 - Improve RocksDB Java API performance
 - Reduce the opportunity for coding errors in the RocksDB Java API

 ## Process

 We will
 
  - create a prototype FFI branch
  - update the RocksDB Java build to use Java 19
  - implement `FFI Preview API` versions of core RocksDB features (`get()` and `put()`)
  - Extend current JMH benchmarks to also benchmark the new FFI methods. Usefully, JNI and FFI can co-exist peacefully, so that we can use the existing RocksDB Java to do most of the work except for core `get()` and `put()` implementation.

## Implementation

### Pinnable Slices

RocksDB now offers core (C++) API methods using the concept of a [`PinnableSlice`](http://rocksdb.org/blog/2017/08/24/pinnableslice.html) to return fetched data values while reducing copies to a minimum. We take advantage of this to base our central `get()` method(s) on `PinnableSlice`s. Methods mirroring the existing `JNI`-based API can then be implemented in pure Java by wrapping the core `get()`.

So we implement
```java
public record GetPinnableSlice(Status.Code code, Optional<FFIPinnableSlice> pinnableSlice) {}

public GetPinnableSlice getPinnableSlice(
      final ColumnFamilyHandle columnFamilyHandle, final byte[] key)
```
and we wrap that to provide
```java
public record GetBytes(Status.Code code, byte[] value, long size) {}

public GetBytes get(final ColumnFamilyHandle columnFamilyHandle, final byte[] key)
```

## Benchmark Results

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000 -p keySize=128 -p valueSize=32768 -p columnFamilyTestType="no_column_family" GetBenchmarks.ffiGet GetBenchmarks.preallocatedGet
```

GetBenchmarks.ffiGet                     no_column_family        1000        128        32768  thrpt   25  208982.025 ±  4281.104  ops/s
GetBenchmarks.ffiGetPinnableSlice        no_column_family        1000        128        32768  thrpt   25  631637.064 ± 32032.341  ops/s
GetBenchmarks.preallocatedGet            no_column_family        1000        128        32768  thrpt   25  176491.968 ±  2698.894  ops/s
