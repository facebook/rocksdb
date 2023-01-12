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

### How JNI Works

`JNI` requires a preprocessing step during build/compilation to generate header files which are linked into by Pure Java code. `C++` implementations of the methods in the headers are implemented. Corresponding `native` methods are declared in Java and the whole is linked together.

Code in the `C++` methods uses what amounts to a `JNI` library to access Java values and objects and to create Java objects in response.

### How FFI Works

`FFI` provides the facility for Java to call existing native (in our case C++) code from Pure Java without having to generate support files during compilation steps. `FFI` does support an external tool (`jextract`) which makes generating common boilerplate easier and less error prone, but we choose to start prototyping without it, in part better to understand how things really work.

`FFI` does its job by providing 2 things
1. A model for allocating, reading and writing native memory and native structures within that memory
2. A model for discovering and calling native methods with parameters consisting of native memory references and/or values

The called `C++` is invoked entirely natively. It does not have to access any Java objects to retrieve data it needs. Therefore existing packages in `C++` and other sufficiently low level languages can be called from `Java` without having to implement stubs in the `C++`.

### Our Approach

While we could in principle avoid writing any C++, it is truly mind-boggling to think about how to lay out C++ structures in `Java`, so to begin with it is easier to write some very simple stubs in C++ which can immediately call into the object-oriented core of RocksDB. We define a few simple structures with which to pass parameters to and receive results from the `C`-like method(s) we implement.

The first method we implement is
```C
extern "C" int rocksdb_ffi_get(ROCKSDB_NAMESPACE::DB* db,
                               ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf,
                               rocksdb_input_slice_t* key,
                               rocksdb_output_slice_t* value)
```
our input structure is
```C
typedef struct rocksdb_input_slice {
  const char* data;
  size_t size;
} rocksdb_input_slice_t;
```
and our output structure is
```C
typedef struct rocksdb_output_slice {
  const char* data;
  size_t size;
  ROCKSDB_NAMESPACE::PinnableSlice* pinnable_slice;
} rocksdb_output_slice_t;
```

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
java --enable-preview --enable-native-access=ALL-UNNAMED -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000,100000 -p keySize=128 -p valueSize=4096,32768 -p columnFamilyTestType="no_column_family" GetBenchmarks.ffiGet GetBenchmarks.ffiPreallocatedGet GetBenchmarks.get GetBenchmarks.preallocatedByteBufferGet
```
Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (valueSize)   Mode  Cnt       Score       Error  Units
GetBenchmarks.ffiGet                           no_column_family        1000        128         4096  thrpt   25  216264.521 ±  5064.949  ops/s
GetBenchmarks.ffiGet                           no_column_family        1000        128        32768  thrpt   25   72842.402 ±   962.917  ops/s
GetBenchmarks.ffiGet                           no_column_family      100000        128         4096  thrpt   25  186828.809 ±  3783.224  ops/s
GetBenchmarks.ffiGet                           no_column_family      100000        128        32768  thrpt   25   70760.762 ±   798.062  ops/s
GetBenchmarks.ffiGetPinnableSlice              no_column_family        1000        128         4096  thrpt   11  323278.311 ± 15350.480  ops/s
GetBenchmarks.ffiGetPinnableSlice              no_column_family        1000        128        32768  thrpt   11  320001.496 ± 12194.165  ops/s
GetBenchmarks.ffiGetPinnableSlice              no_column_family      100000        128         4096  thrpt   20  259207.135 ± 13027.294  ops/s
GetBenchmarks.ffiGetPinnableSlice              no_column_family      100000        128        32768  thrpt   25  243099.411 ±  8058.344  ops/s
GetBenchmarks.ffiPreallocatedGet               no_column_family        1000        128         4096  thrpt   17  277894.024 ±  9749.571  ops/s
GetBenchmarks.ffiPreallocatedGet               no_column_family        1000        128        32768  thrpt   25  128061.008 ±  5055.099  ops/s
GetBenchmarks.ffiPreallocatedGet               no_column_family      100000        128         4096  thrpt   25  220541.564 ±  5584.571  ops/s
GetBenchmarks.ffiPreallocatedGet               no_column_family      100000        128        32768  thrpt   25  121156.753 ±  2959.019  ops/s
GetBenchmarks.get                              no_column_family        1000        128         4096  thrpt   25  425373.590 ± 11322.522  ops/s
GetBenchmarks.get                              no_column_family        1000        128        32768  thrpt   25   33773.970 ±   248.451  ops/s
GetBenchmarks.get                              no_column_family      100000        128         4096  thrpt   25   80270.043 ±  1452.013  ops/s
GetBenchmarks.get                              no_column_family      100000        128        32768  thrpt   25   32793.864 ±   200.596  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family        1000        128         4096  thrpt   25  614322.268 ±  4332.494  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family        1000        128        32768  thrpt   25   44277.221 ±   363.899  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family      100000        128         4096  thrpt   25   90748.674 ±  1539.931  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family      100000        128        32768  thrpt   25   41996.446 ±   196.801  ops/s

Try again with blackholes and random reads
```bash
java --enable-preview --enable-native-access=ALL-UNNAMED -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000,100000 -p keySize=128 -p valueSize=4096,32768 -p columnFamilyTestType="no_column_family" org.rocksdb.jmh.GetBenchmarks
```

## Appendix - Java 19 installation

I followed the instructions to install [Azul](https://docs.azul.com/core/zulu-openjdk/install/debian). Then you still need to pick the right java locally:
```bash
sudo update-alternatives --config java
sudo update-alternatives --config javac
```
And set `JAVA_HOME` appropriately. In my case, `sudo update-alternatives --config java` listed a few JVMs thus:
```
  0            /usr/lib/jvm/bellsoft-java8-full-amd64/bin/java   20803123  auto mode
  1            /usr/lib/jvm/bellsoft-java8-full-amd64/bin/java   20803123  manual mode
  2            /usr/lib/jvm/java-11-openjdk-amd64/bin/java       1111      manual mode
* 3            /usr/lib/jvm/zulu19/bin/java                      2193001   manual mode
```
so I did this:
```bash
export JAVA_HOME=/usr/lib/jvm/zulu19
```

The default maven on ubuntu (3.6.3) is incompatible with Java 19. You will need to install a later [Maven](https://maven.apache.org/install.html), and use it. I used `3.8.7` successfully.