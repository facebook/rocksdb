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

While we could in principle avoid writing any C++, it is truly mind-boggling to think about how to lay out C++ structures in `Java`, so to begin with it is easier to write some very simple stubs in C++ which can immediately call into the object-oriented core of RocksDB. We define structures with which to pass parameters to and receive results from the `C`-like method(s) we implement.

#### `C++` Side

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

#### `Java` Side

 - An `FFIMethod` class to advertise a `java.lang.invoke.MethodHandle` for each of our helper stubs

 ```java
  public static MethodHandle Get; // handle which refers to the rocksdb_ffi_get method in C++
  public static MethodHandle ResetOutput; // handle which refers to the rocksdb_ffi_reset_output method in C++
```
 - An `FFILayout` class to describe each of the passed structures (`rocksdb_input_slice` and `rocksdb_output_slice`) in `Java` terms

 ```java
 public static class InputSlice {
  static final GroupLayout Layout = ...
  static final VarHandle Data = ...
  static final VarHandle Size =  ...
 };

 public static class OutputSlice {
  static final GroupLayout Layout = ...
  static final VarHandle Data = ...
  static final VarHandle Size =  ...
 };
 ```

 - The `FFIDB` class, which implements the public Java FFI API methods, making use of `FFIMethod` and `FFILayout` to make the code for each individual method as idiomatic and efficient as possible. This class also contains `java.lang.foreign.MemorySession` and `java.lang.foreign.SegmentAllocator` objects which control the lifetime of native memory sessions and allow us to allocate lifetime-limited native memory which can be written and read by Java, and passed to native methods.

 ```java
 public GetPinnableSlice getPinnableSlice(
      final ColumnFamilyHandle columnFamilyHandle, final byte[] key)
 ```

The flow of any RocksDB FFI API method becomes:
 1. Allocate `MemorySegment`s for `C++` structures using `Layout`s from `FFILayout`
 2. Write to the allocated structures using `VarHandle`s from `FFILayout`
 3. Invoke the native method using the `MethodHandle` from `FFIMethod` and addresses of instantiated `MemorySegment`s, or value types, as parameters
 4. Read the call result and the output parameter(s), again using `VarHandle`s from `FFILayout` to perform the mapping.

For the `getPinnableSlice()` method, on successful return from an invocation of `rocksdb_ffi_get()`, the `OutputSlice` will contain the `data` and `size` fields of a pinnable slice (see below) containing the requested value. A `MemorySegment` referring to the native memory of the pinnable slice can then be constructed, and used to retrieve the value in whatever fashion we choose.

### Pinnable Slices

RocksDB offers core (C++) API methods using the concept of a [`PinnableSlice`](http://rocksdb.org/blog/2017/08/24/pinnableslice.html) to return fetched data values while reducing copies to a minimum. We take advantage of this to base our central `get()` method(s) on `PinnableSlice`s. Methods mirroring the existing `JNI`-based API can then be implemented in pure Java by wrapping the core `get()`.

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

Full benchmark on Ubuntu, with some new benchmarks.
  - Random key order of `get()`
  - black holes for all the data, to make sure nothing is optimized away
```bash
java --enable-preview --enable-native-access=ALL-UNNAMED -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000,100000 -p keySize=128 -p valueSize=4096,32768,130072 -p columnFamilyTestType="no_column_family" -rf csv org.rocksdb.jmh.GetBenchmarks
```

GetBenchmarks.ffiGetOutputSlice                      no_column_family      100000        128         4096  thrpt   25   64291.419 ±  1151.756  ops/s
GetBenchmarks.ffiGetOutputSlice                      no_column_family      100000        128        32768  thrpt   25   35393.140 ±   384.554  ops/s
GetBenchmarks.ffiGetOutputSlice                      no_column_family      100000        128       130072  thrpt   25   16530.851 ±    99.670  ops/s
GetBenchmarks.ffiGetPinnableSlice                    no_column_family        1000        128         4096  thrpt   12  313545.690 ± 16898.814  ops/s
GetBenchmarks.ffiGetPinnableSlice                    no_column_family        1000        128        32768  thrpt   25   41810.122 ±   724.093  ops/s
GetBenchmarks.ffiGetPinnableSlice                    no_column_family        1000        128       130072  thrpt   25   17750.190 ±   108.777  ops/s
GetBenchmarks.ffiGetPinnableSlice                    no_column_family      100000        128         4096  thrpt   25   69277.791 ±  1210.060  ops/s
GetBenchmarks.ffiGetPinnableSlice                    no_column_family      100000        128        32768  thrpt   25   39351.842 ±   620.683  ops/s
GetBenchmarks.ffiGetPinnableSlice                    no_column_family      100000        128       130072  thrpt   25   18732.541 ±    92.323  ops/s
GetBenchmarks.ffiGetRandom                           no_column_family        1000        128         4096  thrpt   25  205367.451 ±  5164.557  ops/s
GetBenchmarks.ffiGetRandom                           no_column_family        1000        128        32768  thrpt   25   34612.870 ±   234.713  ops/s
GetBenchmarks.ffiGetRandom                           no_column_family        1000        128       130072  thrpt   25   12751.282 ±   109.208  ops/s
GetBenchmarks.ffiGetRandom                           no_column_family      100000        128         4096  thrpt   25   56741.818 ±  1300.424  ops/s
GetBenchmarks.ffiGetRandom                           no_column_family      100000        128        32768  thrpt   25   27485.112 ±   171.642  ops/s
GetBenchmarks.ffiGetRandom                           no_column_family      100000        128       130072  thrpt   25   12381.439 ±    98.442  ops/s
GetBenchmarks.ffiPreallocatedGet                     no_column_family        1000        128         4096  thrpt   18  271751.737 ±  9780.256  ops/s
GetBenchmarks.ffiPreallocatedGet                     no_column_family        1000        128        32768  thrpt   25   35637.209 ±   405.370  ops/s
GetBenchmarks.ffiPreallocatedGet                     no_column_family        1000        128       130072  thrpt   25   15145.986 ±    98.809  ops/s
GetBenchmarks.ffiPreallocatedGet                     no_column_family      100000        128         4096  thrpt   25   63117.034 ±  1215.064  ops/s
GetBenchmarks.ffiPreallocatedGet                     no_column_family      100000        128        32768  thrpt   25   34275.593 ±   374.325  ops/s
GetBenchmarks.ffiPreallocatedGet                     no_column_family      100000        128       130072  thrpt   25   15990.968 ±   104.256  ops/s
GetBenchmarks.ffiPreallocatedGetRandom               no_column_family        1000        128         4096  thrpt   20  258443.188 ± 14367.215  ops/s
GetBenchmarks.ffiPreallocatedGetRandom               no_column_family        1000        128        32768  thrpt   25   43861.215 ±   630.738  ops/s
GetBenchmarks.ffiPreallocatedGetRandom               no_column_family        1000        128       130072  thrpt   25   15898.718 ±   102.746  ops/s
GetBenchmarks.ffiPreallocatedGetRandom               no_column_family      100000        128         4096  thrpt   25   60738.330 ±   751.122  ops/s
GetBenchmarks.ffiPreallocatedGetRandom               no_column_family      100000        128        32768  thrpt   25   32985.292 ±   392.325  ops/s
GetBenchmarks.ffiPreallocatedGetRandom               no_column_family      100000        128       130072  thrpt   25   15216.205 ±   110.312  ops/s
GetBenchmarks.get                                    no_column_family        1000        128         4096  thrpt   25  431788.444 ±  3634.249  ops/s
GetBenchmarks.get                                    no_column_family        1000        128        32768  thrpt   25   33664.813 ±   201.615  ops/s
GetBenchmarks.get                                    no_column_family        1000        128       130072  thrpt   25   12799.269 ±    48.901  ops/s
GetBenchmarks.get                                    no_column_family      100000        128         4096  thrpt   25   79331.711 ±  1183.335  ops/s
GetBenchmarks.get                                    no_column_family      100000        128        32768  thrpt   25   32350.676 ±   373.255  ops/s
GetBenchmarks.get                                    no_column_family      100000        128       130072  thrpt   25   13387.083 ±    52.474  ops/s
GetBenchmarks.preallocatedByteBufferGet              no_column_family        1000        128         4096  thrpt   25  630485.342 ±  5278.869  ops/s
GetBenchmarks.preallocatedByteBufferGet              no_column_family        1000        128        32768  thrpt   25   44259.667 ±   391.591  ops/s
GetBenchmarks.preallocatedByteBufferGet              no_column_family        1000        128       130072  thrpt   25   16633.185 ±    90.660  ops/s
GetBenchmarks.preallocatedByteBufferGet              no_column_family      100000        128         4096  thrpt   25   87879.488 ±  1339.351  ops/s
GetBenchmarks.preallocatedByteBufferGet              no_column_family      100000        128        32768  thrpt   25   41779.097 ±   294.259  ops/s
GetBenchmarks.preallocatedByteBufferGet              no_column_family      100000        128       130072  thrpt   25   17471.879 ±    61.928  ops/s
GetBenchmarks.preallocatedByteBufferGetRandom        no_column_family        1000        128         4096  thrpt   25  595411.259 ±  9503.950  ops/s
GetBenchmarks.preallocatedByteBufferGetRandom        no_column_family        1000        128        32768  thrpt   25   54021.417 ±   572.407  ops/s
GetBenchmarks.preallocatedByteBufferGetRandom        no_column_family        1000        128       130072  thrpt   25   17630.588 ±    72.573  ops/s
GetBenchmarks.preallocatedByteBufferGetRandom        no_column_family      100000        128         4096  thrpt   25   82298.320 ±   615.037  ops/s
GetBenchmarks.preallocatedByteBufferGetRandom        no_column_family      100000        128        32768  thrpt   25   39524.029 ±   355.461  ops/s
GetBenchmarks.preallocatedByteBufferGetRandom        no_column_family      100000        128       130072  thrpt   25   16740.203 ±    83.641  ops/s
GetBenchmarks.preallocatedGet                        no_column_family        1000        128         4096  thrpt   25  634987.901 ±  8801.232  ops/s
GetBenchmarks.preallocatedGet                        no_column_family        1000        128        32768  thrpt   25   44281.741 ±   218.671  ops/s
GetBenchmarks.preallocatedGet                        no_column_family        1000        128       130072  thrpt   25   16624.144 ±   100.715  ops/s
GetBenchmarks.preallocatedGet                        no_column_family      100000        128         4096  thrpt   25   90174.327 ±  1027.447  ops/s
GetBenchmarks.preallocatedGet                        no_column_family      100000        128        32768  thrpt   25   41856.085 ±   581.119  ops/s
GetBenchmarks.preallocatedGet                        no_column_family      100000        128       130072  thrpt   25   17399.334 ±   100.069  ops/s

Try again, with all together and profiling
```bash
java --enable-preview --enable-native-access=ALL-UNNAMED -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000,100000 -p keySize=128 -p valueSize=4096,65536 -p columnFamilyTestType="no_column_family" -rf csv org.rocksdb.jmh.GetBenchmarks -prof stack
```

## Appendix
### Processing

Use [`q`](http://harelba.github.io/q/) to select the csv output for analysis and graphing.

```bash
q "select Benchmark,Score from jmhrun.csv where keyCount=1000 and valueSize=4096" -d, -H -C readwrite
```

### Java 19 installation

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