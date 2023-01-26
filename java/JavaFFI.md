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

While we could in principle avoid writing any C++, C++ objects and classes are not easily supported, so to begin with it is easier to write some very simple `C`-like methods/stubs in C++ which can immediately call into the object-oriented core of RocksDB. We define structures with which to pass parameters to and receive results from the `C`-like method(s) we implement.

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

 We implement an `FFIMethod` class to advertise a `java.lang.invoke.MethodHandle` for each of our helper stubs

 ```java
  public static MethodHandle GetPinnable; // handle which refers to the rocksdb_ffi_get_pinnable method in C++
  public static MethodHandle ResetPinnable; // handle which refers to the rocksdb_ffi_reset_pinnable method in C++
```
We also implement an `FFILayout` class to describe each of the passed structures (`rocksdb_input_slice` , `rocksdb_pinnable_slice` and `rocksdb_output_slice`) in `Java` terms

 ```java
 public static class InputSlice {
  static final GroupLayout Layout = ...
  static final VarHandle Data = ...
  static final VarHandle Size =  ...
 };

 public static class PinnableSlice {
  static final GroupLayout Layout = ...
  static final VarHandle Data = ...
  static final VarHandle Size =  ...
  static final VarHandle IsPinned =  ...
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

For the `getPinnableSlice()` method, on successful return from an invocation of `rocksdb_ffi_get()`, the `PinnableSlice` will contain the `data` and `size` fields of a pinnable slice (see below) containing the requested value. A `MemorySegment` referring to the native memory of the pinnable slice can then be constructed, and used to retrieve the value in whatever fashion we choose.

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

We extended existing RocksDB Java JNI benchmarks with new benchmarks based on FFI. Full benchmark run on Ubuntu, including new benchmarks.

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=100000 -p keySize=128 -p valueSize=4096,65536 -p columnFamilyTestType="no_column_family" -rf csv org.rocksdb.jmh.GetBenchmarks
```

[Results](./jmh/plot/jmh-result.csv)
![Plot](./jmh/plot/jmh-result-select.png)

### Discussion

We have plotted the performance (more operations is better) of a selection of benchmarks,
```bash
q "select Benchmark,Score,Error from ./plot/jmh-result-edit.csv where keyCount=100000 and valueSize=65536" -d, -H
```

 - benchmarks with `ffi` in their name are implemented using the FFI mechanisms
 - other benchmarks are previously implemented JNI-based benchmarks

 If we compare like with like, for basic `get()`
 - `ffiGet` vs `get`
 - The JNI version is faster

 For preallocated `get()` where the result buffer is supplied to the method, avoiding an allocation of a fresh result buffer on each call (and the test recycles its result buffers), then the same small difference persists
 - `ffiPreallocatedGet` vs `preallocatedGet`
 - `preallocatedGet` is faster

 We implemented some methods where the key for the `get()` is randomized, so that any ordering effects can be accounted for. The same differences persisted.

 The FFI interface gives us a natural way to expose RocksDB's [pinnable slice](http://rocksdb.org/blog/2017/08/24/pinnableslice.html) mechanism. When we provide a benchmark which accesses the raw `PinnableSlice` API, as expected this is the fastest method of any; however we are not comparing like with like:
 - `ffiGetPinnableSlice` returns a handle to the RocksDB memory containing the slice, and presents that as an FFI `MemorySegment`. No copying of the memory in the segment occurs.

 In fact, we implement the new FFI-based `get()` method using the new FFI-based `getPinnableSlice()` method, and copying out the result. So the `ffiGet` and `ffiPreallocatedGet` benchmarks use this mechanism underneath.

 In an effort to discover whether using the Java APIs to copy from the pinnable slice backed `MemorySegment` was a problem, we implemented a separate `ffiGetOutputSlice` benchmark which copies the result into a (Java allocated native memory) segment at the C++ side.
 - `ffiGetOutputSlice` while faster than `ffiPreallocatedGet` is still slower than `preallocatedGet`.

 It's reasonable to expect that the extra FFI call to C++ to release the pinned slice has some cost, but a null FFI method call is extremely fast. And as `ffiGetOutputSlice` is still not as fast, we remain unclear where the performance is going.

## Appendix

### Running

Edit this to give you what you want...
```bash
java --enable-preview --enable-native-access=ALL-UNNAMED -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=100000 -p keySize=128 -p valueSize=4096,65536 -p columnFamilyTestType="no_column_family" -rf csv org.rocksdb.jmh.GetBenchmarks -wi 1 -to 1m -i 1
```

### Processing

Use [`q`](http://harelba.github.io/q/) to select the csv output for analysis and graphing.

 - Note that we edited the column headings for easier processing

```bash
q "select Benchmark,Score,Error from ./plot/jmh-result.csv where keyCount=100000 and valueSize=65536" -d, -H -C readwrite
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