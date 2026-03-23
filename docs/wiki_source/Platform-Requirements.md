We detail the minimum requirements for compiling RocksDB and optionally RocksJava, and running RocksJava binaries supplied via Maven Central.

# Compiling
## All platforms
* **Java**: [OpenJDK 1.7+](https://adoptopenjdk.net/) (required only for RocksJava)
* Tools:
    * curl (recommended; required only for RocksJava)
* Libraries:
    * [gflags](https://gflags.github.io/gflags/) 2.0+ (required for testing and benchmark code)
    * [zlib](http://www.zlib.net/) 1.2.8+ (optional)
    * [bzip2](http://www.bzip.org/) 1.0.6+ (optional)
    * [lz4](https://github.com/lz4/lz4) r131+ (optional)
    * [snappy](http://google.github.io/snappy/) 1.1.3+ (optional)
    * [zstandard](http://www.zstd.net) 0.5.1+ (optional)

## Linux
* **Architecture**: x86 / x86_64 / arm64 / ppc64le / s390x
* **C/C++ Compiler**: GCC 4.8+ or Clang
* Tools:
    * GNU Make or [CMake](https://cmake.org/download/) 3.14.5+

## macOS
* **Architecture**: x86_64 / arm64 (Apple Silicon)
* **OS**: macOS 10.12+
* **C/C++ Compiler**: Apple XCode Clang
* Tools:
    * GNU Make or [CMake](https://cmake.org/download/) 3.14.5+

## Windows
* **Architecture**: x86_64
* **OS**: Windows 7+
* **C/C++ Compiler**: Microsoft Visual Studio 2015+
* **Java**: [OpenJDK 1.7+](https://java.oracle.com/) (required only for RocksJava)
* Tools:
    * [CMake](https://cmake.org/download/) 3.14.5+

# RocksJava Binaries
The minimum requirements for running the official RocksJava binaries from [Maven Central](https://search.maven.org/search?q=g:org.rocksdb%20a:rocksdbjni).

For all platforms the native component of the binaries is statically linked, and so requires very little. The Java component requires [OpenJDK 1.7+](https://adoptopenjdk.net/).

The binaries are built using Docker containers on real hardware, you can find our Docker build containers here: https://github.com/evolvedbinary/docker-rocksjava

## Linux

For Linux we provide binaries built for either GNU Lib C, or Musl based platforms (since RocksJava 6.5.2).

| Architecture | glibc version (minimum) | muslc version (minimum) |
|--------------|-------------------------|-------------------------|
| x86          | 2.12                    | 1.1.16                  |
| x86_64       | 2.12                    | 1.1.16                  |
| aarch64      | 2.17                    | 1.1.16                  |
| ppc64le      | 2.17                    | 1.1.16                  |
| s390x (z10)  | 2.17                    | 1.1.16                  |

## macOS
* **Architecture**: x86_64
* **OS**: macOS 10.12+

## Windows

The Windows binaries are built on Windows Server 2012 with Visual Studio 2015.

* **Architecture**: x86_64
* **OS**: Windows 7+
* **libc**: Microsoft Visual C++ 2015 Redistributable (x64) - 14.0.24215
