# Building on Windows

This is a simple step-by-step explanation of how to build RocksDB (or RocksJava) and all of the 3rd-party libraries on Microsoft Windows 11 with Visual Studio 2022.

## Pre-requisites
1. Microsoft Visual Studio 2022 (Community) with "Desktop development with C++" installed
2. Power Shell
3. Java 8 - I used BellSoft Liberica JDK 8 https://bell-sw.com/pages/downloads/
4. [CMake](https://cmake.org/) - I used version 3.31.6 installed from the 64bit MSI installer
5. [Ccache](https://ccache.dev/)

An easy way to install the pre-requisites is to install and use the [Chocolately](https://community.chocolatey.org/) package manager for Windows and simply run the following from cmd.exe:

```cmd
> choco install visualstudio2022-workload-nativedesktop
> choco install choco install liberica8jdkfull
> choco install cmake
> choco install ccache
```

## Steps
Create a directory somewhere on your machine that will be used as a container for both the RocksDB source code and that of its 3rd-party dependencies, typically this is within your home directory. In these example we use `C:\Users\aretter\code`, from hereon in I will just refer to it as `CODE_HOME`; which can be set as an environment variable (in Powershell), i.e. `$Env:CODE_HOME = "C:\Users\aretter\code"`.

There are a number of 3rd party libraries that we need to build from source first that will be linked with RocksDB. 
All of the following are executed from the "**Developer PowerShell for VS2022**":

### Build GFlags
```powershell
cd $Env:CODE_HOME
wget https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.zip -OutFile gflags.zip
Expand-Archive gflags.zip -DestinationPath .
cd gflags-2.2.2
mkdir target
cd target
cmake -G "Visual Studio 17 2022" -A x64 ..
msbuild gflags.sln /p:Configuration=Debug /p:Platform=x64
msbuild gflags.sln /p:Configuration=Release /p:Platform=x64
```

The resultant static library can be found in `$Env:CODE_HOME\gflags-2.2.2\target\lib\Debug\gflags_static_debug.lib` or `$Env:CODE_HOME\gflags-2.2.2\target\lib\Release\gflags_static.lib`.

### Build Snappy
```powershell
cd $Env:CODE_HOME
wget https://github.com/google/snappy/archive/refs/tags/1.1.9.zip -OutFile snappy.zip
Expand-Archive snappy.zip -DestinationPath .
cd snappy-1.1.9
mkdir target
cd target
cmake -G "Visual Studio 17 2022" -A x64 -DCMAKE_GENERATOR_PLATFORM=x64 -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF ..
msbuild Snappy.sln /p:Configuration=Debug /p:Platform=x64
msbuild Snappy.sln /p:Configuration=Release /p:Platform=x64
```

The resultant static library can be found in `$Env:CODE_HOME\snappy-1.1.9\build\Debug\snappy.lib` or `$Env:CODE_HOME\snappy-1.1.9\build\Release\snappy.lib`.

### Build LZ4

```powershell
cd $Env:CODE_HOME
wget https://github.com/lz4/lz4/archive/refs/tags/v1.9.4.zip -OutFile lz4.zip
Expand-Archive lz4.zip -DestinationPath .
cd lz4-1.9.4
mkdir target
cd target
cmake -G "Visual Studio 17 2022" -A x64 -S ../build/cmake 
msbuild LZ4.sln /p:Configuration=Debug /p:Platform=x64
msbuild LZ4.sln /p:Configuration=Release /p:Platform=x64
```

The resultant static library can be found in `$Env:CODE_HOME\lz4-1.9.4\target\Debug` or `$Env:CODE_HOME\lz4-1.9.4\target\Release`.

### Build ZLib
```powershell
cd $Env:CODE_HOME
wget https://github.com/madler/zlib/releases/download/v1.3.1/zlib131.zip -OutFile zlib.zip
Expand-Archive zlib.zip -DestinationPath .
cd zlib-1.3.1
mkdir target
cd target
cmake -G "Visual Studio 17 2022" -A x64 -S ..
msbuild zlib.sln /p:Configuration=Debug /p:Platform=x64
msbuild zlib.sln /p:Configuration=Release /p:Platform=x64
```

The resultant static library can be found in `$Env:CODE_HOME\zlib-1.3.1\target\Debug\zlibstaticd.lib` or `$Env:CODE_HOME\zlib-1.3.1\target\Release\zlibstatic.lib`.

### Build ZStd
```powershell
cd $Env:CODE_HOME
wget https://github.com/facebook/zstd/archive/refs/tags/v1.5.5.zip -OutFile zstd.zip
Expand-Archive zstd.zip -DestinationPath .
cd zstd-1.5.5
mkdir target
cd target
cmake -G "Visual Studio 17 2022" -A x64 -S ../build/cmake 
msbuild zstd.sln /p:Configuration=Debug /p:Platform=x64
msbuild zstd.sln /p:Configuration=Release /p:Platform=x64
```

The resultant static library can be found in `$Env:CODE_HOME\zstd-1.5.5\target\lib\Debug\zstd_static.lib` or `$Env:CODE_HOME\zstd-1.5.5\target\lib\Release\zstd_static.lib`.

### Build RocksDB
```powershell
cd $Env:CODE_HOME
wget https://github.com/facebook/rocksdb/archive/refs/heads/main.zip -OutFile rocksdb.zip
Expand-Archive rocksdb.zip -DestinationPath .
cd rocksdb-main
```
Edit the file `%CODE_HOME%\rocksdb-main\thirdparty.inc` to have these changes:

```
set(GFLAGS_HOME $ENV{THIRDPARTY_HOME}/gflags-2.2.2)
set(GFLAGS_INCLUDE ${GFLAGS_HOME}/target/include)
set(GFLAGS_LIB_DEBUG ${GFLAGS_HOME}/target/lib/Debug/gflags_static.lib)
set(GFLAGS_LIB_RELEASE ${GFLAGS_HOME}/target/lib/Release/gflags_static.lib)

set(SNAPPY_HOME $ENV{THIRDPARTY_HOME}/snappy-1.1.9)
set(SNAPPY_INCLUDE ${SNAPPY_HOME} ${SNAPPY_HOME}/target)
set(SNAPPY_LIB_DEBUG ${SNAPPY_HOME}/target/Debug/snappy.lib)
set(SNAPPY_LIB_RELEASE ${SNAPPY_HOME}/target/Release/snappy.lib)

set(LZ4_HOME $ENV{THIRDPARTY_HOME}/lz4-1.9.4)
set(LZ4_INCLUDE ${LZ4_HOME}/lib)
set(LZ4_LIB_DEBUG ${LZ4_HOME}/target/Debug/lz4.lib)
set(LZ4_LIB_RELEASE ${LZ4_HOME}/target/Release/lz4.lib)

set(ZLIB_HOME $ENV{THIRDPARTY_HOME}/zlib-1.3.1)
set(ZLIB_INCLUDE ${ZLIB_HOME} ${ZLIB_HOME}/target )
set(ZLIB_LIB_DEBUG ${ZLIB_HOME}/target/Debug/zlibstatic.lib)
set(ZLIB_LIB_RELEASE ${ZLIB_HOME}/target/Release/zlibstatic.lib)

set(ZSTD_HOME $ENV{THIRDPARTY_HOME}/zstd-1.5.5)
set(ZSTD_INCLUDE ${ZSTD_HOME}/lib ${ZSTD_HOME}/lib/dictBuilder)
set(ZSTD_LIB_DEBUG ${ZSTD_HOME}/target/lib/Debug/zstd_static.lib)
set(ZSTD_LIB_RELEASE ${ZSTD_HOME}/target/lib/Release/zstd_static.lib)
```

And then finally to compile RocksDB itself:

* **NOTE**: The default CMake build will generate MSBuild project files which include the `/arch:AVX2` flag. If you have this CPU extension instruction set, then the generated binaries will also only work on other CPU's with AVX2. If you want to create a build which has no specific CPU extensions, then you should also pass the `-DPORTABLE=1` flag in the `cmake` arguments below.

* **NOTE**: The build options below include `-DXPRESS=1` which enables Microsoft XPRESS compression. This requires Windows 10 or newer to work reliably and is not backwards compatible with older versions of Windows. At present we build RocksJava releases without XPRESS.

* **NOTE**: Optionally RocksDBJava can also be built with the option `-DWITH_JNI_EXPORT_ROCKSDB=ON`. This causes the RockDB C API to also be exported in the build JNI binaries. Using the option requires the MSVC `dumpbin` tool to be present on the path or same directory as the msvc compiler.

```powershell
mkdir build
cd build
$Env:JAVA_HOME="C:\Program Files\BellSoft\LibericaJDK-8-Full"
$Env:THIRDPARTY_HOME="C:\Users\aretter\code"
$env:Path = $env:JAVA_HOME + ";" + $env:Path #CMake find(JNI) have a bug and doesn't support JAVA_HOME 
cmake -G "Visual Studio 17 2022" -A x64 -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=20 -DJNI=1 -DGFLAGS=1 -DSNAPPY=1 -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF -DSNAPPY_HAVE_BMI2=0 -DLZ4=1 -DZLIB=1 -DZSTD=1 -DXPRESS=1 ..
msbuild rocksdb.sln /p:Configuration=Release /m -property:Platform=x64 -target:rocksdb-shared`;rocksdbjni_test_classes`;rocksdbjni`;rocksdbjni_test_classes
```
Then the resulting libraries can be found in : 
 * Static library : `build/Release/rocksdb.lib`
 * Dynamic library: `rocksdb-shared.dll`
 * Java jar archive: `build/java/rocksdbjni_classes.jar`
 * Java native library: `build/java/Release/rocksdbjni.dll`

## Run RocksDB Java tests
The easiest way to run the tests for RocksDB Java is via `ctest` from `build/java` directory:

```
cd build\java
ctest -C Release -j <number of CPU cores> 
```