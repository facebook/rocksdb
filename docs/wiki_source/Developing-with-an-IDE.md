This page provides a few details for helping if you choose to develop RocksDB via an IDE (Integrated Development Environment). It should be treated as advice.

* RocksDB ships with two build systems `Make` and `CMake`, some environments will work best with one or the other. The `Make` build system is more fully developed than that of the `CMake` build system, however the `CMake` system does offer the advantage that it can generate configuration files for working with several different IDEs.

* RocksJava has two aspects, the C++ JNI code which links with RocksDB's C++ code, and its Java code. The RocksJava C++ JNI code can be developed with the same IDE that you use for working with RocksDB's C++ code. The Java code of RocksJava can be worked with via any IDE that support Apache Maven. A template `pom.xml` for RocksJava is provided in `java/target/pom.xml.template`, a `pom.xml` can be generated from this and used as an IDE project file by simply running `make rocksdbjavageneratepom`.

## XCode
You can generate an XCode project for RocksDB using CMake:
```
cd rocksdb
mkdir build
cd build

cmake -G Xcode ..
```

You can then open the `build/` folder as an existing project in XCode.

## Microsoft Visual Studio
**NOTE**: These instructions are for the full Studio product and NOT for `Visual Studio Code`.

You can generate a Visual Studio project for RocksDB using CMake:
```
cd rocksdb
mkdir build
cd build

cmake -G "Visual Studio 14 Win64" ..
```

However, for more details on available project options and targets for Visual Studio, see [Building on Windows](https://github.com/facebook/rocksdb/wiki/Building-on-Windows).
