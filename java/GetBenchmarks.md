# RocksDB Get Performance Benchmarks

```
make clean jclean
DEBUG_LEVEL=0 make -j12 rocksdbjava
(cd java/target; cp rocksdbjni-7.9.0-osx.jar rocksdbjni-7.9.0-SNAPSHOT-osx.jar)
mvn install:install-file -Dfile=./java/target/rocksdbjni-7.9.0-SNAPSHOT-osx.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=7.9.0-SNAPSHOT -Dpackaging=jar
pushd java/jmh
mvn clean package
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000 -p keySize=128 -p valueSize=32768 -p columnFamilyTestType="no_column_family" GetBenchmarks
```

## Before
Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (multiGetSize)  (valueSize)   Mode  Cnt          Score        Error  Units
GetBenchmarks.get                              no_column_family        1000        128             N/A        32768  thrpt   25      43496.578 ±   5743.090  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family        1000        128             N/A        32768  thrpt   25      70765.578 ±    697.548  ops/s
GetBenchmarks.preallocatedGet                  no_column_family        1000        128             N/A        32768  thrpt   25      69883.554 ±    944.184  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family        1000        128            1000        32768  thrpt   25  140265650.899 ± 243554.856  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family        1000        128           10000        32768  thrpt   25  140418577.108 ± 127203.078  ops/s

## After fixing byte[] (.get and .preallocatedGet)

Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (multiGetSize)  (valueSize)   Mode  Cnt          Score        Error  Units
GetBenchmarks.get                              no_column_family        1000        128             N/A        32768  thrpt   25     149207.681 ±   2261.671  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family        1000        128             N/A        32768  thrpt   25      68920.489 ±   1574.664  ops/s
GetBenchmarks.preallocatedGet                  no_column_family        1000        128             N/A        32768  thrpt   25     177399.022 ±   2107.375  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family        1000        128            1000        32768  thrpt   25  140517887.107 ± 374116.853  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family        1000        128           10000        32768  thrpt   25  140543814.981 ± 360838.996  ops/s

## After fixing ByteBuffer (.preallocatedByteBufferGet)

* This is a partial fix, there is more to come.

Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (multiGetSize)  (valueSize)   Mode  Cnt          Score        Error  Units
GetBenchmarks.get                              no_column_family        1000        128             N/A        32768  thrpt   25     150389.259 ±   1371.473  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family        1000        128             N/A        32768  thrpt   25     179919.468 ±   1670.714  ops/s
GetBenchmarks.preallocatedGet                  no_column_family        1000        128             N/A        32768  thrpt   25     178261.938 ±   2630.571  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family        1000        128            1000        32768  thrpt   25  141080504.260 ± 164570.571  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family        1000        128           10000        32768  thrpt   25  140294874.217 ± 599553.650  ops/s

## After fixing ByteBuffer fully (.preallocatedByteBufferGet)