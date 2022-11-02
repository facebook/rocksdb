# RocksDB Get Performance Benchmarks

```
make clean jclean
DEBUG_LEVEL=0 make -j12 rocksdbjava
(cd java/target; cp rocksdbjni-7.9.0-osx.jar rocksdbjni-7.9.0-SNAPSHOT-osx.jar)
mvn install:install-file -Dfile=./java/target/rocksdbjni-7.9.0-SNAPSHOT-osx.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=7.9.0-SNAPSHOT -Dpackaging=jar
pushd java/jmh
mvn clean package
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000 -p keySize=128 -p valueSize=32768 -p columnFamilyTestType="no_column_family"
```

## Before

Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (valueSize)   Mode  Cnt      Score      Error  Units
GetBenchmarks.get                              no_column_family        1000        128        32768  thrpt   25  44078.370 ± 8098.866  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family        1000        128        32768  thrpt   25  68989.341 ± 1105.917  ops/s
GetBenchmarks.preallocatedGet                  no_column_family        1000        128        32768  thrpt   25  68218.899 ± 1432.267  ops/s
