# JMH Benchmarks for RocksJava

These are micro-benchmarks for RocksJava functionality, using [JMH (Java Microbenchmark Harness)](https://openjdk.java.net/projects/code-tools/jmh/).

## Compiling

**Note**: This uses a specific build of RocksDB that is set in the `<version>` element of the `dependencies` section of the `pom.xml` file. If you are testing local changes you should build and install a SNAPSHOT version of rocksdbjni, and update the `pom.xml` of rocksdbjni-jmh file to test with this.

For instance, this is how to install the OSX jar you just built for 9.8.0

```bash
$ mvn install:install-file -Dfile=./java/target/rocksdbjni-9.8.0-osx.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=9.8.0-SNAPSHOT -Dpackaging=jar
```

```bash
$ mvn package
```

## Running
```bash
$ java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar
```

NOTE: you can append `-help` to the command above to see all of the JMH runtime options.

#### Before (multi)

Benchmark                                       (columnFamilyTestType)  (keyCount)  (keySize)  (multiGetSize)  (nthMissingKey)  (valueSize)   Mode  Cnt    Score   Error  Units
MultiNotFoundBenchmarks.multiNotFoundBBEvens          no_column_family      100000         16            1000                2           16  thrpt   15  607.494 ± 1.560  ops/s
MultiNotFoundBenchmarks.multiNotFoundBBOdds           no_column_family      100000         16            1000                2           16  thrpt   15  531.760 ± 1.968  ops/s
MultiNotFoundBenchmarks.multiNotFoundListEvens        no_column_family      100000         16            1000                2           16  thrpt   15  914.955 ± 2.927  ops/s
MultiNotFoundBenchmarks.multiNotFoundListOdds         no_column_family      100000         16            1000                2           16  thrpt   15  711.232 ± 2.201  ops/s

#### Before (single)

Benchmark                              (columnFamilyTestType)  (keyCount)  (keySize)  (nthMissingKey)  (valueSize)   Mode  Cnt       Score       Error  Units
GetNotFoundBenchmarks.getNotFoundEven        no_column_family      100000         12                2           16  thrpt   15  291802.037 ±  1082.526  ops/s
GetNotFoundBenchmarks.getNotFoundOdd         no_column_family      100000         12                2           16  thrpt   15  405500.054 ± 15590.921  ops/s

#### After (single)
`getNotFoundEven` should be fixed, BUT why should `getNotFoundOdd` be faster than before ? Pushing the exception stack (try) seems mad...

Benchmark                              (columnFamilyTestType)  (keyCount)  (keySize)  (nthMissingKey)  (valueSize)   Mode  Cnt       Score      Error  Units
GetNotFoundBenchmarks.getNotFoundEven        no_column_family      100000         12                2           16  thrpt   15  850779.017 ± 5680.454  ops/s
GetNotFoundBenchmarks.getNotFoundOdd         no_column_family      100000         12                2           16  thrpt   15  771496.888 ± 7186.347  ops/s
Benchmark                              (columnFamilyTestType)  (keyCount)  (keySize)  (nthMissingKey)  (valueSize)   Mode  Cnt       Score      Error  Units
GetNotFoundBenchmarks.getNotFoundEven        no_column_family      100000         12                2           16  thrpt   25  846618.159 ± 4648.865  ops/s
GetNotFoundBenchmarks.getNotFoundOdd         no_column_family      100000         12                2           16  thrpt   25  767894.897 ± 2713.019  ops/s



