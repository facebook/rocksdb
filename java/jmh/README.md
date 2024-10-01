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

ah, multi doesn't use this... irrelevant

#### Before (single)

Benchmark                              (columnFamilyTestType)  (keyCount)  (keySize)  (nthMissingKey)  (valueSize)   Mode  Cnt       Score      Error  Units
GetNotFoundBenchmarks.getNotFoundEven        no_column_family      100000         12                2           16  thrpt   15  289245.405 ± 1727.615  ops/s
GetNotFoundBenchmarks.getNotFoundOdd         no_column_family      100000         12                2           16  thrpt   15  717300.753 ± 7040.178  ops/s

#### After (single)

Benchmark                              (columnFamilyTestType)  (keyCount)  (keySize)  (nthMissingKey)  (valueSize)   Mode  Cnt       Score      Error  Units
GetNotFoundBenchmarks.getNotFoundEven        no_column_family      100000         12                2           16  thrpt   15  846319.264 ± 3018.297  ops/s
GetNotFoundBenchmarks.getNotFoundOdd         no_column_family      100000         12                2           16  thrpt   15  710512.885 ± 7933.743  ops/s




