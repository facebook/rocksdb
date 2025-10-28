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

### Performance bug in Java `get()`

See this [issue](https://github.com/facebook/rocksdb/issues/13023)

#### Before fix (single)

Benchmark                              (columnFamilyTestType)  (keyCount)  (keySize)  (nthMissingKey)  (valueSize)   Mode  Cnt       Score      Error  Units
GetNotFoundBenchmarks.getNotFoundEven        no_column_family      100000         12                2           16  thrpt   15  289245.405 ± 1727.615  ops/s
GetNotFoundBenchmarks.getNotFoundOdd         no_column_family      100000         12                2           16  thrpt   15  717300.753 ± 7040.178  ops/s

#### After fix (single)

Benchmark                              (columnFamilyTestType)  (keyCount)  (keySize)  (nthMissingKey)  (valueSize)   Mode  Cnt       Score       Error  Units
GetNotFoundBenchmarks.getNotFoundEven        no_column_family      100000         12                2           16  thrpt   15  856149.231 ± 10159.858  ops/s
GetNotFoundBenchmarks.getNotFoundOdd         no_column_family      100000         12                2           16  thrpt   15  726021.153 ±  4047.159  ops/s



