# JMH Benchmarks for RocksJava

These are micro-benchmarks for RocksJava functionality, using [JMH (Java Microbenchmark Harness)](https://openjdk.java.net/projects/code-tools/jmh/).

## Compiling

**Note**: This uses a specific build of RocksDB that is set in the `<version>` element of the `dependencies` section of the `pom.xml` file. If you are testing local changes you should build and install a SNAPSHOT version of rocksdbjni, and update the `pom.xml` of rocksdbjni-jmh file to test with this.

RocksJava jar can be installed to local Maven repository with command similar to
```bash
$ mvn install:install-file \
   -Dfile=/path/to/target/rocksdbjni-[version]-[platform].jar \
   -DgroupId=org.rocksdb \
   -DartifactId=rocksdbjni \
   -Dversion=[version]-SNAPSHOT \
   -Dpackaging=jar \
   -DgeneratePom=true
```

```bash
$ mvn package
```

## Running
```bash
$ java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar
```

NOTE: you can append `-help` to the command above to see all of the JMH runtime options.
