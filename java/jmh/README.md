# JMH Benchmarks for RocksJava

These are micro-benchmarks for RocksJava functionality, using [JMH (Java Microbenchmark Harness)](https://openjdk.java.net/projects/code-tools/jmh/).

## Compiling

**Note**: This uses a specific build of RocksDB that is set in the `<version>` element of the `dependencies` section of the `pom.xml` file. If you are testing local changes you should build and install a SNAPSHOT version of rocksdbjni, and update the `pom.xml` of rocksdbjni-jmh file to test with this.

**Note** all shell commands are executed from the root of the `rocksdb` directory

For instance, this is how to install the OSX jar you just built for ${VER}

```bash
$ VER=8.1.0
$ (cd java; mvn install:install-file -Dfile=./target/rocksdbjni-${VER}-osx.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=${VER} -Dpackaging=jar)
```

```bash
$ (cd java/jmh; mvn package)
```

## Running
```bash
$ (cd java/jmh; java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar)
```

NOTE: you can append `-help` to the command above to see all of the JMH runtime options.
