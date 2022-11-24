# RocksDB Get Performance Benchmarks

Results associated with [Improve Java API `get()` performance by reducing copies](https://github.com/facebook/rocksdb/pull/10970)

## Build/Run

Mac
```
make clean jclean
DEBUG_LEVEL=0 make -j12 rocksdbjava
(cd java/target; cp rocksdbjni-7.9.0-osx.jar rocksdbjni-7.9.0-SNAPSHOT-osx.jar)
mvn install:install-file -Dfile=./java/target/rocksdbjni-7.9.0-SNAPSHOT-osx.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=7.9.0-SNAPSHOT -Dpackaging=jar
```

Linux
```
make clean jclean
DEBUG_LEVEL=0 make -j12 rocksdbjava
(cd java/target; cp rocksdbjni-7.9.0-linux64.jar rocksdbjni-7.9.0-SNAPSHOT-linux64.jar)
mvn install:install-file -Dfile=./java/target/rocksdbjni-7.9.0-SNAPSHOT-linux64.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=7.9.0-SNAPSHOT -Dpackaging=jar
```

Build jmh test package, on either platform
```
pushd java/jmh
mvn clean package
```

A quick test run, just as a sanity check, using a small number of keys, would be
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000 -p keySize=128 -p valueSize=32768 -p columnFamilyTestType="no_column_family" GetBenchmarks
```
The long performance run (as big as we can make it on our Ubuntu box without filling the disk)
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000,50000 -p keySize=128 -p valueSize=1024,16384 -p columnFamilyTestType="1_column_family","20_column_families" GetBenchmarks.get GetBenchmarks.preallocatedByteBufferGet GetBenchmarks.preallocatedGet
```

## Results (small runs, Mac)

These are run on a 10-core M1 with 64GB of memory and 2TB of SSD.
They probably reflect the absolute best case for this optimization, hitting in-memory buffers and completely eliminating a buffer copy.

### Before
Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (multiGetSize)  (valueSize)   Mode  Cnt          Score        Error  Units
GetBenchmarks.get                              no_column_family        1000        128             N/A        32768  thrpt   25      43496.578 ±   5743.090  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family        1000        128             N/A        32768  thrpt   25      70765.578 ±    697.548  ops/s
GetBenchmarks.preallocatedGet                  no_column_family        1000        128             N/A        32768  thrpt   25      69883.554 ±    944.184  ops/s

### After fixing byte[] (.get and .preallocatedGet)

Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (multiGetSize)  (valueSize)   Mode  Cnt          Score        Error  Units
GetBenchmarks.get                              no_column_family        1000        128             N/A        32768  thrpt   25     149207.681 ±   2261.671  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family        1000        128             N/A        32768  thrpt   25      68920.489 ±   1574.664  ops/s
GetBenchmarks.preallocatedGet                  no_column_family        1000        128             N/A        32768  thrpt   25     177399.022 ±   2107.375  ops/s

### After fixing ByteBuffer (.preallocatedByteBufferGet)

Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (multiGetSize)  (valueSize)   Mode  Cnt          Score        Error  Units
GetBenchmarks.get                              no_column_family        1000        128             N/A        32768  thrpt   25     150389.259 ±   1371.473  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family        1000        128             N/A        32768  thrpt   25     179919.468 ±   1670.714  ops/s
GetBenchmarks.preallocatedGet                  no_column_family        1000        128             N/A        32768  thrpt   25     178261.938 ±   2630.571  ops/s
## Results (Ubuntu, big runs)
These take 3-4 hours
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000,50000 -p keySize=128 -p valueSize=1024,16384 -p columnFamilyTestType="1_column_family","20_column_families" GetBenchmarks.get GetBenchmarks.preallocatedByteBufferGet GetBenchmarks.preallocatedGet
```
It's clear that all `get()` variants have noticeably improved performance, though not the spectacular gains of the M1.
### With fixes for all of the `get()` instances

Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (valueSize)   Mode  Cnt        Score       Error  Units
GetBenchmarks.get                               1_column_family        1000        128         1024  thrpt   25   935648.793 ± 22879.910  ops/s
GetBenchmarks.get                               1_column_family        1000        128        16384  thrpt   25   204366.301 ±  1326.570  ops/s
GetBenchmarks.get                               1_column_family       50000        128         1024  thrpt   25   693451.990 ± 19822.720  ops/s
GetBenchmarks.get                               1_column_family       50000        128        16384  thrpt   25    50473.768 ±   497.335  ops/s
GetBenchmarks.get                            20_column_families        1000        128         1024  thrpt   25   550118.874 ± 14289.009  ops/s
GetBenchmarks.get                            20_column_families        1000        128        16384  thrpt   25   120545.549 ±   648.280  ops/s
GetBenchmarks.get                            20_column_families       50000        128         1024  thrpt   25   235671.353 ±  2231.195  ops/s
GetBenchmarks.get                            20_column_families       50000        128        16384  thrpt   25    12463.887 ±  1950.746  ops/s
GetBenchmarks.preallocatedByteBufferGet         1_column_family        1000        128         1024  thrpt   25  1196026.040 ± 35435.729  ops/s
GetBenchmarks.preallocatedByteBufferGet         1_column_family        1000        128        16384  thrpt   25   403252.655 ±  3287.054  ops/s
GetBenchmarks.preallocatedByteBufferGet         1_column_family       50000        128         1024  thrpt   25   829965.448 ± 16945.452  ops/s
GetBenchmarks.preallocatedByteBufferGet         1_column_family       50000        128        16384  thrpt   25    63798.042 ±  1292.858  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families        1000        128         1024  thrpt   25   724557.253 ± 12710.828  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families        1000        128        16384  thrpt   25   176846.615 ±  1121.644  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families       50000        128         1024  thrpt   25   263553.764 ±  1304.243  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families       50000        128        16384  thrpt   25    14721.693 ±  2574.240  ops/s
GetBenchmarks.preallocatedGet                   1_column_family        1000        128         1024  thrpt   25  1093947.765 ± 42846.276  ops/s
GetBenchmarks.preallocatedGet                   1_column_family        1000        128        16384  thrpt   25   391629.913 ±  4039.965  ops/s
GetBenchmarks.preallocatedGet                   1_column_family       50000        128         1024  thrpt   25   769332.958 ± 24180.749  ops/s
GetBenchmarks.preallocatedGet                   1_column_family       50000        128        16384  thrpt   25    61712.038 ±   423.494  ops/s
GetBenchmarks.preallocatedGet                20_column_families        1000        128         1024  thrpt   25   694684.465 ±  5484.205  ops/s
GetBenchmarks.preallocatedGet                20_column_families        1000        128        16384  thrpt   25   172383.593 ±   841.679  ops/s
GetBenchmarks.preallocatedGet                20_column_families       50000        128         1024  thrpt   25   257447.351 ±  1388.667  ops/s
GetBenchmarks.preallocatedGet                20_column_families       50000        128        16384  thrpt   25    13418.522 ±  2418.619  ops/s

### Baseline (no fixes)

Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (valueSize)   Mode  Cnt        Score       Error  Units
GetBenchmarks.get                               1_column_family        1000        128         1024  thrpt   25   866745.224 ±  8834.629  ops/s
GetBenchmarks.get                               1_column_family        1000        128        16384  thrpt   25   184332.195 ±  2304.217  ops/s
GetBenchmarks.get                               1_column_family       50000        128         1024  thrpt   25   666794.288 ± 16150.684  ops/s
GetBenchmarks.get                               1_column_family       50000        128        16384  thrpt   25    47221.788 ±   433.165  ops/s
GetBenchmarks.get                            20_column_families        1000        128         1024  thrpt   25   551513.636 ±  7763.681  ops/s
GetBenchmarks.get                            20_column_families        1000        128        16384  thrpt   25   113117.720 ±   580.738  ops/s
GetBenchmarks.get                            20_column_families       50000        128         1024  thrpt   25   238675.555 ±  1758.978  ops/s
GetBenchmarks.get                            20_column_families       50000        128        16384  thrpt   25    11639.390 ±  1459.765  ops/s
GetBenchmarks.preallocatedByteBufferGet         1_column_family        1000        128         1024  thrpt   25  1153617.917 ± 26350.028  ops/s
GetBenchmarks.preallocatedByteBufferGet         1_column_family        1000        128        16384  thrpt   25   401710.334 ±  4324.539  ops/s
GetBenchmarks.preallocatedByteBufferGet         1_column_family       50000        128         1024  thrpt   25   809384.073 ± 13833.871  ops/s
GetBenchmarks.preallocatedByteBufferGet         1_column_family       50000        128        16384  thrpt   25    59279.005 ±   443.207  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families        1000        128         1024  thrpt   25   715466.403 ±  6591.375  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families        1000        128        16384  thrpt   25   175279.163 ±   910.923  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families       50000        128         1024  thrpt   25   263295.180 ±   856.456  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families       50000        128        16384  thrpt   25    14001.928 ±  2462.067  ops/s
GetBenchmarks.preallocatedGet                   1_column_family        1000        128         1024  thrpt   25  1072866.854 ± 27030.592  ops/s
GetBenchmarks.preallocatedGet                   1_column_family        1000        128        16384  thrpt   25   383950.853 ±  4510.654  ops/s
GetBenchmarks.preallocatedGet                   1_column_family       50000        128         1024  thrpt   25   764395.469 ± 10097.417  ops/s
GetBenchmarks.preallocatedGet                   1_column_family       50000        128        16384  thrpt   25    56851.330 ±   388.029  ops/s
GetBenchmarks.preallocatedGet                20_column_families        1000        128         1024  thrpt   25   668518.593 ±  9764.117  ops/s
GetBenchmarks.preallocatedGet                20_column_families        1000        128        16384  thrpt   25   171309.695 ±   875.895  ops/s
GetBenchmarks.preallocatedGet                20_column_families       50000        128         1024  thrpt   25   256057.801 ±   954.621  ops/s
GetBenchmarks.preallocatedGet                20_column_families       50000        128        16384  thrpt   25    13319.380 ±  2126.654  ops/s

### Comparison

It does at least look best when the data is cached. That is to say, smallest number of column families, and least keys.

GetBenchmarks.get                               1_column_family        1000        128        16384  thrpt   25   204366.301 ±  1326.570  ops/s
GetBenchmarks.get                               1_column_family        1000        128        16384  thrpt   25   184332.195 ±  2304.217  ops/s

GetBenchmarks.get                               1_column_family       50000        128        16384  thrpt   25    50473.768 ±   497.335  ops/s
GetBenchmarks.get                               1_column_family       50000        128        16384  thrpt   25    47221.788 ±   433.165  ops/s

GetBenchmarks.get                            20_column_families        1000        128        16384  thrpt   25   120545.549 ±   648.280  ops/s
GetBenchmarks.get                            20_column_families        1000        128        16384  thrpt   25   113117.720 ±   580.738  ops/s

GetBenchmarks.get                            20_column_families       50000        128        16384  thrpt   25    12463.887 ±  1950.746  ops/s
GetBenchmarks.get                            20_column_families       50000        128        16384  thrpt   25    11639.390 ±  1459.765  ops/s

### Baseline
25 minute run, small number of keys
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000 -p keySize=128 -p valueSize=32768 -p columnFamilyTestType="no_column_families" GetBenchmarks.get GetBenchmarks.preallocatedByteBufferGet GetBenchmarks.preallocatedGet
```

Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (valueSize)   Mode  Cnt      Score     Error  Units
GetBenchmarks.get                            no_column_families        1000        128        32768  thrpt   25  32344.908 ± 296.651  ops/s
GetBenchmarks.preallocatedByteBufferGet      no_column_families        1000        128        32768  thrpt   25  45266.968 ± 424.514  ops/s
GetBenchmarks.preallocatedGet                no_column_families        1000        128        32768  thrpt   25  43531.088 ± 291.785  ops/s

### Optimized

Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (valueSize)   Mode  Cnt      Score     Error  Units
GetBenchmarks.get                            no_column_families        1000        128        32768  thrpt   25  37463.716 ± 235.744  ops/s
GetBenchmarks.preallocatedByteBufferGet      no_column_families        1000        128        32768  thrpt   25  48946.105 ± 466.463  ops/s
GetBenchmarks.preallocatedGet                no_column_families        1000        128        32768  thrpt   25  47143.624 ± 576.763  ops/s

## Conclusion

The performance improvement is real.

