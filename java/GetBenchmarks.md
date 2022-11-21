# RocksDB Get Performance Benchmarks

Mac
```
make clean jclean
DEBUG_LEVEL=0 make -j12 rocksdbjava
(cd java/target; cp rocksdbjni-7.9.0-osx.jar rocksdbjni-7.9.0-SNAPSHOT-osx.jar)
mvn install:install-file -Dfile=./java/target/rocksdbjni-7.9.0-SNAPSHOT-osx.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=7.9.0-SNAPSHOT -Dpackaging=jar
pushd java/jmh
mvn clean package
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000 -p keySize=128 -p valueSize=32768 -p columnFamilyTestType="no_column_family" GetBenchmarks
# test the onees that may have been affected by the change(s)
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar -p keyCount=1000,100000 -p keySize=128 -p valueSize=32768 GetBenchmarks.get GetBenchmarks.preallocatedByteBufferGet GetBenchmarks.preallocatedGet
```

Linux
```
make clean jclean
DEBUG_LEVEL=0 make -j12 rocksdbjava
(cd java/target; cp rocksdbjni-7.9.0-linux64.jar rocksdbjni-7.9.0-SNAPSHOT-linux64.jar)
mvn install:install-file -Dfile=./java/target/rocksdbjni-7.9.0-SNAPSHOT-linux64.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=7.9.0-SNAPSHOT -Dpackaging=jar
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
* We can potentially remove the _not in the buffer cache_ copy if we play a clever trick with an allocator.

Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (multiGetSize)  (valueSize)   Mode  Cnt          Score        Error  Units
GetBenchmarks.get                              no_column_family        1000        128             N/A        32768  thrpt   25     150389.259 ±   1371.473  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family        1000        128             N/A        32768  thrpt   25     179919.468 ±   1670.714  ops/s
GetBenchmarks.preallocatedGet                  no_column_family        1000        128             N/A        32768  thrpt   25     178261.938 ±   2630.571  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family        1000        128            1000        32768  thrpt   25  141080504.260 ± 164570.571  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family        1000        128           10000        32768  thrpt   25  140294874.217 ± 599553.650  ops/s

## More exhaustive tests after partially fixing ByteBuffer (.preallocatedByteBufferGet)

Benchmark                                (columnFamilyTestType)  (keyCount)  (keySize)  (multiGetSize)  (valueSize)   Mode  Cnt          Score        Error  Units
GetBenchmarks.get                              no_column_family        1000        128             N/A          128  thrpt   25    1040276.805 ±   3898.461  ops/s
GetBenchmarks.get                              no_column_family        1000        128             N/A        32768  thrpt   25     152818.233 ±    692.914  ops/s
GetBenchmarks.get                              no_column_family       10000        128             N/A          128  thrpt   25    1006185.514 ±   5792.652  ops/s
GetBenchmarks.get                              no_column_family       10000        128             N/A        32768  thrpt   25     144471.995 ±    538.742  ops/s
GetBenchmarks.get                              no_column_family      100000        128             N/A          128  thrpt   25     831732.444 ±   2294.178  ops/s
GetBenchmarks.get                              no_column_family      100000        128             N/A        32768  thrpt   25     143192.638 ±    398.704  ops/s
GetBenchmarks.get                            20_column_families        1000        128             N/A          128  thrpt   25    1107766.188 ±   8050.314  ops/s
GetBenchmarks.get                            20_column_families        1000        128             N/A        32768  thrpt   25     217647.241 ±   1908.581  ops/s
GetBenchmarks.get                            20_column_families       10000        128             N/A          128  thrpt   25     778699.934 ±   3443.921  ops/s
GetBenchmarks.get                            20_column_families       10000        128             N/A        32768  thrpt   25     105104.236 ±   2022.214  ops/s
GetBenchmarks.get                            20_column_families      100000        128             N/A          128  thrpt   25     561165.898 ±   1586.678  ops/s
GetBenchmarks.get                            20_column_families      100000        128             N/A        32768  thrpt   25       5718.893 ±     64.869  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family        1000        128             N/A          128  thrpt   25    1081739.161 ±   9602.010  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family        1000        128             N/A        32768  thrpt   25     187284.845 ±   1553.376  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family       10000        128             N/A          128  thrpt   25    1043906.755 ±   6952.511  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family       10000        128             N/A        32768  thrpt   25     165457.287 ±    461.168  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family      100000        128             N/A          128  thrpt   25     865971.949 ±   1932.030  ops/s
GetBenchmarks.preallocatedByteBufferGet        no_column_family      100000        128             N/A        32768  thrpt   25     163411.699 ±    402.599  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families        1000        128             N/A          128  thrpt   25    1210377.042 ±  10180.450  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families        1000        128             N/A        32768  thrpt   25     277470.866 ±    569.519  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families       10000        128             N/A          128  thrpt   25     820732.161 ±   3385.851  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families       10000        128             N/A        32768  thrpt   25     123967.554 ±   3236.331  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families      100000        128             N/A          128  thrpt   25     577932.701 ±    930.964  ops/s
GetBenchmarks.preallocatedByteBufferGet      20_column_families      100000        128             N/A        32768  thrpt   25       5983.240 ±     50.000  ops/s
GetBenchmarks.preallocatedGet                  no_column_family        1000        128             N/A          128  thrpt   25    1037981.138 ±  15170.615  ops/s
GetBenchmarks.preallocatedGet                  no_column_family        1000        128             N/A        32768  thrpt   25     185562.520 ±    778.349  ops/s
GetBenchmarks.preallocatedGet                  no_column_family       10000        128             N/A          128  thrpt   25    1013894.445 ±  10635.658  ops/s
GetBenchmarks.preallocatedGet                  no_column_family       10000        128             N/A        32768  thrpt   25     164349.246 ±    728.333  ops/s
GetBenchmarks.preallocatedGet                  no_column_family      100000        128             N/A          128  thrpt   25     853305.608 ±   5843.986  ops/s
GetBenchmarks.preallocatedGet                  no_column_family      100000        128             N/A        32768  thrpt   25     162455.146 ±    796.872  ops/s
GetBenchmarks.preallocatedGet                20_column_families        1000        128             N/A          128  thrpt   25    1201097.934 ±   4083.034  ops/s
GetBenchmarks.preallocatedGet                20_column_families        1000        128             N/A        32768  thrpt   25     274395.085 ±    701.075  ops/s
GetBenchmarks.preallocatedGet                20_column_families       10000        128             N/A          128  thrpt   25     822409.028 ±   3162.956  ops/s
GetBenchmarks.preallocatedGet                20_column_families       10000        128             N/A        32768  thrpt   25     122689.824 ±   3496.732  ops/s
GetBenchmarks.preallocatedGet                20_column_families      100000        128             N/A          128  thrpt   25     576797.561 ±   1497.424  ops/s
GetBenchmarks.preallocatedGet                20_column_families      100000        128             N/A        32768  thrpt   25       5938.253 ±     57.218  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family        1000        128            1000          128  thrpt   25  140498176.976 ± 120228.409  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family        1000        128            1000        32768  thrpt   25  140320876.522 ± 444686.741  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family        1000        128           10000          128  thrpt   25  140235106.013 ± 526086.189  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family        1000        128           10000        32768  thrpt   25  140523065.502 ± 103123.816  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family       10000        128           10000          128  thrpt   25  140462546.623 ± 120001.347  ops/s
MultiGetBenchmarks.multiGet10                  no_column_family       10000        128           10000        32768  thrpt   25  140360152.129 ± 135896.362  ops/s
MultiGetBenchmarks.multiGet10                20_column_families        1000        128              10          128  thrpt   25      74636.136 ±    115.336  ops/s
MultiGetBenchmarks.multiGet10                20_column_families        1000        128              10        32768  thrpt   25      12166.378 ±     26.008  ops/s
MultiGetBenchmarks.multiGet10                20_column_families        1000        128             100          128  thrpt   25       8089.363 ±      8.772  ops/s
MultiGetBenchmarks.multiGet10                20_column_families        1000        128             100        32768  thrpt   25       1226.271 ±     10.547  ops/s
MultiGetBenchmarks.multiGet10                20_column_families        1000        128            1000          128  thrpt   25  140350419.000 ± 311679.888  ops/s
MultiGetBenchmarks.multiGet10                20_column_families        1000        128            1000        32768  thrpt   25  140448195.322 ±  96845.044  ops/s
MultiGetBenchmarks.multiGet10                20_column_families        1000        128           10000          128  thrpt   25  140377225.804 ± 138921.466  ops/s
MultiGetBenchmarks.multiGet10                20_column_families        1000        128           10000        32768  thrpt   25  140368554.238 ± 307575.160  ops/s
MultiGetBenchmarks.multiGet10                20_column_families       10000        128              10          128  thrpt   25      72554.453 ±     89.592  ops/s
MultiGetBenchmarks.multiGet10                20_column_families       10000        128              10        32768  thrpt   25      11402.359 ±     81.836  ops/s
MultiGetBenchmarks.multiGet10                20_column_families       10000        128             100          128  thrpt   25       7834.694 ±     44.687  ops/s
MultiGetBenchmarks.multiGet10                20_column_families       10000        128             100        32768  thrpt   25       1144.018 ±     12.139  ops/s
MultiGetBenchmarks.multiGet10                20_column_families       10000        128            1000          128  thrpt   25        792.457 ±      2.196  ops/s
MultiGetBenchmarks.multiGet10                20_column_families       10000        128            1000        32768  thrpt   25         90.063 ±      2.045  ops/s
MultiGetBenchmarks.multiGet10                20_column_families       10000        128           10000          128  thrpt   25  138220650.146 ± 642344.824  ops/s
MultiGetBenchmarks.multiGet10                20_column_families       10000        128           10000        32768  thrpt   25  139526734.800 ± 140420.678  ops/s
MultiGetBenchmarks.multiGet10                20_column_families      100000        128              10          128  thrpt   25      63310.557 ±    976.018  ops/s
MultiGetBenchmarks.multiGet10                20_column_families      100000        128             100          128  thrpt   25       7051.948 ±     35.128  ops/s
MultiGetBenchmarks.multiGet10                20_column_families      100000        128            1000          128  thrpt   25        709.563 ±      3.243  ops/s
MultiGetBenchmarks.multiGet10                20_column_families      100000        128           10000          128  thrpt   25         73.235 ±      0.392  ops/s


## After fixing ByteBuffer fully (.preallocatedByteBufferGet)