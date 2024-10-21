Calculate the `writeBatchAllocation` to accomodate `(keySize + valueSize) * numOpsPerFlush` (with a bit left over). It would be better to have the benchmark do it,
but short of that, this will do.

We attempt to run a benchmark which is fairly favourable to the native batch optimization; it shows a noticeable improvement.

```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="32" -p numOpsPerFlush="1000" -p writeBatchAllocation="65536"
```

Benchmark                                 (keyCount)  (keySize)  (numOpsPerFlush)  (valueSize)  (writeBatchAllocation)   Mode  Cnt       Score       Error  Units
WriteBatchBenchmarks.putWriteBatch            100000         16              1000           32                   65536  thrpt    5  564652.796 ± 22017.217  ops/s
WriteBatchBenchmarks.putWriteBatchBB          100000         16              1000           32                   65536  thrpt    5  596095.928 ± 12792.723  ops/s
WriteBatchBenchmarks.putWriteBatchNative      100000         16              1000           32                   65536  thrpt    5  684490.980 ± 13319.385  ops/s

Even for a smaller `numOpsPerFlush` the benefit is still there:
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatchNative WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="32" -p numOpsPerFlush="100" -p writeBatchAllocation="8192"
```

Benchmark                                 (keyCount)  (keySize)  (numOpsPerFlush)  (valueSize)  (writeBatchAllocation)   Mode  Cnt       Score       Error  Units
WriteBatchBenchmarks.putWriteBatch            100000         16               100           32                    8192  thrpt    5  551033.585 ± 18798.769  ops/s
WriteBatchBenchmarks.putWriteBatchBB          100000         16               100           32                    8192  thrpt    5  576664.078 ± 24682.534  ops/s
WriteBatchBenchmarks.putWriteBatchNative      100000         16               100           32                    8192  thrpt    5  659665.168 ± 35928.313  ops/s

If we vary a couple of things, the benefits should reduce
* As the size of the value field (`valueSize`) increases, copying costs will increase compared to JNI transition costs, reducing the performance of the native implementation.
* As the flush frequency increases (`numOpsPerFlush` reduces), the saving in JNI transition costs will be reduced, and the higher copying costs will outweigh them.

```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatchNative WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="256" -p numOpsPerFlush="100" -p writeBatchAllocation="65536"
```

Benchmark                                 (keyCount)  (keySize)  (numOpsPerFlush)  (valueSize)  (writeBatchAllocation)   Mode  Cnt       Score       Error  Units
WriteBatchBenchmarks.putWriteBatch            100000         16               100          256                   65536  thrpt    5  584309.855 ± 16922.050  ops/s
WriteBatchBenchmarks.putWriteBatchBB          100000         16               100          256                   65536  thrpt    5  619882.196 ± 17649.728  ops/s
WriteBatchBenchmarks.putWriteBatchNative      100000         16               100          256                   65536  thrpt    5  702672.627 ± 15477.305  ops/s

```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatchNative WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="4096" -p numOpsPerFlush="100" -p writeBatchAllocation="512000"
```

Benchmark                                 (keyCount)  (keySize)  (numOpsPerFlush)  (valueSize)  (writeBatchAllocation)   Mode  Cnt       Score       Error  Units
WriteBatchBenchmarks.putWriteBatch            100000         16               100         4096                  512000  thrpt    5  136114.674 ± 11634.283  ops/s
WriteBatchBenchmarks.putWriteBatchBB          100000         16               100         4096                  512000  thrpt    5  155720.617 ± 13434.961  ops/s
WriteBatchBenchmarks.putWriteBatchNative      100000         16               100         4096                  512000  thrpt    5  154348.308 ± 21103.553  ops/s

Even the above still shows a benefit for the native optimization. Configure to even fewer ops per flush:

Benchmark                                 (keyCount)  (keySize)  (numOpsPerFlush)  (valueSize)  (writeBatchAllocation)   Mode  Cnt       Score      Error  Units
WriteBatchBenchmarks.putWriteBatch            100000         16                10         4096                   65536  thrpt    5  158433.899 ± 3523.703  ops/s
WriteBatchBenchmarks.putWriteBatchBB          100000         16                10         4096                   65536  thrpt    5  153061.239 ± 5961.755  ops/s
WriteBatchBenchmarks.putWriteBatchNative      100000         16                10         4096                   65536  thrpt    5  164148.683 ± 6658.029  ops/s

If we flush on every operation, it's not even noticeably worse. At least it's not *better*, that would be discombobulating.

```
Benchmark                                 (keyCount)  (keySize)  (numOpsPerFlush)  (valueSize)  (writeBatchAllocation)   Mode  Cnt      Score      Error  Units
WriteBatchBenchmarks.putWriteBatch            100000         16                 1         4096                    5120  thrpt    5  70141.011 ± 9200.389  ops/s
WriteBatchBenchmarks.putWriteBatchBB          100000         16                 1         4096                    5120  thrpt    5  65674.181 ± 7762.742  ops/s
WriteBatchBenchmarks.putWriteBatchNative      100000         16                 1         4096                    5120  thrpt    5  69614.284 ± 1721.151  ops/s
```

Checking that larger values reduce the throughput (sanity check)

```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatchNative WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="16384" -p numOpsPerFlush="1" -p writeBatchAllocation="20000
```

Benchmark                                 (keyCount)  (keySize)  (numOpsPerFlush)  (valueSize)  (writeBatchAllocation)   Mode  Cnt      Score      Error  Units
WriteBatchBenchmarks.putWriteBatch            100000         16                 1        16384                   20000  thrpt    5  28357.427 ± 3106.298  ops/s
WriteBatchBenchmarks.putWriteBatchBB          100000         16                 1        16384                   20000  thrpt    5  28569.271 ± 4432.971  ops/s
WriteBatchBenchmarks.putWriteBatchNative      100000         16                 1        16384                   20000  thrpt    5  27323.964 ± 4072.435  ops/s


