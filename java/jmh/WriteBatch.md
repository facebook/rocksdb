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
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatchNative WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="16384" -p numOpsPerFlush="1" -p writeBatchAllocation="20000"
```

Benchmark                                 (keyCount)  (keySize)  (numOpsPerFlush)  (valueSize)  (writeBatchAllocation)   Mode  Cnt      Score      Error  Units
WriteBatchBenchmarks.putWriteBatch            100000         16                 1        16384                   20000  thrpt    5  28357.427 ± 3106.298  ops/s
WriteBatchBenchmarks.putWriteBatchBB          100000         16                 1        16384                   20000  thrpt    5  28569.271 ± 4432.971  ops/s
WriteBatchBenchmarks.putWriteBatchNative      100000         16                 1        16384                   20000  thrpt    5  27323.964 ± 4072.435  ops/s

```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatchNative WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="16384" -p numOpsPerFlush="1000" -p writeBatchAllocation="20000"
```

Benchmark                                 (keyCount)  (keySize)  (numOpsPerFlush)  (valueSize)  (writeBatchAllocation)   Mode  Cnt      Score      Error  Units
WriteBatchBenchmarks.putWriteBatch            100000         16              1000        16384                   20000  thrpt    5  41329.098 ± 6230.805  ops/s
WriteBatchBenchmarks.putWriteBatchBB          100000         16              1000        16384                   20000  thrpt    5  42738.300 ± 6917.658  ops/s
WriteBatchBenchmarks.putWriteBatchNative      100000         16              1000        16384                   20000  thrpt    5  39963.741 ± 8203.994  ops/s

Why has `putWriteBatch` performance improved ? Because the write batch on both sides auto-extends, so that we do in fact end up cacheing on the LHS. In the default case,
we are writing to the batch, but not to the DB, and that saves us a lot of work.

It does make sense that the performance of `putWriteBatchNative()` is a bit lower than `putWriteBatch()`, because we do the single extra copy (Java buffer to C++ buffer),
but we also do some extending copies on the Java side.

With a bigger allocation, it turns out that not much changes:
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatchNative WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="16384" -p numOpsPerFlush="1000" -p writeBatchAllocation="1000000"
```

Benchmark                                 (keyCount)  (keySize)  (numOpsPerFlush)  (valueSize)  (writeBatchAllocation)   Mode  Cnt      Score      Error  Units
WriteBatchBenchmarks.putWriteBatch            100000         16              1000        16384                 1000000  thrpt    5  42011.749 ± 7963.955  ops/s
WriteBatchBenchmarks.putWriteBatchBB          100000         16              1000        16384                 1000000  thrpt    5  39739.591 ± 7123.974  ops/s
WriteBatchBenchmarks.putWriteBatchNative      100000         16              1000        16384                 1000000  thrpt    5  40875.350 ± 3258.271  ops/s

We return to the first benchmark, but we have added writing to direct `ByteBuffer`(s) for the native case. The direct buffer does not seem particularly better:
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="32" -p numOpsPerFlush="1000" -p writeBatchAllocation="65536"
```

Benchmark                                   (keyCount)  (keySize)  (numOpsPerFlush)  (valueSize)  (writeBatchAllocation)   Mode  Cnt       Score       Error  Units
WriteBatchBenchmarks.putWriteBatch              100000         16              1000           32                   65536  thrpt    5  557242.202 ± 17512.382  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000           32                   65536  thrpt    5  583374.020 ± 28841.795  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000           32                   65536  thrpt    5  673348.944 ± 38294.671  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000           32                   65536  thrpt    5  659737.742 ± 35603.782  ops/s

Comparison of flushing or not flushing to DB:
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="32" -p numOpsPerFlush="1000" -p writeBatchAllocation="65536" -p flushToDB="false","true"
```

Benchmark                                   (flushToDB)  (keyCount)  (keySize)  (numOpsPerFlush)  (valueSize)  (writeBatchAllocation)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatch                false      100000         16              1000           32                   65536  thrpt    5  1970917.255 ± 114625.329  ops/s
WriteBatchBenchmarks.putWriteBatch                 true      100000         16              1000           32                   65536  thrpt    5   559047.169 ±  27133.980  ops/s
WriteBatchBenchmarks.putWriteBatchBB              false      100000         16              1000           32                   65536  thrpt    5  2372266.300 ±  60716.826  ops/s
WriteBatchBenchmarks.putWriteBatchBB               true      100000         16              1000           32                   65536  thrpt    5   590029.296 ±  16491.281  ops/s
WriteBatchBenchmarks.putWriteBatchNative          false      100000         16              1000           32                   65536  thrpt    5  5621695.425 ± 937171.415  ops/s
WriteBatchBenchmarks.putWriteBatchNative           true      100000         16              1000           32                   65536  thrpt    5   678418.305 ±  20722.752  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB        false      100000         16              1000           32                   65536  thrpt    5  6289385.062 ±  38791.265  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB         true      100000         16              1000           32                   65536  thrpt    5   658870.746 ±  29760.767  ops/s


