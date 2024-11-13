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

Having implemented the C++ native layout, and lazy Native creation (cache entirely at Java-side until buffer is full ). Note that `numOpsPerFlush` becomes
`numOpsPerBatch` (flushes now happen when the allocation size is full).
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="256" -p numOpsPerBatch="1000" -p writeBatchAllocation="16384" -p writeToDB="false","true"
```

Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatch              100000         16              1000          256                   16384        false  thrpt    5  1809216.300 ±  57078.510  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16              1000          256                   16384         true  thrpt    5   620227.011 ±  16644.354  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000          256                   16384        false  thrpt    5  2267923.359 ±  61432.146  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000          256                   16384         true  thrpt    5   655527.482 ±  17516.492  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          256                   16384        false  thrpt    5  5917708.661 ± 153556.231  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          256                   16384         true  thrpt    5   765216.679 ±  48786.125  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          256                   16384        false  thrpt    5  5145648.128 ± 199300.105  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          256                   16384         true  thrpt    5   755784.632 ±  33956.045  ops/s

Now try with smaller values and a bigger allocation, at this point 1000 ops per batch should fit in the buffer before flushing, so we always use the direct `std::string` copy
rather than the slice-based append to an existing native (C++) batch:
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="32" -p numOpsPerBatch="1000" -p writeBatchAllocation="65536" -p writeToDB="false","true"
```

Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatch              100000         16              1000           32                   65536        false  thrpt    5  2026120.677 ±  88735.879  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16              1000           32                   65536         true  thrpt    5   548593.384 ±  27048.655  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000           32                   65536        false  thrpt    5  2569210.495 ±  90157.689  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000           32                   65536         true  thrpt    5   572886.442 ±  15231.671  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000           32                   65536        false  thrpt    5  8924580.866 ± 710874.604  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000           32                   65536         true  thrpt    5   667643.484 ±  29138.914  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000           32                   65536        false  thrpt    5  7389058.364 ±  53381.417  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000           32                   65536         true  thrpt    5   655343.059 ±  18138.854  ops/s

It seems as if our improvements have made things faster.
Now, the direct `WriteBatchInternal::SetContents(this, slice)` optimization for `write()`, which we forgot.
Oh, this doesn't seem to do anything. Possibly not surprising. Anyway:

Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatch              100000         16              1000           32                   65536        false  thrpt    5  1999785.560 ±  37197.062  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16              1000           32                   65536         true  thrpt    5   563111.695 ±  24366.706  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000           32                   65536        false  thrpt    5  2443222.443 ±  69809.922  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000           32                   65536         true  thrpt    5   590703.788 ±  16387.123  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000           32                   65536        false  thrpt    5  8381060.909 ± 921173.791  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000           32                   65536         true  thrpt    5   696306.630 ±  26451.370  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000           32                   65536        false  thrpt    5  7389406.149 ± 123636.016  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000           32                   65536         true  thrpt    5   679611.535 ±  25301.620  ops/s

Let's implement `WriteBatchInternal::AppendContents` so we can bulk copy into batches which are not empty, with a single bulk copy;
the method takes a slice as the source, and we can wrap our Java data in a slice without the extra copy.
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="32" -p numOpsPerBatch="1000" -p writeBatchAllocation="65536" -p writeToDB="false","true"
```

Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatch              100000         16              1000           32                   65536        false  thrpt    5  2030753.269 ±  53379.170  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16              1000           32                   65536         true  thrpt    5   562150.653 ±  21639.050  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000           32                   65536        false  thrpt    5  2494298.838 ±  82514.631  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000           32                   65536         true  thrpt    5   587032.672 ±  22981.974  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000           32                   65536        false  thrpt    5  8876414.527 ± 740311.406  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000           32                   65536         true  thrpt    5   689480.793 ±  30970.367  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000           32                   65536        false  thrpt    5  7239323.548 ±  39419.729  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000           32                   65536         true  thrpt    5   669122.684 ±  29489.719  ops/s

It is not noticeable whether this has improved things at all.

#### Write to Disk cost

Doing reciprocals, we see that a single disk flush takes on the order of 1300ns, whatever test we run, while the rest of the work, filling the buffer
all the way from Java API to a filled C++ write batch, uses much less; 400-500ns in the unoptimized case, 100-150ns in the optimized case.

Is it faster with smaller `writeBatchAllocation` ? No, with a *larger* `valueSize`. That is very strange.
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatchNative -p keySize="16" -p valueSize="32","256" -p numOpsPerBatch="1000" -p writeBatchAllocation="16384" -p writeToDB="false","true"
```

Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score         Error  Units
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000           32                   16384        false  thrpt    5  6835705.554 ± 1649956.225  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000           32                   16384         true  thrpt    5   652477.466 ±   25227.488  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          256                   16384        false  thrpt    5  6914405.804 ±  247664.581  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          256                   16384         true  thrpt    5   788421.956 ±   14139.600  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000           32                   16384        false  thrpt    5  7152538.336 ±  608612.453  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000           32                   16384         true  thrpt    5   675255.795 ±   21625.118  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          256                   16384        false  thrpt    5  6138683.412 ±  341601.329  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          256                   16384         true  thrpt    5   765880.991 ±  124660.492  ops/s

Try with bigger values again; no, that doesn't continue.

```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatchNative -p keySize="16" -p valueSize="1024" -p numOpsPerBatch="1000" -p writeBatchAllocation="16384","65536" -p writeToDB="false","true"
```

Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000         1024                   16384        false  thrpt    5  3705640.216 ± 212868.320  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000         1024                   16384         true  thrpt    5   671026.357 ±  20855.005  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000         1024                   65536        false  thrpt    5  3296549.327 ± 107540.277  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000         1024                   65536         true  thrpt    5   630657.248 ±  13768.848  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000         1024                   16384        false  thrpt    5  3812078.996 ± 180304.337  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000         1024                   16384         true  thrpt    5   683035.620 ± 123785.908  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000         1024                   65536        false  thrpt    5  3631071.702 ± 807536.740  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000         1024                   65536         true  thrpt    5   699458.572 ±  82936.444  ops/s

Is there a sweet spot ? Go fishing for a sweet spot. Not much different at `valueSize=128`
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatchNative -p keySize="16" -p valueSize="128" -p numOpsPerBatch="1000" -p writeBatchAllocation="16384","32768" -p writeToDB="false","true"
```

Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          128                   16384        false  thrpt    5  8103640.977 ± 219823.845  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          128                   16384         true  thrpt    5   741601.971 ±  28977.595  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          128                   32768        false  thrpt    5  8193900.249 ± 110319.561  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          128                   32768         true  thrpt    5   724902.064 ±  38304.424  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          128                   16384        false  thrpt    5  7010472.637 ± 125612.331  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          128                   16384         true  thrpt    5   736644.331 ±  88418.678  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          128                   32768        false  thrpt    5  4325684.678 ± 915025.890  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          128                   32768         true  thrpt    5   740681.780 ±  25206.705  ops/s

Is this the sweet spot ? Closer..
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatchNative -p keySize="16" -p valueSize="512" -p numOpsPerBatch="1000" -p writeBatchAllocation="32768","65536" -p writeToDB="false","true"
```

Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          512                   32768        false  thrpt    5  4756041.130 ± 383170.481  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          512                   32768         true  thrpt    5   821957.334 ±  51372.767  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          512                   65536        false  thrpt    5  4248174.918 ± 179376.066  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          512                   65536         true  thrpt    5   798093.749 ±  60403.366  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          512                   32768        false  thrpt    5  2952858.776 ± 755579.916  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          512                   32768         true  thrpt    5   834461.957 ±  20883.789  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          512                   65536        false  thrpt    5  3182838.961 ± 291931.023  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          512                   65536         true  thrpt    5   888238.794 ±  39985.192  ops/s

Now we have applied a whole lot of best-case configuration to the database.
A big test:
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="128","512" -p keyCount="100000","10000000" -p numOpsPerBatch="20","1000" -p writeBatchAllocation="32768","131072" -p writeToDB="false","true"
```

Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score         Error  Units
WriteBatchBenchmarks.putWriteBatch              100000         16                20          128                   32768        false  thrpt    5  1994634.794 ±   98970.527  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16                20          128                   32768         true  thrpt    5   514232.031 ±   27834.158  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16                20          128                  131072        false  thrpt    5  1981257.750 ±   13983.908  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16                20          128                  131072         true  thrpt    5   521010.805 ±   11686.683  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16                20          512                   32768        false  thrpt    5  1918165.211 ±  108796.670  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16                20          512                   32768         true  thrpt    5   550648.830 ±   27126.774  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16                20          512                  131072        false  thrpt    5  2008651.071 ±   77468.602  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16                20          512                  131072         true  thrpt    5   558736.746 ±   10655.762  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16              1000          128                   32768        false  thrpt    5  1939349.744 ±   31049.902  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16              1000          128                   32768         true  thrpt    5   590596.591 ±   38011.067  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16              1000          128                  131072        false  thrpt    5  1930112.210 ±  391678.254  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16              1000          128                  131072         true  thrpt    5   591285.536 ±   19088.806  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16              1000          512                   32768        false  thrpt    5  1668076.301 ±   73832.520  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16              1000          512                   32768         true  thrpt    5   644228.748 ±    9481.671  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16              1000          512                  131072        false  thrpt    5  1719337.972 ±   81194.975  ops/s
WriteBatchBenchmarks.putWriteBatch              100000         16              1000          512                  131072         true  thrpt    5   690494.611 ±    8656.002  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20          128                   32768        false  thrpt    5  1947952.615 ±   43123.145  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20          128                   32768         true  thrpt    5   818239.819 ±   41303.163  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20          128                  131072        false  thrpt    5  2005488.645 ±   69185.114  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20          128                  131072         true  thrpt    5   792387.092 ±   44190.959  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20          512                   32768        false  thrpt    5  1873266.977 ±   48474.494  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20          512                   32768         true  thrpt    5   552352.339 ±   26475.092  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20          512                  131072        false  thrpt    5  1857514.639 ±   37303.285  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20          512                  131072         true  thrpt    5   550421.364 ±   42872.156  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16              1000          128                   32768        false  thrpt    5  1939598.959 ±   56764.691  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16              1000          128                   32768         true  thrpt    5  1065737.109 ±   27941.728  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16              1000          128                  131072        false  thrpt    5  1811106.410 ±  615865.203  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16              1000          128                  131072         true  thrpt    5  1018116.313 ±  116158.443  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16              1000          512                   32768        false  thrpt    5  1034195.705 ±  285669.652  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16              1000          512                   32768         true  thrpt    5   689779.135 ±   37873.546  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16              1000          512                  131072        false  thrpt    5  1208107.140 ±   46540.640  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16              1000          512                  131072         true  thrpt    5   670583.664 ±   62691.607  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16                20          128                   32768        false  thrpt    5  2184649.256 ±   46290.286  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16                20          128                   32768         true  thrpt    5   539101.860 ±    3772.644  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16                20          128                  131072        false  thrpt    5  2234285.181 ±   39813.095  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16                20          128                  131072         true  thrpt    5   534808.690 ±   12107.541  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16                20          512                   32768        false  thrpt    5  2142348.083 ±   71933.341  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16                20          512                   32768         true  thrpt    5   602473.848 ±   22841.184  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16                20          512                  131072        false  thrpt    5  2152143.835 ±   40259.672  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16                20          512                  131072         true  thrpt    5   602856.176 ±    9354.474  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000          128                   32768        false  thrpt    5  2061939.941 ±  190802.840  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000          128                   32768         true  thrpt    5   623982.622 ±   15404.956  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000          128                  131072        false  thrpt    5  2151470.460 ±   48008.599  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000          128                  131072         true  thrpt    5   628316.219 ±   10718.588  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000          512                   32768        false  thrpt    5  1588211.403 ±  251295.305  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000          512                   32768         true  thrpt    5   750807.191 ±   16909.818  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000          512                  131072        false  thrpt    5  1549081.387 ±  216800.341  ops/s
WriteBatchBenchmarks.putWriteBatchBB            100000         16              1000          512                  131072         true  thrpt    5   679292.499 ±   84631.804  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20          128                   32768        false  thrpt    5  2188388.149 ±   33673.295  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20          128                   32768         true  thrpt    5   631063.945 ±  100935.098  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20          128                  131072        false  thrpt    5  2165415.268 ±   91546.540  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20          128                  131072         true  thrpt    5   561631.876 ±  160817.987  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20          512                   32768        false  thrpt    5  2253022.848 ±   14015.634  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20          512                   32768         true  thrpt    5   378483.126 ±   85518.874  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20          512                  131072        false  thrpt    5  2157195.591 ±   61412.154  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20          512                  131072         true  thrpt    5   367320.025 ±   70011.913  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16              1000          128                   32768        false  thrpt    5  2052154.111 ±   49906.697  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16              1000          128                   32768         true  thrpt    5   615279.291 ±  148443.061  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16              1000          128                  131072        false  thrpt    5  2172964.306 ±   49687.379  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16              1000          128                  131072         true  thrpt    5   586300.535 ±   59300.387  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16              1000          512                   32768        false  thrpt    5  1494331.573 ±  187197.084  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16              1000          512                   32768         true  thrpt    5   416489.688 ±   22704.477  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16              1000          512                  131072        false  thrpt    5  1802256.282 ±  362698.353  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16              1000          512                  131072         true  thrpt    5   396553.679 ±   76027.459  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16                20          128                   32768        false  thrpt    5  3781659.264 ±  152500.747  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16                20          128                   32768         true  thrpt    5   359820.235 ±   15540.420  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16                20          128                  131072        false  thrpt    5  1313741.475 ±  300614.319  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16                20          128                  131072         true  thrpt    5   348148.412 ±   21147.683  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16                20          512                   32768        false  thrpt    5  4229468.390 ±  219930.290  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16                20          512                   32768         true  thrpt    5   397932.968 ±   23568.604  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16                20          512                  131072        false  thrpt    5  1417638.773 ±   67105.032  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16                20          512                  131072         true  thrpt    5   338535.199 ±   43533.490  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          128                   32768        false  thrpt    5  5596313.129 ± 1178308.172  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          128                   32768         true  thrpt    5   739468.246 ±   94415.618  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          128                  131072        false  thrpt    5  6807317.566 ± 1613781.390  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          128                  131072         true  thrpt    5   633522.013 ±  245470.111  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          512                   32768        false  thrpt    5  3957568.510 ±  303390.364  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          512                   32768         true  thrpt    5   584895.689 ±    9217.456  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          512                  131072        false  thrpt    5  3598593.918 ±   74908.624  ops/s
WriteBatchBenchmarks.putWriteBatchNative        100000         16              1000          512                  131072         true  thrpt    5   586780.507 ±   12641.339  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20          128                   32768        false  thrpt    5  4615269.228 ±  318100.340  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20          128                   32768         true  thrpt    5   640629.747 ±  119698.520  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20          128                  131072        false  thrpt    5  1914094.630 ±  112549.928  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20          128                  131072         true  thrpt    5   568790.760 ±  142617.827  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20          512                   32768        false  thrpt    5  4983283.529 ±  246944.198  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20          512                   32768         true  thrpt    5   424577.671 ±   67495.578  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20          512                  131072        false  thrpt    5  1620116.848 ±   53478.580  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20          512                  131072         true  thrpt    5   360530.864 ±   68454.597  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16              1000          128                   32768        false  thrpt    5  5009738.985 ±  735755.535  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16              1000          128                   32768         true  thrpt    5   856290.149 ±  161731.849  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16              1000          128                  131072        false  thrpt    5  6736300.140 ±  782076.151  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16              1000          128                  131072         true  thrpt    5   826444.545 ±  136177.407  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16              1000          512                   32768        false  thrpt    5  3506541.312 ±  194742.985  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16              1000          512                   32768         true  thrpt    5   502703.379 ±  318219.641  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16              1000          512                  131072        false  thrpt    5  3787360.442 ±  791200.685  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16              1000          512                  131072         true  thrpt    5   471563.304 ±  271530.914  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16                20          128                   32768        false  thrpt    5  1842261.340 ±  462071.324  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16                20          128                   32768         true  thrpt    5   427727.050 ±   36432.464  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16                20          128                  131072        false  thrpt    5   813510.072 ±   21987.697  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16                20          128                  131072         true  thrpt    5   346696.933 ±   20367.271  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16                20          512                   32768        false  thrpt    5  1974257.037 ±   74627.656  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16                20          512                   32768         true  thrpt    5   469822.293 ±   44361.742  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16                20          512                  131072        false  thrpt    5   816630.045 ±   20806.796  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16                20          512                  131072         true  thrpt    5   322099.511 ±   17895.851  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          128                   32768        false  thrpt    5  3869035.623 ±  167802.409  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          128                   32768         true  thrpt    5   620326.534 ±   22847.015  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          128                  131072        false  thrpt    5  3513794.639 ±  219317.140  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          128                  131072         true  thrpt    5   636879.682 ±  105699.070  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          512                   32768        false  thrpt    5  2842027.829 ±  865572.380  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          512                   32768         true  thrpt    5   629381.581 ±   87552.284  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          512                  131072        false  thrpt    5  3729089.180 ±  430030.920  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB      100000         16              1000          512                  131072         true  thrpt    5   585324.520 ±   29549.182  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20          128                   32768        false  thrpt    5  2009331.114 ±  160788.018  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20          128                   32768         true  thrpt    5   562034.802 ±   70601.167  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20          128                  131072        false  thrpt    5   814759.369 ±   22541.846  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20          128                  131072         true  thrpt    5   418832.627 ±   93855.322  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20          512                   32768        false  thrpt    5  1965273.494 ±  125303.462  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20          512                   32768         true  thrpt    5   378341.162 ±   58117.794  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20          512                  131072        false  thrpt    5   747196.706 ±  143417.239  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20          512                  131072         true  thrpt    5   235014.194 ±  121319.788  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16              1000          128                   32768        false  thrpt    5  2940783.025 ±  581900.599  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16              1000          128                   32768         true  thrpt    5   750611.997 ±  139231.725  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16              1000          128                  131072        false  thrpt    5  2957245.198 ±  287884.218  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16              1000          128                  131072         true  thrpt    5   698458.738 ±  103584.091  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16              1000          512                   32768        false  thrpt    5  1743183.590 ±  242724.948  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16              1000          512                   32768         true  thrpt    5   456460.481 ±  115072.852  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16              1000          512                  131072        false  thrpt    5  2444006.186 ±  253947.724  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16              1000          512                  131072         true  thrpt    5   455112.994 ±  143318.605  ops/s

Drilling down into this
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="128" -p keyCount="10000000" -p numOpsPerBatch="20" -p writeBatchAllocation="32768" -p writeToDB="false","true"
```

Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatch            10000000         16                20          128                   32768        false  thrpt    5  2018015.520 ±  71514.023  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20          128                   32768         true  thrpt    5   819463.972 ±  32667.016  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20          128                   32768        false  thrpt    5  2505762.104 ± 150404.906  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20          128                   32768         true  thrpt    5   882490.027 ±  74943.387  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20          128                   32768        false  thrpt    5  5936118.009 ± 224982.507  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20          128                   32768         true  thrpt    5  1061789.069 ±  55199.002  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20          128                   32768        false  thrpt    5  2214493.009 ± 381333.739  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20          128                   32768         true  thrpt    5   840295.162 ±  25139.519  ops/s

Doing reciprocals on the above, each put(16,128) in 32768 costs about 700ns to take from the buffer to disk.
Also notice that for these small value sizes, direct ByteBuffers are costly.

Bigger values ?
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="1024" -p keyCount="10000000" -p numOpsPerBatch="20" -p writeBatchAllocation="32768" -p writeToDB="false","true"
```
Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatch            10000000         16                20         1024                   32768        false  thrpt    5  2017404.119 ±  59861.488  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20         1024                   32768         true  thrpt    5   376290.865 ±  20218.388  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20         1024                   32768        false  thrpt    5  2440549.554 ± 100116.852  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20         1024                   32768         true  thrpt    5   395785.076 ±  19294.622  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20         1024                   32768        false  thrpt    5  5639723.952 ± 119895.896  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20         1024                   32768         true  thrpt    5   400665.870 ±  31826.773  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20         1024                   32768        false  thrpt    5  2067697.238 ± 293055.280  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20         1024                   32768         true  thrpt    5   374231.158 ±  61434.945  ops/s

The multiplier is now about 2.5, and there is something up with `putWriteBatchNativeBB`

Let's try it with bigger (again) values (and a correspondingly bigger buffer).

```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="8192" -p keyCount="10000000" -p numOpsPerBatch="20" -p writeBatchAllocation="252144" -p writeToDB="false","true"
```
Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatch            10000000         16                20         8192                  252144        false  thrpt    5  1234401.137 ±  80471.379  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20         8192                  252144         true  thrpt    5    93249.200 ±  13953.396  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20         8192                  252144        false  thrpt    5  1693440.397 ± 528137.756  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20         8192                  252144         true  thrpt    5   101030.443 ±  13005.551  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20         8192                  252144        false  thrpt    5  1032241.059 ±  47510.769  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20         8192                  252144         true  thrpt    5    96245.474 ±   6482.100  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20         8192                  252144        false  thrpt    5   444845.866 ±  27037.396  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20         8192                  252144         true  thrpt    5    91558.973 ±   1858.495  ops/s

By this point it is too much, and the native version is slower. That is because we are copying each 8K value into a batch, and then copying it again. The double copy costs more than the JNI transition.

Look for an intermediate value that is the break even point.
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="2048" -p keyCount="10000000" -p numOpsPerBatch="20" -p writeBatchAllocation="65536" -p writeToDB="false","true"
```
Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatch            10000000         16                20         2048                   65536        false  thrpt    5  1846631.237 ±  92639.225  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20         2048                   65536         true  thrpt    5   215601.016 ± 142715.826  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20         2048                   65536        false  thrpt    5  2439068.111 ± 100554.548  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20         2048                   65536         true  thrpt    5   229483.784 ±  16029.693  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20         2048                   65536        false  thrpt    5  3081924.039 ±  19579.361  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20         2048                   65536         true  thrpt    5   237511.658 ±  29012.205  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20         2048                   65536        false  thrpt    5  1356193.656 ±  69701.592  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20         2048                   65536         true  thrpt    5   222624.597 ± 110026.748  ops/s

Try 4K. It looks as if that is too much to buffer.
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="4096" -p keyCount="10000000" -p numOpsPerBatch="20" -p writeBatchAllocation="131072" -p writeToDB="false","true"
```
Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatch            10000000         16                20         4096                  131072        false  thrpt    5  1463367.133 ± 141985.505  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20         4096                  131072         true  thrpt    5   155953.245 ±  89574.450  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20         4096                  131072        false  thrpt    5  1851367.366 ±  53748.581  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20         4096                  131072         true  thrpt    5   159231.217 ± 173148.460  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20         4096                  131072        false  thrpt    5  1346049.102 ±  45539.848  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20         4096                  131072         true  thrpt    5   158061.800 ± 170511.768  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20         4096                  131072        false  thrpt    5   754858.475 ±  33532.008  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20         4096                  131072         true  thrpt    5   155553.572 ± 161342.629  ops/s

Post discussion w/Adam re what approach 2 really means. There are allocation problems with the bug buffers:
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="4096" -p keyCount="10000000" -p numOpsPerBatch="20" -p writeBatchAllocation="131072","1048576" -p writeToDB="false"
```
Benchmark                                   (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatch            10000000         16                20         4096                  131072        false  thrpt    5  1428966.369 ± 239204.719  ops/s
WriteBatchBenchmarks.putWriteBatch            10000000         16                20         4096                 1048576        false  thrpt    5  1443725.288 ± 100061.577  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20         4096                  131072        false  thrpt    5  1832802.918 ± 271788.384  ops/s
WriteBatchBenchmarks.putWriteBatchBB          10000000         16                20         4096                 1048576        false  thrpt    5  1785256.140 ± 147587.132  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20         4096                  131072        false  thrpt    5  1297397.023 ± 141812.134  ops/s
WriteBatchBenchmarks.putWriteBatchNative      10000000         16                20         4096                 1048576        false  thrpt    5   269969.262 ±  25532.216  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20         4096                  131072        false  thrpt    5   775264.705 ±  21106.839  ops/s
WriteBatchBenchmarks.putWriteBatchNativeBB    10000000         16                20         4096                 1048576        false  thrpt    5   128399.860 ±   9716.046  ops/s

Performance bugfix for native direct buffers, and re-run the test above (names have changed):
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="4096" -p keyCount="10000000" -p numOpsPerBatch="20" -p writeBatchAllocation="131072","1048576" -p writeToDB="false"
```

Benchmark                                  (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score        Error  Units
WriteBatchBenchmarks.putWriteBatchD          10000000         16                20         4096                  131072        false  thrpt    5  1745523.157 ±  59357.699  ops/s
WriteBatchBenchmarks.putWriteBatchD          10000000         16                20         4096                 1048576        false  thrpt    5  1728123.495 ±  22161.392  ops/s
WriteBatchBenchmarks.putWriteBatchI          10000000         16                20         4096                  131072        false  thrpt    5  1355974.869 ± 178671.448  ops/s
WriteBatchBenchmarks.putWriteBatchI          10000000         16                20         4096                 1048576        false  thrpt    5  1302243.857 ±  36220.245  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD    10000000         16                20         4096                  131072        false  thrpt    5  3209782.626 ± 983925.494  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD    10000000         16                20         4096                 1048576        false  thrpt    5  3866666.948 ± 527815.669  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI    10000000         16                20         4096                  131072        false  thrpt    5  1656172.994 ±  65982.655  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI    10000000         16                20         4096                 1048576        false  thrpt    5   608036.465 ± 148801.987  ops/s

It turns out that the *indirect* version (`putWriteBatchNativeI`) uses `JNI.GetByteArrayElements()` and that does a copy in the tests I am running; I have previously seen it not do
a copy. Might be a size thing ? If we use a critical section instead (and there are difficulties in general with this, see the JNI documentation etc) we get back the performance that has gone missing. This matters particularly because we are passing a large array, which we don't need the entirety of, so there is lots of unncessary copying.

```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p keySize="16" -p valueSize="4096" -p keyCount="10000000" -p numOpsPerBatch="20" -p writeBatchAllocation="131072","1048576" -p writeToDB="false"
```

Benchmark                                  (keyCount)  (keySize)  (numOpsPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt        Score         Error  Units
WriteBatchBenchmarks.putWriteBatchD          10000000         16                20         4096                  131072        false  thrpt    5  1749305.054 ±   31228.287  ops/s
WriteBatchBenchmarks.putWriteBatchD          10000000         16                20         4096                 1048576        false  thrpt    5  1739204.577 ±   34841.845  ops/s
WriteBatchBenchmarks.putWriteBatchI          10000000         16                20         4096                  131072        false  thrpt    5  1330461.139 ±   30573.622  ops/s
WriteBatchBenchmarks.putWriteBatchI          10000000         16                20         4096                 1048576        false  thrpt    5  1344626.672 ±   66659.690  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD    10000000         16                20         4096                  131072        false  thrpt    5  3654634.857 ±  792083.344  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD    10000000         16                20         4096                 1048576        false  thrpt    5  2978031.325 ±  562536.419  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI    10000000         16                20         4096                  131072        false  thrpt    5  3053646.626 ±  521910.691  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI    10000000         16                20         4096                 1048576        false  thrpt    5  3930996.978 ± 2093063.739  ops/s


Benchmark                                  (keyCount)  (keySize)  (totalBytesPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt      Score      Error  Units
WriteBatchBenchmarks.putWriteBatchD         100000000         16                786432           64                 1048576        false  thrpt    5    241.374 ±   30.992  ops/s
WriteBatchBenchmarks.putWriteBatchD         100000000         16                786432          128                 1048576        false  thrpt    5    424.038 ±   44.943  ops/s
WriteBatchBenchmarks.putWriteBatchD         100000000         16                786432          256                 1048576        false  thrpt    5    775.144 ±   19.402  ops/s
WriteBatchBenchmarks.putWriteBatchD         100000000         16                786432          512                 1048576        false  thrpt    5   1465.918 ±   46.305  ops/s
WriteBatchBenchmarks.putWriteBatchD         100000000         16                786432         1024                 1048576        false  thrpt    5   2787.693 ±   49.073  ops/s
WriteBatchBenchmarks.putWriteBatchD         100000000         16                786432         2048                 1048576        false  thrpt    5   5275.758 ±  112.121  ops/s
WriteBatchBenchmarks.putWriteBatchD         100000000         16                786432         4096                 1048576        false  thrpt    5   9001.684 ±  319.406  ops/s
WriteBatchBenchmarks.putWriteBatchD         100000000         16                786432         8192                 1048576        false  thrpt    5  16991.277 ± 1330.708  ops/s
WriteBatchBenchmarks.putWriteBatchD         100000000         16                786432        16384                 1048576        false  thrpt    5  24042.561 ± 6248.791  ops/s
WriteBatchBenchmarks.putWriteBatchI         100000000         16                786432           64                 1048576        false  thrpt    5    205.666 ±   22.067  ops/s
WriteBatchBenchmarks.putWriteBatchI         100000000         16                786432          128                 1048576        false  thrpt    5    351.582 ±    5.262  ops/s
WriteBatchBenchmarks.putWriteBatchI         100000000         16                786432          256                 1048576        false  thrpt    5    636.701 ±   24.462  ops/s
WriteBatchBenchmarks.putWriteBatchI         100000000         16                786432          512                 1048576        false  thrpt    5   1097.180 ±   83.034  ops/s
WriteBatchBenchmarks.putWriteBatchI         100000000         16                786432         1024                 1048576        false  thrpt    5   2023.310 ±   76.271  ops/s
WriteBatchBenchmarks.putWriteBatchI         100000000         16                786432         2048                 1048576        false  thrpt    5   3239.493 ±   98.232  ops/s
WriteBatchBenchmarks.putWriteBatchI         100000000         16                786432         4096                 1048576        false  thrpt    5   4622.377 ±  233.571  ops/s
WriteBatchBenchmarks.putWriteBatchI         100000000         16                786432         8192                 1048576        false  thrpt    5   5824.237 ±  540.594  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD   100000000         16                786432           64                 1048576        false  thrpt    5    382.390 ±   89.003  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD   100000000         16                786432          128                 1048576        false  thrpt    5    634.345 ±  189.679  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD   100000000         16                786432          256                 1048576        false  thrpt    5   1234.878 ±  230.826  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD   100000000         16                786432          512                 1048576        false  thrpt    5   2474.477 ±  306.164  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD   100000000         16                786432         1024                 1048576        false  thrpt    5   6022.944 ± 1799.616  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD   100000000         16                786432         2048                 1048576        false  thrpt    5   9459.141 ± 2052.980  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD   100000000         16                786432         4096                 1048576        false  thrpt    5  15193.353 ±  189.104  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD   100000000         16                786432         8192                 1048576        false  thrpt    5  18030.736 ±  551.071  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD   100000000         16                786432        16384                 1048576        false  thrpt    5  18423.381 ±  217.163  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI   100000000         16                786432           64                 1048576        false  thrpt    5    443.215 ±   19.601  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI   100000000         16                786432          128                 1048576        false  thrpt    5    814.434 ±   76.677  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI   100000000         16                786432          256                 1048576        false  thrpt    5   1473.472 ±  135.858  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI   100000000         16                786432          512                 1048576        false  thrpt    5   2866.096 ±  173.726  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI   100000000         16                786432         1024                 1048576        false  thrpt    5   5285.291 ± 1011.674  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI   100000000         16                786432         2048                 1048576        false  thrpt    5   8916.047 ± 2458.490  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI   100000000         16                786432         4096                 1048576        false  thrpt    5  16421.919 ±  430.594  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI   100000000         16                786432         8192                 1048576        false  thrpt    5  19099.258 ±  662.897  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI   100000000         16                786432        16384                 1048576        false  thrpt    5  20908.435 ±  713.644  ops/s

Bigger values
```
java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar WriteBatchBenchmarks.putWriteBatch -p valueSize="16384","32768","65536"
```

Benchmark                                  (keyCount)  (keySize)  (totalBytesPerBatch)  (valueSize)  (writeBatchAllocation)  (writeToDB)   Mode  Cnt      Score       Error  Units
WriteBatchBenchmarks.putWriteBatchD         100000000         16                786432        16384                 1048576        false  thrpt    5  24017.065 ±  3746.162  ops/s
WriteBatchBenchmarks.putWriteBatchD         100000000         16                786432        32768                 1048576        false  thrpt    5  32141.105 ±  7691.849  ops/s
WriteBatchBenchmarks.putWriteBatchD         100000000         16                786432        65536                 1048576        false  thrpt    5  36123.681 ± 10101.056  ops/s
WriteBatchBenchmarks.putWriteBatchI         100000000         16                786432        65536                 1048576        false  thrpt    5   3914.889 ±   321.617  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD   100000000         16                786432        16384                 1048576        false  thrpt    5  21102.966 ±   831.320  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD   100000000         16                786432        32768                 1048576        false  thrpt    5  23948.615 ±   707.475  ops/s
WriteBatchBenchmarks.putWriteBatchNativeD   100000000         16                786432        65536                 1048576        false  thrpt    5  27335.158 ±  1193.039  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI   100000000         16                786432        16384                 1048576        false  thrpt    5  21187.936 ±  1002.761  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI   100000000         16                786432        32768                 1048576        false  thrpt    5  24116.338 ±  1273.320  ops/s
WriteBatchBenchmarks.putWriteBatchNativeI   100000000         16                786432        65536                 1048576        false  thrpt    5  28444.625 ±  1482.244  ops/s

