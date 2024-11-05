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

