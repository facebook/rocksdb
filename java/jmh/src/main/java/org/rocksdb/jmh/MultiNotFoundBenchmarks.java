package org.rocksdb.jmh;

import static org.rocksdb.util.KVUtils.ba;
import static org.rocksdb.util.KVUtils.keys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.rocksdb.*;
import org.rocksdb.util.FileUtils;

@State(Scope.Thread)
public class MultiNotFoundBenchmarks {
  @Param({"no_column_family", "20_column_families"}) String columnFamilyTestType;

  @Param({"100000"}) int keyCount;

  /**
   * Don't create every n-th key
   * Usually, just use "2" for making the
   */
  @Param({"2", "16"}) int nthMissingKey;

  @Param({
      "10",
      "100",
      "1000",
      "10000",
  })
  int multiGetSize;

  @Param({"16"}) int valueSize;

  @Param({"16"}) int keySize; // big enough

  Path dbDir;
  DBOptions options;
  int cfs = 0; // number of column families
  private AtomicInteger cfHandlesIdx;
  ColumnFamilyHandle[] cfHandles;
  RocksDB db;
  private final AtomicInteger keyIndex = new AtomicInteger();

  private List<ColumnFamilyHandle> defaultCFHandles = new ArrayList<>();

  @Setup(Level.Trial)
  public void setup() throws IOException, RocksDBException {
    RocksDB.loadLibrary();

    dbDir = Files.createTempDirectory("rocksjava-multiget-benchmarks");

    options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);

    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

    if ("20_column_families".equals(columnFamilyTestType)) {
      cfs = 20;
    }

    if (cfs > 0) {
      cfHandlesIdx = new AtomicInteger(1);
      for (int i = 1; i <= cfs; i++) {
        cfDescriptors.add(new ColumnFamilyDescriptor(ba("cf" + i)));
      }
    }

    final List<ColumnFamilyHandle> cfHandlesList = new ArrayList<>(cfDescriptors.size());
    db = RocksDB.open(options, dbDir.toAbsolutePath().toString(), cfDescriptors, cfHandlesList);
    cfHandles = cfHandlesList.toArray(new ColumnFamilyHandle[0]);

    // store initial data for retrieving via get
    for (int j = 0; j < keyCount; j++) {
      if (j % nthMissingKey != 0) {
        final byte[] paddedValue = Arrays.copyOf(ba("value" + j), valueSize);
        db.put(ba("key" + j), paddedValue);
      }
    }

    // store initial data for retrieving via get - column families
    for (int i = 0; i < cfs; i++) {
      for (int j = 0; j < keyCount; j++) {
        if (j % nthMissingKey != 0) {
          final byte[] paddedValue = Arrays.copyOf(ba("value" + j), valueSize);
          db.put(cfHandles[i], ba("key" + j), paddedValue);
        }
      }
    }

    // build a big list of default column families for efficient passing
    final ColumnFamilyHandle defaultCFH = db.getDefaultColumnFamily();
    for (int i = 0; i < keyCount; i++) {
      defaultCFHandles.add(defaultCFH);
    }

    // list of random cfs
    try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
      db.flush(flushOptions);
    }
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    for (final ColumnFamilyHandle cfHandle : cfHandles) {
      cfHandle.close();
    }
    db.close();
    options.close();
    FileUtils.delete(dbDir);
  }

  private int next(final int inc, final int limit) {
    int idx;
    int nextIdx;
    while (true) {
      idx = keyIndex.get();
      nextIdx = idx + inc;
      if (nextIdx >= limit) {
        nextIdx = inc;
      }

      if (keyIndex.compareAndSet(idx, nextIdx)) {
        break;
      }
    }

    if (nextIdx >= limit) {
      return -1;
    } else {
      return idx;
    }
  }

  ByteBuffer keysBuffer;
  ByteBuffer valuesBuffer;

  List<ByteBuffer> valueBuffersList;
  List<ByteBuffer> keyBuffersList;

  @Setup
  public void allocateSliceBuffers() {
    keysBuffer = ByteBuffer.allocateDirect(keyCount * keySize);
    valuesBuffer = ByteBuffer.allocateDirect(keyCount * valueSize);
    valueBuffersList = new ArrayList<>();
    keyBuffersList = new ArrayList<>();
    for (int i = 0; i < keyCount; i++) {
      valueBuffersList.add(valuesBuffer.slice(i * valueSize, valueSize));
      keyBuffersList.add(keysBuffer.slice(i * keySize, keySize));
    }
  }

  @TearDown
  public void freeSliceBuffers() {
    valueBuffersList.clear();
  }

  private List<byte[]> filter(List<byte[]> before, final int initial, final int step) {
    final List<byte[]> after = new ArrayList<>(before.size());
    for (int i = initial; i < before.size(); i += step) {
      after.add(before.get(i));
    }
    return after;
  }

  private List<ByteBuffer> filterByteBuffers(
      List<ByteBuffer> before, final int initial, final int step) {
    final List<ByteBuffer> after = new ArrayList<>(before.size());
    for (int i = initial; i < before.size(); i += step) {
      after.add(before.get(i));
    }
    return after;
  }

  /**
   * Perform get on the even-numbered keys, which should not exist
   * This should be faster (certainly not slower) than the even-numbered keys,
   * but has at times been found to be slower due to implementation issues in the not-found path.
   *
   * @throws RocksDBException
   */
  @Benchmark
  public void multiNotFoundListEvens() throws RocksDBException {
    final int fromKeyIdx = next(2 * multiGetSize, keyCount);
    if (fromKeyIdx >= 0) {
      final List<byte[]> keys = filter(keys(fromKeyIdx, fromKeyIdx + 2 * multiGetSize), 0, 2);
      final List<byte[]> valueResults = db.multiGetAsList(keys);
      for (final byte[] result : valueResults) {
        if (result != null)
          throw new RuntimeException("Test wrongly returned a value");
      }
    }
  }

  /**
   * Perform get on the odd-numbered keys, which should exist
   * This is for reference comparison w/ non-existent keys, see above
   *
   * @throws RocksDBException
   */
  @Benchmark
  public void multiNotFoundListOdds() throws RocksDBException {
    final int fromKeyIdx = next(2 * multiGetSize, keyCount);
    if (fromKeyIdx >= 0) {
      final List<byte[]> keys = filter(keys(fromKeyIdx, fromKeyIdx + 2 * multiGetSize), 1, 2);
      final List<byte[]> valueResults = db.multiGetAsList(keys);
      for (final byte[] result : valueResults) {
        if (result.length != valueSize)
          throw new RuntimeException("Test valueSize assumption wrong");
      }
    }
  }

  @Benchmark
  public List<byte[]> multiNotFoundBBEvens() throws RocksDBException {
    final int fromKeyIdx = next(2 * multiGetSize, keyCount);
    if (fromKeyIdx >= 0) {
      final List<ByteBuffer> keys =
          filterByteBuffers(keys(keyBuffersList, fromKeyIdx, fromKeyIdx + 2 * multiGetSize), 0, 2);
      final List<ByteBuffer> values =
          valueBuffersList.subList(fromKeyIdx, fromKeyIdx + multiGetSize);
      final List<ByteBufferGetStatus> statusResults = db.multiGetByteBuffers(keys, values);
      for (final ByteBufferGetStatus result : statusResults) {
        if (result.status.getCode() != Status.Code.NotFound)
          throw new RuntimeException("Test status should be NotFound, was: " + result.status);
      }
    }
    return new ArrayList<>();
  }

  @Benchmark
  public List<byte[]> multiNotFoundBBOdds() throws RocksDBException {
    final int fromKeyIdx = next(2 * multiGetSize, keyCount);
    if (fromKeyIdx >= 0) {
      final List<ByteBuffer> keys =
          filterByteBuffers(keys(keyBuffersList, fromKeyIdx, fromKeyIdx + 2 * multiGetSize), 1, 2);
      final List<ByteBuffer> values =
          valueBuffersList.subList(fromKeyIdx, fromKeyIdx + multiGetSize);
      final List<ByteBufferGetStatus> statusResults = db.multiGetByteBuffers(keys, values);
      for (final ByteBufferGetStatus result : statusResults) {
        if (result.status.getCode() != Status.Code.Ok)
          throw new RuntimeException("Test status not OK: " + result.status);
        if (result.value.limit() != valueSize)
          throw new RuntimeException("Test valueSize assumption wrong");
      }
    }
    return new ArrayList<>();
  }

  public static void main(final String[] args) throws RunnerException {
    final org.openjdk.jmh.runner.options.Options opt =
        new OptionsBuilder()
            .include(MultiNotFoundBenchmarks.class.getSimpleName())
            .forks(1)
            .jvmArgs("-ea")
            .warmupIterations(1)
            .measurementIterations(2)
            .forks(2)
            .param("columnFamilyTestType=", "no_column_family")
            .param("multiGetSize=", "1000")
            .param("keyCount=", "10000")
            .param("nthMissingKey=", "2")
            .output("jmh_output")
            .build();

    new Runner(opt).run();
  }
}
