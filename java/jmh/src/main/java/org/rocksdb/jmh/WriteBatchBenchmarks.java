package org.rocksdb.jmh;

import static org.rocksdb.util.KVUtils.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.rocksdb.*;

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
public class WriteBatchBenchmarks {
  @Setup(Level.Trial)
  public void setup() throws Exception {
    RocksDB.loadLibrary();
  }

  @Param({"100000000"}) int keyCount;

  // maximum number of bytes needed to encode numbers (key or value) so we can pad to size and
  // layout easily
  static final int KEY_VALUE_MAX_WIDTH = 10;

  private Path dbPath;
  private RocksDB rocksDB;
  private DBOptions dbOptions;
  private ColumnFamilyHandle[] cfHandles;

  private final AtomicLong index = new AtomicLong(0);

  /**
   * Base class for thread state of write batch tests
   *
   * This class is responsible for deciding when a batch should be closed/opened
   * by counting the number of operations performed on it.
   */
  public abstract static class WriteBatchThreadBase<TBatch extends WriteBatchInterface> {

    // 3/4 of writeBatchAllocation
    // which leaves a bit of spare space in the allocated buffer, for key, overhead
    // - this just counts value.size()
    @Param({"786432"}) int totalBytesPerBatch;

    @Param({"1048576"}) int writeBatchAllocation;
    @Param({"50000"}) int bufferedPutLimit;

    @Param({"false"}) boolean writeToDB;

    TBatch writeBatch;

    RocksDB rocksDB;

    @Setup
    public void setup(WriteBatchBenchmarks bm) {
      this.rocksDB = bm.rocksDB;
    }
  }

  /**
   * Holds a standard (cross JNI at every operation) write batch
   */
  @State(Scope.Thread)
  public static class WriteBatchThreadDefault extends WriteBatchThreadBase<WriteBatch> {

    public void startBatch() {
      writeBatch = new WriteBatch(writeBatchAllocation);
    }

    public void stopBatch() throws RocksDBException {
      if (writeBatch != null) {
        try {
          if (writeToDB) {
            rocksDB.write(new WriteOptions(), writeBatch);
          }
        } finally {
          writeBatch.close();
          writeBatch = null;
        }
      }
    }

    @TearDown
    public void teardown() throws RocksDBException {
      stopBatch();
    }
  }

  /**
   * Holds a JavaNative (buffer ops on the Java side) write batch
   */
  public abstract static class WriteBatchThreadNative
      extends WriteBatchThreadBase<WriteBatchJavaNative> {

    public void stopBatch() throws RocksDBException {
      if (writeBatch != null) {
        try {
          if (writeToDB) {
            rocksDB.write(new WriteOptions(), writeBatch);
          }
        } finally {
          writeBatch.close();
          writeBatch = null;
        }
      }
    }

    @TearDown
    public void teardown() throws RocksDBException {
      stopBatch();
    }
  }

  @State(Scope.Thread)
  public static class WriteBatchThreadNativeArray extends WriteBatchThreadNative {

    public void startBatch() {
      writeBatch = WriteBatchJavaNative.allocate(writeBatchAllocation, bufferedPutLimit);
    }
  }

  @State(Scope.Thread)
  public static class WriteBatchThreadNativeDirect extends WriteBatchThreadNative {

    public void startBatch() {
      writeBatch = WriteBatchJavaNative.allocateDirect(writeBatchAllocation, bufferedPutLimit);
    }
  }

  public static class BaseData {
    @Param({"16"}) int keySize;
    @Param({"64", "128", "256", "512", "1024", "2048", "4096", "8192", "16384"}) int valueSize;
  }

  @State(Scope.Thread)
  public static class ByteArrayData extends BaseData {
    private byte[] key;
    private byte[] value;

    @Setup
    public void setup() {
      key = new byte[keySize];
      value = new byte[valueSize];
    }
  }

  @State(Scope.Thread)
  public static class ByteBufferData extends BaseData {
    private ByteBuffer key;
    private ByteBuffer value;

    private final byte[] fill = new byte[KEY_VALUE_MAX_WIDTH];

    @Setup
    public void setup() {
      key = ByteBuffer.allocateDirect(keySize);
      value = ByteBuffer.allocateDirect(valueSize);

      Arrays.fill(fill, (byte) '0');
    }
  }

  @Setup(Level.Trial)
  public void createDb() throws IOException, RocksDBException {
    dbPath = Files.createTempDirectory("JMH").toAbsolutePath();
    System.out.println("temp dir: " + dbPath);
    try (ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()) {
      columnFamilyOptions //.setCompressionType(CompressionType.LZ4_COMPRESSION)
          .setLevelCompactionDynamicLevelBytes(true);
      List<ColumnFamilyDescriptor> descriptors = List.of(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
          new ColumnFamilyDescriptor("cf2".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
      List<ColumnFamilyHandle> cfHandles = new ArrayList<>(2);
      BlockBasedTableConfig tableFormatConfig = new BlockBasedTableConfig();
      tableFormatConfig.setBlockSize(16384L)
          .setCacheIndexAndFilterBlocks(true)
          .setPinL0FilterAndIndexBlocksInCache(true);
      Options options = new Options();
      options.setTableFormatConfig(tableFormatConfig);
      dbOptions = new DBOptions(options)
                      .setCreateIfMissing(true)
                      .setCreateMissingColumnFamilies(true)
                      .setBytesPerSync(1048576)
                      .setMaxBackgroundJobs(6);
      rocksDB = RocksDB.open(dbOptions, dbPath.toString(), descriptors, cfHandles);
      this.cfHandles = cfHandles.toArray(new ColumnFamilyHandle[0]);
    }
  }

  @TearDown(Level.Trial)
  public void closeDb() throws IOException {
    Arrays.stream(cfHandles).forEach(ColumnFamilyHandle::close);
    rocksDB.close();
    dbOptions.close();

    try (var files = Files.walk(dbPath).sorted(Comparator.reverseOrder())) {
      files.forEach(file -> {
        try {
          Files.delete(file);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }

    Files.deleteIfExists(dbPath);
  }

  @Benchmark
  public void putWriteBatchI(WriteBatchThreadDefault batch, ByteArrayData data)
      throws RocksDBException {

    batch.startBatch();
    long opBytes = 0L;
    while (opBytes < batch.totalBytesPerBatch) {

      long i = index.getAndIncrement() % keyCount;

      baFillValue(data.key, "key", i, KEY_VALUE_MAX_WIDTH, (byte) '0');
      baFillValue(data.value, "value", i, KEY_VALUE_MAX_WIDTH, (byte) '0');

      batch.writeBatch.put(data.key, data.value);

      opBytes += data.keySize + data.valueSize;
    }
    batch.stopBatch();
  }

  @Benchmark
  public void putWriteBatchNativeI(WriteBatchThreadNativeArray batch, ByteArrayData data)
      throws RocksDBException {

    batch.startBatch();
    long opBytes = 0L;
    while (opBytes < batch.totalBytesPerBatch) {
      long i = index.getAndIncrement() % keyCount;

      baFillValue(data.key, "key", i, KEY_VALUE_MAX_WIDTH, (byte) '0');
      baFillValue(data.value, "value", i, KEY_VALUE_MAX_WIDTH, (byte) '0');

      batch.writeBatch.put(data.key, data.value);

      opBytes += data.keySize + data.valueSize;
    }
    batch.stopBatch();
  }

  @Benchmark
  public void putWriteBatchD(WriteBatchThreadDefault batch, ByteBufferData data)
      throws RocksDBException {

    batch.startBatch();
    long opBytes = 0L;
    while (opBytes < batch.totalBytesPerBatch) {

      long i = index.getAndIncrement() % keyCount;

      bbFillValue(data.key, "key", i, KEY_VALUE_MAX_WIDTH, data.fill);
      bbFillValue(data.value, "value", i, KEY_VALUE_MAX_WIDTH, data.fill);

      batch.writeBatch.put(data.key, data.value);

      opBytes += data.keySize + data.valueSize;
    }
    batch.stopBatch();
  }

  @Benchmark
  public void putWriteBatchNativeD(WriteBatchThreadNativeDirect batch, ByteBufferData data)
      throws RocksDBException {

    batch.startBatch();
    long opBytes = 0L;
    while (opBytes < batch.totalBytesPerBatch) {

      long i = index.getAndIncrement() % keyCount;

      bbFillValue(data.key, "key", i, KEY_VALUE_MAX_WIDTH, data.fill);
      bbFillValue(data.value, "value", i, KEY_VALUE_MAX_WIDTH, data.fill);

      batch.writeBatch.put(data.key, data.value);

      opBytes += data.keySize + data.valueSize;
    }
    batch.stopBatch();
  }

  public static void main(String[] args) throws RunnerException {
    org.openjdk.jmh.runner.options.Options opt =
        new OptionsBuilder()
            .param("valueSize", "16384")
    .include("WriteBatchBenchmarks.putWriteBatchI").forks(0).build();

    new Runner(opt).run();
  }
}
