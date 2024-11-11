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

  @Param({"100000"}) int keyCount;

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
    @Param({"1000"}) int numOpsPerBatch;

    @Param({"131072"}) int writeBatchAllocation;

    @Param({"true", "false"}) boolean writeToDB;

    final AtomicInteger opIndex = new AtomicInteger();

    TBatch writeBatch;

    RocksDB rocksDB;

    public abstract void startBatch();

    public abstract void stopBatch() throws RocksDBException;

    public void put(final byte[] key, final byte[] value) throws RocksDBException {
      int index = opIndex.getAndIncrement();
      if (index % numOpsPerBatch == 0) {
        startBatch();
      }
      writeBatch.put(key, value);
      if ((index + 1) % numOpsPerBatch == 0) {
        stopBatch();
      }
    }

    public void put(final ByteBuffer key, final ByteBuffer value) throws RocksDBException {
      int index = opIndex.getAndIncrement();
      if (index % numOpsPerBatch == 0) {
        startBatch();
      }
      writeBatch.put(key, value);
      if ((index + 1) % numOpsPerBatch == 0) {
        stopBatch();
      }
    }

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
    @Override
    public void startBatch() {
      writeBatch = new WriteBatch(writeBatchAllocation);
    }

    @Override
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
    @Override
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
    @Override
    public void startBatch() {
      writeBatch = WriteBatchJavaNative.allocate(writeBatchAllocation);
    }
  }

  @State(Scope.Thread)
  public static class WriteBatchThreadNativeDirect extends WriteBatchThreadNative {
    @Override
    public void startBatch() {
      writeBatch = WriteBatchJavaNative.allocateDirect(writeBatchAllocation);
    }
  }

  public static class BaseData {
    @Param({"16", "64", "256"}) int keySize;
    @Param({"16", "1024", "65536"}) int valueSize;
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
      List<ColumnFamilyDescriptor> descriptors =
          List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
              new ColumnFamilyDescriptor(
                  "cf2".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
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
  public void putWriteBatch(WriteBatchThreadDefault batch, ByteArrayData data)
      throws RocksDBException {
    long i = index.getAndIncrement() % keyCount;

    baFillValue(data.key, "key", i, KEY_VALUE_MAX_WIDTH, (byte) '0');
    baFillValue(data.value, "value", i, KEY_VALUE_MAX_WIDTH, (byte) '0');

    batch.put(data.key, data.value);
  }

  @Benchmark
  public void putWriteBatchNative(WriteBatchThreadNativeArray batch, ByteArrayData data)
      throws RocksDBException {
    long i = index.getAndIncrement() % keyCount;

    baFillValue(data.key, "key", i, KEY_VALUE_MAX_WIDTH, (byte) '0');
    baFillValue(data.value, "value", i, KEY_VALUE_MAX_WIDTH, (byte) '0');

    batch.put(data.key, data.value);
  }

  @Benchmark
  public void putWriteBatchBB(WriteBatchThreadDefault batch, ByteBufferData data)
      throws RocksDBException {
    long i = index.getAndIncrement() % keyCount;

    bbFillValue(data.key, "key", i, KEY_VALUE_MAX_WIDTH, data.fill);
    bbFillValue(data.value, "value", i, KEY_VALUE_MAX_WIDTH, data.fill);

    batch.put(data.key, data.value);
  }

  @Benchmark
  public void putWriteBatchNativeBB(WriteBatchThreadNativeDirect batch, ByteBufferData data)
      throws RocksDBException {
    long i = index.getAndIncrement() % keyCount;

    bbFillValue(data.key, "key", i, KEY_VALUE_MAX_WIDTH, data.fill);
    bbFillValue(data.value, "value", i, KEY_VALUE_MAX_WIDTH, data.fill);

    batch.put(data.key, data.value);
  }

  public static void main(String[] args) throws RunnerException {
    org.openjdk.jmh.runner.options.Options opt =
        new OptionsBuilder().include("WriteBatchBenchmarks.putWriteBatchNativeBB").forks(0).build();

    new Runner(opt).run();
  }
}
