package org.rocksdb.jmh;

import org.openjdk.jmh.annotations.*;
import org.rocksdb.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.rocksdb.util.KVUtils.ba;

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
public class WriteBatchBenchmarks {
  @Setup(Level.Trial)
  public void setup() throws Exception {
    RocksDB.loadLibrary();
  }

  @Param ({"10000"}) int numOpsPerFlush;
  //@Param({"8", "16", "64", "256"}) int keySize;

  //@Param({"16", "64", "256", "1024", "65536"}) int valueSize;


  private Path dbPath;
  private RocksDB rocksDB;
  private DBOptions dbOptions;
  private ColumnFamilyHandle[] cfHandles;

  private final AtomicLong index = new AtomicLong(0);

  @State(Scope.Thread)
  public static class ThreadState {
    private WriteBatch writeBatchBaseline;

    private WriteBatchJavaNative writeBatchJavaNative;
  }

  private final byte[] key = new byte[] {'k', 'e', 'y', '\0', '\0', '\0', '\0'};
  private final byte[] value = new byte[] {'v', 'a', 'l', 'u', 'e', '\0', '\0', '\0', '\0'};

  @Setup(Level.Iteration)
  public void createCreateDb() throws IOException, RocksDBException {
    dbPath = Files.createTempDirectory("JMH").toAbsolutePath();
    System.out.println("temp dir: " + dbPath);
    List<ColumnFamilyDescriptor> descriptors =
        List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
            new ColumnFamilyDescriptor("cf2".getBytes(StandardCharsets.UTF_8)));
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>(2);
    dbOptions = new DBOptions();
    dbOptions.setCreateIfMissing(true);
    dbOptions.setCreateMissingColumnFamilies(true);
    rocksDB = RocksDB.open(dbOptions, dbPath.toString(), descriptors, cfHandles);
    this.cfHandles = cfHandles.toArray(new ColumnFamilyHandle[cfHandles.size()]);
  }

  @TearDown(Level.Iteration)
  public void closeDb(ThreadState threadState) throws IOException {
    Arrays.stream(cfHandles).forEach(ColumnFamilyHandle::close);
    rocksDB.close();
    dbOptions.close();

    if (threadState.writeBatchBaseline != null) {
      threadState.writeBatchBaseline.close();
      threadState.writeBatchBaseline = null;
    }

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
  public void putWriteBatch(ThreadState threadState) throws RocksDBException {

    long keyIndex = index.getAndIncrement() % numOpsPerFlush;

    if (keyIndex == 0) {
      threadState.writeBatchBaseline = new WriteBatch();
    }

    threadState.writeBatchBaseline.put(ba("key" + keyIndex), ba("value" + keyIndex));

    if (keyIndex == numOpsPerFlush -1) {
      threadState.writeBatchBaseline.close();
    }
  }

  @Benchmark
  public void putWriteBatchNative(ThreadState threadState) throws RocksDBException {

    long keyIndex = index.getAndIncrement() % numOpsPerFlush;
    if (keyIndex == 0) {
      threadState.writeBatchJavaNative = new WriteBatchJavaNative();
    }

    threadState.writeBatchJavaNative.put(ba("key" + keyIndex), ba("value" + keyIndex));

    if (keyIndex == numOpsPerFlush -1) {
      threadState.writeBatchJavaNative.flush();
      threadState.writeBatchJavaNative.close();
    }
  }

  //@Benchmark
  public void putWriteBatchByteArray() throws RocksDBException {
    WriteBatch wb = new WriteBatch();
    try {
      for (int i = 0; i < numOpsPerFlush; i++) {
        key[3] = (byte) i;
        key[4] = (byte) (i >>> 8);
        key[5] = (byte) (i >>> 16);
        key[6] = (byte) (i >>> 24);

        value[5] = (byte) i;
        value[6] = (byte) (i >>> 8);
        value[7] = (byte) (i >>> 16);
        value[8] = (byte) (i >>> 24);

        wb.put(key, value);
      }
    } finally {
      wb.close();
    }
  }

  //@Benchmark
  public void putWriteBatchCF() throws RocksDBException {
    WriteBatch wb = new WriteBatch();
    try {
      for (int i = 0; i < numOpsPerFlush; i++) {
        wb.put(cfHandles[i % 2], ba("key" + i), ba("value" + i));
      }
    } finally {
      wb.close();
    }
  }

  //@Benchmark
  public void putWriteBatchNativeByteArray() throws RocksDBException {
    WriteBatchJavaNative wb = new WriteBatchJavaNative();
    try {
      for (int i = 0; i < numOpsPerFlush; i++) {
        key[3] = (byte) i;
        key[4] = (byte) (i >>> 8);
        key[5] = (byte) (i >>> 16);
        key[6] = (byte) (i >>> 24);

        value[5] = (byte) i;
        value[6] = (byte) (i >>> 8);
        value[7] = (byte) (i >>> 16);
        value[8] = (byte) (i >>> 24);

        wb.put(key, value);
      }
      wb.flush();
    } finally {
      wb.close();
    }
  }

  //@Benchmark
  public void putWriteBatchNativeCF() throws RocksDBException {
    WriteBatchJavaNative wb = new WriteBatchJavaNative();
    try {
      for (int i = 0; i < numOpsPerFlush; i++) {
        wb.put(cfHandles[i % 2], ba("key" + i), ba("value" + i));
      }
      wb.flush();
    } finally {
      wb.close();
    }
  }
}
