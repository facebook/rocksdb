package org.rocksdb.jmh;
import static org.rocksdb.util.KVUtils.ba;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.rocksdb.*;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
public class WriteBatchBenchmarks {
  @Setup(Level.Trial)
  public void setup() throws Exception {
    RocksDB.loadLibrary();
  }

  private static final int N = 10_000;

  private Path dbPath;
  private RocksDB rocksDB;
  private DBOptions dbOptions;
  private ColumnFamilyHandle[] cfHandles;
  private byte[] key = new byte[] {'k', 'e', 'y', '\0', '\0', '\0', '\0'};
  private byte[] value = new byte[] {'v', 'a', 'l', 'u', 'e', '\0', '\0', '\0', '\0'};

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
  public void closeDb() throws IOException {
    Arrays.stream(cfHandles).forEach(ColumnFamilyHandle::close);
    rocksDB.close();
    dbOptions.close();

    Files.walk(dbPath).sorted(Comparator.reverseOrder()).forEach(x -> {
      try {
        Files.delete(x);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    Files.deleteIfExists(dbPath);
  }

  @Benchmark
  @OperationsPerInvocation(N)
  public void putWriteBatch() throws RocksDBException {
    WriteBatch wb = new WriteBatch();
    try {
      for (int i = 0; i < N; i++) {
        wb.put(ba("key" + i), ba("value" + i));
      }
    } finally {
      wb.close();
    }
  }

  @Benchmark
  @OperationsPerInvocation(N)
  public void putWriteBatchByteArray() throws RocksDBException {
    WriteBatch wb = new WriteBatch();
    try {
      for (int i = 0; i < N; i++) {
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

  @Benchmark
  @OperationsPerInvocation(N)
  public void putWriteBatchCF() throws RocksDBException {
    WriteBatch wb = new WriteBatch();
    try {
      for (int i = 0; i < N; i++) {
        wb.put(cfHandles[i % 2], ba("key" + i), ba("value" + i));
      }
    } finally {
      wb.close();
    }
  }

  @Benchmark
  @OperationsPerInvocation(N)
  public void putWriteBatchNative() throws RocksDBException {
    WriteBatchJavaNative wb = new WriteBatchJavaNative();
    try {
      for (int i = 0; i < N; i++) {
        wb.put(ba("key" + i), ba("value" + i));
      }
      wb.flush();
    } finally {
      wb.close();
    }
  }

  @Benchmark
  @OperationsPerInvocation(N)
  public void putWriteBatchNativeByteArray() throws RocksDBException {
    WriteBatchJavaNative wb = new WriteBatchJavaNative();
    try {
      for (int i = 0; i < N; i++) {
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

  @Benchmark
  @OperationsPerInvocation(N)
  public void putWriteBatchNativeCF() throws RocksDBException {
    WriteBatchJavaNative wb = new WriteBatchJavaNative();
    try {
      for (int i = 0; i < N; i++) {
        wb.put(cfHandles[i % 2], ba("key" + i), ba("value" + i));
      }
      wb.flush();
    } finally {
      wb.close();
    }
  }
}
