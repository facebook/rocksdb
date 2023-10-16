/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 *  This source code is licensed under both the GPLv2 (found in the
 *  COPYING file in the root directory) and Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory).
 */
package org.rocksdb.jmh;

import static org.rocksdb.util.KVUtils.ba;
import static org.rocksdb.util.KVUtils.writeToByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.*;
import org.rocksdb.*;
import org.rocksdb.util.FileUtils;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 3, time = 10)
public class WriteBatchBenchmarks {
  @Setup(Level.Trial)
  public void setup() throws IOException, RocksDBException {
    RocksDB.loadLibrary();
  }

  private static final int N = 10_000;

  @Benchmark
  @OperationsPerInvocation(N)
  public void put() throws RocksDBException {
    WriteBatch wb = new WriteBatch();
    try {
      for (int i = 0; i < N; i++) {
        wb.put(ba("key" + i), ba("value" + i));
      }
    } finally {
      wb.close();
    }
  }

  private static final ByteBuffer nioKeyBuffer = ByteBuffer.allocateDirect(1024);
  private static final ByteBuffer nioValueBuffer = ByteBuffer.allocateDirect(1024);

  @Benchmark
  @OperationsPerInvocation(N)
  public void putWithByteBuffer() throws RocksDBException {
    WriteBatch wb = new WriteBatch();
    try {
      for (int i = 0; i < N; i++) {
        writeToByteBuffer(nioKeyBuffer, "key" + i);
        writeToByteBuffer(nioValueBuffer, "value" + i);
        wb.put(nioKeyBuffer, nioValueBuffer);
      }
    } finally {
      wb.close();
    }
  }

  private static final ByteBuf keyBuffer = PooledByteBufAllocator.DEFAULT.directBuffer();
  private static final ByteBuf valueBuffer = PooledByteBufAllocator.DEFAULT.directBuffer();

  @Benchmark
  @OperationsPerInvocation(N)
  public void putWithAddress() throws RocksDBException {
    WriteBatch wb = new WriteBatch();
    try {
      for (int i = 0; i < N; i++) {
        keyBuffer.clear();
        ByteBufUtil.writeAscii(keyBuffer, "key");
        keyBuffer.writeInt(i);
        valueBuffer.clear();
        ByteBufUtil.writeAscii(valueBuffer, "value");
        valueBuffer.writeInt(i);

        wb.put(keyBuffer.memoryAddress(), keyBuffer.readableBytes(), valueBuffer.memoryAddress(),
            valueBuffer.readableBytes());
      }
    } finally {
      wb.close();
    }
  }
}
