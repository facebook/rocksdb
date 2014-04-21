// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rocksdb.benchmark;

import java.lang.Runnable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

class Stats {
  int id_;
  long start_;
  long finish_;
  double seconds_;
  long done_;
  long found_;
  long lastOpTime_;
  long nextReport_;
  long bytes_;
  StringBuilder message_;
  boolean excludeFromMerge_;

  Stats(int id) {
    id_ = id;
    nextReport_ = 100;
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = System.nanoTime();
    lastOpTime_ = start_;
    finish_ = start_;
    found_ = 0;
    message_ = new StringBuilder("");
    excludeFromMerge_ = false;
  }

  void merge(final Stats other) {
    if (other.excludeFromMerge_) {
      return;
    }

    done_ += other.done_;
    found_ += other.found_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.length() == 0) {
      message_ = other.message_;
    }
  }

  void stop() {
    finish_ = System.nanoTime();
    seconds_ = (double) (finish_ - start_) / 1000000;
  }

  void addMessage(String msg) {
    if (message_.length() > 0) {
      message_.append(" ");
    }
    message_.append(msg);
  }

  void setId(int id) { id_ = id; }
  void setExcludeFromMerge() { excludeFromMerge_ = true; }

  void finishedSingleOp(int bytes) {
    done_++;
    lastOpTime_ = System.nanoTime();
    bytes_ += bytes;
    if (done_ >= nextReport_) {
      if (nextReport_ < 1000) {
        nextReport_ += 100;
      } else if (nextReport_ < 5000) {
        nextReport_ += 500;
      } else if (nextReport_ < 10000) {
        nextReport_ += 1000;
      } else if (nextReport_ < 50000) {
        nextReport_ += 5000;
      } else if (nextReport_ < 100000) {
        nextReport_ += 10000;
      } else if (nextReport_ < 500000) {
        nextReport_ += 50000;
      } else {
        nextReport_ += 100000;
      }
      System.err.printf("... Task %s finished %d ops%30s\r", id_, done_, "");
    }
  }

  void report(String name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    StringBuilder extra = new StringBuilder("");
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      extra.append(String.format("%6.1f MB/s", (bytes_ / 1048576.0) / elapsed));
    }
    extra.append(message_.toString());
    double elapsed = (finish_ - start_) * 1e-6;
    double throughput = (double) done_ / elapsed;

    System.out.format("%-12s : %11.3f micros/op %d ops/sec;%s%s\n",
            name, elapsed * 1e6 / done_,
            (long) throughput, (extra.length() == 0 ? "" : " "), extra.toString());
  }
}

public class DbBenchmark {
  enum Order {
    SEQUENTIAL,
    RANDOM
  }

  enum DBState {
    FRESH,
    EXISTING
  }

  static {
    System.loadLibrary("rocksdbjni");
  }

  abstract class BenchmarkTask implements Callable<Stats> {
    public BenchmarkTask(
        int tid, long randSeed, long numEntries, long keyRange) {
      tid_ = tid;
      rand_ = new Random(randSeed + tid * 1000);
      numEntries_ = numEntries;
      keyRange_ = keyRange;
      stats_ = new Stats(tid);
    }

    @Override public Stats call() throws RocksDBException {
      stats_.start_ = System.nanoTime();
      runTask();
      stats_.finish_ = System.nanoTime();
      return stats_;
    }

    abstract protected void runTask() throws RocksDBException;

    protected int tid_;
    protected Random rand_;
    protected long numEntries_;
    protected long keyRange_;
    protected Stats stats_;

    protected void getFixedKey(byte[] key, long sn) {
      generateKeyFromLong(key, sn);
    }

    protected void getRandomKey(byte[] key, long range) {
      generateKeyFromLong(key, Math.abs(rand_.nextLong() % range));
    }
  }

  abstract class WriteTask extends BenchmarkTask {
    public WriteTask(
        int tid, long randSeed, long numEntries, long keyRange,
        WriteOptions writeOpt, long entriesPerBatch) {
      super(tid, randSeed, numEntries, keyRange);
      writeOpt_ = writeOpt;
      entriesPerBatch_ = entriesPerBatch;
      maxWritesPerSecond_ = -1;
    }

    public WriteTask(
        int tid, long randSeed, long numEntries, long keyRange,
        WriteOptions writeOpt, long entriesPerBatch, long maxWritesPerSecond) {
      super(tid, randSeed, numEntries, keyRange);
      writeOpt_ = writeOpt;
      entriesPerBatch_ = entriesPerBatch;
      maxWritesPerSecond_ = maxWritesPerSecond;
    }

    @Override public void runTask() throws RocksDBException {
      if (numEntries_ != DbBenchmark.this.num_) {
        stats_.message_.append(String.format(" (%d ops)", numEntries_));
      }
      byte[] key = new byte[keySize_];
      byte[] value = new byte[valueSize_];

      try {
        if (entriesPerBatch_ == 1) {
          for (long i = 0; i < numEntries_; ++i) {
            getKey(key, i, keyRange_);
            db_.put(writeOpt_, key, DbBenchmark.this.gen_.generate(valueSize_));
            stats_.finishedSingleOp(keySize_ + valueSize_);
            writeRateControl(i);
            if (isFinished()) {
              return;
            }
          }
        } else {
          for (long i = 0; i < numEntries_; i += entriesPerBatch_) {
            WriteBatch batch = new WriteBatch();
            for (long j = 0; j < entriesPerBatch_; j++) {
              getKey(key, i + j, keyRange_);
              batch.put(key, DbBenchmark.this.gen_.generate(valueSize_));
              stats_.finishedSingleOp(keySize_ + valueSize_);
            }
            db_.write(writeOpt_, batch);
            batch.dispose();
            writeRateControl(i);
            if (isFinished()) {
              return;
            }
          }
        }
      } catch (InterruptedException e) {
        // thread has been terminated.
      }
    }

    protected void writeRateControl(long writeCount)
        throws InterruptedException {
      if (maxWritesPerSecond_ <= 0) return;
      long minInterval =
          writeCount * TimeUnit.SECONDS.toNanos(1) / maxWritesPerSecond_;
      long interval = System.nanoTime() - stats_.start_;
      if (minInterval - interval > TimeUnit.MILLISECONDS.toNanos(1)) {
        TimeUnit.NANOSECONDS.sleep(minInterval - interval);
      }
    }

    abstract protected void getKey(byte[] key, long id, long range);
    protected WriteOptions writeOpt_;
    protected long entriesPerBatch_;
    protected long maxWritesPerSecond_;
  }

  class WriteSequentialTask extends WriteTask {
    public WriteSequentialTask(
        int tid, long randSeed, long numEntries, long keyRange,
        WriteOptions writeOpt, long entriesPerBatch) {
      super(tid, randSeed, numEntries, keyRange,
            writeOpt, entriesPerBatch);
    }
    public WriteSequentialTask(
        int tid, long randSeed, long numEntries, long keyRange,
        WriteOptions writeOpt, long entriesPerBatch,
        long maxWritesPerSecond) {
      super(tid, randSeed, numEntries, keyRange,
            writeOpt, entriesPerBatch,
            maxWritesPerSecond);
    }
    @Override protected void getKey(byte[] key, long id, long range) {
      getFixedKey(key, id);
    }
  }

  class WriteRandomTask extends WriteTask {
    public WriteRandomTask(
        int tid, long randSeed, long numEntries, long keyRange,
        WriteOptions writeOpt, long entriesPerBatch) {
      super(tid, randSeed, numEntries, keyRange,
            writeOpt, entriesPerBatch);
    }
    public WriteRandomTask(
        int tid, long randSeed, long numEntries, long keyRange,
        WriteOptions writeOpt, long entriesPerBatch,
        long maxWritesPerSecond) {
      super(tid, randSeed, numEntries, keyRange,
            writeOpt, entriesPerBatch,
            maxWritesPerSecond);
    }
    @Override protected void getKey(byte[] key, long id, long range) {
      getRandomKey(key, range);
    }
  }

  class ReadRandomTask extends BenchmarkTask {
    public ReadRandomTask(
        int tid, long randSeed, long numEntries, long keyRange) {
      super(tid, randSeed, numEntries, keyRange);
    }
    @Override public void runTask() throws RocksDBException {
      stats_.found_ = 0;
      byte[] key = new byte[keySize_];
      byte[] value = new byte[valueSize_];
      for (long i = 0; i < numEntries_; i++) {
        getRandomKey(key, numEntries_);
        int len = db_.get(key, value);
        if (len != RocksDB.NOT_FOUND) {
          stats_.found_++;
          stats_.finishedSingleOp(keySize_ + valueSize_);
        } else {
          stats_.finishedSingleOp(keySize_);
        }
        if (isFinished()) {
          return;
        }
      }
    }
  }

  class ReadSequentialTask extends BenchmarkTask {
    public ReadSequentialTask(
        int tid, long randSeed, long numEntries, long keyRange, long initId) {
      super(tid, randSeed, numEntries, keyRange);
      initId_ = initId;
    }
    @Override public void runTask() throws RocksDBException {
      // make sure we have enough things to read in sequential
      if (numEntries_ > keyRange_ - initId_) {
        numEntries_ = keyRange_ - initId_;
      }
      throw new UnsupportedOperationException();
    }
    private long initId_;
  }

  public DbBenchmark(Map<Flag, Object> flags) throws Exception {
    benchmarks_ = (List<String>) flags.get(Flag.benchmarks);
    num_ = (Integer) flags.get(Flag.num);
    threadNum_ = (Integer) flags.get(Flag.threads);
    reads_ = (Integer) (flags.get(Flag.reads) == null ?
        flags.get(Flag.num) : flags.get(Flag.reads));
    keySize_ = (Integer) flags.get(Flag.key_size);
    valueSize_ = (Integer) flags.get(Flag.value_size);
    writeBufferSize_ = (Integer) flags.get(Flag.write_buffer_size) > 0 ?
        (Integer) flags.get(Flag.write_buffer_size) : 0;
    compressionRatio_ = (Double) flags.get(Flag.compression_ratio);
    useExisting_ = (Boolean) flags.get(Flag.use_existing_db);
    randSeed_ = (Long) flags.get(Flag.seed);
    databaseDir_ = (String) flags.get(Flag.db);
    writesPerSeconds_ = (Integer) flags.get(Flag.writes_per_second);
    cacheSize_ = (Long) flags.get(Flag.cache_size);
    gen_ = new RandomGenerator(compressionRatio_);
    memtable_ = (String) flags.get(Flag.memtablerep);
    maxWriteBufferNumber_ = (Integer) flags.get(Flag.max_write_buffer_number);
    prefixSize_ = (Integer) flags.get(Flag.prefix_size);
    keysPerPrefix_ = (Integer) flags.get(Flag.keys_per_prefix);
    hashBucketCount_ = (Long) flags.get(Flag.hash_bucket_count);
    usePlainTable_ = (Boolean) flags.get(Flag.use_plain_table);
    finishLock_ = new Object();
  }

  private void prepareOptions(Options options) {
    options.setCacheSize(cacheSize_);
    if (!useExisting_) {
      options.setCreateIfMissing(true);
    }
    if (memtable_.equals("skip_list")) {
      options.setMemTableConfig(new SkipListMemTableConfig());
    } else if (memtable_.equals("vector")) {
      options.setMemTableConfig(new VectorMemTableConfig());
    } else if (memtable_.equals("hash_linkedlist")) {
      options.setMemTableConfig(
          new HashLinkedListMemTableConfig()
              .setBucketCount(hashBucketCount_));
      options.useFixedLengthPrefixExtractor(prefixSize_);
    } else if (memtable_.equals("hash_skiplist") ||
               memtable_.equals("prefix_hash")) {
      options.setMemTableConfig(
          new HashSkipListMemTableConfig()
              .setBucketCount(hashBucketCount_));
      options.useFixedLengthPrefixExtractor(prefixSize_);
    } else {
      System.err.format(
          "unable to detect the specified memtable, " +
          "use the default memtable factory %s%n",
          options.memTableFactoryName());
    }
    if (usePlainTable_) {
      options.setSstFormatConfig(
          new PlainTableConfig().setKeySize(keySize_));
    }
  }

  private void run() throws RocksDBException {
    if (!useExisting_) {
      destroyDb();
    }
    Options options = new Options();
    prepareOptions(options);
    open(options);

    printHeader(options);

    for (String benchmark : benchmarks_) {
      List<Callable<Stats>> tasks = new ArrayList<Callable<Stats>>();
      List<Callable<Stats>> bgTasks = new ArrayList<Callable<Stats>>();
      WriteOptions writeOpt = new WriteOptions();
      int currentTaskId = 0;
      boolean known = true;

      if (benchmark.equals("fillseq")) {
        tasks.add(new WriteSequentialTask(
            currentTaskId++, randSeed_, num_, num_, writeOpt, 1));
      } else if (benchmark.equals("fillbatch")) {
        tasks.add(new WriteRandomTask(
            currentTaskId++, randSeed_, num_ / 1000, num_, writeOpt, 1000));
      } else if (benchmark.equals("fillrandom")) {
        tasks.add(new WriteRandomTask(
            currentTaskId++, randSeed_, num_, num_, writeOpt, 1));
      } else if (benchmark.equals("fillsync")) {
        writeOpt.setSync(true);
        tasks.add(new WriteRandomTask(
            currentTaskId++, randSeed_, num_ / 1000, num_ / 1000,
            writeOpt, 1));
      } else if (benchmark.equals("readseq")) {
        for (int t = 0; t < threadNum_; ++t) {
          tasks.add(new ReadSequentialTask(
              currentTaskId++, randSeed_, reads_ / threadNum_,
              num_, (num_ / threadNum_) * t));
        }
      } else if (benchmark.equals("readrandom")) {
        for (int t = 0; t < threadNum_; ++t) {
          tasks.add(new ReadRandomTask(
              currentTaskId++, randSeed_, reads_ / threadNum_, num_));
        }
      } else if (benchmark.equals("readwhilewriting")) {
        WriteTask writeTask = new WriteRandomTask(
            -1, randSeed_, Long.MAX_VALUE, num_, writeOpt, 1, writesPerSeconds_);
        writeTask.stats_.setExcludeFromMerge();
        bgTasks.add(writeTask);
        for (int t = 0; t < threadNum_; ++t) {
          tasks.add(new ReadRandomTask(
              currentTaskId++, randSeed_, reads_ / threadNum_, num_));
        }
      } else if (benchmark.equals("readhot")) {
        for (int t = 0; t < threadNum_; ++t) {
          tasks.add(new ReadRandomTask(
              currentTaskId++, randSeed_, reads_ / threadNum_, num_ / 100));
        }
      } else if (benchmark.equals("delete")) {
        destroyDb();
        open(options);
      } else {
        known = false;
        System.err.println("Unknown benchmark: " + benchmark);
      }
      if (known) {
        ExecutorService executor = Executors.newCachedThreadPool();
        ExecutorService bgExecutor = Executors.newCachedThreadPool();
        try {
          // measure only the main executor time
          List<Future<Stats>> bgResults = new ArrayList<Future<Stats>>();
          for (Callable bgTask : bgTasks) {
            bgResults.add(bgExecutor.submit(bgTask));
          }
          start();
          List<Future<Stats>> results = executor.invokeAll(tasks);
          executor.shutdown();
          boolean finished = executor.awaitTermination(10, TimeUnit.SECONDS);
          if (!finished) {
            System.out.format(
                "Benchmark %s was not finished before timeout.",
                benchmark);
            executor.shutdownNow();
          }
          setFinished(true);
          bgExecutor.shutdown();
          finished = bgExecutor.awaitTermination(10, TimeUnit.SECONDS);
          if (!finished) {
            System.out.format(
                "Benchmark %s was not finished before timeout.",
                benchmark);
            bgExecutor.shutdownNow();
          }

          stop(benchmark, results, currentTaskId);
        } catch (InterruptedException e) {
          System.err.println(e);
        }
      }
      writeOpt.dispose();
    }
    options.dispose();
    db_.close();
  }

  private void printHeader(Options options) {
    int kKeySize = 16;
    System.out.printf("Keys:     %d bytes each\n", kKeySize);
    System.out.printf("Values:   %d bytes each (%d bytes after compression)\n",
        valueSize_,
        (int) (valueSize_ * compressionRatio_ + 0.5));
    System.out.printf("Entries:  %d\n", num_);
    System.out.printf("RawSize:  %.1f MB (estimated)\n",
        ((kKeySize + valueSize_) * num_) / 1048576.0);
    System.out.printf("FileSize:   %.1f MB (estimated)\n",
        (((kKeySize + valueSize_ * compressionRatio_) * num_)
            / 1048576.0));
    System.out.format("Memtable Factory: %s%n", options.memTableFactoryName());
    System.out.format("Prefix:   %d bytes%n", prefixSize_);
    printWarnings();
    System.out.printf("------------------------------------------------\n");
  }

  void printWarnings() {
    boolean assertsEnabled = false;
    assert assertsEnabled = true; // Intentional side effect!!!
    if (assertsEnabled) {
      System.out.printf(
          "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
    }
  }

  private void open(Options options) throws RocksDBException {
    db_ = RocksDB.open(options, databaseDir_);
  }

  private void start() {
    setFinished(false);
    startTime_ = System.nanoTime();
  }

  private void stop(
      String benchmark, List<Future<Stats>> results, int concurrentThreads) {
    long endTime = System.nanoTime();
    double elapsedSeconds =
        1.0d * (endTime - startTime_) / TimeUnit.SECONDS.toNanos(1);

    Stats stats = new Stats(-1);
    int taskFinishedCount = 0;
    for (Future<Stats> result : results) {
      if (result.isDone()) {
        try {
          Stats taskStats = result.get(3, TimeUnit.SECONDS);
          if (!result.isCancelled()) {
            taskFinishedCount++;
          }
          stats.merge(taskStats);
        } catch (Exception e) {
          // then it's not successful, the output will indicate this
        }
      }
    }

    System.out.printf(
        "%-16s : %11.5f micros/op; %6.1f MB/s; %d / %d task(s) finished.\n",
        benchmark, elapsedSeconds * 1e6 / num_,
        (stats.bytes_ / 1048576.0) / elapsedSeconds,
        taskFinishedCount, concurrentThreads);
  }

  public void generateKeyFromLong(byte[] slice, long n) {
    assert(n >= 0);
    int startPos = 0;

    if (keysPerPrefix_ > 0) {
      long numPrefix = (num_ + keysPerPrefix_ - 1) / keysPerPrefix_;
      long prefix = n % numPrefix;
      int bytesToFill = Math.min(prefixSize_, 8);
      for (int i = 0; i < bytesToFill; ++i) {
        slice[i] = (byte) (prefix % 256);
        prefix /= 256;
      }
      for (int i = 8; i < bytesToFill; ++i) {
        slice[i] = '0';
      }
      startPos = bytesToFill;
    }

    for (int i = slice.length - 1; i >= startPos; --i) {
      slice[i] = (byte) ('0' + (n % 10));
      n /= 10;
    }
  }

  private void destroyDb() {
    if (db_ != null) {
      db_.close();
    }
    // TODO(yhchiang): develop our own FileUtil
    // FileUtil.deleteDir(databaseDir_);
  }

  private void printStats() {
  }

  static void printHelp() {
    System.out.println("usage:");
    for (Flag flag : Flag.values()) {
      System.out.format("  --%s%n    %s%n",
          flag.name(),
          flag.desc());
      if (flag.getDefaultValue() != null) {
        System.out.format("    DEFAULT: %s%n",
            flag.getDefaultValue().toString());
      }
      System.out.println("");
    }
  }

  public static void main(String[] args) throws Exception {
    Map<Flag, Object> flags = new EnumMap<Flag, Object>(Flag.class);
    for (Flag flag : Flag.values()) {
      if (flag.getDefaultValue() != null) {
        flags.put(flag, flag.getDefaultValue());
      }
    }
    for (String arg : args) {
      boolean valid = false;
      if (arg.equals("--help") || arg.equals("-h")) {
        printHelp();
        System.exit(0);
      }
      if (arg.startsWith("--")) {
        try {
          String[] parts = arg.substring(2).split("=");
          if (parts.length >= 1) {
            Flag key = Flag.valueOf(parts[0]);
            if (key != null) {
              Object value = null;
              if (parts.length >= 2) {
                value = key.parseValue(parts[1]);
              }
              flags.put(key, value);
              valid = true;
            }
          }
        }
        catch (Exception e) {
        }
      }
      if (!valid) {
        System.err.println("Invalid argument " + arg);
        System.exit(1);
      }
    }
    new DbBenchmark(flags).run();
  }

  private enum Flag {
    benchmarks(
        Arrays.asList(
            "fillseq",
            "readrandom",
            "fillrandom"),
        "Comma-separated list of operations to run in the specified order\n" +
        "\tActual benchmarks:\n" +
        "\t\tfillseq          -- write N values in sequential key order in async mode.\n" +
        "\t\tfillrandom       -- write N values in random key order in async mode.\n" +
        "\t\tfillbatch        -- write N/1000 batch where each batch has 1000 values\n" +
        "\t\t                    in random key order in sync mode.\n" +
        "\t\tfillsync         -- write N/100 values in random key order in sync mode.\n" +
        "\t\tfill100K         -- write N/1000 100K values in random order in async mode.\n" +
        "\t\treadseq          -- read N times sequentially.\n" +
        "\t\treadrandom       -- read N times in random order.\n" +
        "\t\treadhot          -- read N times in random order from 1% section of DB.\n" +
        "\t\treadwhilewriting -- measure the read performance of multiple readers\n" +
        "\t\t                    with a bg single writer.  The write rate of the bg\n" +
        "\t\t                    is capped by --writes_per_second.\n" +
        "\tMeta Operations:\n" +
        "\t\tdelete            -- delete DB") {
      @Override public Object parseValue(String value) {
        return new ArrayList<String>(Arrays.asList(value.split(",")));
      }
    },

    compression_ratio(0.5d,
        "Arrange to generate values that shrink to this fraction of\n" +
        "\ttheir original size after compression") {
      @Override public Object parseValue(String value) {
        return Double.parseDouble(value);
      }
    },

    use_existing_db(false,
        "If true, do not destroy the existing database.  If you set this\n" +
        "\tflag and also specify a benchmark that wants a fresh database,\n" +
        "\tthat benchmark will fail.") {
      @Override public Object parseValue(String value) {
        return Boolean.parseBoolean(value);
      }
    },

    num(1000000,
        "Number of key/values to place in database.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },

    threads(1,
        "Number of concurrent threads to run.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },

    reads(null,
        "Number of read operations to do.  If negative, do --nums reads.") {
      @Override
      public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },

    key_size(16,
        "The size of each key in bytes.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },

    value_size(100,
        "The size of each value in bytes.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },

    write_buffer_size(4 << 20,
        "Number of bytes to buffer in memtable before compacting\n" +
        "\t(initialized to default value by 'main'.)") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },

    max_write_buffer_number(2,
             "The number of in-memory memtables. Each memtable is of size\n" +
             "\twrite_buffer_size.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },

    prefix_size(0, "Controls the prefix size for HashSkipList, HashLinkedList,\n" +
                   "\tand plain table.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },

    keys_per_prefix(0, "Controls the average number of keys generated\n" +
             "\tper prefix, 0 means no special handling of the prefix,\n" +
             "\ti.e. use the prefix comes with the generated random number.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },

    memtablerep("skip_list",
        "The memtable format.  Available options are\n" +
        "\tskip_list,\n" +
        "\tvector,\n" +
        "\thash_linkedlist,\n" +
        "\thash_skiplist (prefix_hash.)") {
      @Override public Object parseValue(String value) {
        return value;
      }
    },

    hash_bucket_count(SizeUnit.MB,
        "The number of hash buckets used in the hash-bucket-based\n" +
        "\tmemtables.  Memtables that currently support this argument are\n" +
        "\thash_linkedlist and hash_skiplist.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },

    writes_per_second(10000,
        "The write-rate of the background writer used in the\n" +
        "\t`readwhilewriting` benchmark.  Non-positive number indicates\n" +
        "\tusing an unbounded write-rate in `readwhilewriting` benchmark.") {
      @Override public Object parseValue(String value) {
        return Integer.parseInt(value);
      }
    },

    use_plain_table(false,
        "Use plain-table sst format.") {
      @Override public Object parseValue(String value) {
        return Boolean.parseBoolean(value);
      }
    },

    cache_size(-1L,
        "Number of bytes to use as a cache of uncompressed data.\n" +
        "\tNegative means use default settings.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },

    seed(0L,
        "Seed base for random number generators.") {
      @Override public Object parseValue(String value) {
        return Long.parseLong(value);
      }
    },


    db("/tmp/rocksdbjni-bench",
       "Use the db with the following name.") {
      @Override public Object parseValue(String value) {
        return value;
      }
    };

    private Flag(Object defaultValue, String desc) {
      defaultValue_ = defaultValue;
      desc_ = desc;
    }

    protected abstract Object parseValue(String value);

    public Object getDefaultValue() {
      return defaultValue_;
    }

    public String desc() {
      return desc_;
    }

    private final Object defaultValue_;
    private final String desc_;
  }

  private static class RandomGenerator {
    private final byte[] data_;
    private int dataLength_;
    private int position_;

    private RandomGenerator(double compressionRatio) {
      // We use a limited amount of data over and over again and ensure
      // that it is larger than the compression window (32KB), and also
      // large enough to serve all typical value sizes we want to write.
      Random rand = new Random(301);
      dataLength_ = 1048576 + 100;
      data_ = new byte[dataLength_];
      // TODO(yhchiang): mimic test::CompressibleString?
      for (int i = 0; i < dataLength_; ++i) {
        data_[i] = (byte) (' ' + rand.nextInt(95));
      }
    }

    private byte[] generate(int length) {
      if (position_ + length > data_.length) {
        position_ = 0;
        assert (length < data_.length);
      }
      return Arrays.copyOfRange(data_, position_, position_ + length);
    }
  }

  boolean isFinished() {
    synchronized(finishLock_) {
      return isFinished_;
    }
  }

  void setFinished(boolean flag) {
    synchronized(finishLock_) {
      isFinished_ = flag;
    }
  }

  RocksDB db_;
  final List<String> benchmarks_;
  final int num_;
  final int reads_;
  final int keySize_;
  final int valueSize_;
  final int threadNum_;
  final int writesPerSeconds_;
  final long randSeed_;
  final long cacheSize_;
  final boolean useExisting_;
  final String databaseDir_;
  final double compressionRatio_;
  RandomGenerator gen_;
  long startTime_;

  // memtable related
  final int writeBufferSize_;
  final int maxWriteBufferNumber_;
  final int prefixSize_;
  final int keysPerPrefix_;
  final String memtable_;
  final long hashBucketCount_;

  // sst format related
  boolean usePlainTable_;

  Object finishLock_;
  boolean isFinished_;
}
