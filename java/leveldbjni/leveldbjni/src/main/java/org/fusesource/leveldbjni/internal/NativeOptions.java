/*
 * Copyright (C) 2011, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *    * Neither the name of FuseSource Corp. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.fusesource.leveldbjni.internal;

import org.fusesource.hawtjni.runtime.JniClass;
import org.fusesource.hawtjni.runtime.JniField;
import org.fusesource.hawtjni.runtime.JniMethod;

import static org.fusesource.hawtjni.runtime.ClassFlag.CPP;
import static org.fusesource.hawtjni.runtime.ClassFlag.STRUCT;
import static org.fusesource.hawtjni.runtime.FieldFlag.CONSTANT;
import static org.fusesource.hawtjni.runtime.FieldFlag.FIELD_SKIP;
import static org.fusesource.hawtjni.runtime.MethodFlag.CONSTANT_INITIALIZER;

/**
 * Provides a java interface to the C++ rocksdb::Options class.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@JniClass(name="rocksdb::Options", flags={STRUCT, CPP})
public class NativeOptions {

    static {
        NativeDB.LIBRARY.load();
        init();
    }

    @JniMethod(flags={CONSTANT_INITIALIZER})
    private static final native void init();

    @JniField(flags={CONSTANT}, cast="Env*", accessor="rocksdb::Env::Default()")
    private static long DEFAULT_ENV;

    private boolean create_if_missing = false;
    private boolean error_if_exists = false;
    private boolean paranoid_checks = false;
    @JniField(cast="size_t")
    private long write_buffer_size = 4 << 20;
    @JniField(cast="size_t")
    private long max_write_buffer_number = 2;
    @JniField(cast="size_t")
    private long block_size = 4086;
    private int max_open_files = 1000;
    private int block_restart_interval = 16;
    private boolean no_block_cache = false;
    private boolean use_fsync = false;
    private int num_levels = 7;
    private int level0_file_num_compaction_trigger = 4;
    private int level0_slowdown_writes_trigger = 8;
    private int level0_stop_writes_trigger = 12;
    private int max_mem_compaction_level = 2;
    private int target_file_size_base = 2 * 1048576;
    private int target_file_size_multiplier = 1;

    @JniField(flags={FIELD_SKIP})
    private NativeComparator comparatorObject = NativeComparator.BYTEWISE_COMPARATOR;
    @JniField(cast="const rocksdb::Comparator*")
    private long comparator = comparatorObject.pointer();

    @JniField(cast="uint64_t")
    private long max_bytes_for_level_base = 10 * 1048576;
    private int max_bytes_for_level_multiplier = 10;
    private int expanded_compaction_factor = 25;
    private int source_compaction_factor = 1;
    private int max_grandparent_overlap_factor = 10;
    private boolean disableDataSync = false;
    private int db_stats_log_interval = 1800;
    private boolean disable_seek_compaction = false;
    @JniField(cast="uint64_t")
    private long delete_obsolete_files_period_micros = 0;
    private int max_background_compactions = 1;

    @JniField(cast="size_t")
    private long max_log_file_size = 0;
    private double rate_limit = 0.0;
    private int table_cache_numshardbits = 4;
    private boolean disable_auto_compactions = false;

    @JniField(cast="uint64_t")
    private long WAL_ttl_seconds = 0;

    @JniField(flags={FIELD_SKIP})
    private NativeLogger infoLogObject = null;
    @JniField(cast="rocksdb::Logger*")
    private long info_log = 0;

    @JniField(cast="rocksdb::Env*")
    private long env = DEFAULT_ENV;
    @JniField(cast="rocksdb::Cache*")
    private long block_cache = 0;
    @JniField(flags={FIELD_SKIP})
    private NativeCache cache;

    @JniField(cast="rocksdb::CompressionType")
    private int compression = NativeCompressionType.kSnappyCompression.value;

    public NativeOptions createIfMissing(boolean value) {
        this.create_if_missing = value;
        return this;
    }
    public boolean createIfMissing() {
        return create_if_missing;
    }

    public NativeOptions errorIfExists(boolean value) {
        this.error_if_exists = value;
        return this;
    }
    public boolean errorIfExists() {
        return error_if_exists;
    }

    public NativeOptions paranoidChecks(boolean value) {
        this.paranoid_checks = value;
        return this;
    }
    public boolean paranoidChecks() {
        return paranoid_checks;
    }

    public NativeOptions writeBufferSize(long value) {
        this.write_buffer_size = value;
        return this;
    }
    public long writeBufferSize() {
        return write_buffer_size;
    }

    public NativeOptions maxOpenFiles(int value) {
        this.max_open_files = value;
        return this;
    }
    public int maxOpenFiles() {
        return max_open_files;
    }

    public NativeOptions blockRestartInterval(int value) {
        this.block_restart_interval = value;
        return this;
    }
    public int blockRestartInterval() {
        return block_restart_interval;
    }

    public NativeOptions blockSize(long value) {
        this.block_size = value;
        return this;
    }
    public long blockSize() {
        return block_size;
    }

//    @JniField(cast="Env*")
//    private long env = DEFAULT_ENV;

    public NativeComparator comparator() {
        return comparatorObject;
    }

    public NativeOptions comparator(NativeComparator comparator) {
        if( comparator==null ) {
            throw new IllegalArgumentException("comparator cannot be null");
        }
        this.comparatorObject = comparator;
        this.comparator = comparator.pointer();
        return this;
    }

    public NativeLogger infoLog() {
        return infoLogObject;
    }

    public NativeOptions infoLog(NativeLogger logger) {
        this.infoLogObject = logger;
        if( logger ==null ) {
            this.info_log = 0;
        } else {
            this.info_log = logger.pointer();
        }
        return this;
    }

    public NativeCompressionType compression() {
        if(compression == NativeCompressionType.kNoCompression.value) {
            return NativeCompressionType.kNoCompression;
        } else if(compression == NativeCompressionType.kSnappyCompression.value) {
            return NativeCompressionType.kSnappyCompression;
        } else {
            return NativeCompressionType.kSnappyCompression;
        }
    }

    public NativeOptions compression(NativeCompressionType compression) {
        this.compression = compression.value;
        return this;
    }

    public NativeCache cache() {
        return cache;
    }

    public NativeOptions cache(NativeCache cache) {
        this.cache = cache;
        if( cache!=null ) {
            this.block_cache = cache.pointer();
        } else {
            this.block_cache = 0;
        }
        return this;
    }

    public int numLevels() {
      return this.num_levels;
    }

    public NativeOptions numLevels(int numLevels) {
      this.num_levels = numLevels;
      return this;
    }

    public int level0FileNumCompactionTrigger() {
      return this.level0_file_num_compaction_trigger;
    }

    public NativeOptions level0FileNumCompactionTrigger(int n) {
      this.level0_file_num_compaction_trigger = n;
      return this;
    }

    public int level0SlowdownWritesTrigger() {
      return this.level0_slowdown_writes_trigger;
    }

    public NativeOptions level0SlowdownWritesTrigger(int n) {
      this.level0_slowdown_writes_trigger = n;
      return this;
    }

    public int level0StopWritesTrigger() {
      return this.level0_stop_writes_trigger;
    }

    public NativeOptions level0StopWritesTrigger(int n) {
      this.level0_stop_writes_trigger = n;
      return this;
    }

    public int maxMemCompactionLevel() {
      return this.max_mem_compaction_level;
    }

    public NativeOptions maxMemCompactionLevel(int n) {
      this.max_mem_compaction_level = n;
      return this;
    }

    public int targetFileSizeBase() {
      return this.target_file_size_base;
    }

    public NativeOptions targetFileSizeBase(int n) {
      this.target_file_size_base = n;
      return this;
    }

    public int targetFileSizeMultiplier() {
      return this.target_file_size_multiplier;
    }

    public NativeOptions targetFileSizeMultiplier(int n) {
      this.target_file_size_multiplier = n;
      return this;
    }

    public long maxBytesLevelBase() {
      return this.max_bytes_for_level_base;
    }

    public NativeOptions maxBytesLevelBase(long n) {
      this.max_bytes_for_level_base = n;
      return this;
    }

    public int maxBytesLevelMultiplier() {
      return this.max_bytes_for_level_multiplier;
    }

    public NativeOptions maxBytesLevelMultiplier(int n) {
      this.max_bytes_for_level_multiplier = n;
      return this;
    }

    public int expandedCompactionFactor() {
      return this.expanded_compaction_factor;
    }

    public NativeOptions expandedCompactionFactor(int n) {
      this.expanded_compaction_factor = n;
      return this;
    }

    public int sourceCompactionFactor() {
      return this.source_compaction_factor;
    }

    public NativeOptions sourceCompactionFactor(int n) {
      this.source_compaction_factor = n;
      return this;
    }

    public int maxGrandparentOverlapFactor() {
      return this.max_grandparent_overlap_factor;
    }

    public NativeOptions maxGrandparentOverlapFactor(int n) {
      this.max_grandparent_overlap_factor = n;
      return this;
    }

    public boolean disableDataSync() {
      return this.disableDataSync;
    }

    public NativeOptions disableDataSync(boolean flag) {
      this.disableDataSync = flag;
      return this;
    }

    public int dbStatsLogInterval() {
      return this.db_stats_log_interval;
    }

    public NativeOptions dbStatsLogInterval(int n) {
      this.db_stats_log_interval = n;
      return this;
    }

    public boolean disableSeekCompaction() {
      return this.disable_seek_compaction;
    }

    public NativeOptions disableSeekCompaction(boolean flag) {
      this.disable_seek_compaction = flag;
      return this;
    }

    public long deleteObsoleteFilesMicros() {
      return this.delete_obsolete_files_period_micros;
    }

    public NativeOptions deleteObsoleteFilesMicros(long micros) {
      this.delete_obsolete_files_period_micros = micros;
      return this;
    }

    public int maxBackgroundCompactions() {
      return this.max_background_compactions;
    }

    public NativeOptions maxBackgroundCompactions(int n) {
      this.max_background_compactions = n;
      return this;
    }

    public long maxLogFileSize() {
      return this.max_log_file_size;
    }

    public NativeOptions maxLogFileSize(long s) {
      this.max_log_file_size = s;
      return this;
    }

    public double rateLimit() {
      return this.rate_limit;
    }

    public NativeOptions rateLimit(double rate) {
      this.rate_limit = rate;
      return this;
    }

    public int tableCacheNumShardBits() {
      return this.table_cache_numshardbits;
    }

    public NativeOptions tableCacheNumShardBits(int n) {
      this.table_cache_numshardbits = n;
      return this;
  }

  public boolean disableAutoCompactions() {
    return this.disable_auto_compactions;
  }

  public NativeOptions disableAutoCompactions(boolean b) {
    this.disable_auto_compactions = b;
    return this;
  }

  public long WALttlSeconds() {
    return this.WAL_ttl_seconds;
  }

  public NativeOptions WALttlSeconds(long n) {
    this.WAL_ttl_seconds = n;
    return this;
  }

  public boolean noBlockCache() {
    return this.no_block_cache;
  }

  public NativeOptions noBlockCache(boolean b) {
    this.no_block_cache = b;
    return this;
  }

  public boolean useFsync() {
    return this.use_fsync;
  }

  public NativeOptions useFsync(boolean b) {
    this.use_fsync = b;
    return this;
  }

  public long maxWriteBufferNumber() {
    return this.max_write_buffer_number;
  }

  public NativeOptions maxWriteBufferNumber(long n) {
    this.max_write_buffer_number = n;
    return this;
  }
}
