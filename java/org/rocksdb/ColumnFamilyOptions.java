// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * ColumnFamilyOptions to control the behavior of a database.  It will be used
 * during the creation of a {@link org.rocksdb.RocksDB} (i.e., RocksDB.open()).
 *
 * If {@link #dispose()} function is not called, then it will be GC'd automatically
 * and native resources will be released as part of the process.
 */
public class ColumnFamilyOptions extends RocksObject
    implements ColumnFamilyOptionsInterface {
  static {
    RocksDB.loadLibrary();
  }

  /**
   * Construct ColumnFamilyOptions.
   *
   * This constructor will create (by allocating a block of memory)
   * an {@code rocksdb::DBOptions} in the c++ side.
   */
  public ColumnFamilyOptions() {
    super();
    newColumnFamilyOptions();
  }

  @Override
  public ColumnFamilyOptions optimizeForPointLookup(
      long blockCacheSizeMb) {
    optimizeForPointLookup(nativeHandle_,
        blockCacheSizeMb);
    return this;
  }

  @Override
  public ColumnFamilyOptions optimizeLevelStyleCompaction() {
    optimizeLevelStyleCompaction(nativeHandle_,
        DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET);
    return this;
  }

  @Override
  public ColumnFamilyOptions optimizeLevelStyleCompaction(
      long memtableMemoryBudget) {
    optimizeLevelStyleCompaction(nativeHandle_,
        memtableMemoryBudget);
    return this;
  }

  @Override
  public ColumnFamilyOptions optimizeUniversalStyleCompaction() {
    optimizeUniversalStyleCompaction(nativeHandle_,
        DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET);
    return this;
  }

  @Override
  public ColumnFamilyOptions optimizeUniversalStyleCompaction(
      long memtableMemoryBudget) {
    optimizeUniversalStyleCompaction(nativeHandle_,
        memtableMemoryBudget);
    return this;
  }

  @Override
  public ColumnFamilyOptions setComparator(BuiltinComparator builtinComparator) {
    assert(isInitialized());
    setComparatorHandle(nativeHandle_, builtinComparator.ordinal());
    return this;
  }

  @Override
  public ColumnFamilyOptions setComparator(AbstractComparator comparator) {
    assert (isInitialized());
    setComparatorHandle(nativeHandle_, comparator.nativeHandle_);
    comparator_ = comparator;
    return this;
  }

  @Override
  public ColumnFamilyOptions setMergeOperatorName(String name) {
    setMergeOperatorName(nativeHandle_, name);
    return this;
  }

  @Override
  public ColumnFamilyOptions setMergeOperator(MergeOperator mergeOperator) {
    setMergeOperator(nativeHandle_, mergeOperator.newMergeOperatorHandle());
    return this;
  }

  @Override
  public ColumnFamilyOptions setWriteBufferSize(long writeBufferSize)
      throws RocksDBException {
    assert(isInitialized());
    setWriteBufferSize(nativeHandle_, writeBufferSize);
    return this;
  }

  @Override
  public long writeBufferSize()  {
    assert(isInitialized());
    return writeBufferSize(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxWriteBufferNumber(
      int maxWriteBufferNumber) {
    assert(isInitialized());
    setMaxWriteBufferNumber(nativeHandle_, maxWriteBufferNumber);
    return this;
  }

  @Override
  public int maxWriteBufferNumber() {
    assert(isInitialized());
    return maxWriteBufferNumber(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMinWriteBufferNumberToMerge(
      int minWriteBufferNumberToMerge) {
    setMinWriteBufferNumberToMerge(nativeHandle_, minWriteBufferNumberToMerge);
    return this;
  }

  @Override
  public int minWriteBufferNumberToMerge() {
    return minWriteBufferNumberToMerge(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions useFixedLengthPrefixExtractor(int n) {
    assert(isInitialized());
    useFixedLengthPrefixExtractor(nativeHandle_, n);
    return this;
  }

  @Override
  public ColumnFamilyOptions setCompressionType(CompressionType compressionType) {
    setCompressionType(nativeHandle_, compressionType.getValue());
    return this;
  }

  @Override
  public CompressionType compressionType() {
    return CompressionType.values()[compressionType(nativeHandle_)];
  }

  @Override
  public ColumnFamilyOptions setNumLevels(int numLevels) {
    setNumLevels(nativeHandle_, numLevels);
    return this;
  }

  @Override
  public int numLevels() {
    return numLevels(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setLevelZeroFileNumCompactionTrigger(
      int numFiles) {
    setLevelZeroFileNumCompactionTrigger(
        nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int levelZeroFileNumCompactionTrigger() {
    return levelZeroFileNumCompactionTrigger(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setLevelZeroSlowdownWritesTrigger(
      int numFiles) {
    setLevelZeroSlowdownWritesTrigger(nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int levelZeroSlowdownWritesTrigger() {
    return levelZeroSlowdownWritesTrigger(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setLevelZeroStopWritesTrigger(int numFiles) {
    setLevelZeroStopWritesTrigger(nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int levelZeroStopWritesTrigger() {
    return levelZeroStopWritesTrigger(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxMemCompactionLevel(
      int maxMemCompactionLevel) {
    setMaxMemCompactionLevel(nativeHandle_, maxMemCompactionLevel);
    return this;
  }

  @Override
  public int maxMemCompactionLevel() {
    return maxMemCompactionLevel(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setTargetFileSizeBase(long targetFileSizeBase) {
    setTargetFileSizeBase(nativeHandle_, targetFileSizeBase);
    return this;
  }

  @Override
  public long targetFileSizeBase() {
    return targetFileSizeBase(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setTargetFileSizeMultiplier(int multiplier) {
    setTargetFileSizeMultiplier(nativeHandle_, multiplier);
    return this;
  }

  @Override
  public int targetFileSizeMultiplier() {
    return targetFileSizeMultiplier(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxBytesForLevelBase(
      long maxBytesForLevelBase) {
    setMaxBytesForLevelBase(nativeHandle_, maxBytesForLevelBase);
    return this;
  }

  @Override
  public long maxBytesForLevelBase() {
    return maxBytesForLevelBase(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxBytesForLevelMultiplier(int multiplier) {
    setMaxBytesForLevelMultiplier(nativeHandle_, multiplier);
    return this;
  }

  @Override
  public int maxBytesForLevelMultiplier() {
    return maxBytesForLevelMultiplier(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setExpandedCompactionFactor(int expandedCompactionFactor) {
    setExpandedCompactionFactor(nativeHandle_, expandedCompactionFactor);
    return this;
  }

  @Override
  public int expandedCompactionFactor() {
    return expandedCompactionFactor(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setSourceCompactionFactor(int sourceCompactionFactor) {
    setSourceCompactionFactor(nativeHandle_, sourceCompactionFactor);
    return this;
  }

  @Override
  public int sourceCompactionFactor() {
    return sourceCompactionFactor(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxGrandparentOverlapFactor(
      int maxGrandparentOverlapFactor) {
    setMaxGrandparentOverlapFactor(nativeHandle_, maxGrandparentOverlapFactor);
    return this;
  }

  @Override
  public int maxGrandparentOverlapFactor() {
    return maxGrandparentOverlapFactor(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setSoftRateLimit(double softRateLimit) {
    setSoftRateLimit(nativeHandle_, softRateLimit);
    return this;
  }

  @Override
  public double softRateLimit() {
    return softRateLimit(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setHardRateLimit(double hardRateLimit) {
    setHardRateLimit(nativeHandle_, hardRateLimit);
    return this;
  }

  @Override
  public double hardRateLimit() {
    return hardRateLimit(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setRateLimitDelayMaxMilliseconds(
      int rateLimitDelayMaxMilliseconds) {
    setRateLimitDelayMaxMilliseconds(
        nativeHandle_, rateLimitDelayMaxMilliseconds);
    return this;
  }

  @Override
  public int rateLimitDelayMaxMilliseconds() {
    return rateLimitDelayMaxMilliseconds(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setArenaBlockSize(long arenaBlockSize)
      throws RocksDBException {
    setArenaBlockSize(nativeHandle_, arenaBlockSize);
    return this;
  }

  @Override
  public long arenaBlockSize() {
    return arenaBlockSize(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setDisableAutoCompactions(boolean disableAutoCompactions) {
    setDisableAutoCompactions(nativeHandle_, disableAutoCompactions);
    return this;
  }

  @Override
  public boolean disableAutoCompactions() {
    return disableAutoCompactions(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setPurgeRedundantKvsWhileFlush(
      boolean purgeRedundantKvsWhileFlush) {
    setPurgeRedundantKvsWhileFlush(
        nativeHandle_, purgeRedundantKvsWhileFlush);
    return this;
  }

  @Override
  public boolean purgeRedundantKvsWhileFlush() {
    return purgeRedundantKvsWhileFlush(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setCompactionStyle(CompactionStyle compactionStyle) {
    setCompactionStyle(nativeHandle_, compactionStyle.getValue());
    return this;
  }

  @Override
  public CompactionStyle compactionStyle() {
    return CompactionStyle.values()[compactionStyle(nativeHandle_)];
  }

  @Override
  public ColumnFamilyOptions setVerifyChecksumsInCompaction(
      boolean verifyChecksumsInCompaction) {
    setVerifyChecksumsInCompaction(
        nativeHandle_, verifyChecksumsInCompaction);
    return this;
  }

  @Override
  public boolean verifyChecksumsInCompaction() {
    return verifyChecksumsInCompaction(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setFilterDeletes(boolean filterDeletes) {
    setFilterDeletes(nativeHandle_, filterDeletes);
    return this;
  }

  @Override
  public boolean filterDeletes() {
    return filterDeletes(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxSequentialSkipInIterations(long maxSequentialSkipInIterations) {
    setMaxSequentialSkipInIterations(nativeHandle_, maxSequentialSkipInIterations);
    return this;
  }

  @Override
  public long maxSequentialSkipInIterations() {
    return maxSequentialSkipInIterations(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMemTableConfig(MemTableConfig config)
      throws RocksDBException {
    memTableConfig_ = config;
    setMemTableFactory(nativeHandle_, config.newMemTableFactoryHandle());
    return this;
  }

  @Override
  public String memTableFactoryName() {
    assert(isInitialized());
    return memTableFactoryName(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setTableFormatConfig(TableFormatConfig config) {
    tableFormatConfig_ = config;
    setTableFactory(nativeHandle_, config.newTableFactoryHandle());
    return this;
  }

  @Override
  public String tableFactoryName() {
    assert(isInitialized());
    return tableFactoryName(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setInplaceUpdateSupport(boolean inplaceUpdateSupport) {
    setInplaceUpdateSupport(nativeHandle_, inplaceUpdateSupport);
    return this;
  }

  @Override
  public boolean inplaceUpdateSupport() {
    return inplaceUpdateSupport(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setInplaceUpdateNumLocks(long inplaceUpdateNumLocks)
      throws RocksDBException {
    setInplaceUpdateNumLocks(nativeHandle_, inplaceUpdateNumLocks);
    return this;
  }

  @Override
  public long inplaceUpdateNumLocks() {
    return inplaceUpdateNumLocks(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMemtablePrefixBloomBits(int memtablePrefixBloomBits) {
    setMemtablePrefixBloomBits(nativeHandle_, memtablePrefixBloomBits);
    return this;
  }

  @Override
  public int memtablePrefixBloomBits() {
    return memtablePrefixBloomBits(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMemtablePrefixBloomProbes(int memtablePrefixBloomProbes) {
    setMemtablePrefixBloomProbes(nativeHandle_, memtablePrefixBloomProbes);
    return this;
  }

  @Override
  public int memtablePrefixBloomProbes() {
    return memtablePrefixBloomProbes(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setBloomLocality(int bloomLocality) {
    setBloomLocality(nativeHandle_, bloomLocality);
    return this;
  }

  @Override
  public int bloomLocality() {
    return bloomLocality(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxSuccessiveMerges(long maxSuccessiveMerges)
      throws RocksDBException {
    setMaxSuccessiveMerges(nativeHandle_, maxSuccessiveMerges);
    return this;
  }

  @Override
  public long maxSuccessiveMerges() {
    return maxSuccessiveMerges(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMinPartialMergeOperands(int minPartialMergeOperands) {
    setMinPartialMergeOperands(nativeHandle_, minPartialMergeOperands);
    return this;
  }

  @Override
  public int minPartialMergeOperands() {
    return minPartialMergeOperands(nativeHandle_);
  }

  /**
   * Release the memory allocated for the current instance
   * in the c++ side.
   */
  @Override protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }

  private native void newColumnFamilyOptions();
  private native void disposeInternal(long handle);

  private native void optimizeForPointLookup(long handle,
      long blockCacheSizeMb);
  private native void optimizeLevelStyleCompaction(long handle,
      long memtableMemoryBudget);
  private native void optimizeUniversalStyleCompaction(long handle,
      long memtableMemoryBudget);
  private native void setComparatorHandle(long handle, int builtinComparator);
  private native void setComparatorHandle(long optHandle, long comparatorHandle);
  private native void setMergeOperatorName(
      long handle, String name);
  private native void setMergeOperator(
      long handle, long mergeOperatorHandle);
  private native void setWriteBufferSize(long handle, long writeBufferSize)
      throws RocksDBException;
  private native long writeBufferSize(long handle);
  private native void setMaxWriteBufferNumber(
      long handle, int maxWriteBufferNumber);
  private native int maxWriteBufferNumber(long handle);
  private native void setMinWriteBufferNumberToMerge(
      long handle, int minWriteBufferNumberToMerge);
  private native int minWriteBufferNumberToMerge(long handle);
  private native void setCompressionType(long handle, byte compressionType);
  private native byte compressionType(long handle);
  private native void useFixedLengthPrefixExtractor(
      long handle, int prefixLength);
  private native void setNumLevels(
      long handle, int numLevels);
  private native int numLevels(long handle);
  private native void setLevelZeroFileNumCompactionTrigger(
      long handle, int numFiles);
  private native int levelZeroFileNumCompactionTrigger(long handle);
  private native void setLevelZeroSlowdownWritesTrigger(
      long handle, int numFiles);
  private native int levelZeroSlowdownWritesTrigger(long handle);
  private native void setLevelZeroStopWritesTrigger(
      long handle, int numFiles);
  private native int levelZeroStopWritesTrigger(long handle);
  private native void setMaxMemCompactionLevel(
      long handle, int maxMemCompactionLevel);
  private native int maxMemCompactionLevel(long handle);
  private native void setTargetFileSizeBase(
      long handle, long targetFileSizeBase);
  private native long targetFileSizeBase(long handle);
  private native void setTargetFileSizeMultiplier(
      long handle, int multiplier);
  private native int targetFileSizeMultiplier(long handle);
  private native void setMaxBytesForLevelBase(
      long handle, long maxBytesForLevelBase);
  private native long maxBytesForLevelBase(long handle);
  private native void setMaxBytesForLevelMultiplier(
      long handle, int multiplier);
  private native int maxBytesForLevelMultiplier(long handle);
  private native void setExpandedCompactionFactor(
      long handle, int expandedCompactionFactor);
  private native int expandedCompactionFactor(long handle);
  private native void setSourceCompactionFactor(
      long handle, int sourceCompactionFactor);
  private native int sourceCompactionFactor(long handle);
  private native void setMaxGrandparentOverlapFactor(
      long handle, int maxGrandparentOverlapFactor);
  private native int maxGrandparentOverlapFactor(long handle);
  private native void setSoftRateLimit(
      long handle, double softRateLimit);
  private native double softRateLimit(long handle);
  private native void setHardRateLimit(
      long handle, double hardRateLimit);
  private native double hardRateLimit(long handle);
  private native void setRateLimitDelayMaxMilliseconds(
      long handle, int rateLimitDelayMaxMilliseconds);
  private native int rateLimitDelayMaxMilliseconds(long handle);
  private native void setArenaBlockSize(
      long handle, long arenaBlockSize) throws RocksDBException;
  private native long arenaBlockSize(long handle);
  private native void setDisableAutoCompactions(
      long handle, boolean disableAutoCompactions);
  private native boolean disableAutoCompactions(long handle);
  private native void setCompactionStyle(long handle, byte compactionStyle);
  private native byte compactionStyle(long handle);
  private native void setPurgeRedundantKvsWhileFlush(
      long handle, boolean purgeRedundantKvsWhileFlush);
  private native boolean purgeRedundantKvsWhileFlush(long handle);
  private native void setVerifyChecksumsInCompaction(
      long handle, boolean verifyChecksumsInCompaction);
  private native boolean verifyChecksumsInCompaction(long handle);
  private native void setFilterDeletes(
      long handle, boolean filterDeletes);
  private native boolean filterDeletes(long handle);
  private native void setMaxSequentialSkipInIterations(
      long handle, long maxSequentialSkipInIterations);
  private native long maxSequentialSkipInIterations(long handle);
  private native void setMemTableFactory(long handle, long factoryHandle);
  private native String memTableFactoryName(long handle);
  private native void setTableFactory(long handle, long factoryHandle);
  private native String tableFactoryName(long handle);
  private native void setInplaceUpdateSupport(
      long handle, boolean inplaceUpdateSupport);
  private native boolean inplaceUpdateSupport(long handle);
  private native void setInplaceUpdateNumLocks(
      long handle, long inplaceUpdateNumLocks) throws RocksDBException;
  private native long inplaceUpdateNumLocks(long handle);
  private native void setMemtablePrefixBloomBits(
      long handle, int memtablePrefixBloomBits);
  private native int memtablePrefixBloomBits(long handle);
  private native void setMemtablePrefixBloomProbes(
      long handle, int memtablePrefixBloomProbes);
  private native int memtablePrefixBloomProbes(long handle);
  private native void setBloomLocality(
      long handle, int bloomLocality);
  private native int bloomLocality(long handle);
  private native void setMaxSuccessiveMerges(
      long handle, long maxSuccessiveMerges) throws RocksDBException;
  private native long maxSuccessiveMerges(long handle);
  private native void setMinPartialMergeOperands(
      long handle, int minPartialMergeOperands);
  private native int minPartialMergeOperands(long handle);

  MemTableConfig memTableConfig_;
  TableFormatConfig tableFormatConfig_;
  AbstractComparator comparator_;
}
