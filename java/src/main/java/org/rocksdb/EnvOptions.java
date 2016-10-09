// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public class EnvOptions extends RocksObject {
  static {
    RocksDB.loadLibrary();
  }

  public EnvOptions() {
    super(newEnvOptions());
  }

  public EnvOptions setUseOsBuffer(final boolean useOsBuffer) {
    setUseOsBuffer(nativeHandle_, useOsBuffer);
    return this;
  }

  public boolean useOsBuffer() {
    assert(isOwningHandle());
    return useOsBuffer(nativeHandle_);
  }

  public EnvOptions setUseMmapReads(final boolean useMmapReads) {
    setUseMmapReads(nativeHandle_, useMmapReads);
    return this;
  }

  public boolean useMmapReads() {
    assert(isOwningHandle());
    return useMmapReads(nativeHandle_);
  }

  public EnvOptions setUseMmapWrites(final boolean useMmapWrites) {
    setUseMmapWrites(nativeHandle_, useMmapWrites);
    return this;
  }

  public boolean useMmapWrites() {
    assert(isOwningHandle());
    return useMmapWrites(nativeHandle_);
  }

  public EnvOptions setUseDirectReads(final boolean useDirectReads) {
    setUseDirectReads(nativeHandle_, useDirectReads);
    return this;
  }

  public boolean useDirectReads() {
    assert(isOwningHandle());
    return useDirectReads(nativeHandle_);
  }

  public EnvOptions setUseDirectWrites(final boolean useDirectWrites) {
    setUseDirectWrites(nativeHandle_, useDirectWrites);
    return this;
  }

  public boolean useDirectWrites() {
    assert(isOwningHandle());
    return useDirectWrites(nativeHandle_);
  }

  public EnvOptions setAllowFallocate(final boolean allowFallocate) {
    setAllowFallocate(nativeHandle_, allowFallocate);
    return this;
  }

  public boolean allowFallocate() {
    assert(isOwningHandle());
    return allowFallocate(nativeHandle_);
  }

  public EnvOptions setSetFdCloexec(final boolean setFdCloexec) {
    setSetFdCloexec(nativeHandle_, setFdCloexec);
    return this;
  }

  public boolean setFdCloexec() {
    assert(isOwningHandle());
    return setFdCloexec(nativeHandle_);
  }

  public EnvOptions setBytesPerSync(final long bytesPerSync) {
    setBytesPerSync(nativeHandle_, bytesPerSync);
    return this;
  }

  public long bytesPerSync() {
    assert(isOwningHandle());
    return bytesPerSync(nativeHandle_);
  }

  public EnvOptions setFallocateWithKeepSize(final boolean fallocateWithKeepSize) {
    setFallocateWithKeepSize(nativeHandle_, fallocateWithKeepSize);
    return this;
  }

  public boolean fallocateWithKeepSize() {
    assert(isOwningHandle());
    return fallocateWithKeepSize(nativeHandle_);
  }

  public EnvOptions setCompactionReadaheadSize(final long compactionReadaheadSize) {
    setCompactionReadaheadSize(nativeHandle_, compactionReadaheadSize);
    return this;
  }

  public long compactionReadaheadSize() {
    assert(isOwningHandle());
    return compactionReadaheadSize(nativeHandle_);
  }

  public EnvOptions setRandomAccessMaxBufferSize(final long randomAccessMaxBufferSize) {
    setRandomAccessMaxBufferSize(nativeHandle_, randomAccessMaxBufferSize);
    return this;
  }

  public long randomAccessMaxBufferSize() {
    assert(isOwningHandle());
    return randomAccessMaxBufferSize(nativeHandle_);
  }

  public EnvOptions setWritableFileMaxBufferSize(final long writableFileMaxBufferSize) {
    setWritableFileMaxBufferSize(nativeHandle_, writableFileMaxBufferSize);
    return this;
  }

  public long writableFileMaxBufferSize() {
    assert(isOwningHandle());
    return writableFileMaxBufferSize(nativeHandle_);
  }

  public EnvOptions setRateLimiterConfig(final RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    setRateLimiter(nativeHandle_, rateLimiterConfig.newRateLimiterHandle());
    return this;
  }

  public RateLimiterConfig rateLimiterConfig() {
    assert(isOwningHandle());
    return rateLimiterConfig;
  }

  private native static long newEnvOptions();

  @Override protected final native void disposeInternal(final long handle);

  private native void setUseOsBuffer(final long handle, final boolean useOsBuffer);

  private native boolean useOsBuffer(final long handle);

  private native void setUseMmapReads(final long handle, final boolean useMmapReads);

  private native boolean useMmapReads(final long handle);

  private native void setUseMmapWrites(final long handle, final boolean useMmapWrites);

  private native boolean useMmapWrites(final long handle);

  private native void setUseDirectReads(final long handle, final boolean useDirectReads);

  private native boolean useDirectReads(final long handle);

  private native void setUseDirectWrites(final long handle, final boolean useDirectWrites);

  private native boolean useDirectWrites(final long handle);

  private native void setAllowFallocate(final long handle, final boolean allowFallocate);

  private native boolean allowFallocate(final long handle);

  private native void setSetFdCloexec(final long handle, final boolean setFdCloexec);

  private native boolean setFdCloexec(final long handle);

  private native void setBytesPerSync(final long handle, final long bytesPerSync);

  private native long bytesPerSync(final long handle);

  private native void setFallocateWithKeepSize(
      final long handle, final boolean fallocateWithKeepSize);

  private native boolean fallocateWithKeepSize(final long handle);

  private native void setCompactionReadaheadSize(
      final long handle, final long compactionReadaheadSize);

  private native long compactionReadaheadSize(final long handle);

  private native void setRandomAccessMaxBufferSize(
      final long handle, final long randomAccessMaxBufferSize);

  private native long randomAccessMaxBufferSize(final long handle);

  private native void setWritableFileMaxBufferSize(
      final long handle, final long writableFileMaxBufferSize);

  private native long writableFileMaxBufferSize(final long handle);

  private native void setRateLimiter(final long handle, final long rateLimiterHandle);

  private RateLimiterConfig rateLimiterConfig;
}
