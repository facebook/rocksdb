// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

/**
 * This class controls the behaviour
 * of Java implementations of
 * AbstractComparator
 *
 * Note that dispose() must be called before a ComparatorOptions
 * instance becomes out-of-scope to release the allocated memory in C++.
 */
public class ComparatorOptions extends RocksObject {
  public ComparatorOptions() {
    super(newComparatorOptions());
  }

  /**
   * Get the synchronisation type used to guard the reused buffers.
   * Only used if {@link #maxReusedBufferSize()} &gt; 0
   * Default: {@link ReusedSynchronisationType#ADAPTIVE_MUTEX}
   *
   * @return the synchronisation type
   */
  public ReusedSynchronisationType reusedSynchronisationType() {
    assert(isOwningHandle());
    return ReusedSynchronisationType.getReusedSynchronisationType(
        reusedSynchronisationType(nativeHandle_));
  }

  /**
   * Set the synchronisation type used to guard the reused buffers.
   * Only used if {@link #maxReusedBufferSize()} &gt; 0
   * Default: {@link ReusedSynchronisationType#ADAPTIVE_MUTEX}
   *
   * @param reusedSynchronisationType the synchronisation type
   *
   * @return the reference to the current comparator options.
   */
  public ComparatorOptions setReusedSynchronisationType(
      final ReusedSynchronisationType reusedSynchronisationType) {
    assert (isOwningHandle());
    setReusedSynchronisationType(nativeHandle_,
        reusedSynchronisationType.getValue());
    return this;
  }

  /**
   * Indicates if a direct byte buffer (i.e. outside of the normal
   * garbage-collected heap) is used, as opposed to a non-direct byte buffer
   * which is a wrapper around an on-heap byte[].
   *
   * Default: true
   *
   * @return true if a direct byte buffer will be used, false otherwise
   */
  public boolean useDirectBuffer() {
    assert(isOwningHandle());
    return useDirectBuffer(nativeHandle_);
  }

  /**
   * Controls whether a direct byte buffer (i.e. outside of the normal
   * garbage-collected heap) is used, as opposed to a non-direct byte buffer
   * which is a wrapper around an on-heap byte[].
   *
   * Default: true
   *
   * @param useDirectBuffer true if a direct byte buffer should be used,
   *     false otherwise
   * @return the reference to the current comparator options.
   */
  public ComparatorOptions setUseDirectBuffer(final boolean useDirectBuffer) {
    assert(isOwningHandle());
    setUseDirectBuffer(nativeHandle_, useDirectBuffer);
    return this;
  }

  /**
   * Maximum size of a buffer (in bytes) that will be reused.
   * Comparators will use 5 of these buffers,
   * so the retained memory size will be 5 * max_reused_buffer_size.
   * When a buffer is needed for transferring data to a callback,
   * if it requires less than {@code maxReuseBufferSize}, then an
   * existing buffer will be reused, else a new buffer will be
   * allocated just for that callback.
   *
   * Default: 64 bytes
   *
   * @return the maximum size of a buffer which is reused,
   *     or 0 if reuse is disabled
   */
  public int maxReusedBufferSize() {
    assert(isOwningHandle());
    return maxReusedBufferSize(nativeHandle_);
  }

  /**
   * Sets the maximum size of a buffer (in bytes) that will be reused.
   * Comparators will use 5 of these buffers,
   * so the retained memory size will be 5 * max_reused_buffer_size.
   * When a buffer is needed for transferring data to a callback,
   * if it requires less than {@code maxReuseBufferSize}, then an
   * existing buffer will be reused, else a new buffer will be
   * allocated just for that callback.
   *
   * Default: 64 bytes
   *
   * @param maxReusedBufferSize the maximum size for a buffer to reuse, or 0 to
   *     disable reuse
   *
   * @return the maximum size of a buffer which is reused
   */
  public ComparatorOptions setMaxReusedBufferSize(final int maxReusedBufferSize) {
    assert(isOwningHandle());
    setMaxReusedBufferSize(nativeHandle_, maxReusedBufferSize);
    return this;
  }

  private native static long newComparatorOptions();
  private native byte reusedSynchronisationType(final long handle);
  private native void setReusedSynchronisationType(final long handle,
      final byte reusedSynchronisationType);
  private native boolean useDirectBuffer(final long handle);
  private native void setUseDirectBuffer(final long handle,
      final boolean useDirectBuffer);
  private native int maxReusedBufferSize(final long handle);
  private native void setMaxReusedBufferSize(final long handle,
      final int maxReuseBufferSize);
  @Override protected final native void disposeInternal(final long handle);
}
