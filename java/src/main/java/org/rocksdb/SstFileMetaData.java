// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The metadata that describes a SST file.
 */
public class SstFileMetaData extends RocksObject {
  static {
    RocksDB.loadLibrary();
  }

  /**
   * Called from JNI C++ or subclass
   * @param nativeHandle pointer to the C++ object
   */
  public SstFileMetaData(final long nativeHandle) {
    super(nativeHandle);
  }

  public SstFileMetaData() {
    super(newSstFileMetaData());
  }

  /**
   * @param fileName the file name
   * @param path the file path
   * @param size the size of the file
   * @param smallestSeqno the smallest sequence number
   * @param largestSeqno the largest sequence number
   * @param smallestKey the smallest key
   * @param largestKey the largest key
   * @param numReadsSampled the number of reads sampled
   * @param beingCompacted true if the file is being compacted, false otherwise
   * @param numEntries the number of entries
   * @param numDeletions the number of deletions
   */
  public SstFileMetaData(final String fileName, final String path, final long size,
      final long smallestSeqno, final long largestSeqno, final byte[] smallestKey,
      final byte[] largestKey, final long numReadsSampled, final boolean beingCompacted,
      final long numEntries, final long numDeletions) {
    super(newSstFileMetaData(fileName, path, size, smallestSeqno, largestSeqno, smallestKey,
        smallestKey.length, largestKey, largestKey.length, numReadsSampled, beingCompacted,
        numEntries, numDeletions));
  }

  /**
   * Get the name of the file.
   *
   * @return the name of the file.
   */
  public String fileName() {
    return fileName(nativeHandle_);
  }

  /**
   * Get the full path where the file locates.
   *
   * @return the full path
   */
  public String path() {
    return path(nativeHandle_);
  }

  /**
   * Get the file size in bytes.
   *
   * @return file size
   */
  public long size() {
    return size(nativeHandle_);
  }

  /**
   * Get the smallest sequence number in file.
   *
   * @return the smallest sequence number
   */
  public long smallestSeqno() {
    return smallestSeqno(nativeHandle_);
  }

  /**
   * Get the largest sequence number in file.
   *
   * @return the largest sequence number
   */
  public long largestSeqno() {
    return largestSeqno(nativeHandle_);
  }

  /**
   * Get the smallest user defined key in the file.
   *
   * @return the smallest user defined key
   */
  public byte[] smallestKey() {
    return smallestKey(nativeHandle_);
  }

  /**
   * Get the largest user defined key in the file.
   *
   * @return the largest user defined key
   */
  public byte[] largestKey() {
    return largestKey(nativeHandle_);
  }

  /**
   * Get the number of times the file has been read.
   *
   * @return the number of times the file has been read
   */
  public long numReadsSampled() {
    return numReadsSampled(nativeHandle_);
  }

  /**
   * Returns true if the file is currently being compacted.
   *
   * @return true if the file is currently being compacted, false otherwise.
   */
  public boolean beingCompacted() {
    return beingCompacted(nativeHandle_);
  }

  /**
   * Get the number of entries.
   *
   * @return the number of entries.
   */
  public long numEntries() {
    return numEntries(nativeHandle_);
  }

  /**
   * Get the number of deletions.
   *
   * @return the number of deletions.
   */
  public long numDeletions() {
    return numDeletions(nativeHandle_);
  }

  private static native long newSstFileMetaData();
  private static native long newSstFileMetaData(final String fileName, final String path,
      final long size, final long smallestSeqno, final long largestSeqno, final byte[] smallestKey,
      final int smallestKeyLen, final byte[] largestKey, final int largestKeyLen,
      final long numReadsSampled, final boolean beingCompacted, final long numEntries,
      final long numDeletions);
  @Override protected native void disposeInternal(final long handle);

  private native String fileName(final long handle);
  private native String path(final long handle);
  private native long size(final long handle);
  private native long smallestSeqno(final long handle);
  private native long largestSeqno(final long handle);
  private native byte[] smallestKey(final long handle);
  private native byte[] largestKey(final long handle);
  private native long numReadsSampled(final long handle);
  private native boolean beingCompacted(final long handle);
  private native long numEntries(final long handle);
  private native long numDeletions(final long handle);
}
