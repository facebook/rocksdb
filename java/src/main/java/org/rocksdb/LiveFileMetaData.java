// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The full set of metadata associated with each SST file.
 */
public class LiveFileMetaData extends SstFileMetaData {

  /**
   * Called from JNI C++
   */
  private LiveFileMetaData(final long nativeHandle) {
    super(nativeHandle);
  }

  public LiveFileMetaData() {
    super(newLiveFileMetaData());
  }

  /**
   * @param columnFamilyName the name of the column family
   * @param level the level at which the file resides
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
  public LiveFileMetaData(final byte[] columnFamilyName, final int level, final String fileName,
      final String path, final long size, final long smallestSeqno, final long largestSeqno,
      final byte[] smallestKey, final byte[] largestKey, final long numReadsSampled,
      final boolean beingCompacted, final long numEntries, final long numDeletions) {
    super(newLiveFileMetaData(columnFamilyName, columnFamilyName.length, level, fileName, path,
        size, smallestSeqno, largestSeqno, smallestKey, smallestKey.length, largestKey,
        largestKey.length, numReadsSampled, beingCompacted, numEntries, numDeletions));
  }

  /**
   * Get the name of the column family.
   *
   * @return the name of the column family
   */
  public byte[] columnFamilyName() {
    return columnFamilyName(nativeHandle_);
  }

  /**
   * Get the level at which this file resides.
   *
   * @return the level at which the file resides.
   */
  public int level() {
    return level(nativeHandle_);
  }

  private static native long newLiveFileMetaData();
  private static native long newLiveFileMetaData(final byte[] columnFamilyName,
      final int columnFamilyNameLen, final int level, final String fileName, final String path,
      final long size, final long smallestSeqno, final long largestSeqno, final byte[] smallestKey,
      final int smallestKeyLen, final byte[] largestKey, final int largestKeyLen,
      final long numReadsSampled, final boolean beingCompacted, final long numEntries,
      final long numDeletions);
  @Override protected native void disposeInternal(final long handle);

  private native byte[] columnFamilyName(final long handle);
  private native int level(final long handle);
}
