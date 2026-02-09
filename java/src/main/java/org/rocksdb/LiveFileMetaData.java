// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The full set of metadata associated with each SST file.
 */
@SuppressWarnings("PMD.MissingStaticMethodInNonInstantiatableClass")
public class LiveFileMetaData extends SstFileMetaData {
  private final byte[] columnFamilyName;
  private final int level;

  /**
   * Called from JNI C++
   */
  private LiveFileMetaData(final byte[] columnFamilyName, final int level, final String fileName,
      final String path, final long size, final long smallestSeqno, final long largestSeqno,
      final byte[] smallestKey, final byte[] largestKey, final long numReadsSampled,
      final boolean beingCompacted, final long numEntries, final long numDeletions,
      final byte[] fileChecksum) {
    super(fileName, path, size, smallestSeqno, largestSeqno, smallestKey, largestKey,
        numReadsSampled, beingCompacted, numEntries, numDeletions, fileChecksum);
    this.columnFamilyName = columnFamilyName;
    this.level = level;
  }

  /**
   * Get the name of the column family.
   *
   * @return the name of the column family
   */
  @SuppressWarnings("PMD.MethodReturnsInternalArray")
  public byte[] columnFamilyName() {
    return columnFamilyName;
  }

  /**
   * Get the level at which this file resides.
   *
   * @return the level at which the file resides.
   */
  public int level() {
    return level;
  }

  public long newLiveFileMetaDataHandle() {
    return newLiveFileMetaDataHandle(columnFamilyName(), columnFamilyName().length, level(),
        fileName(), path(), size(), smallestSeqno(), largestSeqno(), smallestKey(),
        smallestKey().length, largestKey(), largestKey().length, numReadsSampled(),
        beingCompacted(), numEntries(), numDeletions());
  }

  private native long newLiveFileMetaDataHandle(final byte[] columnFamilyName,
      final int columnFamilyNameLength, final int level, final String fileName, final String path,
      final long size, final long smallestSeqno, final long largestSeqno, final byte[] smallestKey,
      final int smallestKeyLength, final byte[] largestKey, final int largestKeyLength,
      final long numReadsSampled, final boolean beingCompacted, final long numEntries,
      final long numDeletions);
}
