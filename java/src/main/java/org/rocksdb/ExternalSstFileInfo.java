// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.io.File;

public class ExternalSstFileInfo extends RocksObject {
  public ExternalSstFileInfo() {
    super(newExternalSstFileInfo());
  }

  public ExternalSstFileInfo(final String filePath, final String smallestKey,
      final String largestKey, final long sequenceNumber, final long fileSize, final int numEntries,
      final int version) {
    super(newExternalSstFileInfo(ensureNotNullFilePath(filePath),
        ensureNotNullSmallestKey(smallestKey), ensureNotNullLargestKey(largestKey), sequenceNumber,
        fileSize, numEntries, version));
  }

  private static String ensureNotNullFilePath(final String filePath) {
    if (filePath == null) {
      throw new NullPointerException("filePath is null.");
    }
    return filePath;
  }

  private static String ensureNotNullSmallestKey(final String smallestKey) {
    if (smallestKey == null) {
      throw new NullPointerException("smallestKey is null.");
    }
    return smallestKey;
  }

  private static String ensureNotNullLargestKey(final String largestKey) {
    if (largestKey == null) {
      throw new NullPointerException("largestKey is null.");
    }
    return largestKey;
  }

  public void setFilePath(final String filePath) {
    setFilePath(nativeHandle_, filePath);
  }

  public String filePath() {
    return filePath(nativeHandle_);
  }

  public void setSmallestKey(final String smallestKey) {
    setSmallestKey(nativeHandle_, smallestKey);
  }

  public String smallestKey() {
    return smallestKey(nativeHandle_);
  }

  public void setLargestKey(final String largestKey) {
    setLargestKey(nativeHandle_, largestKey);
  }

  public String largestKey() {
    return largestKey(nativeHandle_);
  }

  public void setSequenceNumber(final long sequenceNumber) {
    setSequenceNumber(nativeHandle_, sequenceNumber);
  }

  public long sequenceNumber() {
    return sequenceNumber(nativeHandle_);
  }

  public void setFileSize(final long fileSize) {
    setFileSize(nativeHandle_, fileSize);
  }

  public long fileSize() {
    return fileSize(nativeHandle_);
  }

  public void setNumEntries(final int numEntries) {
    setNumEntries(nativeHandle_, numEntries);
  }

  public int numEntries() {
    return numEntries(nativeHandle_);
  }

  public void setVersion(final int version) {
    setVersion(nativeHandle_, version);
  }

  public int version() {
    return version(nativeHandle_);
  }

  private native static long newExternalSstFileInfo();

  private native static long newExternalSstFileInfo(final String filePath, final String smallestKey,
      final String largestKey, final long sequenceNumber, final long fileSize, final int numEntries,
      final int version);

  private native void setFilePath(final long handle, final String filePath);

  private native String filePath(final long handle);

  private native void setSmallestKey(final long handle, final String smallestKey);

  private native String smallestKey(final long handle);

  private native void setLargestKey(final long handle, final String largestKey);

  private native String largestKey(final long handle);

  private native void setSequenceNumber(final long handle, final long sequenceNumber);

  private native long sequenceNumber(final long handle);

  private native void setFileSize(final long handle, final long fileSize);

  private native long fileSize(final long handle);

  private native void setNumEntries(final long handle, final int numEntries);

  private native int numEntries(final long handle);

  private native void setVersion(final long handle, final int version);

  private native int version(final long handle);

  @Override protected final native void disposeInternal(final long handle);
}
