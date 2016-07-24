// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class ExternalSstFileInfoTest {
  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource = new RocksMemoryResource();

  public static final Random rand = PlatformRandomHelper.getPlatformSpecificRandomFactory();

  @Test
  public void createExternalSstFileInfoWithoutParameters() {
    try (final ExternalSstFileInfo info = new ExternalSstFileInfo()) {
      assertThat(info).isNotNull();
    }
  }

  @Test
  public void createExternalSstFileInfoWithParameters() {
    final String filePath = "path/to/sst";
    final String smallestKey = "min";
    final String largestKey = "max";
    final long sequenceNumber = rand.nextLong();
    final long fileSize = rand.nextLong();
    final int numEntries = rand.nextInt();
    final int version = rand.nextInt();
    try (final ExternalSstFileInfo info = new ExternalSstFileInfo(
             filePath, smallestKey, largestKey, sequenceNumber, fileSize, numEntries, version)) {
      assertThat(info).isNotNull();
    }
  }

  @Test
  public void filePath() {
    try (final ExternalSstFileInfo info = new ExternalSstFileInfo()) {
      final String stringVale = "path/to/sst";
      info.setFilePath(stringVale);
      assertThat(info.filePath()).isEqualTo(stringVale);
    }
  }

  @Test
  public void smallestKey() {
    try (final ExternalSstFileInfo info = new ExternalSstFileInfo()) {
      final String stringValue = "min";
      info.setSmallestKey(stringValue);
      assertThat(info.smallestKey()).isEqualTo(stringValue);
    }
  }

  @Test
  public void largestKey() {
    try (final ExternalSstFileInfo info = new ExternalSstFileInfo()) {
      final String stringValue = "max";
      info.setLargestKey(stringValue);
      assertThat(info.largestKey()).isEqualTo(stringValue);
    }
  }

  @Test
  public void sequenceNumber() {
    try (final ExternalSstFileInfo info = new ExternalSstFileInfo()) {
      final long longValue = rand.nextLong();
      info.setSequenceNumber(longValue);
      assertThat(info.sequenceNumber()).isEqualTo(longValue);
    }
  }

  @Test
  public void fileSize() {
    try (final ExternalSstFileInfo info = new ExternalSstFileInfo()) {
      final long longValue = Math.max(1, rand.nextLong());
      info.setFileSize(longValue);
      assertThat(info.fileSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void numEntries() {
    try (final ExternalSstFileInfo info = new ExternalSstFileInfo()) {
      final int intValue = Math.max(1, rand.nextInt());
      info.setNumEntries(intValue);
      assertThat(info.numEntries()).isEqualTo(intValue);
    }
  }

  @Test
  public void version() {
    try (final ExternalSstFileInfo info = new ExternalSstFileInfo()) {
      final int intValue = rand.nextInt();
      info.setVersion(intValue);
      assertThat(info.version()).isEqualTo(intValue);
    }
  }
}
