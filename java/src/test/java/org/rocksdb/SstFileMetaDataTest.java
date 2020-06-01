// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.ClassRule;
import org.junit.Test;

public class SstFileMetaDataTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  public static final Random rand = PlatformRandomHelper.getPlatformSpecificRandomFactory();

  @Test
  public void createSstFileMetaDataWithoutParameters() {
    try (final SstFileMetaData metadata = new SstFileMetaData()) {
      assertThat(metadata).isNotNull();
    }
  }

  @Test
  public void fileName() {
    try (final SstFileMetaData sstFileMetaData = new SstFileMetaData()) {
      assertThat(sstFileMetaData.fileName()).isEqualTo("");
    }
  }

  @Test
  public void path() {
    try (final SstFileMetaData sstFileMetaData = new SstFileMetaData()) {
      assertThat(sstFileMetaData.path()).isEqualTo("");
    }
  }

  @Test
  public void size() {
    try (final SstFileMetaData sstFileMetaData = new SstFileMetaData()) {
      assertThat(sstFileMetaData.size()).isEqualTo(0);
    }
  }

  @Test
  public void smallestSeqno() {
    try (final SstFileMetaData sstFileMetaData = new SstFileMetaData()) {
      assertThat(sstFileMetaData.smallestSeqno()).isEqualTo(0);
    }
  }

  @Test
  public void largestSeqno() {
    try (final SstFileMetaData sstFileMetaData = new SstFileMetaData()) {
      assertThat(sstFileMetaData.largestSeqno()).isEqualTo(0);
    }
  }

  @Test
  public void smallestKey() {
    try (final SstFileMetaData sstFileMetaData = new SstFileMetaData()) {
      assertThat(sstFileMetaData.smallestKey()).isEmpty();
    }
  }

  @Test
  public void largestKey() {
    try (final SstFileMetaData sstFileMetaData = new SstFileMetaData()) {
      assertThat(sstFileMetaData.largestKey()).isEmpty();
    }
  }

  @Test
  public void numReadsSampled() {
    try (final SstFileMetaData sstFileMetaData = new SstFileMetaData()) {
      assertThat(sstFileMetaData.numReadsSampled()).isEqualTo(0);
    }
  }

  @Test
  public void beingCompacted() {
    try (final SstFileMetaData sstFileMetaData = new SstFileMetaData()) {
      assertThat(sstFileMetaData.beingCompacted()).isEqualTo(false);
    }
  }

  @Test
  public void numEntries() {
    try (final SstFileMetaData sstFileMetaData = new SstFileMetaData()) {
      assertThat(sstFileMetaData.numEntries()).isEqualTo(0);
    }
  }

  @Test
  public void numDeletions() {
    try (final SstFileMetaData sstFileMetaData = new SstFileMetaData()) {
      assertThat(sstFileMetaData.numDeletions()).isEqualTo(0);
    }
  }
}
