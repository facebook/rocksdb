// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;
import org.rocksdb.CompactRangeOptions.BottommostLevelCompaction;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactRangeOptionsTest {

  static {
    RocksDB.loadLibrary();
  }

  @Test
  public void exclusiveManualCompaction() {
    try (final CompactRangeOptions opt = new CompactRangeOptions()) {
      opt.setExclusiveManualCompaction(false);
      assertThat(opt.exclusiveManualCompaction()).isEqualTo(false);
      opt.setExclusiveManualCompaction(true);
      assertThat(opt.exclusiveManualCompaction()).isEqualTo(true);
    }
  }

  @Test
  public void bottommostLevelCompaction() {
    try (final CompactRangeOptions opt = new CompactRangeOptions()) {
      BottommostLevelCompaction value = BottommostLevelCompaction.kSkip;
      opt.setBottommostLevelCompaction(value);
      assertThat(opt.bottommostLevelCompaction()).isEqualTo(value);
      value = BottommostLevelCompaction.kForce;
      opt.setBottommostLevelCompaction(value);
      assertThat(opt.bottommostLevelCompaction()).isEqualTo(value);
      value = BottommostLevelCompaction.kIfHaveCompactionFilter;
      opt.setBottommostLevelCompaction(value);
      assertThat(opt.bottommostLevelCompaction()).isEqualTo(value);
      value = BottommostLevelCompaction.kForceOptimized;
      opt.setBottommostLevelCompaction(value);
      assertThat(opt.bottommostLevelCompaction()).isEqualTo(value);
    }
  }

  @Test
  public void changeLevel() {
    try (final CompactRangeOptions opt = new CompactRangeOptions()) {
      opt.setChangeLevel(false);
      assertThat(opt.changeLevel()).isEqualTo(false);
      opt.setChangeLevel(true);
      assertThat(opt.changeLevel()).isEqualTo(true);
    }
  }

  @Test
  public void targetLevel() {
    try (final CompactRangeOptions opt = new CompactRangeOptions()) {
      int value = 2;
      opt.setTargetLevel(value);
      assertThat(opt.targetLevel()).isEqualTo(value);
      value = 3;
      opt.setTargetLevel(value);
      assertThat(opt.targetLevel()).isEqualTo(value);
    }
  }

  @Test
  public void targetPathId() {
    try (final CompactRangeOptions opt = new CompactRangeOptions()) {
      int value = 2;
      opt.setTargetPathId(value);
      assertThat(opt.targetPathId()).isEqualTo(value);
      value = 3;
      opt.setTargetPathId(value);
      assertThat(opt.targetPathId()).isEqualTo(value);
    }
  }

  @Test
  public void allowWriteStall() {
    try (final CompactRangeOptions opt = new CompactRangeOptions()) {
      opt.setAllowWriteStall(false);
      assertThat(opt.allowWriteStall()).isEqualTo(false);
      opt.setAllowWriteStall(true);
      assertThat(opt.allowWriteStall()).isEqualTo(true);
    }
  }

  @Test
  public void maxSubcompactions() {
    try (final CompactRangeOptions opt = new CompactRangeOptions()) {
      int value = 2;
      opt.setMaxSubcompactions(value);
      assertThat(opt.maxSubcompactions()).isEqualTo(value);
      value = 3;
      opt.setMaxSubcompactions(value);
      assertThat(opt.maxSubcompactions()).isEqualTo(value);
    }
  }
}
