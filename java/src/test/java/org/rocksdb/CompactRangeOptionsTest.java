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
      opt.setBottommostLevelCompaction(BottommostLevelCompaction.kSkip);
      assertThat(opt.bottommostLevelCompaction()).isEqualTo(BottommostLevelCompaction.kSkip);
      opt.setBottommostLevelCompaction(BottommostLevelCompaction.kForce);
      assertThat(opt.bottommostLevelCompaction()).isEqualTo(BottommostLevelCompaction.kForce);
      opt.setBottommostLevelCompaction(BottommostLevelCompaction.kIfHaveCompactionFilter);
      assertThat(opt.bottommostLevelCompaction()).isEqualTo(BottommostLevelCompaction.kIfHaveCompactionFilter);
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
      opt.setTargetLevel(2);
      assertThat(opt.targetLevel()).isEqualTo(2);
      opt.setTargetLevel(3);
      assertThat(opt.targetLevel()).isEqualTo(3);
    }
  }

  @Test
  public void targetPathId() {
    try (final CompactRangeOptions opt = new CompactRangeOptions()) {
      opt.setTargetPathId(2);
      assertThat(opt.targetPathId()).isEqualTo(2);
      opt.setTargetPathId(3);
      assertThat(opt.targetPathId()).isEqualTo(3);
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
      opt.setMaxSubcompactions(2);
      assertThat(opt.maxSubcompactions()).isEqualTo(2);
      opt.setMaxSubcompactions(3);
      assertThat(opt.maxSubcompactions()).isEqualTo(3);
    }
  }
}
