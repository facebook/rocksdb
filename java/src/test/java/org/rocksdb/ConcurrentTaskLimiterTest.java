//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class ConcurrentTaskLimiterTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  private static final String NAME = "name";

  private ConcurrentTaskLimiter concurrentTaskLimiter;

  @Before
  public void beforeTest() {
    concurrentTaskLimiter = new ConcurrentTaskLimiterImpl(NAME, 3);
  }

  @Test
  public void name() {
    assertThat(concurrentTaskLimiter.name()).isEqualTo(NAME);
  }

  @Test
  public void outstandingTask() {
    assertThat(concurrentTaskLimiter.outstandingTask()).isEqualTo(0);
  }

  @Test
  public void setMaxOutstandingTask() {
    assertThat(concurrentTaskLimiter.setMaxOutstandingTask(4)).isEqualTo(concurrentTaskLimiter);
    assertThat(concurrentTaskLimiter.outstandingTask()).isEqualTo(0);
  }

  @Test
  public void resetMaxOutstandingTask() {
    assertThat(concurrentTaskLimiter.resetMaxOutstandingTask()).isEqualTo(concurrentTaskLimiter);
    assertThat(concurrentTaskLimiter.outstandingTask()).isEqualTo(0);
  }

  @After
  public void afterTest() {
    concurrentTaskLimiter.close();
  }
}
