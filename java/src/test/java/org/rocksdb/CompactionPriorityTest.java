// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionPriorityTest {

  @Test(expected = IllegalArgumentException.class)
  public void failIfIllegalByteValueProvided() {
    CompactionPriority.getCompactionPriority((byte) -1);
  }

  @Test
  public void getCompactionPriority() {
    assertThat(CompactionPriority.getCompactionPriority(
        CompactionPriority.OldestLargestSeqFirst.getValue()))
            .isEqualTo(CompactionPriority.OldestLargestSeqFirst);
  }

  @Test
  public void valueOf() {
    assertThat(CompactionPriority.valueOf("OldestSmallestSeqFirst")).
        isEqualTo(CompactionPriority.OldestSmallestSeqFirst);
  }
}
