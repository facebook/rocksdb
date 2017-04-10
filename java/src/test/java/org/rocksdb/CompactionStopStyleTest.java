// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionStopStyleTest {

  @Test(expected = IllegalArgumentException.class)
  public void failIfIllegalByteValueProvided() {
    CompactionStopStyle.getCompactionStopStyle((byte) -1);
  }

  @Test
  public void getCompactionStopStyle() {
    assertThat(CompactionStopStyle.getCompactionStopStyle(
        CompactionStopStyle.CompactionStopStyleTotalSize.getValue()))
            .isEqualTo(CompactionStopStyle.CompactionStopStyleTotalSize);
  }

  @Test
  public void valueOf() {
    assertThat(CompactionStopStyle.valueOf("CompactionStopStyleSimilarSize")).
        isEqualTo(CompactionStopStyle.CompactionStopStyleSimilarSize);
  }
}
