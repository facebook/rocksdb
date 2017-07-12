// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class WALRecoveryModeTest {

  @Test
  public void getWALRecoveryMode() {
    for (final WALRecoveryMode walRecoveryMode : WALRecoveryMode.values()) {
      assertThat(WALRecoveryMode.getWALRecoveryMode(walRecoveryMode.getValue()))
          .isEqualTo(walRecoveryMode);
    }
  }
}
