// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.rocksdb.WriteOptions;

import static org.assertj.core.api.Assertions.assertThat;

public class WriteOptionsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @AfterClass
  public static void printMessage(){
    System.out.println("Passed WriteOptionsTest.");
  }

  @Test
  public void shouldTestWriteOptions(){
    WriteOptions writeOptions = new WriteOptions();
    writeOptions.setDisableWAL(true);
    assertThat(writeOptions.disableWAL()).isTrue();
    writeOptions.setDisableWAL(false);
    assertThat(writeOptions.disableWAL()).isFalse();
    writeOptions.setSync(true);
    assertThat(writeOptions.sync()).isTrue();
    writeOptions.setSync(false);
    assertThat(writeOptions.sync()).isFalse();
  }
}
