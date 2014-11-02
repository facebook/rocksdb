// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import java.util.Random;

import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.rocksdb.ReadOptions;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadOptionsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @AfterClass
  public static void printMessage(){
    System.out.println("Passed ReadOptionsTest.");
  }

  @Test
  public void shouldTestReadOptions() {
    ReadOptions opt = new ReadOptions();
    Random rand = new Random();
    { // VerifyChecksums test
      boolean boolValue = rand.nextBoolean();
      opt.setVerifyChecksums(boolValue);
      assertThat(opt.verifyChecksums()).isEqualTo(boolValue);
    }

    { // FillCache test
      boolean boolValue = rand.nextBoolean();
      opt.setFillCache(boolValue);
      assertThat(opt.fillCache()).isEqualTo(boolValue);
    }

    { // Tailing test
      boolean boolValue = rand.nextBoolean();
      opt.setTailing(boolValue);
      assertThat(opt.tailing()).isEqualTo(boolValue);
    }

    { // Snapshot null test
      opt.setSnapshot(null);
      assertThat(opt.snapshot()).isNull();
    }
    opt.dispose();
  }

  @Test
  public void shouldFailVerifyChecksumUninitialized(){
    ReadOptions readOptions = setupUninitializedReadOptions(
        exception);
    readOptions.setVerifyChecksums(true);
  }

  @Test
  public void shouldFailSetFillCacheUninitialized(){
    ReadOptions readOptions = setupUninitializedReadOptions(
        exception);
    readOptions.setFillCache(true);
  }

  @Test
  public void shouldFailFillCacheUninitialized(){
    ReadOptions readOptions = setupUninitializedReadOptions(
        exception);
    readOptions.fillCache();
  }

  @Test
  public void shouldFailSetTailingUninitialized(){
    ReadOptions readOptions = setupUninitializedReadOptions(
        exception);
    readOptions.setTailing(true);
  }

  @Test
  public void shouldFailTailingUninitialized(){
    ReadOptions readOptions = setupUninitializedReadOptions(
        exception);
    readOptions.tailing();
  }

  @Test
  public void shouldFailSetSnapshotUninitialized(){
    ReadOptions readOptions = setupUninitializedReadOptions(
        exception);
    readOptions.setSnapshot(null);
  }

  @Test
  public void shouldFailSnapshotUninitialized(){
    ReadOptions readOptions = setupUninitializedReadOptions(
        exception);
    readOptions.snapshot();
  }

  private ReadOptions setupUninitializedReadOptions(
      ExpectedException exception) {
    ReadOptions readOptions = new ReadOptions();
    readOptions.dispose();
    exception.expect(AssertionError.class);
    return readOptions;
  }
}
