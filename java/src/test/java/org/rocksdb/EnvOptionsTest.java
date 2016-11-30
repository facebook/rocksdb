// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class EnvOptionsTest {
  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource = new RocksMemoryResource();

  public static final Random rand = PlatformRandomHelper.getPlatformSpecificRandomFactory();

  @Test
  public void useOsBuffer() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setUseOsBuffer(boolValue);
      assertThat(envOptions.useOsBuffer()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useMmapReads() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setUseMmapReads(boolValue);
      assertThat(envOptions.useMmapReads()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useMmapWrites() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setUseMmapWrites(boolValue);
      assertThat(envOptions.useMmapWrites()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useDirectReads() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setUseDirectReads(boolValue);
      assertThat(envOptions.useDirectReads()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useDirectWrites() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setUseDirectWrites(boolValue);
      assertThat(envOptions.useDirectWrites()).isEqualTo(boolValue);
    }
  }

  @Test
  public void allowFallocate() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setAllowFallocate(boolValue);
      assertThat(envOptions.allowFallocate()).isEqualTo(boolValue);
    }
  }

  @Test
  public void setFdCloexecs() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setSetFdCloexec(boolValue);
      assertThat(envOptions.setFdCloexec()).isEqualTo(boolValue);
    }
  }

  @Test
  public void bytesPerSync() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final long longValue = rand.nextLong();
      envOptions.setBytesPerSync(longValue);
      assertThat(envOptions.bytesPerSync()).isEqualTo(longValue);
    }
  }

  @Test
  public void fallocateWithKeepSize() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setFallocateWithKeepSize(boolValue);
      assertThat(envOptions.fallocateWithKeepSize()).isEqualTo(boolValue);
    }
  }

  @Test
  public void compactionReadaheadSize() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final int intValue = rand.nextInt();
      envOptions.setCompactionReadaheadSize(intValue);
      assertThat(envOptions.compactionReadaheadSize()).isEqualTo(intValue);
    }
  }

  @Test
  public void randomAccessMaxBufferSize() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final int intValue = rand.nextInt();
      envOptions.setRandomAccessMaxBufferSize(intValue);
      assertThat(envOptions.randomAccessMaxBufferSize()).isEqualTo(intValue);
    }
  }

  @Test
  public void writableFileMaxBufferSize() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final int intValue = rand.nextInt();
      envOptions.setWritableFileMaxBufferSize(intValue);
      assertThat(envOptions.writableFileMaxBufferSize()).isEqualTo(intValue);
    }
  }

  @Test
  public void rateLimiterConfig() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final RateLimiterConfig rateLimiterConfig1 =
          new GenericRateLimiterConfig(1000, 100 * 1000, 1);
      envOptions.setRateLimiterConfig(rateLimiterConfig1);
      assertThat(envOptions.rateLimiterConfig()).isEqualTo(rateLimiterConfig1);

      final RateLimiterConfig rateLimiterConfig2 = new GenericRateLimiterConfig(1000);
      envOptions.setRateLimiterConfig(rateLimiterConfig2);
      assertThat(envOptions.rateLimiterConfig()).isEqualTo(rateLimiterConfig2);
    }
  }
}
