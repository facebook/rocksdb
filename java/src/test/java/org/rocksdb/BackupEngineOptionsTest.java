// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class BackupEngineOptionsTest {
  private final static String ARBITRARY_PATH =
      System.getProperty("java.io.tmpdir");

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void backupDir() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      assertThat(backupEngineOptions.backupDir()).isEqualTo(ARBITRARY_PATH);
    }
  }

  @Test
  public void env() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      assertThat(backupEngineOptions.backupEnv()).isNull();

      try(final Env env = new RocksMemEnv(Env.getDefault())) {
        backupEngineOptions.setBackupEnv(env);
        assertThat(backupEngineOptions.backupEnv()).isEqualTo(env);
      }
    }
  }

  @Test
  public void shareTableFiles() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final boolean value = rand.nextBoolean();
      backupEngineOptions.setShareTableFiles(value);
      assertThat(backupEngineOptions.shareTableFiles()).isEqualTo(value);
    }
  }

  @Test
  public void infoLog() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      assertThat(backupEngineOptions.infoLog()).isNull();

      try(final Options options = new Options();
          final Logger logger = new Logger(options){
            @Override
            protected void log(InfoLogLevel infoLogLevel, String logMsg) {

            }
          }) {
        backupEngineOptions.setInfoLog(logger);
        assertThat(backupEngineOptions.infoLog()).isEqualTo(logger);
      }
    }
  }

  @Test
  public void sync() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final boolean value = rand.nextBoolean();
      backupEngineOptions.setSync(value);
      assertThat(backupEngineOptions.sync()).isEqualTo(value);
    }
  }

  @Test
  public void destroyOldData() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH);) {
      final boolean value = rand.nextBoolean();
      backupEngineOptions.setDestroyOldData(value);
      assertThat(backupEngineOptions.destroyOldData()).isEqualTo(value);
    }
  }

  @Test
  public void backupLogFiles() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final boolean value = rand.nextBoolean();
      backupEngineOptions.setBackupLogFiles(value);
      assertThat(backupEngineOptions.backupLogFiles()).isEqualTo(value);
    }
  }

  @Test
  public void backupRateLimit() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final long value = Math.abs(rand.nextLong());
      backupEngineOptions.setBackupRateLimit(value);
      assertThat(backupEngineOptions.backupRateLimit()).isEqualTo(value);
      // negative will be mapped to 0
      backupEngineOptions.setBackupRateLimit(-1);
      assertThat(backupEngineOptions.backupRateLimit()).isEqualTo(0);
    }
  }

  @Test
  public void backupRateLimiter() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      assertThat(backupEngineOptions.backupEnv()).isNull();

      try(final RateLimiter backupRateLimiter =
              new RateLimiter(999)) {
        backupEngineOptions.setBackupRateLimiter(backupRateLimiter);
        assertThat(backupEngineOptions.backupRateLimiter()).isEqualTo(backupRateLimiter);
      }
    }
  }

  @Test
  public void restoreRateLimit() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final long value = Math.abs(rand.nextLong());
      backupEngineOptions.setRestoreRateLimit(value);
      assertThat(backupEngineOptions.restoreRateLimit()).isEqualTo(value);
      // negative will be mapped to 0
      backupEngineOptions.setRestoreRateLimit(-1);
      assertThat(backupEngineOptions.restoreRateLimit()).isEqualTo(0);
    }
  }

  @Test
  public void restoreRateLimiter() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      assertThat(backupEngineOptions.backupEnv()).isNull();

      try(final RateLimiter restoreRateLimiter =
              new RateLimiter(911)) {
        backupEngineOptions.setRestoreRateLimiter(restoreRateLimiter);
        assertThat(backupEngineOptions.restoreRateLimiter()).isEqualTo(restoreRateLimiter);
      }
    }
  }

  @Test
  public void shareFilesWithChecksum() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      boolean value = rand.nextBoolean();
      backupEngineOptions.setShareFilesWithChecksum(value);
      assertThat(backupEngineOptions.shareFilesWithChecksum()).isEqualTo(value);
    }
  }

  @Test
  public void maxBackgroundOperations() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final int value = rand.nextInt();
      backupEngineOptions.setMaxBackgroundOperations(value);
      assertThat(backupEngineOptions.maxBackgroundOperations()).isEqualTo(value);
    }
  }

  @Test
  public void callbackTriggerIntervalSize() {
    try (final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH)) {
      final long value = rand.nextLong();
      backupEngineOptions.setCallbackTriggerIntervalSize(value);
      assertThat(backupEngineOptions.callbackTriggerIntervalSize()).isEqualTo(value);
    }
  }

  @Test
  public void failBackupDirIsNull() {
    exception.expect(IllegalArgumentException.class);
    try (final BackupEngineOptions opts = new BackupEngineOptions(null)) {
      //no-op
    }
  }

  @Test
  public void failBackupDirIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.backupDir();
    }
  }

  @Test
  public void failSetShareTableFilesIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.setShareTableFiles(true);
    }
  }

  @Test
  public void failShareTableFilesIfDisposed() {
    try (BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.shareTableFiles();
    }
  }

  @Test
  public void failSetSyncIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.setSync(true);
    }
  }

  @Test
  public void failSyncIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.sync();
    }
  }

  @Test
  public void failSetDestroyOldDataIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.setDestroyOldData(true);
    }
  }

  @Test
  public void failDestroyOldDataIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.destroyOldData();
    }
  }

  @Test
  public void failSetBackupLogFilesIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.setBackupLogFiles(true);
    }
  }

  @Test
  public void failBackupLogFilesIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.backupLogFiles();
    }
  }

  @Test
  public void failSetBackupRateLimitIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.setBackupRateLimit(1);
    }
  }

  @Test
  public void failBackupRateLimitIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.backupRateLimit();
    }
  }

  @Test
  public void failSetRestoreRateLimitIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.setRestoreRateLimit(1);
    }
  }

  @Test
  public void failRestoreRateLimitIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.restoreRateLimit();
    }
  }

  @Test
  public void failSetShareFilesWithChecksumIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.setShareFilesWithChecksum(true);
    }
  }

  @Test
  public void failShareFilesWithChecksumIfDisposed() {
    try (final BackupEngineOptions options = setupUninitializedBackupEngineOptions(exception)) {
      options.shareFilesWithChecksum();
    }
  }

  private BackupEngineOptions setupUninitializedBackupEngineOptions(ExpectedException exception) {
    final BackupEngineOptions backupEngineOptions = new BackupEngineOptions(ARBITRARY_PATH);
    backupEngineOptions.close();
    exception.expect(AssertionError.class);
    return backupEngineOptions;
  }
}
