package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class AbstractLoggerTest {
  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  private AtomicInteger logMessageCounter = new AtomicInteger();

  @Test
  public void customLogger() throws RocksDBException {
    RocksDB db = null;
    logMessageCounter.set(0);
    try {

      // Setup options
      final Options options = new Options().
          setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL).
          setCreateIfMissing(true);

      // Create new logger with max log level passed by options
      AbstractLogger abstractLogger = new AbstractLogger(options) {
        @Override
        protected void log(InfoLogLevel infoLogLevel, String logMsg) {
          assertThat(logMsg).isNotNull();
          assertThat(logMsg.length()).isGreaterThan(0);
          logMessageCounter.incrementAndGet();
        }
      };

      // Set custom logger to options
      options.setLogger(abstractLogger);

      db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());

      // there should be more than zero received log messages in
      // debug level.
      assertThat(logMessageCounter.get()).isGreaterThan(0);
    } finally {
      if (db != null) {
        db.close();
      }
    }
    logMessageCounter.set(0);
  }


  @Test
  public void fatalLogger() throws RocksDBException {
    RocksDB db = null;
    logMessageCounter.set(0);

    try {
      // Setup options
      final Options options = new Options().
          setInfoLogLevel(InfoLogLevel.FATAL_LEVEL).
          setCreateIfMissing(true);

      // Create new logger with max log level passed by options
      AbstractLogger abstractLogger = new AbstractLogger(options) {
        @Override
        protected void log(InfoLogLevel infoLogLevel, String logMsg) {
          assertThat(logMsg).isNotNull();
          assertThat(logMsg.length()).isGreaterThan(0);
          logMessageCounter.incrementAndGet();
        }
      };

      // Set custom logger to options
      options.setLogger(abstractLogger);

      db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());

      // there should be zero messages
      // using fatal level as log level.
      assertThat(logMessageCounter.get()).isEqualTo(0);
    } finally {
      if (db != null) {
        db.close();
      }
    }
    logMessageCounter.set(0);
  }

  @Test
  public void dbOptionsLogger() throws RocksDBException {
    RocksDB db = null;
    AbstractLogger abstractLogger = null;
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

    logMessageCounter.set(0);
    try {
      // Setup options
      final DBOptions options = new DBOptions().
          setInfoLogLevel(InfoLogLevel.FATAL_LEVEL).
          setCreateIfMissing(true);

      // Create new logger with max log level passed by options
      abstractLogger = new AbstractLogger(options) {
        @Override
        protected void log(InfoLogLevel infoLogLevel, String logMsg) {
          assertThat(logMsg).isNotNull();
          assertThat(logMsg.length()).isGreaterThan(0);
          logMessageCounter.incrementAndGet();
        }
      };

      // Set custom logger to options
      options.setLogger(abstractLogger);
      db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
          cfDescriptors, cfHandles);
      // there should be zero messages
      // using fatal level as log level.
      assertThat(logMessageCounter.get()).isEqualTo(0);
      logMessageCounter.set(0);
    } finally {
      for (ColumnFamilyHandle columnFamilyHandle : cfHandles) {
        columnFamilyHandle.dispose();
      }
      if (db != null) {
        db.close();
      }
      if (abstractLogger != null) {
        abstractLogger.dispose();
      }
    }
  }

  @Test
  public void setInfoLogLevel() {
    AbstractLogger abstractLogger = null;
    try {
      // Setup options
      final Options options = new Options().
          setInfoLogLevel(InfoLogLevel.FATAL_LEVEL).
          setCreateIfMissing(true);

      // Create new logger with max log level passed by options
      abstractLogger = new AbstractLogger(options) {
        @Override
        protected void log(InfoLogLevel infoLogLevel, String logMsg) {
          assertThat(logMsg).isNotNull();
          assertThat(logMsg.length()).isGreaterThan(0);
          logMessageCounter.incrementAndGet();
        }
      };
      assertThat(abstractLogger.infoLogLevel()).
          isEqualTo(InfoLogLevel.FATAL_LEVEL);
      abstractLogger.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
      assertThat(abstractLogger.infoLogLevel()).
          isEqualTo(InfoLogLevel.DEBUG_LEVEL);
    } finally {
      if (abstractLogger != null) {
        abstractLogger.dispose();
      }
    }
  }

  @Test
  public void changeLogLevelAtRuntime() throws RocksDBException {
    RocksDB db = null;
    logMessageCounter.set(0);

    try {
      // Setup options
      final Options options = new Options().
          setInfoLogLevel(InfoLogLevel.FATAL_LEVEL).
          setCreateIfMissing(true);

      // Create new logger with max log level passed by options
      AbstractLogger abstractLogger = new AbstractLogger(options) {
        @Override
        protected void log(InfoLogLevel infoLogLevel, String logMsg) {
          assertThat(logMsg).isNotNull();
          assertThat(logMsg.length()).isGreaterThan(0);
          logMessageCounter.incrementAndGet();
        }
      };

      // Set custom logger to options
      options.setLogger(abstractLogger);
      db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());

      // there should be zero messages
      // using fatal level as log level.
      assertThat(logMessageCounter.get()).isEqualTo(0);

      // change log level to debug level
      abstractLogger.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);

      db.put("key".getBytes(), "value".getBytes());
      db.flush(new FlushOptions().setWaitForFlush(true));

      // messages shall be received due to previous actions.
      assertThat(logMessageCounter.get()).isNotEqualTo(0);

    } finally {
      if (db != null) {
        db.close();
      }
    }
    logMessageCounter.set(0);
  }
}
