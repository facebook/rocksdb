## Logging

By default, a RocksDB database will not perform any logging. However, as a RocksJava user, you still might be interested in compaction statistics, write stalls, flush times, etc.

RocksJava supports two mechanisms for logging: Java-based logging, and native logging:

* In Java-based logging, you provide a Java class that extends `org.rocksdb.Logger`.
    This class has a number of callback methods that are invoked by RocksDB via JNI for each message to be logged. It is up to you to decide where to send these messages in your callback methods. It is typical for developers to implement this as a simple bridge to a popular Java logging framework such as [SLF4J](https://www.slf4j.org) (see below for an example), [Log4j](https://logging.apache.org/log4j), etc. Note that JNI imposes a small performance overhead for each function call.

* In native logging, a native logger (i.e. one that is implemented in C++ and compiled into the RocksDB library) is invoked for each log message. Native loggers have a much lower performance overhead than Java loggers, but either require you to implement your logger in C++ and build a custom RocksDB library, or make use of one of the RocksDB provided native loggers.

## Configuring a Java Logger

Once you have a RocksJava `Options` or `DBOptions` object, you can call `options.setLogger(LoggerInterface)` and pass it an instance of your logging class (which must extend `org.rocksdb.Logger`), which can be constructed either by:

1. Passing in `options` directly. This is deprecated as of RocksDB 8.11.0, but supported in all versions.
2. Passing in an `InfoLogLevel`. This is supported as of RocksDB 8.11.0 and newer, and is the recommended approach.

For example an [SLF4J](https://www.slf4j.org/) logging framework bridge:

```java
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDbSlf4jLogger extends Logger {

  private static final Logger logger = LoggerFactory.getLogger(RocksDbSlf4jLogger.class);

  public RocksDbSlf4jLogger(InfoLogLevel logLevel) {
    super(logLevel);
  }

  @Override
  protected void log(InfoLogLevel logLevel, String message) {
    switch (logLevel) {
      case FATAL_LEVEL:
      case ERROR_LEVEL:
        logger.error(message);
        break;

      case WARN_LEVEL:
        logger.warn(message);
        break;

      case INFO_LEVEL:
        logger.info(message);
        break;

      case DEBUG_LEVEL:
        logger.debug(message);
        break;

      default:
      log4jLogger.trace(message);
    }
  }

}

```

Then to use this:
```java
try (Logger slf4jLogger = new RocksDbSlf4jLogger(InfoLogLevel.INFO)) {

  options.setLogger(slf4jLogger);

}
```

> [!IMPORTANT]  
> Your Logger will have an underlying C++ abstraction allocated to it in memory (as does every `RocksCallbackObject` and `RocksObject` in RocksJava), therefore to cleanup the memory when you are finished with your Logger you must call the `Logger#close()` method or construct your Logger within a Java [`try-with-resources` statement](https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html). For more details about memory management in RocksJava see: [RocksJava - Basics - Memory Management](https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#memory-management).

## Configuring a Native Logger

You can configure a Native Logger with the same `options.setLogger(LoggerInterface)` method, on either `Options` or `DBOptions`. Currently, RocksDB only provides a single native logger: the stderr (Standard Error) logger, otherwuse implementing your own [native logger](https://github.com/facebook/rocksdb/wiki/Logger) in C++ is also possible.

You can configure the `stderr` native logger from RocksJava as follows:

``` java
try (StdErrLogger stdErrLogger = new StdErrLogger(InfoLogLevel.DEBUG_LEVEL)) {

  options.setLogger(stdErrLogger);

}
```

If you are opening multiple RocksDB databases from the same RocksJava process, it will be hard to differentiate which logs belong to which RocksDB database. If you were writing your own Java-based logger, you could embed the database name in your logger. To achieve similar functionality with native loggers, you can create a `StderrLogger` with a log prefix option. The prefix will be appended to each log message after the timestamp and thread ID.

``` java
try (StdErrLogger stdErrLoggerForFooDB = new StdErrLogger(InfoLogLevel.DEBUG_LEVEL, "[Foo DB] ")) {

  fooOptions.setLogger(stdErrLoggerForFooDB);
  RocksDB.open(fooOptions, ...)

}

try (StdErrLogger stdErrLoggerForBarDB = new StdErrLogger(InfoLogLevel.DEBUG_LEVEL, "[Bar DB] ")) {
  barOptions.setLogger(stdErrLoggerForBarDB);
  RocksDB.open(barOptions, ...)
}
```