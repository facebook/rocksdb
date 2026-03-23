# Logger and Info Log

**Files:** include/rocksdb/env.h (Logger class, InfoLogLevel enum), logging/auto_roll_logger.h, logging/env_logger.h, logging/event_logger.h, logging/log_buffer.h, logging/logging.h

## Overview

RocksDB writes operational information to a LOG file in the database directory. This includes startup configuration, compaction/flush events, background errors, and periodic stats dumps. The logging system is built on the Logger interface with configurable log levels, automatic file rotation, and structured event logging.

## InfoLogLevel

The InfoLogLevel enum (see include/rocksdb/env.h) defines log severity levels:

| Level | Value | Description |
|-------|-------|-------------|
| DEBUG_LEVEL | 0 | Detailed debugging information |
| INFO_LEVEL | 1 | Normal operational information |
| WARN_LEVEL | 2 | Warning conditions |
| ERROR_LEVEL | 3 | Error conditions |
| FATAL_LEVEL | 4 | Fatal errors |
| HEADER_LEVEL | 5 | Header information (always logged regardless of level) |

The default log level is INFO_LEVEL in release builds and DEBUG_LEVEL in debug builds (see Logger::kDefaultLogLevel).

Set the log level via DBOptions::info_log_level (see include/rocksdb/options.h). Only messages at or above this level are written to the LOG file.

## Logger Interface

The Logger class (see include/rocksdb/env.h) defines the logging interface:

- Logv(format, va_list) -- write a log entry (base overload)
- Logv(InfoLogLevel, format, va_list) -- write with explicit level
- GetInfoLogLevel() / SetInfoLogLevel() -- query/modify the active log level
- Flush() -- flush to OS buffers
- Close() -- finalize and close (recommended before destruction)

Users can provide a custom Logger implementation via DBOptions::info_log. If not set, RocksDB creates a default file-based logger writing to the database directory.

## Log Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| info_log_level | INFO_LEVEL (release) | Minimum log level to output |
| max_log_file_size | 0 (unlimited) | Maximum LOG file size before rotation |
| log_file_time_to_roll | 0 (disabled) | Seconds before time-based rotation |
| keep_log_file_num | 1000 | Number of old LOG files to retain (must be > 0) |
| db_log_dir | empty (use DB dir) | Directory for LOG files |

See DBOptions in include/rocksdb/options.h for these options.

## AutoRollLogger

When max_log_file_size > 0 or log_file_time_to_roll > 0, RocksDB uses AutoRollLogger (see logging/auto_roll_logger.h) which automatically rotates the LOG file based on size or time. When db_log_dir is empty, old log files are named LOG.old.<timestamp> in the database directory. When db_log_dir is set, old log files use a database-path-derived prefix (replacing special characters with _ and appending _LOG) inside the log directory, so multiple databases can share a log directory without filename conflicts. Old log files are retained up to keep_log_file_num files; old-log trimming runs both on rollover and on logger startup.

**Note:** keep_log_file_num must be greater than 0; setting it to 0 returns InvalidArgument.

AutoRollLogger also supports header logging via LogHeader() -- header entries are re-written to every new log file after rotation, ensuring that configuration information is always present at the start of each log file.

## Logging Macros

Internal code uses macros defined in logging/logging.h for level-based logging:

| Macro | Level |
|-------|-------|
| ROCKS_LOG_DEBUG(logger, fmt, ...) | DEBUG_LEVEL |
| ROCKS_LOG_INFO(logger, fmt, ...) | INFO_LEVEL |
| ROCKS_LOG_WARN(logger, fmt, ...) | WARN_LEVEL |
| ROCKS_LOG_ERROR(logger, fmt, ...) | ERROR_LEVEL |
| ROCKS_LOG_FATAL(logger, fmt, ...) | FATAL_LEVEL |
| ROCKS_LOG_HEADER(logger, fmt, ...) | HEADER_LEVEL |

These macros automatically prepend [file:line] to the log message (except HEADER_LEVEL) for source traceability.

## Event Logger

The EventLogger (see logging/event_logger.h) provides structured JSON event logging for significant database events (compaction start/finish, flush, file creation/deletion). These structured events are interspersed with regular log messages in the LOG file and can be parsed programmatically.

## Log Buffer

The LogBuffer class (see logging/log_buffer.h) buffers log messages and flushes them all at once. This is used during operations that hold the DB mutex -- messages are buffered while the mutex is held and flushed after the mutex is released, avoiding I/O under the lock.

## LOG File Contents

A typical LOG file contains:

- **Header**: RocksDB version, build info, and all Options values at database open
- **Operational events**: compaction started/finished, flush started/finished, file creation/deletion
- **Periodic stats**: rocksdb.stats output every stats_dump_period_sec seconds
- **Warnings and errors**: write stalls, background errors, recovery events
- **Column family operations**: creation, dropping, options changes

## Debugging Tips

- Set info_log_level = DEBUG_LEVEL for maximum detail (may generate very large LOG files)
- Check the LOG file tail after crashes for the last error messages
- Search for "Compaction" in the LOG to trace compaction history
- Search for "stall" to find write stall events
- Search for "error" or "Corruption" for error conditions
- Use db_log_dir to redirect LOG files when the database directory is on slow storage
