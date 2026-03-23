# Introduction

RocksDB supports a generalized message logging infrastructure. RocksDB caters to a variety of use cases -- from low power mobile systems to high end servers running distributed applications. The framework helps extent the message logging infrastructure as per the use case requirements. The mobile app might need a relatively simpler logging mechanism, compared to a server running mission critical application. It also provides a means to integrate RocksDB log messages with the embedded application logging infrastructure. 

# Existing Logger Implementations

The [Logger](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/env.h#L663) class provides the interface definition for logging messages from RocksDB. 

The various implementations of Logger available are:

| Implementation        | Use           |
| ------------- |:-------------:| 
| NullLogger | /dev/null equivalent for logger| 
| StderrLogger| Pipes the messages to std::err equivalent| 
| HdfsLogger| Logs messages to HDFS|
| PosixLogger| Logs messages to POSIX file|
| AutoRollLogger| Automatically rolls files as they reach a certain size. Typically used for servers|
| EnvLogger| Log using an arbitrary Env|
| WinLogger| Specialized logger for Windows OS|

# Writing Your Custom Logger

Users are encouraged to write their own logging infrastructure as per the use case by extending any one of the existing logger implementations.

# Auto Roll Logger

With the default logger, the log files can grow to very large. It makes it harder for users to budget the disk space for it, especially for databases that are relatively small. Auto roll logger can help cap the disk usage by information logs. An auto roll logger will be automatically created on top of the logger defined in Env, if a user sets `options.max_log_file_size`, each file will be capped with that size, and combine the option with `options.keep_log_file_num` will have the total size of info log files under control.

`options.log_file_time_to_roll` can make log file rolling triggered by time.