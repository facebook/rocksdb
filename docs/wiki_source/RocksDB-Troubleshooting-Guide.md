# Troubleshooting for RocksDB users 

## Information Logs
Usually, looking at information logs to see whether there are something abnormal is the first step of troubleshooting.

By default, RocksDB’s information logs residence in the same directory as your database. Look for `LOG` for the most recent log file, and `LOG.*` files for older ones. If you’ve explicitly set `option.db_log_dir`, find the logs there. The log file names will be contain information of the paths of your database.

Some users have overridden the default logger or use non-default ones and they’ll need to find the logs accordingly.
Be aware that the default log level (see `DBOptions.info_log_level` [here](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview#database-debug-logsaka-info-logs)) is `INFO`. With `DEBUG` level you’ll see more information, and with `WARN` level less.

With the default logger, a log line might look like this:
```
[timestamp] [thread-ID] [file + line number] [column family name] [statement]
```
For example:

```
2019/09/17-13:42:20.597910 7fef48069300 [_impl/db_impl_compaction_flush.cc:1473] [default] Manual compaction starting
```
After the timestamp, `7fef48069300` is the thread ID. Since usually a flush or compaction happens in one same thread, the thread ID might help you correlate which lines belong to the same job. `_impl/db_impl_compaction_flush.cc:1473` shows the source file and line number where the line is logged. It might be cut short to avoid long log lines. `default` is the column family name.

## Examine data on disk: `ldb` and `sst_dump` 
If you see that RocksDB has returned unexpected results. You may consider to examine the database files and see whether the data on disk is wrong and if it is wrong in what way. `ldb` is the best tool to do that (ldb stands for LevelDB and we never renamed it, more info[here](https://github.com/facebook/rocksdb/wiki/Administration-and-Data-Access-Tool#ldb-tool)).

Start with examining the data with `get` and `scan` ldb commands. These commands will only examine keys visible to users. If you want to examine invisible keys, e.g. tombstones, older versions, you can use `idump` command. Another useful command is `manifest_dump`, which shows the LSM-tree structure of the database. This can help narrow down the problem to LSM-tree metadata or certain SST files.

If you use a customized comparator, merge operator or Env, you may need to build a customized `ldb` for it to work with your database. You can do it by building a binary which calls `LDBTool::Run()` with customized options, or register your plug in using object registry before calling the function. See `ldb_tools.h` for details. 

To examine a specific SST file, use `sst_dump` tool. Starting with `scan` to examine the logical data. If needed, command `raw` can help examine data in more details.

See `./ldb --help`, `./sst_dump --help` and [[Administration and Data Access Tool]] for more details about the two tools.

## Debugging Performance Problems
It’s a good idea to start with statistics. [[Statistics]] and the information returned by `DB::GetProperty()` are two good places to look. Information Logs contain some performance information about each flush or compaction. For slow reads, [[Perf Context and IO Stats Context]] can help break down the latency. [[RocksDB Tuning Guide]] has some information about RocksDB performance.

## Asking For Help
We use GitHub issues only for bug reports, and use [RocksDB's Google Group](https://groups.google.com/forum/#!forum/rocksdb) or [Facebook Group](https://www.facebook.com/groups/rocksdb.dev/) for other issues. It’s not always clear to users whether an observed behavior is a RocksDB bug or not. Pick one using your best judgement.

To help the community to help more efficiently, provide as much information as possible. Try to include:
* Your environment.
* The language binding you are using: C++, C, Java, or third-party bindings.
* RocksDB release number.
* Options used (e.g. paste the option file under the DB directory).
* Findings when examining the database with `ldb` or `sst_dump`, related statistics, etc.
* It may be helpful to paste the compaction summary.
* You can even consider to attach the information files. Data itself is not logged there.

When reporting bugs, [MyRocks's bug reporting guidelines](https://github.com/facebook/mysql-5.6/wiki/Reporting-bugs-and-asking-for-help) might be helpful too.

For performance related questions, it may be helpful to check [[How to ask a performance related question]].

# Troubleshooting for RocksDB developers 

While RocksDB developers can also follow the troubleshooting advice for RocksDB users, a handful of other tools may be more suited writing new features or updating existing parts of the code.

* `ROCKS_LOG_INFO` : print statements written to the RocksDB LOGs can come in very handy. This macro can be used virtually anywhere in the code base, and follow a syntax similar to `printf` statements: `ROCKS_LOG_INFO(db_options_.info_log, "Foo bar: %d", baz)` where `db_options_` is an `ImmutableDBOptions` object, `"Foo bar: %d"` is the printed statements, where `%d` is replaced by the integer value of `baz`.
* `gdb` is an excellent, general debugging tool for C and C++. You can find many tutorials online (e.g.: [here](http://www.cs.toronto.edu/~krueger/csc209h/tut/gdb_tutorial.html)). To use `gdb` for RocksDB, compile the executable with the `DEBUG_LEVEL=2` to access certain internal variables commonly optimized by the compiler. 

