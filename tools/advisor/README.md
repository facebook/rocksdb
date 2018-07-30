# Rocksdb Optimizer

## Motivation

The performance of Rocksdb is highly contingent on its tuning. However,
because of the complexity of its underlying technology and a large number of
configurable parameters, a good configuration is hard to obtain. The aim of
the python command-line tool, Rocksdb Optimizer, is to automate the process of
suggesting an improved configuration to users based on advice from Rocksdb
experts.

## Overview

Experts share their wisdom as rules comprising of conditions and suggestions
in the INI format (refer [rules.ini](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/rules.ini)).
Users provide the starting Rocksdb
configuration that they want to improve upon (as the familiar Rocksdb
OPTIONS file). The [Optimizer](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/db_config_optimizer.py) runs an optimization loop: use benchmark
runner (such as [DBBenchRunner](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/db_bench_runner.py) based on
[db_bench](https://github.com/facebook/rocksdb/blob/master/tools/db_bench_tool.cc)) to run
experiments with provided configuration; get resulting data sources
(Rocksdb logs, options, statistics, perf context, etc.); provide them to the
[Rules Engine](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/rule_parser.py)
which uses rules from experts to parse the data-sources
and trigger appropriate rules. The suggestions corresponding to the triggered
rules are applied to the configuration and an updated configuration is
obtained which is again fed to the benchmark runner and the optimization loop
continues.

The aim of the optimization loop is to explore the Rocksdb configuration
space and find a set of options that improve Rocksdb performance.
Performance is measured by a metric such as throughput. Finally, the
Optimizer returns a Rocksdb configuration that provides better performance
than the user's starting configuration.


## Usage

### Prerequisites
The Optimizer tool needs the following to run:
* python3
* [db_bench tool binary](https://github.com/facebook/rocksdb/wiki/Performance-Benchmarks)

### Running the tool
An example command to run the tool:

```shell
cd rocksdb/tools/advisor
python3 -m advisor.config_optimizer_example --base_db_path=/tmp/rocksdbtest-155919/dbbench --rocksdb_options=temp/OPTIONS_boot.tmp --misc_options bloom_bits=2 --rules_spec=advisor/rules.ini --stats_dump_period_sec=20 --benchrunner_module=advisor.db_bench_runner --benchrunner_class=DBBenchRunner --benchrunner_pos_args ./../../db_bench readwhilewriting use_existing_db=true duration=90

```
### Command-line arguments

Most important amongst all the input that the Optimizer needs, are the rules
spec and starting Rocksdb configuration. The configuration is provided as the
familiar Rocksdb Options file (refer [example](https://github.com/facebook/rocksdb/blob/master/examples/rocksdb_option_file_example.ini)).
The Rules spec is written in the INI format (more details in
[rules.ini](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/rules.ini)).

In brief, a Rule is made of conditions and is triggered when all its
constituent conditions are triggered. When triggered, a Rule suggests changes
(increase/decrease/set to a suggested value) to certain Rocksdb options that
aim to improve Rocksdb performance. Every Condition has a 'source' i.e.
the data source that would be checked for triggering that condition.
For example, a log Condition (with 'source=LOG') is triggered if a particular
'regex' is found in the Rocksdb LOG files. As of now the Rules Engine
supports 3 types of Conditions (and consequently data-sources):
LOG, OPTIONS, TIME_SERIES. The TIME_SERIES data can be sourced from the
Rocksdb [statistics](https://github.com/facebook/rocksdb/blob/master/include/rocksdb/statistics.h)
or [perf context](https://github.com/facebook/rocksdb/blob/master/include/rocksdb/perf_context.h).
Also, db_bench is the benchmark runner used by the Optimizer to search the configuration space.

The output of the tool explains how the Optimizer is performing the random
walk in the configuration space and how updates to the configuration are
affecting Rocksdb performance.

For more information about the remaining command-line arguments, run:

```shell
cd rocksdb/tools/advisor
python3 -m advisor.config_optimizer_example --help
```

## Running the tests

Since the optimization algorithm is not deterministic, it's hard to provide
end-to-end tests for the Optimizer. Hence, unit tests for the code have been
added to the [test/](https://github.com/facebook/rocksdb/tree/master/tools/advisor/test)
directory. For example, to run the unit tests for db_log_parser.py:

```shell
cd rocksdb/tools/advisor
python3 -m unittest -v test.test_db_log_parser
```

## Future Work

The tool has great potential for improvement, some ideas for enhancement are:
* improving the Optimizer's configuration search algorithm
* extending support to more benchmark runners like sysbench or LinkBench
by extending the class
[BenchmarkRunner](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/bench_runner.py)
* adding support for more complex Rules, Conditions, and Suggestions
