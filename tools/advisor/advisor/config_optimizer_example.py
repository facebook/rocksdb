# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

import argparse
from advisor.db_config_optimizer import ConfigOptimizer
from advisor.db_benchmark_client import DBBenchRunner
from advisor.db_log_parser import NO_FAM
from advisor.db_options_parser import DatabaseOptions
from advisor.rule_parser import RulesSpec


def main(args):
    # initialise the RulesSpec parser
    rule_spec_parser = RulesSpec(args.rules_spec)
    rule_spec_parser.load_rules_from_spec()
    rule_spec_parser.perform_section_checks()
    # initialise the benchmark runner
    db_bench_runner = DBBenchRunner(
        args.db_bench_script, args.db_bench_env_var
    )
    # initialise the database configuration
    db_options = DatabaseOptions(args.rocksdb_options)
    # set the frequency at which stats are dumped in the LOG file and the
    # location of the LOG file.
    db_log_dump_settings = {
        "DBOptions.stats_dump_period_sec": {NO_FAM: 10},
        "DBOptions.db_log_dir": {NO_FAM: "/tmp/rocksdbtest-155919/dbbench"}
    }
    db_options.update_options(db_log_dump_settings)
    # initialise the configuration optimizer
    config_optimizer = ConfigOptimizer(
        db_bench_runner, db_options, rule_spec_parser, [args.benchmark]
    )
    # run the optimiser to improve the database configuration for given
    # benchmarks, with the help of expert-specified rules
    config_optimizer.run(num_iterations=3)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='This script is used for\
        searching for a better database configuration')
    parser.add_argument('--rocksdb_options', required=True, type=str)
    parser.add_argument('--rules_spec', required=True, type=str)
    # ODS arguments
    parser.add_argument('--ods_client', type=str)
    parser.add_argument('--ods_entities', type=str)
    parser.add_argument('--ods_key_prefix', type=str)
    parser.add_argument('--ods_start_time', type=str)
    parser.add_argument('--ods_end_time', type=str)
    # db_bench_script: absolute path to the db_bench script
    parser.add_argument('--db_bench_script', required=True, type=str)
    parser.add_argument('--benchmark', required=True, type=str)
    parser.add_argument('--db_bench_env_var', nargs='*')
    args = parser.parse_args()
    main(args)
