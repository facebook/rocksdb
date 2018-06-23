import argparse
from advisor.db_benchmark_client import DBBenchHelper
from advisor.db_config_optimizer import ConfigOptimizer
from advisor.db_options_parser import DatabaseOptions


# TODO: Multi-threading for the db_bench executions

# GOAL: to set some values in the options file
def bootstrap_options(options_file, bootstrap_options):
    # type: -> DatabaseOptions
    db_options = DatabaseOptions(options_file)
    bootstrap_dict = {}
    for option in bootstrap_options:
        bootstrap_dict[option.split('=')[0].strip()] = (
            option.split('=')[1].strip()
        )
    current_config = db_options.get_options(list(bootstrap_dict.keys()))
    for option in current_config:
        for col_fam in current_config[option]:
            current_config[option][col_fam] = bootstrap_dict[option]
    db_options.update_options(current_config)
    db_options.generate_options_config(options_file)
    return db_options


def main(args):
    # expected format for args.stat_to_improve is <statistic>:<metric>:<value>
    stat_tokens = args.stat_to_improve.split(':')
    improve_stat = tuple(token.strip().lower() for token in stat_tokens)
    db_options = (
        bootstrap_options(args.rocksdb_options, args.bootstrap_options)
    )
    db_bench_client = (
        DBBenchHelper(args.db_bench_script, args.db_bench_env_var)
    )
    config_optimizer = ConfigOptimizer(
        db_bench_client, db_options, args.advisor_output
    )
    print(config_optimizer.run(
        args.benchmark, improve_stat, args.factor_change, args.num_iterations)
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='This script is used for\
        searching for a better database configuration')
    # '../../../db_bench' --> db_bench_script
    parser.add_argument('--db_bench_script', required=True, type=str)
    parser.add_argument('--benchmark', required=True, type=str)
    parser.add_argument('--stat_to_improve', required=True, type=str)
    parser.add_argument('--advisor_output', required=True, type=str)
    parser.add_argument('--rocksdb_options', required=True, type=str)
    parser.add_argument('--factor_change', default=10, type=int)
    parser.add_argument('--num_iterations', default=10, type=int)
    parser.add_argument('--bootstrap_options', nargs='*')
    parser.add_argument('--db_bench_env_var', nargs='*')
    args = parser.parse_args()
    main(args)
