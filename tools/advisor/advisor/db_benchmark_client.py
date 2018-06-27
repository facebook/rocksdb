from abc import ABC, abstractmethod
from advisor.db_log_parser import DatabaseLogs, NO_FAM
from advisor.db_options_parser import DatabaseOptions
from advisor.db_stats_fetcher import DBBenchStatsParser
import re
import subprocess


'''
NOTE: This is not thread-safe, because the output file is simply overwritten.
'''


class BenchmarkRunner(ABC):
    @abstractmethod
    def get_available_workloads(self):
        pass

    @abstractmethod
    def run_experiment(self):
        # should return the DatabaseLogs, DatabaseOptions, TimeSeriesData
        # objects
        pass


class DBBenchRunner(BenchmarkRunner):
    OUTPUT_FILE = "temp/dbbench_out.tmp"
    ERROR_FILE = "temp/dbbench_err.tmp"

    def __init__(self, client_binary_path, environment_variables):
        self.db_bench_binary = client_binary_path
        self.supported_benchmarks = None
        if not environment_variables:
            self.environment_variables = ''
        else:
            self.environment_variables = environment_variables
        self.get_available_workloads()

    def _get_log_options(self, db_options):
        # get the location of the LOG file and the frequency at which stats are
        # dumped in the LOG file
        log_dir_path = None
        stats_freq_sec = None
        # options to fetch
        dump_period = 'DBOptions.stats_dump_period_sec'
        log_dir = 'DBOptions.db_log_dir'
        wal_dir = 'DBOptions.wal_dir'
        log_options = db_options.get_options([dump_period, log_dir, wal_dir])

        if dump_period in log_options:
            stats_freq_sec = int(log_options[dump_period][NO_FAM])
        if self.environment_variables:
            for env_var in self.environment_variables:
                if re.search('TEST_TMPDIR', env_var):
                    log_dir_path = env_var.split('=')[1].strip() + '/dbbench'
                    break
        if not log_dir_path:
            if log_dir in log_options:
                log_dir_path = log_options[log_dir][NO_FAM]
            if not log_dir_path and wal_dir in log_options:
                log_dir_path = log_options[wal_dir][NO_FAM]
        return (log_dir_path, stats_freq_sec)

    def _run_command(self, command):
        # run db_bench and return the
        out_file = open(self.OUTPUT_FILE, "w+")
        err_file = open(self.ERROR_FILE, "w+")
        print('waiting for db_bench to finish running...executing...')
        print(command)
        try:
            subprocess.call(
                command,
                shell=True,
                stdout=out_file,
                stderr=err_file,
                timeout=600
            )
        except subprocess.TimeoutExpired as e:
            print('db_bench timeout: 10 mins ' + repr(e))
        out_file.close()
        err_file.close()

    def run_experiment(self, benchmarks, db_options):
        # type: (List[str], str) -> str
        for benchmark in benchmarks:
            if benchmark not in self.supported_benchmarks:
                raise ValueError(benchmark + ": not a supported benchmark")
        # get the log options from the OPTIONS file
        log_dir_path, stats_freq_sec = self._get_log_options(db_options)
        # generate an options configuration file
        options_file = db_options.generate_options_config(nonce='12345')
        command = "%s %s --options_file=%s --benchmarks=%s --statistics "
        command = command % (
            " ".join(self.environment_variables),
            self.db_bench_binary,
            options_file,
            ",".join(benchmarks)
        )
        command = command.strip()
        self._run_command(command)

        # Create the LOGS object
        logs_file_prefix = log_dir_path + '/LOG'
        db_logs = DatabaseLogs(
            logs_file_prefix, db_options.get_column_families()
        )
        # Create the STATS object
        db_stats = DBBenchStatsParser(logs_file_prefix, stats_freq_sec)
        return [db_options, db_logs, db_stats]

    def get_available_workloads(self):
        if not self.supported_benchmarks:
            self.supported_benchmarks = []
            command = '%s --help' % self.db_bench_binary
            self._run_command(command)
            with open(self.OUTPUT_FILE, 'r') as fp:
                start = False
                for line in fp:
                    if re.search('available benchmarks', line, re.IGNORECASE):
                        start = True
                        continue
                    elif start:
                        if re.search('meta operations', line, re.IGNORECASE):
                            break
                        benchmark_info = line.strip()
                        if benchmark_info:
                            token_list = benchmark_info.split()
                            if len(token_list) > 2 and token_list[1] == '--':
                                self.supported_benchmarks.append(token_list[0])
                    else:
                        continue
            self.supported_benchmarks = sorted(self.supported_benchmarks)
        return self.supported_benchmarks


# TODO: remove this method, used only for testing
def main():
    db_bench_helper = DBBenchRunner(
        '/home/poojamalik/workspace/rocksdb/db_bench',
        ['TEST_TMPDIR=/dev/shm']
    )
    # populate benchmarks with the available ones in the db_bench tool
    benchmarks = db_bench_helper.get_available_workloads()
    print(benchmarks)
    print()
    run_benchmarks = ['fillrandom']
    options_file = (
        '/home/poojamalik/workspace/rocksdb/tools/advisor/temp/' +
        'OPTIONS-000005.tmp'
    )
    db_options = DatabaseOptions(options_file)
    data_sources = db_bench_helper.run_experiment(run_benchmarks, db_options)
    print(data_sources[0].options_dict)
    print()
    print(data_sources[1].logs_path_prefix)
    print(data_sources[1].column_families)
    print()
    print(data_sources[2].logs_file_prefix)
    print(data_sources[2].stats_freq_sec)


if __name__ == "__main__":
    main()
