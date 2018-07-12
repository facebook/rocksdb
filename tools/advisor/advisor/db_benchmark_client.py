from abc import ABC, abstractmethod
from advisor.db_log_parser import DatabaseLogs, NO_FAM
from advisor.db_options_parser import DatabaseOptions
from advisor.db_stats_fetcher import LogStatsParser, OdsStatsFetcher
import os
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
        # should return a list of DataSource objects
        pass


class DBBenchRunner(BenchmarkRunner):
    OUTPUT_FILE = "temp/dbbench_out.tmp"
    ERROR_FILE = "temp/dbbench_err.tmp"
    DB_PATH = "DB path"
    THROUGHPUT = "ops/sec"

    @staticmethod
    def get_info_log_file_name(db_path):
        file_name = db_path[1:]
        to_be_replaced = re.compile('[^0-9a-zA-Z\-_\.]')
        for character in to_be_replaced.findall(db_path):
            file_name = file_name.replace(character, '_')
        if not file_name.endswith('_'):
            file_name += '_'
        file_name += 'LOG'
        return file_name

    @staticmethod
    def get_opt_args_str(misc_options_dict):
        optional_args_str = ""
        for option_name, option_value in misc_options_dict.items():
            if option_value:
                optional_args_str += (" --" + option_name + "=" + option_value)
        return optional_args_str

    def __init__(self, positional_args, ods_args=None):
        # parse positional_args list appropriately
        self.db_bench_binary = positional_args[0]
        # save ods_args if provided
        self.ods_args = ods_args
        self.supported_benchmarks = None

    def _get_log_options(self, db_options, db_path):
        # get the location of the LOG file and the frequency at which stats are
        # dumped in the LOG file
        log_dir_path = None
        stats_freq_sec = None
        logs_file_prefix = None

        # fetch the options
        dump_period = 'DBOptions.stats_dump_period_sec'
        log_dir = 'DBOptions.db_log_dir'
        log_options = db_options.get_options([dump_period, log_dir])
        if dump_period in log_options:
            stats_freq_sec = int(log_options[dump_period][NO_FAM])
        if log_dir in log_options:
            log_dir_path = log_options[log_dir][NO_FAM]

        if not log_dir_path:
            log_dir_path = db_path
            log_file_name = 'LOG'
        else:
            log_file_name = DBBenchRunner.get_info_log_file_name(db_path)

        if not log_dir_path.endswith('/'):
            log_dir_path += '/'
        logs_file_prefix = log_dir_path + log_file_name

        return (logs_file_prefix, stats_freq_sec)

    def _run_command(self, command):
        # run db_bench and return the
        out_file = open(self.OUTPUT_FILE, "w+")
        err_file = open(self.ERROR_FILE, "w+")
        print('executing... - ' + command)
        subprocess.call(command, shell=True, stdout=out_file, stderr=err_file)
        out_file.close()
        err_file.close()

    def run_experiment(self, db_options):
        # type: (List[str], str) -> str
        # generate an options configuration file
        options_file = db_options.generate_options_config(nonce='12345')
        optional_args_str = DBBenchRunner.get_opt_args_str(
            db_options.get_misc_options()
        )
        command = "%s --options_file=%s --benchmarks=%s --statistics"
        benchmark = 'readwhilewriting'  # 'fillrandom'
        command = command % (self.db_bench_binary, options_file, benchmark)
        command += optional_args_str
        self._run_command(command)

        # get the db_path and throughput of this db_bench experiment
        throughput = None
        db_path = None
        with open(self.OUTPUT_FILE, 'r') as fp:
            for line in fp:
                if line.startswith(benchmark):
                    print(line)  # print output of db_bench run
                    token_list = line.strip().split()
                    for ix, token in enumerate(token_list):
                        if token.startswith(self.THROUGHPUT):
                            throughput = float(token_list[ix - 1])
                            break
                elif line.startswith(self.DB_PATH):
                    db_path = line.split('[')[1].split(']')[0]

        # Create the LOGS object
        # get the log options from the OPTIONS file
        logs_file_prefix, stats_freq_sec = self._get_log_options(
            db_options, db_path
        )
        db_logs = DatabaseLogs(
            logs_file_prefix, db_options.get_column_families()
        )
        # Create the Log STATS object
        db_log_stats = LogStatsParser(logs_file_prefix, stats_freq_sec)
        data_sources = [db_options, db_logs, db_log_stats]
        # Create the ODS STATS object
        if self.ods_args:
            data_sources.append(OdsStatsFetcher(
                self.ods_args['client_script'],
                self.ods_args['entity'],
                self.ods_args['key_prefix']
            ))
        return data_sources, throughput

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
    pos_args = [
        'TEST_TMPDIR=/dev/shm /home/poojamalik/workspace/rocksdb/db_bench'
    ]
    db_bench_helper = DBBenchRunner(pos_args)
    # populate benchmarks with the available ones in the db_bench tool
    benchmarks = db_bench_helper.get_available_workloads()
    print(benchmarks)
    print()
    options_file = (
        '/home/poojamalik/workspace/rocksdb/tools/advisor/temp/' +
        'OPTIONS_12345.tmp'
    )
    misc_options = ["rate_limiter_bytes_per_sec=1024000", "bloom_bits=2"]
    db_options = DatabaseOptions(options_file, misc_options)
    data_sources, _ = db_bench_helper.run_experiment(db_options)
    print(data_sources[0].options_dict)
    print()
    print(data_sources[1].logs_path_prefix)
    if os.path.isfile(data_sources[1].logs_path_prefix):
        print('log file exists!')
    else:
        print('error: log file does not exist!')
    print(data_sources[1].column_families)
    print()
    print(data_sources[2].logs_file_prefix)
    if os.path.isfile(data_sources[2].logs_file_prefix):
        print('log file exists!')
    else:
        print('error: log file does not exist!')
    print(data_sources[2].stats_freq_sec)


if __name__ == "__main__":
    main()
