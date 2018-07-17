from abc import ABC, abstractmethod
from advisor.db_log_parser import DataSource, DatabaseLogs, NO_FAM
from advisor.db_options_parser import DatabaseOptions
from advisor.db_stats_fetcher import (
    LogStatsParser, OdsStatsFetcher, DatabasePerfContext
)
import glob
import os
import re
import subprocess
import time


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
    PERF_CON = "db perf context"

    # Map from Rocksdb option to its corresponding db_bench command-line arg
    OPTION_CMD_LINE_FLAG = {
        "max_subcompactions": "subcompactions"
    }

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
        self.benchmark = positional_args[1]
        self.default_db_options = None
        self.db_bench_args = None
        self.supported_benchmarks = None
        if len(positional_args) > 2:
            self.default_db_options = DatabaseOptions(positional_args[2], None)
            # options list with each option given as "<option>=<value>"
            if len(positional_args) > 3:
                self.db_bench_args = positional_args[3:]
        # save ods_args if provided
        self.ods_args = ods_args
        if not self.default_db_options:
            default_options_file = self._fetch_default_options()
            if default_options_file:
                self.default_db_options = DatabaseOptions(
                    default_options_file, None
                )

    def _fetch_default_options(self):
        command = self.db_bench_binary + ' --duration=1'
        self._run_command(command)
        parsed_output = self._parse_output(get_perf_context=False)
        db_path = parsed_output[self.DB_PATH]
        if not db_path.endswith('/'):
            db_path += '/'
        options_files_regex = db_path + 'OPTIONS*'
        options_files = glob.glob(options_files_regex)
        latest_options_file = None
        if options_files:
            options_files.sort(key=os.path.getmtime, reverse=True)
            latest_options_file = options_files[0]
        return latest_options_file

    def _parse_output(self, get_perf_context=False):
        # get the db_path and throughput of this db_bench experiment
        output = {
            self.THROUGHPUT: None, self.DB_PATH: None, self.PERF_CON: None
        }
        perf_context_begins = False
        with open(self.OUTPUT_FILE, 'r') as fp:
            for line in fp:
                if line.startswith(self.benchmark):
                    print(line)  # print output of db_bench run
                    token_list = line.strip().split()
                    for ix, token in enumerate(token_list):
                        if token.startswith(self.THROUGHPUT):
                            output[self.THROUGHPUT] = (
                                float(token_list[ix - 1])
                            )
                            perf_context_begins = True
                            break
                elif get_perf_context and perf_context_begins:
                    token_list = line.strip().split(',')
                    perf_context = {
                        tk.split('=')[0].strip(): tk.split('=')[1].strip()
                        for tk in token_list
                        if tk
                    }
                    # add timestamp information
                    timestamp = int(time.time())
                    perf_context_ts = {}
                    for stat in perf_context.keys():
                        perf_context_ts[stat] = {
                            timestamp: int(perf_context[stat])
                        }
                    output[self.PERF_CON] = perf_context_ts
                    perf_context_begins = False
                elif line.startswith(self.DB_PATH):
                    output[self.DB_PATH] = (
                        line.split('[')[1].split(']')[0]
                    )
        print('_parse_output:')
        print(output[self.THROUGHPUT])
        print(output[self.DB_PATH])
        print(output[self.PERF_CON])
        return output

    def get_log_options(self, db_options, db_path):
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

    def _build_experiment_command(self, curr_options, db_path):
        command = "%s --benchmarks=%s --statistics --perf_level=3 --db=%s" % (
            self.db_bench_binary, self.benchmark, db_path
        )
        optional_args_str = DBBenchRunner.get_opt_args_str(
            curr_options.get_misc_options()
        )

        if optional_args_str:  # send all options as command-line args
            optional_args_str = ""
            diff = DatabaseOptions.get_options_diff(
                self.default_db_options.get_all_options(),
                curr_options.get_all_options()
            )
            # use diff to extend the optional_args_str
            def_col_fam = 'default'  # name of the default column family
            # db_bench uses the options from the first column family in the
            # options file
            curr_col_fam = curr_options.get_column_families()[0]
            for option in diff:
                cmd_line_arg = ""
                opt_name = option.split('.')[-1]
                if opt_name in self.OPTION_CMD_LINE_FLAG:
                    opt_name = self.OPTION_CMD_LINE_FLAG[opt_name]
                if NO_FAM in diff[option]:
                    if diff[option][NO_FAM][1]:
                        cmd_line_arg = (
                            ' --' + opt_name + '=' +
                            str(diff[option][NO_FAM][1])
                        )
                else:
                    def_val = None
                    curr_val = None
                    if def_col_fam in diff[option]:
                        def_val = diff[option][def_col_fam][0]
                    if curr_col_fam in diff[option]:
                        curr_val = diff[option][curr_col_fam][1]
                    if curr_val and def_val != curr_val:
                        cmd_line_arg = ' --' + opt_name + '=' + curr_val
                optional_args_str += cmd_line_arg
        else:  # use the --options_file flag
            # generate an options configuration file
            options_file = curr_options.generate_options_config(nonce='12345')
            optional_args_str = " --options_file=" + options_file

        # handle the command-line args passed in the constructor
        for cmd_line_arg in self.db_bench_args:
            optional_args_str += (" --" + cmd_line_arg)

        command += optional_args_str
        return command

    def _run_command(self, command):
        # run db_bench and return the
        out_file = open(self.OUTPUT_FILE, "w+")
        err_file = open(self.ERROR_FILE, "w+")
        print('executing... - ' + command)
        subprocess.call(command, shell=True, stdout=out_file, stderr=err_file)
        out_file.close()
        err_file.close()

    def run_experiment(self, db_options, db_path):
        # type: (List[str], str) -> str
        command = self._build_experiment_command(db_options, db_path)
        self._run_command(command)

        parsed_output = self._parse_output(get_perf_context=True)

        # Create the LOGS object
        # get the log options from the OPTIONS file
        logs_file_prefix, stats_freq_sec = self.get_log_options(
            db_options, parsed_output[self.DB_PATH]
        )
        db_logs = DatabaseLogs(
            logs_file_prefix, db_options.get_column_families()
        )
        # Create the Log STATS object
        db_log_stats = LogStatsParser(logs_file_prefix, stats_freq_sec)
        # Create the PerfContext STATS object
        db_perf_context = DatabasePerfContext(
            parsed_output[self.PERF_CON], 0, False
        )
        data_sources = {
            DataSource.Type.DB_OPTIONS: [db_options],
            DataSource.Type.LOG: [db_logs],
            DataSource.Type.TIME_SERIES: [db_log_stats, db_perf_context]
        }
        # Create the ODS STATS object
        if self.ods_args:
            data_sources[DataSource.Type.TIME_SERIES].append(OdsStatsFetcher(
                self.ods_args['client_script'],
                self.ods_args['entity'],
                self.ods_args['key_prefix']
            ))
        return data_sources, parsed_output[self.THROUGHPUT]

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
        '/home/poojamalik/workspace/rocksdb/db_bench',
        'readwhilewriting',
        'temp/OPTIONS_default.tmp',
        'use_existing_db=true',
        'duration=10'
    ]
    db_bench_helper = DBBenchRunner(pos_args)
    # populate benchmarks with the available ones in the db_bench tool
    benchmarks = db_bench_helper.get_available_workloads()
    print(benchmarks)
    print()
    options_file = (
        '/home/poojamalik/workspace/rocksdb/tools/advisor/temp/' +
        'OPTIONS_temp.tmp'
    )
    misc_options = ["rate_limiter_bytes_per_sec=1024000", "bloom_bits=2"]
    db_options = DatabaseOptions(options_file, misc_options)
    data_sources, _ = db_bench_helper.run_experiment(db_options)
    print(data_sources[DataSource.Type.DB_OPTIONS][0].options_dict)
    print()
    print(data_sources[DataSource.Type.LOG][0].logs_path_prefix)
    if os.path.isfile(data_sources[DataSource.Type.LOG][0].logs_path_prefix):
        print('log file exists!')
    else:
        print('error: log file does not exist!')
    print(data_sources[DataSource.Type.LOG][0].column_families)
    print()
    print(data_sources[DataSource.Type.TIME_SERIES][0].logs_file_prefix)
    if (
        os.path.isfile(
            data_sources[DataSource.Type.TIME_SERIES][0].logs_file_prefix
        )
    ):
        print('log file exists!')
    else:
        print('error: log file does not exist!')
    print(data_sources[DataSource.Type.TIME_SERIES][0].stats_freq_sec)
    print(data_sources[DataSource.Type.TIME_SERIES][1].keys_ts)

    db_options = DatabaseOptions(options_file, None)
    data_sources, _ = db_bench_helper.run_experiment(db_options)
    print(data_sources[DataSource.Type.DB_OPTIONS][0].options_dict)
    print()
    print(data_sources[DataSource.Type.LOG][0].logs_path_prefix)
    if os.path.isfile(data_sources[DataSource.Type.LOG][0].logs_path_prefix):
        print('log file exists!')
    else:
        print('error: log file does not exist!')
    print(data_sources[DataSource.Type.LOG][0].column_families)
    print()
    print(data_sources[DataSource.Type.TIME_SERIES][0].logs_file_prefix)
    if (
        os.path.isfile(
            data_sources[DataSource.Type.TIME_SERIES][0].logs_file_prefix
        )
    ):
        print('log file exists!')
    else:
        print('error: log file does not exist!')
    print(data_sources[DataSource.Type.TIME_SERIES][0].stats_freq_sec)
    print(data_sources[DataSource.Type.TIME_SERIES][1].keys_ts)
    print(data_sources[DataSource.Type.TIME_SERIES][1].stats_freq_sec)


if __name__ == "__main__":
    main()
