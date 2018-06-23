from abc import ABC, abstractmethod
import re
import subprocess


'''
NOTE: This is not thread-safe, because the output file is simply overwritten.
'''


class BenchmarkClient(ABC):
    @abstractmethod
    def get_available_workloads(self):
        pass

    @abstractmethod
    def run_experiment(self):
        pass

    @abstractmethod
    def fetch_statistics(self):
        pass


class DBBenchHelper(BenchmarkClient):
    OUTPUT_FILE = "temp/dbbench_out.tmp"
    ERROR_FILE = "temp/dbbench_err.tmp"
    # TODO: set the tmp directory in which the database should be created
    COMMAND = "%s ./%s --options_file=%s --benchmarks=%s --statistics"

    @staticmethod
    def fetch_statistics(db_bench_output, requested_statistics):
        # type: (str, List[str]) -> Dict[str, Dict[str, str]]
        # assumption: the statistics have numeric values
        stats_dict = {}
        with open(db_bench_output, 'r') as fp:
            stats_start = False
            for line in fp:
                if re.match('STATISTICS:', line):
                    stats_start = True
                    continue
                elif stats_start:
                    if line.strip():
                        token_list = line.strip().split()
                        stat = token_list[0]
                        if stat in requested_statistics:
                            stat_values = [
                                token
                                for token in token_list[1:]
                                if token != ':'
                            ]
                            hist_dict = {}
                            for ix, metric in enumerate(stat_values):
                                if ix % 2 == 0:
                                    key = metric.lower()
                                else:
                                    hist_dict[key] = float(metric)
                            stats_dict[stat] = hist_dict
                else:
                    continue
        return stats_dict

    def __init__(self, client_binary_path, environment_variables):
        self.db_bench_binary = client_binary_path
        self.supported_benchmarks = None
        if not environment_variables:
            self.environment_variables = ''
        else:
            self.environment_variables = environment_variables
        self.get_available_workloads()

    def _run_command(self, command, cleanup=True):
        print('executing...')
        print(command)
        out_file = open(self.OUTPUT_FILE, "w+")
        err_file = open(self.ERROR_FILE, "w+")
        process = subprocess.Popen(
            command, shell=True, stdout=out_file, stderr=err_file
        )
        if cleanup:
            print('waiting for db_bench to finish running')
            process.wait()
            out_file.close()
            err_file.close()
        return process

    def run_experiment(self, benchmarks, options_file):
        # type: (List[str], str) -> str
        for benchmark in benchmarks:
            if benchmark not in self.supported_benchmarks:
                raise ValueError(benchmark + ": not a supported benchmark")
        command = self.COMMAND % (
            " ".join(self.environment_variables),
            self.db_bench_binary,
            options_file,
            ",".join(benchmarks)
        )
        command = command.strip()
        self._run_command(command)
        return self.OUTPUT_FILE

    def get_available_workloads(self):
        if not self.supported_benchmarks:
            self.supported_benchmarks = []
            command = './%s --help' % self.db_bench_binary
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


def main():
    db_bench_helper = DBBenchHelper('../../db_bench', '')
    # populate benchmarks with the available ones in the db_bench tool
    benchmarks = db_bench_helper.get_available_workloads()
    print(benchmarks)
    run_benchmarks = ['seekrandomwhilewriting']
    options_file = '../../examples/rocksdb_option_file_example.ini'
    file = db_bench_helper.run_experiment(run_benchmarks, options_file)
    requested_statistics = [
        'rocksdb.blobdb.num.put',
        'rocksdb.compaction.times.micros'
    ]
    print(file)
    print(db_bench_helper.fetch_statistics(file, requested_statistics))


if __name__ == "__main__":
    main()
