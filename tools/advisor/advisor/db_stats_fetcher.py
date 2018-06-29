# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from advisor.db_log_parser import Log
from advisor.db_timeseries_parser import TimeSeriesData, NO_ENTITY
from advisor.rule_parser import Condition, TimeSeriesCondition
import glob
import re
import subprocess
import time


class LogStatsParser(TimeSeriesData):
    STATS = 'STATISTICS:'

    @staticmethod
    def parse_log_line_for_stats(log_line):
        token_list = log_line.strip().split()
        stat_prefix = token_list[0] + '.'
        stat_values = [
            token
            for token in token_list[1:]
            if token != ':'
        ]
        stat_dict = {}
        for ix, metric in enumerate(stat_values):
            if ix % 2 == 0:
                stat_name = stat_prefix + metric
            else:
                stat_dict[stat_name] = float(metric)
        return stat_dict

    def __init__(self, logs_path_prefix, stats_freq_sec):
        super().__init__()
        self.logs_file_prefix = logs_path_prefix
        self.stats_freq_sec = stats_freq_sec
        self.duration_sec = 60

    def get_keys_from_conditions(self, conditions):
        reqd_stats = []
        for cond in conditions:
            reqd_stats.extend(cond.keys)
        reqd_stats = list(set(reqd_stats))  # deduplicate required stats
        return reqd_stats

    def add_to_timeseries(self, log, reqd_stats):
        # type: (Log, List[str]) -> Dict[int, float]
        new_lines = log.get_message().split('\n')
        log_ts = log.get_timestamp()
        # the first line in the log does not contain any statistics
        for line in new_lines[1:]:
            stats_on_line = self.parse_log_line_for_stats(line)
            for stat in stats_on_line:
                if stat in reqd_stats:
                    if stat not in self.keys_ts[NO_ENTITY]:
                        self.keys_ts[NO_ENTITY][stat] = {}
                    self.keys_ts[NO_ENTITY][stat][log_ts] = stats_on_line[stat]

    def fetch_timeseries(self, statistics):
        self.keys_ts = {NO_ENTITY: {}}
        for file_name in glob.glob(self.logs_file_prefix + '*'):
            with open(file_name, 'r') as db_logs:
                new_log = None
                for line in db_logs:
                    if Log.is_new_log(line):
                        if (
                            new_log and
                            re.search(self.STATS, new_log.get_message())
                        ):
                            self.add_to_timeseries(new_log, statistics)
                        new_log = Log(line, column_families=[])
                    else:
                        # To account for logs split into multiple lines
                        new_log.append_message(line)
            # Check for the last log in the file.
            if new_log and re.search(self.STATS, new_log.get_message()):
                self.add_to_timeseries(new_log, statistics)


class OdsStatsFetcher(TimeSeriesData):
    # class constants
    OUTPUT_FILE = 'temp/stats_out.tmp'
    ERROR_FILE = 'temp/stats_err.tmp'
    COMMAND = "%s --entity=%s --key=%s --tstart=%s --tend=%s --showtime"

    # static methods
    @staticmethod
    def _get_string_in_quotes(value):
        return '"' + str(value) + '"'

    @staticmethod
    def _get_time_value_pair(pair_string):
        pair_string = pair_string.replace('[', '')
        pair_string = pair_string.replace(']', '')
        pair = pair_string.split(',')
        first = int(pair[0].strip())
        second = float(pair[1].strip())
        return [first, second]

    def __init__(self, client, entities, key_prefix=None):
        super().__init__()
        self.client = client
        self.entities = entities
        self.key_prefix = key_prefix
        self.stats_freq_sec = 60
        self.duration_sec = 60
        self.end_time = int(time.time())
        self.start_time = self.end_time - (3 * 60 * 60)

    def execute_script(self, command):
        print('executing...')
        print(command)
        out_file = open(self.OUTPUT_FILE, "w+")
        err_file = open(self.ERROR_FILE, "w+")
        subprocess.call(command, shell=True, stdout=out_file, stderr=err_file)
        out_file.close()
        err_file.close()

    def attach_prefix_to_keys(self, keys):
        complete_keys = []
        for key in keys:
            if key.startswith('[]'):
                if not self.key_prefix:
                    raise ValueError('OdsStatsFetcher: need key prefix')
                complete_keys.append(self.key_prefix + key[2:])
            else:
                complete_keys.append(key)
        return complete_keys

    def fetch_timeseries(self, statistics):
        command = self.COMMAND % (
            self.client,
            self._get_string_in_quotes(self.entities),
            self._get_string_in_quotes(','.join(statistics)),
            self._get_string_in_quotes(self.start_time),
            self._get_string_in_quotes(self.end_time)
        )
        self.execute_script(command)
        # Parse ODS output and populate the 'keys_ts' map
        self.keys_ts = {}
        with open(self.OUTPUT_FILE, 'r') as fp:
            for line in fp:
                token_list = line.strip().split('\t')
                entity = token_list[0]
                key = token_list[1]
                if entity not in self.keys_ts:
                    self.keys_ts[entity] = {}
                if key not in self.keys_ts[entity]:
                    self.keys_ts[entity][key] = {}
                list_of_lists = [
                    self._get_time_value_pair(pair_string)
                    for pair_string in token_list[2].split('],')
                ]
                value = {pair[0]: pair[1] for pair in list_of_lists}
                self.keys_ts[entity][key] = value

    def get_keys_from_conditions(self, conditions):
        reqd_stats = []
        for cond in conditions:
            reqd_stats.extend(self.attach_prefix_to_keys(cond.keys))
        reqd_stats = list(set(reqd_stats))  # deduplicate required stats
        return reqd_stats

    def fetch_rate_url(self, entities, keys, window_len, display, percent):
        # type: (List[str], List[str], str, str, bool) -> str
        transform_desc = (
            "rate(" + str(window_len) + ",duration=" + str(self.duration_sec)
        )
        if percent:
            transform_desc = transform_desc + ",%)"
        else:
            transform_desc = transform_desc + ")"
        keys = self.attach_prefix_to_keys(keys)

        command = self.COMMAND + " --transform=%s --url=%s"
        command = command % (
            self.client,
            self._get_string_in_quotes(','.join(entities)),
            self._get_string_in_quotes(','.join(keys)),
            self._get_string_in_quotes(self.start_time),
            self._get_string_in_quotes(self.end_time),
            self._get_string_in_quotes(transform_desc),
            self._get_string_in_quotes(display)
        )
        self.execute_script(command)
        url = ""
        with open(self.OUTPUT_FILE, 'r') as fp:
            url = fp.readline()
        return url


# TODO: remove these blocks once the unittests for LogStatsParser are in place
def main():
    # populating the statistics
    log_stats = LogStatsParser('/dev/shm/dbbench/LOG', 60)
    print(log_stats.type)
    print(log_stats.keys_ts)
    print(log_stats.logs_file_prefix)
    print(log_stats.stats_freq_sec)
    print(log_stats.duration_sec)
    statistics = [
        'rocksdb.number.rate_limiter.drains.COUNT',
        'rocksdb.number.block.decompressed.COUNT',
        'rocksdb.db.write.micros.P50',
        'rocksdb.manifest.file.sync.micros.P99'
    ]
    log_stats.fetch_timeseries(statistics)
    print()
    print(log_stats.keys_ts)
    # aggregated statistics
    print()
    print(log_stats.fetch_aggregated_values(
        statistics, TimeSeriesData.AggregationOperator.latest
    ))
    print(log_stats.fetch_aggregated_values(
        statistics, TimeSeriesData.AggregationOperator.oldest
    ))
    print(log_stats.fetch_aggregated_values(
        statistics, TimeSeriesData.AggregationOperator.max
    ))
    print(log_stats.fetch_aggregated_values(
        statistics, TimeSeriesData.AggregationOperator.min
    ))
    print(log_stats.fetch_aggregated_values(
        statistics, TimeSeriesData.AggregationOperator.avg
    ))
    # condition 'evaluate_expression' that evaluates to true
    cond1 = Condition('cond-1')
    cond1 = TimeSeriesCondition.create(cond1)
    cond1.set_parameter('keys', statistics)
    cond1.set_parameter('behavior', 'evaluate_expression')
    cond1.set_parameter('evaluate', 'keys[2]-keys[3]>=0')
    cond1.set_parameter('aggregation_op', 'avg')
    # condition 'evaluate_expression' that evaluates to false
    cond2 = Condition('cond-2')
    cond2 = TimeSeriesCondition.create(cond2)
    cond2.set_parameter('keys', statistics)
    cond2.set_parameter('behavior', 'evaluate_expression')
    cond2.set_parameter('evaluate', '((keys[1]-(2*keys[0]))/100)<3000')
    cond2.set_parameter('aggregation_op', 'latest')
    # check remaining methods
    conditions = [cond1, cond2]
    print()
    print(log_stats.get_keys_from_conditions(conditions))
    log_stats.check_and_trigger_conditions(conditions)
    print()
    print(cond1.get_trigger())
    print(cond2.get_trigger())


if __name__ == '__main__':
    main()
