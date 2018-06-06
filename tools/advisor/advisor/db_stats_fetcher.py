from enum import Enum
from db_log_parser import DataSource
import subprocess


STATS_OUTPUT = 'out.tmp'
STATS_ERROR = 'err.tmp'
RATE_TRANSFORM_DESC = "rate(%,15m,duration=60)"
COMMAND = (
    "%s --entity=%s --key=%s --tstart=%s --tend=%s --transform=%s --showtime"
)


class StatsFetcher(DataSource):
    class Transformation(Enum):
        rate = 1
        avg = 2

    @staticmethod
    def _get_string_in_quotes(value):
        return '"' + str(value) + '"'

    @staticmethod
    def _execute_script(command):
        print('executing...')
        print(command)
        out_file = open(STATS_OUTPUT, 'w+')
        err_file = open(STATS_ERROR, 'w+')
        subprocess.call(command, shell=True, stdout=out_file, stderr=err_file)
        out_file.close()
        err_file.close()

    @staticmethod
    def _get_time_value_pair(pair_string):
        pair_string = pair_string.replace('[', '')
        pair_string = pair_string.replace(']', '')
        pair = pair_string.split(',')
        first = int(pair[0].strip())
        second = float(pair[1].strip())
        return [first, second]

    def __init__(self, client_script, entities, start_time, end_time):
        super().__init__(DataSource.Type.ODS)
        self.client_script = client_script
        self.entities = entities
        self.start_time = start_time
        self.end_time = end_time

    def fetch_burst_epochs(self, key, threshold_lower):
        transform_desc = RATE_TRANSFORM_DESC
        transform_desc = (
            RATE_TRANSFORM_DESC + ',filter(' + str(threshold_lower) + ',)'
        )
        command = COMMAND % (
            self.client_script,
            self._get_string_in_quotes(self.entities),
            self._get_string_in_quotes(key),
            self._get_string_in_quotes(self.start_time),
            self._get_string_in_quotes(self.end_time),
            self._get_string_in_quotes(transform_desc)
        )
        self._execute_script(command)
        pass

    def fetch_avg_values(self, keys):
        transform_desc = 'avg'
        command = COMMAND % (
            self.client_script,
            self._get_string_in_quotes(self.entities),
            self._get_string_in_quotes(','.join(keys)),
            self._get_string_in_quotes(self.start_time),
            self._get_string_in_quotes(self.end_time),
            self._get_string_in_quotes(transform_desc)
        )
        self._execute_script(command)
        pass

    def fetch_url(self, keys, display_type, threshold_lower=None):
        transform_desc = RATE_TRANSFORM_DESC
        if threshold_lower:
            transform_desc = (
                transform_desc + ',filter(' + str(threshold_lower) + ')'
            )
        if not isinstance(keys, str):
            keys = ','.join(keys)

        command = COMMAND + " --url=%s"
        command = command % (
            self.client_script,
            self._get_string_in_quotes(self.entities),
            self._get_string_in_quotes(keys),
            self._get_string_in_quotes(self.start_time),
            self._get_string_in_quotes(self.end_time),
            self._get_string_in_quotes(transform_desc),
            self._get_string_in_quotes(display_type)
        )
        self._execute_script(command)
        url = ""
        with open(STATS_OUTPUT, 'r') as fp:
            url = fp.readline()
        return url

    def parse_stats_output(self, output_type):
        values_dict = {}
        with open(STATS_OUTPUT, 'r') as fp:
            for line in fp:
                token_list = line.strip().split('\t')
                entity = token_list[0]
                key = token_list[1]
                if entity not in values_dict:
                    values_dict[entity] = {}
                if output_type is self.Transformation.rate:
                    list_of_lists = [
                        self._get_time_value_pair(pair_string)
                        for pair_string in token_list[2].split('],')
                    ]
                    value = {pair[0]: pair[1] for pair in list_of_lists}
                elif output_type is self.Transformation.avg:
                    pair = self._get_time_value_pair(token_list[2])
                    value = pair[1]
                values_dict[entity][key] = value
        return values_dict

    def check_and_trigger_conditions(self, conditions):
        # Get the list of all keys whose avg values are required so that
        # we can get all of them in a single call.
        keys_avg_values = []
        for cond in conditions:
            if cond.transformation is self.Transformation.avg:
                keys_avg_values.extend(cond.keys)
            elif cond.transformation is self.Transformation.rate:
                self.fetch_burst_epochs(cond.keys, cond.threshold)
                result = self.parse_stats_output(cond.transformation)
                if result:
                    cond.set_trigger(result)

        self.fetch_avg_values(keys_avg_values)
        result = self.parse_stats_output(self.Transformation.avg)

        for cond in conditions:
            if cond.transformation is self.Transformation.avg:
                entity_evaluation_dict = {}
                for entity in result:
                    keys = []
                    for key in cond.keys:
                        keys.append(result[entity][key])
                    try:
                        entity_evaluation_dict[entity] = eval(cond.expression)
                    except Exception as e:
                        print(e)
                if entity_evaluation_dict:
                    cond.set_trigger(entity_evaluation_dict)
