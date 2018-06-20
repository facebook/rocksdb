# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from abc import ABC, abstractmethod
import glob
import re
from enum import Enum


class DataSource(ABC):
    class Type(Enum):
        LOG = 1
        DB_OPTIONS = 2
        STATS = 3
        PERF_CONTEXT = 4
        ODS = 5

    def __init__(self, type):
        self.type = type

    @abstractmethod
    def check_and_trigger_conditions(self, conditions):
        pass


class Log:
    @staticmethod
    def is_new_log(log_line):
        # The assumption is that a new log will start with a date printed in
        # the below regex format.
        date_regex = '\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d{6}'
        return re.match(date_regex, log_line)

    def __init__(self, log_line):
        token_list = log_line.strip().split()
        self.time = token_list[0]
        self.context = token_list[1]
        self.message = " ".join(token_list[2:])

    def get_time(self):
        return self.time

    def get_context(self):
        return self.context

    def get_message(self):
        return self.message

    def append_message(self, remaining_log):
        self.message = self.message + remaining_log

    def __repr__(self):
        return 'time: ' + self.time + ', context: ' + self.context +\
             ', message: ' + self.message


class DatabaseLogs(DataSource):
    def __init__(self, logs_path_prefix):
        super().__init__(DataSource.Type.LOG)
        self.logs_path_prefix = logs_path_prefix

    def trigger_appropriate_conditions(self, conditions, log):
        conditions_to_be_removed = []
        for cond in conditions:
            if re.search(cond.regex, log.get_message(), re.IGNORECASE):
                cond.set_trigger(log)
                conditions_to_be_removed.append(cond)
        for remove_cond in conditions_to_be_removed:
            conditions.remove(remove_cond)
        return conditions

    def check_and_trigger_conditions(self, conditions):
        for file_name in glob.glob(self.logs_path_prefix + '*'):
            with open(file_name, 'r') as db_logs:
                new_log = None
                for line in db_logs:
                    if not conditions:
                        break
                    if Log.is_new_log(line):
                        if new_log:
                            conditions = self.trigger_appropriate_conditions(
                                conditions,
                                new_log
                            )
                        new_log = Log(line)
                    else:
                        # To account for logs split into multiple lines
                        new_log.append_message(line)
            # Check for the last log in the file.
            if new_log and conditions:
                conditions = self.trigger_appropriate_conditions(
                    conditions,
                    new_log
                )
