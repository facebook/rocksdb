# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from abc import abstractmethod
from enum import Enum
from advisor.db_log_parser import DataSource


class TimeSeriesData(DataSource):
    class Behavior(Enum):
        bursty = 1
        evaluate_expression = 2

    class AggregationOperator(Enum):
        avg = 1
        max = 2
        min = 3
        latest = 4
        oldest = 5

    @abstractmethod
    def fetch_burst_epochs(self):
        # returns Dict[instance/entity, Dict[timestamp, value]]
        pass

    @abstractmethod
    def fetch_aggregated_values(self):
        # returns Dict[instance/entity, Dict[key, aggregated_value]]
        pass

    def __init__(self, start_time, end_time):
        super().__init__(DataSource.Type.TIME_SERIES)
        self.start_time = start_time
        self.end_time = end_time
