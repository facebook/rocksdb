from abc import ABC, abstractmethod
import re


class BenchmarkRunner(ABC):
    @staticmethod
    @abstractmethod
    def is_metric_better(new_metric, old_metric):
        pass

    @abstractmethod
    def run_experiment(self):
        # should return a list of DataSource objects
        pass

    @staticmethod
    def get_info_log_file_name(log_dir, db_path):
        # if the log_dir is explicitly specified in the Rocksdb OPTIONS, then
        # the name of the log file has a prefix created from the db_path; else
        # the default log file name is LOG.
        file_name = ''
        if log_dir:
            # refer GetInfoLogPrefix() in rocksdb/util/filename.cc
            # example db_path: /dev/shm/dbbench
            file_name = db_path[1:]  # to ignore the leading '/' character
            to_be_replaced = re.compile('[^0-9a-zA-Z\-_\.]')
            for character in to_be_replaced.findall(db_path):
                file_name = file_name.replace(character, '_')
            if not file_name.endswith('_'):
                file_name += '_'
        file_name += 'LOG'
        return file_name
