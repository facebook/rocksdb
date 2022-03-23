#!/usr/bin/env python3
#  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

'''Access CircleCI output via a combination of v1 and v2 APIs
In order to finally yield the output of benchmark tests.
'''

import argparse
from ast import Dict
from collections import namedtuple
import datetime
import email
import itertools
import pickle
from keyword import iskeyword
import random
import re
import socket
import struct
import sys
from typing import Callable, List, Mapping
import circleci.api
import requests
from dateutil import parser
import logging

logging.basicConfig(level=logging.DEBUG)
class Configuration:
    graphite_server = 'cherry.evolvedbinary.com'
    graphite_pickle_port = 2004
    circle_user_id = 'e7d4aab13e143360f95e258be0a89b5c8e256773'  # rotate those tokens !!
    circle_vcs = 'github'
    circle_username = 'facebook'
    circle_project = 'rocksdb'
    circle_ci_api = 'https://circleci.com/api/v2'
    graphite_name_prefix = 'rocksdb.benchmark'


class TupleObject:

    def mapper(input_mapping):
        '''
        Convert mappings to namedtuples recursively.
        https://gist.github.com/hangtwenty/5960435
        '''
        if isinstance(input_mapping, Mapping):
            mapping = {}
            for key, value in list(input_mapping.items()):
                if not iskeyword(key):
                    mapping[key] = TupleObject.mapper(value)
            return namedtuple(TupleObject.__name__, mapping.keys())(*mapping.values())
        elif isinstance(input_mapping, list):
            return [TupleObject.mapper(item) for item in input_mapping]
        return input_mapping


class CircleAPIV2:

    workflow_call_range = 20
    pipeline_call_range = 20
    job_call_range = 5

    def __init__(self, user_id: str, vcs: str, username: str, project: str) -> None:
        '''Configure with a CircleCI user id, and a slug (roughly, the project, e.g. github/facebook/rocksdb)
            '''
        self.auth = (user_id, '')
        self.slug = f"{vcs}/{username}/{project}"
        self.service = Configuration.circle_ci_api

    def get_jobs(self) -> List[int]:
        '''TODO AP
        '''
        pass

    def get_workflow_items(self, pipeline_id: str, filter: Callable) -> List[str]:
        '''All the workflow items
        TODO AP filter to where item.name == "benchmark-linux"
        '''
        params = {}
        result = []
        for i in range(CircleAPIV2.workflow_call_range):
            workflows_call = requests.get(
                f"{self.service}/pipeline/{pipeline_id}/workflow", auth=self.auth, params=params)
            workflows_call.raise_for_status()
            workflows = TupleObject.mapper(workflows_call.json())
            result = result + [item.id
                               for item in workflows.items if filter(item)]
            if workflows.next_page_token == None:
                break
            params = {'page-token': workflows.next_page_token}
        return result

    def get_pipeline_ids(self, filter: Callable) -> List[str]:
        params = {}
        result = []
        for i in range(CircleAPIV2.pipeline_call_range):
            pipelines_call = requests.get(
                f"{self.service}/project/{self.slug}/pipeline", auth=self.auth, params=params)
            pipelines_call.raise_for_status()
            pipelines = TupleObject.mapper(pipelines_call.json())
            result = result + [item.id
                               for item in pipelines.items if filter(item)]
            if pipelines.next_page_token == None:
                break
            params = {'page-token': pipelines.next_page_token}

        return result

    def get_jobs(self, workflow_id: str) -> List[int]:
        params = {}
        result = []
        for i in range(CircleAPIV2.job_call_range):
            jobs_call = requests.get(
                f"{self.service}/workflow/{workflow_id}/job", auth=self.auth, params=params)
            jobs_call.raise_for_status()
            jobs = TupleObject.mapper(jobs_call.json())
            result = result + [item.job_number
                               for item in jobs.items]
            if jobs.next_page_token == None:
                break
            params = {'page-token': jobs.next_page_token}

        return result

    def get_job_info(self, job_id: int) -> object:
        job_info_call = requests.get(
            f"{self.service}/project/{self.slug}/job/{job_id}", auth=self.auth)
        job_info_call.raise_for_status()
        return TupleObject.mapper(job_info_call.json())


class Predicates:
    def is_my_pull_request(pipeline):
        try:
            return pipeline.vcs.branch == "pull/9676"
        except AttributeError:
            return False

    def is_benchmark_linux(step):
        try:
            return step.name == "benchmark-linux"
        except AttributeError:
            return False

    def always_true(x):
        return True


def flatten(ll):
    return [item for l in ll for item in l]


class CircleAPIV1:
    '''
    Class to do some things tht still onlly appear possible through the legacy V1 CircleCI API
    '''

    def __init__(self, user_id: str, vcs: str, username: str, project: str) -> None:
        '''Configure with a CircleCI user id, and the vcs, username, project, e.g. github, facebook, rocksdb
        '''
        self.api = circleci.api.Api(user_id)
        self.vcs = vcs
        self.username = username
        self.project = project

    def get_log_action_output_url(self, job_number: int, action_name: str) -> str:
        '''Get the URL of the output of an action.
        '''

        dict = self.api.get_build_info(username=self.username,
                                       project=self.project, build_num=job_number)
        info = TupleObject.mapper(dict)
        for step in info.steps:
            for action in step.actions:
                if action.has_output and action.name == action_name:
                    # found the special action whose content we want
                    return action.output_url
        return None

    def get_log_mime_url(self, job_number: int) -> str:
        '''Use the old CircleCI v1.1 API to get the report URL. The report is the output files, MIME encoded.
        Track it down because we know its name.
        '''

        return self.get_log_action_output_url(job_number, "Output logs as MIME")


class BenchmarkResultException(Exception):
    def __init__(self, message, content):
        super().__init__(self, message)
        self.content = content


class BenchmarkUtils:

    expected_keys = ['ops_sec', 'mb_sec', 'total_size_gb', 'level0_size_gb', 'sum_gb', 'write_amplification',
                     'write_mbps', 'usec_op', 'percentile_50', 'percentile_75',
                     'percentile_99', 'percentile_99.9', 'percentile_99.99', 'uptime',
                     'stall_time', 'stall_percent', 'test_name', 'test_date', 'rocksdb_version',
                     'job_id', 'timestamp']

    metric_keys = ['ops_sec', 'mb_sec', 'total_size_gb', 'level0_size_gb', 'sum_gb', 'write_amplification',
                   'write_mbps', 'usec_op', 'percentile_50', 'percentile_75',
                   'percentile_99', 'percentile_99.9', 'percentile_99.99', 'uptime',
                   'stall_time', 'stall_percent']

    def sanity_check(row):
        if not 'test_name' in row:
            return False
        if row['test_name'] == '':
            return False
        if not 'test_date' in row:
            return False
        if not 'ops_sec' in row:
            return False
        try:
            v = int(row['ops_sec'])
        except (ValueError, TypeError):
            return False
        return True

    def enhance(row):
        (dt, _) = parser.parse(row['test_date'], fuzzy_with_tokens=True)
        row['timestamp'] = int(dt.timestamp())
        return row

    def graphite(row, metric_path=Configuration.graphite_name_prefix):
        '''Convert a row (dictionary of values)
        into the form of object that graphite likes to receive
        ( <path>, (<timestamp>, <value>) )
        '''
        result = []
        for metric_key in BenchmarkUtils.expected_keys:
            metric_id = metric_path + '.' + row['test_name'] + '.' + metric_key
            metric_value = 0.0
            try:
                metric_value = float(row[metric_key])
            except (ValueError, TypeError):
                # metric doesn't have a float value
                continue
            metric = (metric_id, (row['timestamp'], metric_value))
            result.append(metric)
        return result

    def test_graphite(rows, metric_prefix='test.', repeat=10, step_secs=900):
        '''Modify the output of graphite
        Stick a test. in front of the name
        Change times to shift range of samples up to just before now()
        '''
        max_ts = None
        for row in rows:
            (_, (timestamp, _)) = row
            if max_ts == None or timestamp > max_ts:
                max_ts = timestamp
        delta = int(datetime.datetime.now().timestamp() - max_ts)
        rows2 = []
        random.seed()
        for row in rows:
            (metric_id, (timestamp, metric_value)) = row
            for i in range(repeat):
                shoogled_value = int(random.randrange(
                    int(metric_value/2), int(3*metric_value/2+1)))
                rows2.append((metric_prefix + metric_id,
                              (timestamp + delta + (i + 1 - repeat)*step_secs, shoogled_value)))
        return rows2


class ResultParser:
    def __init__(self, field="(\w|[+-:.])+", intrafield="(\s)+", separator="\t"):
        self.field = re.compile(field)
        self.intra = re.compile(intrafield)
        self.sep = re.compile(separator)

    def line(self, l_in: str):
        '''Parse a line into items
        Being clever about separators
        '''
        l = l_in
        row = []
        while l != '':
            match_item = self.field.match(l)
            if match_item:
                item = match_item.group(0)
                row.append(item)
                l = l[len(item):]
            else:
                match_intra = self.intra.match(l)
                if match_intra:
                    intra = match_intra.group(0)
                    # Count the separators
                    # If there are >1 then generate extra blank fields
                    # White space with no true separators fakes up a single separator
                    tabbed = self.sep.split(intra)
                    sep_count = len(tabbed) - 1
                    if sep_count == 0:
                        sep_count = 1
                    for i in range(sep_count-1):
                        row.append('')
                    l = l[len(intra):]
                else:
                    raise BenchmarkResultException(
                        'Invalid TSV line', f"{l_in} at {l}")
        return row

    def parse(self, lines):
        '''Parse something that iterates lines'''
        rows = [self.line(line) for line in lines]
        header = rows[0]
        width = len(header)
        records = [{k: v for (k, v) in itertools.zip_longest(
            header, row[:width])} for row in rows[1:]]
        return records


class BenchmarkResult:

    '''The result of a benchmark run
    which is made up a known set of files
    stored as a multipart MIME message
    at a known URL.

    Processing the result involves fetching the url, parsing the MIME, parsing the known file(s)
    and generating the internal table(s) we want from those files (e.g. .tsv files)
    '''

    report_file = 'report.tsv'

    def __init__(self, job_info: TupleObject, output_url: str):
        self.job_info = job_info
        self.output_url = output_url

    def time_sort_key(self):
        '''Look at the job info retrieved from CircleCI to establish sort order'''
        dt = parser.isoparse(self.job_info.started_at)
        ticks = dt.timestamp()
        return ticks

    def fetch(self):
        '''
        Go and get the output URL
        Interpret it as a multipart MIME message with some attachments
        Build a table (self.files) of these attachments -
        - decoded and indexed by the filename
        '''
        self.files = {}
        message_call = requests.get(f"{self.output_url}")
        message_call.raise_for_status()
        messages = message_call.json()
        if not isinstance(messages, list):
            self.exception = BenchmarkResultException(
                'Output of benchmark is not a list of messages', message_call.text)
            return self
        for item in messages:
            message_str = item['message']
            if message_str is None:
                self.exception = BenchmarkResultException(
                    'Item in benchmark output does not have messgae key', item)
                return self
            message = email.message_from_string(message_str)
            if (message.is_multipart()):
                # parse a multipart message
                for part in message.walk():
                    if part.get_content_disposition() == 'attachment':
                        bytes = part.get_payload(decode=True)
                        charset = part.get_content_charset('iso-8859-1')
                        decoded = bytes.decode(charset, 'replace')
                        self.files[part.get_filename()] = (
                            part._headers, decoded)
            else:
                self.exception = BenchmarkResultException(
                    'Message in output of benchmark is not a multipart message', message_str)
        return self

    def parse_report(self):
        for (filename, (headers, decoded)) in self.files.items():
            if filename == BenchmarkResult.report_file:
                parser = ResultParser()
                report = parser.parse(decoded.split('\n'))
                self.report = report
        return self

    def log_result(self):
        for file in self.files.keys():
            print(file)
            (headers, content) = self.files[file]
            print(headers)
            print(content)


class CircleLogReader:
    '''User level methods for telling us about particular logs'''

    def __init__(self, config_dict: Dict) -> List:

        config = TupleObject.mapper(config_dict)
        self.config = config
        self.api_v1 = CircleAPIV1(
            config.user_id, config.vcs, config.username, config.project)
        self.api_v2 = CircleAPIV2(
            config.user_id, config.vcs, config.username, config.project)

    def get_log_urls(self) -> List[BenchmarkResult]:
        pipeline_ids = self.api_v2.get_pipeline_ids(
            filter=Predicates.is_my_pull_request)
        workflows = flatten([self.api_v2.get_workflow_items(
            pipeline_id, filter=Predicates.is_benchmark_linux) for pipeline_id in pipeline_ids])
        jobs = flatten([self.api_v2.get_jobs(workflow_id)
                       for workflow_id in workflows])

        urls = [self.api_v1.get_log_mime_url(
            job_number=job_id) for job_id in jobs]
        job_infos = [self.api_v2.get_job_info(
            job_id=job_id) for job_id in jobs]

        results = [BenchmarkResult(job_info, output_url) for(
            job_info, output_url) in zip(job_infos, urls)]
        results = sorted(results, key=BenchmarkResult.time_sort_key)
        return results


def fetch_results_from_circle():
    '''Track down the job number
    Drill into the contents to find benchmark logs
    Interpret the results
    '''
    reader = CircleLogReader(
        {'user_id': Configuration.circle_user_id,
         'username': Configuration.circle_username,
         'project': Configuration.circle_project})
    urls = reader.get_log_urls()
    results = [result.fetch().parse_report() for result in urls]
    # Each object with a successful parse will have a .report field
    for result in results:
        result.log_result()
    reports = [result.report for result in results if hasattr(
        result, 'report')]
    return reports


def save_reports_to_local(reports, filename: str):
    file = open(filename, 'wb')
    pickle.dump(reports, file)
    file.close()


def load_reports_from_local(filename: str):
    file = open(filename, 'rb')
    reports = pickle.load(file)
    return reports


def load_reports_from_tsv(filename: str):
    file = open(filename, 'r')
    contents = file.readlines()
    file.close()
    parser = ResultParser()
    report = parser.parse(contents)
    return report


def push_pickle_to_graphite(reports, test_values: bool):
    sanitized = [[BenchmarkUtils.enhance(row)
                 for row in rows if BenchmarkUtils.sanity_check(row)] for rows in reports]
    graphite = []
    for rows in sanitized:
        for row in rows:
            for point in BenchmarkUtils.graphite(row, Configuration.graphite_name_prefix):
                graphite.append(point)

    # Do this if we are asked to create a recent record of test. data points
    if test_values:
        graphite = BenchmarkUtils.test_graphite(graphite)

    logging.debug(f"upload {len(graphite)} to graphite: {graphite}")

    # Careful not to use too modern a protocol for Graphite (it is Python2)
    payload = pickle.dumps(graphite, protocol=1)
    header = struct.pack("!L", len(payload))
    graphite_message = header + payload

    # Now send to Graphite's pickle port (2004 by default)
    sock = socket.socket()
    try:
        sock.connect((Configuration.graphite_server,
                     Configuration.graphite_pickle_port))
    except socket.error:
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d, is Graphite running?" %
                         {'server': Configuration.graphite_server, 'port': Configuration.graphite_pickle_port})

    try:
        ok = sock.sendall(graphite_message)
    except socket.error:
        raise SystemExit("Failed sending data to %(server)s on port %(port)d, is Graphite running?" %
                         {'server': Configuration.graphite_server, 'port': Configuration.graphite_pickle_port})


def main():
    '''Fetch and parse benchmark results from CircleCI
    Save to a pickle file
    Upload to Graphite port
    '''

    parser = argparse.ArgumentParser(
        description='CircleCI benchmark scraper.')

    # --fetch from the CircleCI API into --picklefile <PICKLE-FILE>
    # this takes time because we plough through the CircleCI API
    # --push contents of --picklefile <PICKLE-FILE> to grafana/graphite
    # this is to avoid the cost of --fetch while debugging
    # --all fetch and push, missing out the intermediate picke file
    # demonstrate end-to-end function
    # --tsv
    # push results presented as a TSV file (slightly different proposed format)
    # this is how we get used "inline" on the test machine when running benchmarks
    #
    parser.add_argument('--action', choices=['fetch', 'push', 'all', 'tsv'], default='tsv',
                        help='Which action to perform')
    parser.add_argument('--picklefile', default='reports.pickle',
                        help='File in which to save pickled report')
    parser.add_argument('--tsvfile', default='build_tools/circle_api_scraper_input.txt',
                        help='File from which to read tsv report')
    parser.add_argument('--testvalues', default=False,
                        help='Use as test values; apply a timeshift and preprend "test." to the keys')

    args = parser.parse_args()
    logging.debug(f"Arguments: {args}")
    if args.action == 'all':
        reports = fetch_results_from_circle()
        push_pickle_to_graphite(reports)
        pass
    elif args.action == 'fetch':
        reports = fetch_results_from_circle()
        save_reports_to_local(reports, args.picklefile)
        pass
    elif args.action == 'push':
        reports = load_reports_from_local(args.picklefile)
        push_pickle_to_graphite(reports, args.testvalues)
    elif args.action == 'tsv':
        reports = [load_reports_from_tsv(args.tsvfile)]
        push_pickle_to_graphite(reports, args.testvalues)


if __name__ == '__main__':
    sys.exit(main())
