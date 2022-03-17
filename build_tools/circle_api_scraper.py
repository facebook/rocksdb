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
import csv
import email
import pickle
from keyword import iskeyword
import re
import struct
import sys
from typing import Callable, List, Mapping
import circleci.api
import requests
from dateutil import parser


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
        self.service = "https://circleci.com/api/v2"

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
    '''TODO AP handle \t and \w\w\w... - not really a TSV, but it's what we get'''
    def tsv_to_table(contents):
        table = [
            row for row in csv.reader(contents, delimiter='\t')]
        row_tables = []
        header = table[0]
        for row in table[1:]:
            row_table = {}
            for (key, value) in zip(header, row):
                row_table[key] = value
            row_tables.append(row_table)
        return row_tables


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
        print(row)
        return row

    def parse(self, lines):
        '''Parse something that iterates lines'''
        rows = [self.line(line) for line in lines]
        records = [{k: v for (k, v) in zip(rows[0], row)} for row in rows[1:]]
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
                self.report = BenchmarkUtils.tsv_to_table(decoded.split('\n'))
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
        {'user_id': 'e7d4aab13e143360f95e258be0a89b5c8e256773',
         'vcs': 'github',
         'username': 'facebook',
         'project': 'rocksdb'})
    urls = reader.get_log_urls()
    results = [result.fetch().parse_report() for result in urls]
    # Each object with a successful parse will have a .report field
    for result in results:
        result.log_result()
    reports = [(int(parser.isoparse(result.job_info.started_at).timestamp()), result.report) for result in results if hasattr(
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
    report = BenchmarkUtils.tsv_to_table(contents)
    #return report
    parser = ResultParser()
    report2 = parser.parse(contents)
    return report2

def push_pickle_to_graphite(reports):
    # Careful not to use too modern a protocol for Graphite
    payload = pickle.dumps(reports, protocol=2)
    header = struct.pack("!L", len(payload))
    graphite_message = header + payload

    # Now send to Graphite's pickle port (2004 by default)
    pass


def main():
    '''Fetch and parse benchmark results from CircleCI
    Save to a pickle file
    Upload to Graphite port
    '''

    parser = argparse.ArgumentParser(
        description='CircleCI benchmark scraper.')

    # --save <picklefile> in
    parser.add_argument('--action', choices=['fetch', 'push', 'all', 'tsv'], default='tsv',
                        help='Which action to perform')
    parser.add_argument('--picklefile', default='reports.pickle',
                        help='File in which to save pickled report')
    parser.add_argument('--tsvfile', default='build_tools/circle_api_scraper_input.txt',
                        help='File from which to read tsv report')

    args = parser.parse_args()
    if args.action == 'all':
        reports = fetch_results_from_circle()
        push_pickle_to_graphite(reports)
        pass
    elif args.action == 'fetch':
        reports = fetch_results_from_circle()
        save_reports_to_local(args.picklefile)
        pass
    elif args.action == 'push':
        reports = load_reports_from_local(args.picklefile)
        push_pickle_to_graphite(reports)
    elif args.action == 'tsv':
        reports = load_reports_from_tsv(args.tsvfile)
        push_pickle_to_graphite(reports)


if __name__ == '__main__':
    sys.exit(main())
