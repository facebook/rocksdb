#!/usr/bin/env python3
#  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

'''Access the results of benchmark runs
Send these results on to OpenSearch graphing service
'''

import argparse
import itertools
import os
import re
import sys
import requests
from dateutil import parser
import logging

logging.basicConfig(level=logging.DEBUG)
class Configuration:
    opensearch_user = os.environ['ES_USER']
    opensearch_pass = os.environ['ES_PASS']

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

    def conform_opensearch(row):
        (dt, _) = parser.parse(row['test_date'], fuzzy_with_tokens=True)
        row['test_date'] = dt.isoformat()
        return dict((key.replace('.', '_'), value)
                    for (key, value) in row.items())


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


def load_report_from_tsv(filename: str):
    file = open(filename, 'r')
    contents = file.readlines()
    file.close()
    parser = ResultParser()
    report = parser.parse(contents)
    logging.debug(f"Loaded TSV Report: {report}")
    return report


def push_report_to_opensearch(report, esdocument):
    sanitized = [BenchmarkUtils.conform_opensearch(row)
                 for row in report if BenchmarkUtils.sanity_check(row)]
    logging.debug(f"upload {len(sanitized)} benchmarks to opensearch")
    for single_benchmark in sanitized:
        logging.debug(f"upload benchmark: {single_benchmark}")
        response = requests.post(
            esdocument,
            json=single_benchmark, auth=(os.environ['ES_USER'], os.environ['ES_PASS']))
        logging.debug(
            f"Sent to OpenSearch, status: {response.status_code}, result: {response.text}")
        response.raise_for_status()


def main():
    '''Tool for fetching, parsing and uploading benchmark results to OpenSearch / ElasticSearch
    This tool will

    (1) Open a local tsv benchmark report file
    (2) Upload to OpenSearch document, via https/JSON
    '''

    parser = argparse.ArgumentParser(
        description='CircleCI benchmark scraper.')

    # --tsvfile is the name of the file to read results from
    # --esdocument is the ElasticSearch document to push these results into
    #
    parser.add_argument('--tsvfile', default='build_tools/circle_api_scraper_input.txt',
                        help='File from which to read tsv report')
    parser.add_argument('--esdocument', help='ElasticSearch/OpenSearch document URL to upload report into')

    args = parser.parse_args()
    logging.debug(f"Arguments: {args}")
    reports = load_report_from_tsv(args.tsvfile)
    push_report_to_opensearch(reports, args.esdocument)

if __name__ == '__main__':
    sys.exit(main())
