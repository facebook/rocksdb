#!/usr/bin/env python3
#  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

"""Access the results of benchmark runs
Send these results on to OpenSearch graphing service
"""

import argparse
import itertools
import logging
import os
import re
import sys

import requests
from dateutil import parser

logging.basicConfig(level=logging.DEBUG)


class Configuration:
    opensearch_user = os.environ["ES_USER"]
    opensearch_pass = os.environ["ES_PASS"]


class BenchmarkResultException(Exception):
    def __init__(self, message, content):
        super().__init__(self, message)
        self.content = content


class BenchmarkUtils:

    expected_keys = [
        "ops_sec",
        "mb_sec",
        "lsm_sz",
        "blob_sz",
        "c_wgb",
        "w_amp",
        "c_mbps",
        "c_wsecs",
        "c_csecs",
        "b_rgb",
        "b_wgb",
        "usec_op",
        "p50",
        "p99",
        "p99.9",
        "p99.99",
        "pmax",
        "uptime",
        "stall%",
        "Nstall",
        "u_cpu",
        "s_cpu",
        "rss",
        "test",
        "date",
        "version",
        "job_id",
    ]

    def sanity_check(row):
        if "test" not in row:
            logging.debug(f"not 'test' in row: {row}")
            return False
        if row["test"] == "":
            logging.debug(f"row['test'] == '': {row}")
            return False
        if "date" not in row:
            logging.debug(f"not 'date' in row: {row}")
            return False
        if "ops_sec" not in row:
            logging.debug(f"not 'ops_sec' in row: {row}")
            return False
        try:
            _ = int(row["ops_sec"])
        except (ValueError, TypeError):
            logging.debug(f"int(row['ops_sec']): {row}")
            return False
        try:
            (_, _) = parser.parse(row["date"], fuzzy_with_tokens=True)
        except (parser.ParserError):
            logging.error(
                f"parser.parse((row['date']): not a valid format for date in row: {row}"
            )
            return False
        return True

    def conform_opensearch(row):
        (dt, _) = parser.parse(row["date"], fuzzy_with_tokens=True)
        # create a test_date field, which was previously what was expected
        # repair the date field, which has what can be a WRONG ISO FORMAT, (no leading 0 on single-digit day-of-month)
        # e.g. 2022-07-1T00:14:55 should be 2022-07-01T00:14:55
        row["test_date"] = dt.isoformat()
        row["date"] = dt.isoformat()
        return {key.replace(".", "_"): value for key, value in row.items()}


class ResultParser:
    def __init__(self, field="(\w|[+-:.%])+", intrafield="(\s)+", separator="\t"):
        self.field = re.compile(field)
        self.intra = re.compile(intrafield)
        self.sep = re.compile(separator)

    def ignore(self, l_in: str):
        if len(l_in) == 0:
            return True
        if l_in[0:1] == "#":
            return True
        return False

    def line(self, line_in: str):
        """Parse a line into items
        Being clever about separators
        """
        line = line_in
        row = []
        while line != "":
            match_item = self.field.match(line)
            if match_item:
                item = match_item.group(0)
                row.append(item)
                line = line[len(item) :]
            else:
                match_intra = self.intra.match(line)
                if match_intra:
                    intra = match_intra.group(0)
                    # Count the separators
                    # If there are >1 then generate extra blank fields
                    # White space with no true separators fakes up a single separator
                    tabbed = self.sep.split(intra)
                    sep_count = len(tabbed) - 1
                    if sep_count == 0:
                        sep_count = 1
                    for _ in range(sep_count - 1):
                        row.append("")
                    line = line[len(intra) :]
                else:
                    raise BenchmarkResultException(
                        "Invalid TSV line", f"{line_in} at {line}"
                    )
        return row

    def parse(self, lines):
        """Parse something that iterates lines"""
        rows = [self.line(line) for line in lines if not self.ignore(line)]
        header = rows[0]
        width = len(header)
        records = [
            {k: v for (k, v) in itertools.zip_longest(header, row[:width])}
            for row in rows[1:]
        ]
        return records


def load_report_from_tsv(filename: str):
    file = open(filename, "r")
    contents = file.readlines()
    file.close()
    parser = ResultParser()
    report = parser.parse(contents)
    logging.debug(f"Loaded TSV Report: {report}")
    return report


def push_report_to_opensearch(report, esdocument):
    sanitized = [
        BenchmarkUtils.conform_opensearch(row)
        for row in report
        if BenchmarkUtils.sanity_check(row)
    ]
    logging.debug(
        f"upload {len(sanitized)} sane of {len(report)} benchmarks to opensearch"
    )
    for single_benchmark in sanitized:
        logging.debug(f"upload benchmark: {single_benchmark}")
        response = requests.post(
            esdocument,
            json=single_benchmark,
            auth=(os.environ["ES_USER"], os.environ["ES_PASS"]),
        )
        logging.debug(
            f"Sent to OpenSearch, status: {response.status_code}, result: {response.text}"
        )
        response.raise_for_status()


def push_report_to_null(report):

    for row in report:
        if BenchmarkUtils.sanity_check(row):
            logging.debug(f"row {row}")
            conformed = BenchmarkUtils.conform_opensearch(row)
            logging.debug(f"conformed row {conformed}")


def main():
    """Tool for fetching, parsing and uploading benchmark results to OpenSearch / ElasticSearch
    This tool will

    (1) Open a local tsv benchmark report file
    (2) Upload to OpenSearch document, via https/JSON
    """

    parser = argparse.ArgumentParser(description="CircleCI benchmark scraper.")

    # --tsvfile is the name of the file to read results from
    # --esdocument is the ElasticSearch document to push these results into
    #
    parser.add_argument(
        "--tsvfile",
        default="build_tools/circle_api_scraper_input.txt",
        help="File from which to read tsv report",
    )
    parser.add_argument(
        "--esdocument",
        help="ElasticSearch/OpenSearch document URL to upload report into",
    )
    parser.add_argument(
        "--upload", choices=["opensearch", "none"], default="opensearch"
    )

    args = parser.parse_args()
    logging.debug(f"Arguments: {args}")
    reports = load_report_from_tsv(args.tsvfile)
    if args.upload == "opensearch":
        push_report_to_opensearch(reports, args.esdocument)
    else:
        push_report_to_null(reports)


if __name__ == "__main__":
    sys.exit(main())
