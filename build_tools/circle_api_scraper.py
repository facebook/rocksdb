#!/usr/bin/env python3
#  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

'''Access CircleCI output via a combination of v1 and v2 APIs
In order to finally yield the output of benchmark tests.
'''

import os
import sys
from circleci.api import Api


def api_v1_get_log_mime_url(job_number: int) -> str:
    '''Use the old CircleCI v1.1 API to get the report URL. The report is the output files, MIME encoded.
    Track it down because we know its name.
    TODO (AP) is to abstract the API key, currently it's mine..
    '''
    circleci = Api("e7d4aab13e143360f95e258be0a89b5c8e256773")
    dict = circleci.get_build_info(username="facebook",
                                   project="rocksdb", build_num=job_number)
    for step in dict['steps']:
        for action in step['actions']:
            action_name = action['name']
            if action['has_output'] and "Output logs as MIME" == action_name:
                # found the special action whose content we want
                return action['output_url']
    return 0


def api_v2_find_job() -> int:
    '''Track down the job
    TODO (AP) see insomnia logs for the path to the latest (or multiple) pipelines / workflows / benchmarks
    '''


def main():
    # track down the job number
    job_number = api_v2_find_job()
    # job number can be got from the v2 API
    api_v1_get_log_mime_url(job_number=317985)


if __name__ == '__main__':
    sys.exit(main())
