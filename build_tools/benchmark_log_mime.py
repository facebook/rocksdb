#!/usr/bin/env python3
#  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

'''Take benchmark logs and output them as a Multipart MIME:
    - Designed to bypass failed uploads to artifacts
    - Might just do what we need anyway
'''

import mimetypes
import os
import sys
import argparse
from email.message import EmailMessage


class BenchmarkLogger(object):

    def __init__(self, args):
        self.logdirectory = args.logdirectory

    def build_message(self):
        message = EmailMessage()
        for entry in os.scandir(self.logdirectory):
            if entry.is_file():
                # Guess the content type based on the file's extension.  Encoding
                # will be ignored, although we should check for simple things like
                # gzip'd or compressed files.
                ctype, encoding = mimetypes.guess_type(entry)
                if ctype is None or encoding is not None:
                    # No guess could be made, or the file is encoded (compressed), so
                    # use a generic bag-of-bits type.
                    ctype = 'application/octet-stream'
                maintype, subtype = ctype.split('/', 1)

                with open(entry, 'rb') as fp:
                    text = fp.read()
                message.add_attachment(
                    text, maintype=maintype, subtype=subtype, filename=entry.name)
        self.message = message

    def as_string(self):
        return self.message.as_string()
#
# Main
#


def main():
    parser = argparse.ArgumentParser(
        description='Output files in a directory as a MIME message.')

    # --log <logfile>
    parser.add_argument('--logdirectory', default='/tmp/log',
                        help='Log directory. Default is /tmp/log')

    args = parser.parse_args()
    logger = BenchmarkLogger(args)
    logger.build_message()
    message = logger.as_string()

    if not message:
        print("Error mime-ing log directory %s"
              % logger.logdirectory)
        return 1

    print(message)
    return 0


if __name__ == '__main__':
    sys.exit(main())
