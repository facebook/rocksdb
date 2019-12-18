#!/usr/bin/env python2
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
import glob

# Checks that all python files in the repository are at least free of syntax
# errors. This provides a minimal pre-/post-commit check for python file
# modifications.

count = 0
# Clean this up when we finally upgrade to Python 3
for filename in glob.glob("*.py") + glob.glob("*/*.py") + glob.glob("*/*/*.py") + glob.glob("*/*/*/*.py"):
    source = open(filename, 'r').read() + '\n'
    # Parses and syntax checks the file, throwing on error. (No pyc written.)
    _ = compile(source, filename, 'exec')
    count = count + 1

print("No syntax errors in {0} .py files".format(count))
