#!/usr/bin/env python2
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
import glob

# Checks that all python files in the repository are at least free of syntax
# errors. This provides a minimal pre-/post-commit check for python file
# modifications.

filenames = []
# Avoid scanning all of ./ because there might be other external repos
# linked in.
for base in ["buckifier", "build_tools", "coverage", "tools"]:
    # Clean this up when we finally upgrade to Python 3
    for suff in ["*", "*/*", "*/*/*"]:
        filenames += glob.glob(base + "/" + suff + ".py")

for filename in filenames:
    source = open(filename, 'r').read() + '\n'
    # Parses and syntax checks the file, throwing on error. (No pyc written.)
    _ = compile(source, filename, 'exec')

print("No syntax errors in {0} .py files".format(len(filenames)))
