# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
try:
    from builtins import str
except ImportError:
    from __builtin__ import str
from targets_builder import TARGETSBuilder
import json
import os
import fnmatch
import sys

from util import ColorString

# This script generates TARGETS file for Buck.
# Buck is a build tool specifying dependencies among different build targets.
# User can pass extra dependencies as a JSON object via command line, and this
# script can include these dependencies in the generate TARGETS file.
# Usage:
# $python3 buckifier/buckify_rocksdb.py
# (This generates a TARGET file without user-specified dependency for unit
# tests.)
# $python3 buckifier/buckify_rocksdb.py \
#        '{"fake": {
#                      "extra_deps": [":test_dep", "//fakes/module:mock1"],
#                      "extra_compiler_flags": ["-DROCKSDB_LITE", "-Os"]
#                  }
#         }'
# (Generated TARGETS file has test_dep and mock1 as dependencies for RocksDB
# unit tests, and will use the extra_compiler_flags to compile the unit test
# source.)

# tests to export as libraries for inclusion in other projects
_EXPORTED_TEST_LIBS = ["env_basic_test"]

# Parse src.mk files as a Dictionary of
# VAR_NAME => list of files
def parse_src_mk(repo_path):
    src_mk = repo_path + "/src.mk"
    src_files = {}
    for line in open(src_mk):
        line = line.strip()
        if len(line) == 0 or line[0] == '#':
            continue
        if '=' in line:
            current_src = line.split('=')[0].strip()
            src_files[current_src] = []
        elif '.c' in line:
            src_path = line.split('\\')[0].strip()
            src_files[current_src].append(src_path)
    return src_files


# get all .cc / .c files
def get_cc_files(repo_path):
    cc_files = []
    for root, dirnames, filenames in os.walk(repo_path):  # noqa: B007 T25377293 Grandfathered in
        root = root[(len(repo_path) + 1):]
        if "java" in root:
            # Skip java
            continue
        for filename in fnmatch.filter(filenames, '*.cc'):
            cc_files.append(os.path.join(root, filename))
        for filename in fnmatch.filter(filenames, '*.c'):
            cc_files.append(os.path.join(root, filename))
    return cc_files


# Get non_parallel tests from Makefile
def get_non_parallel_tests(repo_path):
    Makefile = repo_path + "/Makefile"

    s = set({})

    found_non_parallel_tests = False
    for line in open(Makefile):
        line = line.strip()
        if line.startswith("NON_PARALLEL_TEST ="):
            found_non_parallel_tests = True
        elif found_non_parallel_tests:
            if line.endswith("\\"):
                # remove the trailing \
                line = line[:-1]
                line = line.strip()
                s.add(line)
            else:
                # we consumed all the non_parallel tests
                break

    return s

# Parse extra dependencies passed by user from command line
def get_dependencies():
    deps_map = {
        '': {
            'extra_deps': [],
            'extra_compiler_flags': []
        }
    }
    if len(sys.argv) < 2:
        return deps_map

    def encode_dict(data):
        rv = {}
        for k, v in data.items():
            if isinstance(v, dict):
                v = encode_dict(v)
            rv[k] = v
        return rv
    extra_deps = json.loads(sys.argv[1], object_hook=encode_dict)
    for target_alias, deps in extra_deps.items():
        deps_map[target_alias] = deps
    return deps_map


# Prepare TARGETS file for buck
def generate_targets(repo_path, deps_map):
    print(ColorString.info("Generating TARGETS"))
    # parsed src.mk file
    src_mk = parse_src_mk(repo_path)
    # get all .cc files
    cc_files = get_cc_files(repo_path)
    # get non_parallel tests from Makefile
    non_parallel_tests = get_non_parallel_tests(repo_path)

    if src_mk is None or cc_files is None or non_parallel_tests is None:
        return False

    extra_argv = ""
    if len(sys.argv) >= 2:
        # Heuristically quote and canonicalize whitespace for inclusion
        # in how the file was generated.
        extra_argv = " '{0}'".format(" ".join(sys.argv[1].split()))

    TARGETS = TARGETSBuilder("%s/TARGETS" % repo_path, extra_argv)

    # rocksdb_lib
    TARGETS.add_library(
        "rocksdb_lib",
        src_mk["LIB_SOURCES"] +
        # always add range_tree, it's only excluded on ppc64, which we don't use internally
        src_mk["RANGE_TREE_SOURCES"] +
        src_mk["TOOL_LIB_SOURCES"])
    # rocksdb_whole_archive_lib
    TARGETS.add_library(
        "rocksdb_whole_archive_lib",
        src_mk["LIB_SOURCES"] +
        # always add range_tree, it's only excluded on ppc64, which we don't use internally
        src_mk["RANGE_TREE_SOURCES"] +
        src_mk["TOOL_LIB_SOURCES"],
        deps=None,
        headers=None,
        extra_external_deps="",
        link_whole=True)
    # rocksdb_test_lib
    TARGETS.add_library(
        "rocksdb_test_lib",
        src_mk.get("MOCK_LIB_SOURCES", []) +
        src_mk.get("TEST_LIB_SOURCES", []) +
        src_mk.get("EXP_LIB_SOURCES", []) +
        src_mk.get("ANALYZER_LIB_SOURCES", []),
        [":rocksdb_lib"],
        extra_external_deps=""" + [
        ("googletest", None, "gtest"),
    ]""")
    # rocksdb_tools_lib
    TARGETS.add_library(
        "rocksdb_tools_lib",
        src_mk.get("BENCH_LIB_SOURCES", []) +
        src_mk.get("ANALYZER_LIB_SOURCES", []) +
        ["test_util/testutil.cc"],
        [":rocksdb_lib"])
    # rocksdb_cache_bench_tools_lib
    TARGETS.add_library(
        "rocksdb_cache_bench_tools_lib",
        src_mk.get("CACHE_BENCH_LIB_SOURCES", []),
        [":rocksdb_lib"])
    # rocksdb_stress_lib
    TARGETS.add_rocksdb_library(
        "rocksdb_stress_lib",
        src_mk.get("ANALYZER_LIB_SOURCES", [])
        + src_mk.get('STRESS_LIB_SOURCES', [])
        + ["test_util/testutil.cc"])

    print("Extra dependencies:\n{0}".format(json.dumps(deps_map)))

    # Dictionary test executable name -> relative source file path
    test_source_map = {}
    print(src_mk)

    # c_test.c is added through TARGETS.add_c_test(). If there
    # are more than one .c test file, we need to extend
    # TARGETS.add_c_test() to include other C tests too.
    for test_src in src_mk.get("TEST_MAIN_SOURCES_C", []):
        if test_src != 'db/c_test.c':
            print("Don't know how to deal with " + test_src)
            return False
    TARGETS.add_c_test()

    for test_src in src_mk.get("TEST_MAIN_SOURCES", []):
        test = test_src.split('.c')[0].strip().split('/')[-1].strip()
        test_source_map[test] = test_src
        print("" + test + " " + test_src)

    for target_alias, deps in deps_map.items():
        for test, test_src in sorted(test_source_map.items()):
            if len(test) == 0:
                print(ColorString.warning("Failed to get test name for %s" % test_src))
                continue

            test_target_name = \
                test if not target_alias else test + "_" + target_alias
            TARGETS.register_test(
                test_target_name,
                test_src,
                test not in non_parallel_tests,
                json.dumps(deps['extra_deps']),
                json.dumps(deps['extra_compiler_flags']))

            if test in _EXPORTED_TEST_LIBS:
                test_library = "%s_lib" % test_target_name
                TARGETS.add_library(test_library, [test_src], [":rocksdb_test_lib"])
    TARGETS.flush_tests()

    print(ColorString.info("Generated TARGETS Summary:"))
    print(ColorString.info("- %d libs" % TARGETS.total_lib))
    print(ColorString.info("- %d binarys" % TARGETS.total_bin))
    print(ColorString.info("- %d tests" % TARGETS.total_test))
    return True


def get_rocksdb_path():
    # rocksdb = {script_dir}/..
    script_dir = os.path.dirname(sys.argv[0])
    script_dir = os.path.abspath(script_dir)
    rocksdb_path = os.path.abspath(
        os.path.join(script_dir, "../"))

    return rocksdb_path


def exit_with_error(msg):
    print(ColorString.error(msg))
    sys.exit(1)


def main():
    deps_map = get_dependencies()
    # Generate TARGETS file for buck
    ok = generate_targets(get_rocksdb_path(), deps_map)
    if not ok:
        exit_with_error("Failed to generate TARGETS files")

if __name__ == "__main__":
    main()
