# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
try:
    from builtins import object
    from builtins import str
except ImportError:
    from __builtin__ import object
    from __builtin__ import str
import targets_cfg
import pprint

def pretty_list(lst, indent=8):
    if lst is None or len(lst) == 0:
        return ""

    if len(lst) == 1:
        return "\"%s\"" % lst[0]

    separator = "\",\n%s\"" % (" " * indent)
    res = separator.join(sorted(lst))
    res = "\n" + (" " * indent) + "\"" + res + "\",\n" + (" " * (indent - 4))
    return res


class TARGETSBuilder(object):
    def __init__(self, path, extra_argv):
        self.path = path
        self.targets_file = open(path, 'wb')
        header = targets_cfg.rocksdb_target_header_template.format(
            extra_argv=extra_argv)
        self.targets_file.write(header.encode("utf-8"))
        self.total_lib = 0
        self.total_bin = 0
        self.total_test = 0
        self.tests_cfg = ""

    def __del__(self):
        self.targets_file.close()

    def add_library(self, name, srcs, deps=None, headers=None,
                    extra_external_deps="", link_whole=False,
                     external_dependencies=None, extra_test_libs=False):
        if headers is not None:
            headers = "[" + pretty_list(headers) + "]"
        self.targets_file.write(targets_cfg.library_template.format(
            name=name,
            srcs=pretty_list(srcs),
            headers=headers,
            deps=pretty_list(deps),
            extra_external_deps=extra_external_deps,
            link_whole=link_whole,
            external_dependencies=pretty_list(external_dependencies),
            extra_test_libs=extra_test_libs
            ).encode("utf-8"))
        self.total_lib = self.total_lib + 1

    def add_rocksdb_library(self, name, srcs, headers=None,
                            external_dependencies=None):
        if headers is not None:
            headers = "[" + pretty_list(headers) + "]"
        self.targets_file.write(targets_cfg.rocksdb_library_template.format(
            name=name,
            srcs=pretty_list(srcs),
            headers=headers,
            external_dependencies=pretty_list(external_dependencies)
            ).encode("utf-8")
            )
        self.total_lib = self.total_lib + 1

    def add_binary(self, name, srcs, deps=None, extra_preprocessor_flags=None,extra_bench_libs=False):
        self.targets_file.write(targets_cfg.binary_template.format(
            name=name,
            srcs=pretty_list(srcs),
            deps=pretty_list(deps),
            extra_preprocessor_flags=pretty_list(extra_preprocessor_flags),
            extra_bench_libs=extra_bench_libs,
            ).encode("utf-8"))
        self.total_bin = self.total_bin + 1

    def add_c_test(self):
        self.targets_file.write(b"""
add_c_test_wrapper()
""")

    def add_test_header(self):
        self.targets_file.write(b"""
        # Generate a test rule for each entry in ROCKS_TESTS
        # Do not build the tests in opt mode, since SyncPoint and other test code
        # will not be included.
""")

    def add_fancy_bench_config(self, name, bench_config, slow, expected_runtime):
        self.targets_file.write(targets_cfg.fancy_bench_template.format(
                    name=name,
                    bench_config=pprint.pformat(bench_config),
                    slow=slow,
                    expected_runtime=expected_runtime,
                    ).encode("utf-8"))

    def register_test(self,
                      test_name,
                      src,
                      deps,
                      extra_compiler_flags):

        self.targets_file.write(targets_cfg.unittests_template.format(test_name=test_name,test_cc=str(src),deps=deps,
            extra_compiler_flags=extra_compiler_flags).encode("utf-8"))
        self.total_test = self.total_test + 1
