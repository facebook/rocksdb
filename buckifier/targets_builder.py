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
    def __init__(self, path):
        self.path = path
        self.targets_file = open(path, 'w')
        header = targets_cfg.rocksdb_target_header_template
        self.targets_file.write(header)
        self.total_lib = 0
        self.total_bin = 0
        self.total_test = 0
        self.tests_cfg = ""

    def __del__(self):
        self.targets_file.close()

    def add_library(self, name, srcs, deps=None, headers=None,
                    extra_external_deps=""):
        headers_attr_prefix = ""
        if headers is None:
            headers_attr_prefix = "auto_"
            headers = "AutoHeaders.RECURSIVE_GLOB"
        else:
            headers = "[" + pretty_list(headers) + "]"
        self.targets_file.write(targets_cfg.library_template.format(
            name=name,
            srcs=pretty_list(srcs),
            headers_attr_prefix=headers_attr_prefix,
            headers=headers,
            deps=pretty_list(deps),
            extra_external_deps=extra_external_deps))
        self.total_lib = self.total_lib + 1

    def add_rocksdb_library(self, name, srcs, headers=None):
        headers_attr_prefix = ""
        if headers is None:
            headers_attr_prefix = "auto_"
            headers = "AutoHeaders.RECURSIVE_GLOB"
        else:
            headers = "[" + pretty_list(headers) + "]"
        self.targets_file.write(targets_cfg.rocksdb_library_template.format(
            name=name,
            srcs=pretty_list(srcs),
            headers_attr_prefix=headers_attr_prefix,
            headers=headers))
        self.total_lib = self.total_lib + 1

    def add_binary(self, name, srcs, deps=None):
        self.targets_file.write(targets_cfg.binary_template % (
            name,
            pretty_list(srcs),
            pretty_list(deps)))
        self.total_bin = self.total_bin + 1

    def add_c_test(self):
        self.targets_file.write("""
if not is_opt_mode:
    cpp_binary(
        name = "c_test_bin",
        srcs = ["db/c_test.c"],
        arch_preprocessor_flags = ROCKSDB_ARCH_PREPROCESSOR_FLAGS,
        os_preprocessor_flags = ROCKSDB_OS_PREPROCESSOR_FLAGS,
        compiler_flags = ROCKSDB_COMPILER_FLAGS,
        preprocessor_flags = ROCKSDB_PREPROCESSOR_FLAGS,
        deps = [":rocksdb_test_lib"],
    )

if not is_opt_mode:
    custom_unittest(
        "c_test",
        command = [
            native.package_name() + "/buckifier/rocks_test_runner.sh",
            "$(location :{})".format("c_test_bin"),
        ],
        type = "simple",
    )
""")

    def register_test(self,
                      test_name,
                      src,
                      is_parallel,
                      extra_deps,
                      extra_compiler_flags):
        exec_mode = "serial"
        if is_parallel:
            exec_mode = "parallel"
        self.tests_cfg += targets_cfg.test_cfg_template % (
            test_name,
            str(src),
            str(exec_mode),
            extra_deps,
            extra_compiler_flags)

        self.total_test = self.total_test + 1

    def flush_tests(self):
        self.targets_file.write(targets_cfg.unittests_template % self.tests_cfg)
        self.tests_cfg = ""
