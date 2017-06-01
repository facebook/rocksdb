from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import targets_cfg
import pprint


# TODO(tec): replace this with PrettyPrinter
def pretty_list(lst, indent=10):
    if lst is None or len(lst) == 0:
        return ""

    if len(lst) == 1:
        return "\"%s\"" % lst[0]

    separator = "\",\n%s\"" % (" " * indent)
    res = separator.join(lst)
    res = "\n" + (" " * indent) + "\"" + res + "\",\n" + (" " * (indent - 2))
    return res


class TARGETSBuilder:
    def __init__(self, path):
        self.path = path
        self.targets_file = open(path, 'w')
        self.total_lib = 0
        self.total_bin = 0
        self.total_test = 0
        self.tests_cfg = []

    def __del__(self):
        self.targets_file.close()

    def add_library(self, name, srcs, deps="", headers=None):
        if headers is None:
            headers = "AutoHeaders.RECURSIVE_GLOB"
        if deps != "":
            deps = targets_cfg.deps_template % (deps)
        self.targets_file.write(targets_cfg.library_template % (
            name,
            headers,
            pretty_list(srcs),
            deps))
        self.total_lib = self.total_lib + 1

    def register_test(self, test_name, src, is_parallel):
        exec_mode = "serial"
        if is_parallel:
            exec_mode = "parallel"
        self.tests_cfg.append([test_name, str(src), str(exec_mode)])
        self.total_test = self.total_test + 1

    def add_header(self):
        self.targets_file.write(targets_cfg.rocksdb_target_header_template % (
            pprint.PrettyPrinter().pformat(self.tests_cfg)
        ))
        self.tests_cfg = []

    def add_footer(self):
        self.targets_file.write(targets_cfg.rocksdb_target_footer_template)
