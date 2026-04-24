#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import importlib.util
import os
import shutil
import sys
import tempfile
import unittest


_REPORT_PATH = os.path.join(
    os.path.dirname(__file__), "make_check_dbdir_report.py"
)


def load_report_module():
    spec = importlib.util.spec_from_file_location(
        "make_check_dbdir_report_under_test", _REPORT_PATH
    )
    module = importlib.util.module_from_spec(spec)
    old_argv = sys.argv[:]
    try:
        sys.argv = [_REPORT_PATH]
        spec.loader.exec_module(module)
    finally:
        sys.argv = old_argv
    return module


class MakeCheckDbdirReportTest(unittest.TestCase):
    def setUp(self):
        self.test_tmpdir = tempfile.mkdtemp(prefix="make_check_report_test_")
        self.runs_root = os.path.join(self.test_tmpdir, "runs")
        os.makedirs(self.runs_root)

    def tearDown(self):
        shutil.rmtree(self.test_tmpdir)

    def write_file(self, path, size):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(b"x" * size)

    def test_collect_run_diagnostics_sorts_runs_and_db_dirs_by_size(self):
        report = load_report_module()

        run_a = os.path.join(self.runs_root, "db_test-shard-0")
        run_b = os.path.join(self.runs_root, "table_test-shard-1")
        self.write_file(os.path.join(run_a, "case_a", "CURRENT"), 1)
        self.write_file(os.path.join(run_a, "case_a", "000001.sst"), 4)
        self.write_file(os.path.join(run_b, "nested", "case_b", "CURRENT"), 1)
        self.write_file(os.path.join(run_b, "nested", "case_b", "000002.sst.trash"), 8)

        diagnostics = report.collect_run_diagnostics(self.runs_root)

        self.assertEqual(
            ["table_test-shard-1", "db_test-shard-0"],
            [entry["name"] for entry in diagnostics["run_entries"]],
        )
        self.assertEqual(
            ["table_test-shard-1", "db_test-shard-0"],
            [entry["run_name"] for entry in diagnostics["db_entries"]],
        )
        self.assertEqual("nested/case_b", diagnostics["db_entries"][0]["relpath"])
        self.assertEqual("case_a", diagnostics["db_entries"][1]["relpath"])

    def test_build_make_check_disk_report_formats_top_entries(self):
        report = load_report_module()

        run_dir = os.path.join(self.runs_root, "db_test-shard-0")
        self.write_file(os.path.join(run_dir, "db_case", "CURRENT"), 1)
        self.write_file(os.path.join(run_dir, "db_case", "MANIFEST-000001"), 2)
        self.write_file(os.path.join(run_dir, "db_case", "000001.sst"), 3)

        output = report.build_make_check_disk_report(
            self.runs_root,
            self.test_tmpdir,
            top_n=10,
            include_dev_shm=False,
        )

        self.assertIn("=== make check disk usage diagnostics ===", output)
        self.assertIn(f"Runs root: {self.runs_root}", output)
        self.assertIn(
            f"1. 6B  db_test-shard-0  path={run_dir}",
            output,
        )
        self.assertIn(
            f"1. 6B  owner=db_test-shard-0  db_dir=db_case  path={os.path.join(run_dir, 'db_case')}",
            output,
        )


if __name__ == "__main__":
    unittest.main()
