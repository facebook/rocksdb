#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import importlib.util
import os
import shutil
import sys
import tempfile
import unittest


_DB_CRASHTEST_PATH = os.path.join(os.path.dirname(__file__), "db_crashtest.py")
_TEST_DIR_ENV_VAR = "TEST_TMPDIR"
_TEST_EXPECTED_DIR_ENV_VAR = "TEST_TMPDIR_EXPECTED"


def load_db_crashtest_module():
    spec = importlib.util.spec_from_file_location(
        "db_crashtest_under_test", _DB_CRASHTEST_PATH
    )
    module = importlib.util.module_from_spec(spec)
    old_argv = sys.argv[:]
    try:
        sys.argv = [_DB_CRASHTEST_PATH]
        spec.loader.exec_module(module)
    finally:
        sys.argv = old_argv
    return module


class DBCrashTestTest(unittest.TestCase):
    def setUp(self):
        self.test_tmpdir = tempfile.mkdtemp(prefix="db_crashtest_test_")
        self.expected_dir = os.path.join(
            self.test_tmpdir, "rocksdb_crashtest_expected"
        )
        self.old_test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
        self.old_test_expected_tmpdir = os.environ.get(_TEST_EXPECTED_DIR_ENV_VAR)
        os.environ[_TEST_DIR_ENV_VAR] = self.test_tmpdir
        os.environ.pop(_TEST_EXPECTED_DIR_ENV_VAR, None)

    def tearDown(self):
        if self.old_test_tmpdir is None:
            os.environ.pop(_TEST_DIR_ENV_VAR, None)
        else:
            os.environ[_TEST_DIR_ENV_VAR] = self.old_test_tmpdir

        if self.old_test_expected_tmpdir is None:
            os.environ.pop(_TEST_EXPECTED_DIR_ENV_VAR, None)
        else:
            os.environ[_TEST_EXPECTED_DIR_ENV_VAR] = self.old_test_expected_tmpdir

        shutil.rmtree(self.test_tmpdir)

    def load_db_crashtest(self):
        return load_db_crashtest_module()

    def build_params(self, base_params, overrides=None):
        params = dict(base_params)
        params["db"] = self.test_tmpdir
        if overrides:
            params.update(overrides)
        return params

    def create_remote_compaction_output(self, parent_dir, output_name, marker):
        output_dir = os.path.join(parent_dir, output_name)
        os.makedirs(output_dir)
        with open(os.path.join(output_dir, "orphan.sst"), "w") as f:
            f.write(marker)
        return output_dir

    def create_archived_remote_compaction_run(
        self, db_crashtest, dbname, run_suffix, output_name, marker
    ):
        archive_root = os.path.join(
            dbname, db_crashtest._ABANDONED_REMOTE_COMPACTION_OUTPUTS_DIR
        )
        run_dir = os.path.join(
            archive_root,
            f"{db_crashtest._ABANDONED_REMOTE_COMPACTION_OUTPUT_RUN_PREFIX}"
            f"{run_suffix:020d}",
        )
        self.create_remote_compaction_output(run_dir, output_name, marker)
        return run_dir

    def test_setup_expected_values_dir_preserves_existing_contents(self):
        os.makedirs(self.expected_dir)
        marker = os.path.join(self.expected_dir, "marker")
        with open(marker, "w") as f:
            f.write("keep")

        db_crashtest = self.load_db_crashtest()

        expected_dir = db_crashtest.setup_expected_values_dir()

        self.assertEqual(self.expected_dir, expected_dir)
        self.assertTrue(os.path.exists(marker))

    def test_prepare_expected_values_dir_resets_for_fresh_db(self):
        os.makedirs(self.expected_dir)
        marker = os.path.join(self.expected_dir, "marker")
        with open(marker, "w") as f:
            f.write("remove")

        db_crashtest = self.load_db_crashtest()

        db_crashtest.prepare_expected_values_dir(self.expected_dir, True)

        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertFalse(os.path.exists(marker))

    def test_finalize_disables_test_batches_snapshots_when_disable_wal(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {"disable_wal": 1, "test_batches_snapshots": 1},
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(1, finalized["disable_wal"])
        self.assertEqual(0, finalized["test_batches_snapshots"])

    def test_finalize_disables_test_batches_snapshots_for_blob_direct_write(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            dict(
                list(db_crashtest.default_params.items())
                + list(db_crashtest.blob_direct_write_multi_get_entity_params.items())
            ),
            {
                "test_batches_snapshots": 1,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(1, finalized["enable_blob_direct_write"])
        self.assertEqual(1, finalized["disable_wal"])
        self.assertEqual(0, finalized["test_batches_snapshots"])

    def test_cleanup_stale_remote_compaction_outputs_archives_only_tmp_output_dirs(
        self,
    ):
        db_crashtest = self.load_db_crashtest()
        dbname = os.path.join(self.test_tmpdir, "rocksdb_crashtest_blackbox")
        os.makedirs(dbname)

        stale_dir = self.create_remote_compaction_output(
            dbname, "tmp_output_stale", "old remote compaction output"
        )

        backup_dir = os.path.join(dbname, ".backup0")
        os.makedirs(backup_dir)
        live_sst = os.path.join(dbname, "000123.sst")
        with open(live_sst, "w") as f:
            f.write("keep")

        db_crashtest.cleanup_stale_remote_compaction_outputs(dbname)

        self.assertFalse(os.path.exists(stale_dir))
        self.assertTrue(os.path.isdir(backup_dir))
        self.assertTrue(os.path.isfile(live_sst))
        archive_root = os.path.join(
            dbname, db_crashtest._ABANDONED_REMOTE_COMPACTION_OUTPUTS_DIR
        )
        archived_runs = sorted(os.listdir(archive_root))
        self.assertEqual(1, len(archived_runs))
        self.assertTrue(
            os.path.isdir(os.path.join(archive_root, archived_runs[0], "tmp_output_stale"))
        )

    def test_cleanup_stale_remote_compaction_outputs_keeps_last_three_runs(self):
        db_crashtest = self.load_db_crashtest()
        dbname = os.path.join(self.test_tmpdir, "rocksdb_crashtest_whitebox")
        os.makedirs(dbname)

        oldest_run = self.create_archived_remote_compaction_run(
            db_crashtest, dbname, 1, "tmp_output_oldest", "oldest"
        )
        second_oldest_run = self.create_archived_remote_compaction_run(
            db_crashtest, dbname, 2, "tmp_output_old_2", "old_2"
        )
        newest_existing_run = self.create_archived_remote_compaction_run(
            db_crashtest, dbname, 3, "tmp_output_old_3", "old_3"
        )
        current_output = self.create_remote_compaction_output(
            dbname, "tmp_output_current", "current"
        )

        db_crashtest.cleanup_stale_remote_compaction_outputs(dbname)

        self.assertFalse(os.path.exists(current_output))
        self.assertFalse(os.path.exists(oldest_run))
        self.assertTrue(os.path.isdir(second_oldest_run))
        self.assertTrue(os.path.isdir(newest_existing_run))

        archive_root = os.path.join(
            dbname, db_crashtest._ABANDONED_REMOTE_COMPACTION_OUTPUTS_DIR
        )
        archived_runs = sorted(os.listdir(archive_root))
        self.assertEqual(3, len(archived_runs))
        self.assertEqual(
            f"{db_crashtest._ABANDONED_REMOTE_COMPACTION_OUTPUT_RUN_PREFIX}{2:020d}",
            archived_runs[0],
        )
        self.assertEqual(
            f"{db_crashtest._ABANDONED_REMOTE_COMPACTION_OUTPUT_RUN_PREFIX}{3:020d}",
            archived_runs[1],
        )
        self.assertTrue(
            os.path.isdir(
                os.path.join(archive_root, archived_runs[2], "tmp_output_current")
            )
        )

    def test_cleanup_stale_remote_compaction_outputs_ignores_missing_db_dir(self):
        db_crashtest = self.load_db_crashtest()
        missing_db = os.path.join(self.test_tmpdir, "missing_db")

        db_crashtest.cleanup_stale_remote_compaction_outputs(missing_db)

        self.assertFalse(os.path.exists(missing_db))


if __name__ == "__main__":
    unittest.main()
