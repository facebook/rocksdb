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

    def test_strip_expected_sigterm_stderr_suppresses_only_known_lines(self):
        db_crashtest = self.load_db_crashtest()
        stdout = "Received signal 15 (Terminated)\n"
        stderr = (
            "PosixRandomAccessFile::MultiRead: io_uring_submit_and_wait "
            "returned terminal error: -9.\n"
            "PosixRandomAccessFile::MultiRead: io_uring_submit_and_wait "
            "returned terminal error: -9.\n"
        )

        filtered_stdout, filtered_stderr = db_crashtest.strip_expected_sigterm_stderr(
            stdout, stderr, True
        )

        self.assertEqual("", filtered_stderr)
        self.assertEqual(
            stdout
            + "Ignored expected post-SIGTERM stderr while handling timeout:\n"
            + stderr,
            filtered_stdout,
        )

    def test_strip_expected_sigterm_stderr_preserves_other_stderr(self):
        db_crashtest = self.load_db_crashtest()
        stdout = "Received signal 15 (Terminated)\n"
        ignored_line = (
            "PosixRandomAccessFile::MultiRead: io_uring_submit_and_wait "
            "returned terminal error: -9.\n"
        )
        kept_line = "Different stderr line\n"
        stderr = ignored_line + kept_line

        filtered_stdout, filtered_stderr = db_crashtest.strip_expected_sigterm_stderr(
            stdout, stderr, True
        )

        self.assertEqual(kept_line, filtered_stderr)
        self.assertEqual(
            stdout
            + "Ignored expected post-SIGTERM stderr while handling timeout:\n"
            + ignored_line,
            filtered_stdout,
        )

    def test_strip_expected_sigterm_stderr_requires_timeout_and_sigterm_marker(self):
        db_crashtest = self.load_db_crashtest()
        stderr = (
            "PosixRandomAccessFile::MultiRead: io_uring_submit_and_wait "
            "returned terminal error: -9.\n"
        )

        filtered_stdout, filtered_stderr = db_crashtest.strip_expected_sigterm_stderr(
            "Received signal 15 (Terminated)\n", stderr, False
        )
        self.assertEqual("Received signal 15 (Terminated)\n", filtered_stdout)
        self.assertEqual(stderr, filtered_stderr)

        filtered_stdout, filtered_stderr = db_crashtest.strip_expected_sigterm_stderr(
            "other stdout\n", stderr, True
        )
        self.assertEqual("other stdout\n", filtered_stdout)
        self.assertEqual(stderr, filtered_stderr)

    def test_output_matches_no_space_catches_known_failure_strings(self):
        db_crashtest = self.load_db_crashtest()
        open_and_compact_stdout = (
            "Failed to run OpenAndCompact(/dev/shm/rocksdb_test/db): "
            "IO error: No space left on device: While appending to file: "
            "/dev/shm/rocksdb_test/db/tmp_output_1/019471.sst: "
            "No space left on device\n"
        )
        verification_stderr = (
            "Verification failed: SetOptions failed: IO error: Unable to "
            "persist options.: IO error: No space left on device: While "
            "appending to file: /dev/shm/rocksdb_test/db/OPTIONS-084168.dbtmp: "
            "No space left on device\n"
        )

        self.assertTrue(
            db_crashtest.output_matches_no_space(open_and_compact_stdout, "")
        )
        self.assertTrue(
            db_crashtest.output_matches_no_space("", verification_stderr)
        )
        self.assertFalse(
            db_crashtest.output_matches_no_space("", "Permission denied\n")
        )

    def test_file_type_suffix_preserves_compound_suffixes(self):
        db_crashtest = self.load_db_crashtest()

        self.assertEqual(".sst.trash", db_crashtest.file_type_suffix("000123.sst.trash"))
        self.assertEqual(".sst", db_crashtest.file_type_suffix("tmp_output/019471.sst"))
        self.assertEqual(".old.1", db_crashtest.file_type_suffix("/tmp/LOG.old.1"))
        self.assertEqual("<no_ext>", db_crashtest.file_type_suffix("/tmp/CURRENT"))

    def test_build_out_of_space_diagnostics_summarizes_directory_suffixes(self):
        db_crashtest = self.load_db_crashtest()
        db_root = os.path.join(self.test_tmpdir, "rocksdb_crashtest_blackbox")
        remote_output_dir = os.path.join(db_root, "tmp_output_123")
        os.makedirs(remote_output_dir)

        files = {
            os.path.join(db_root, "CURRENT"): 7,
            os.path.join(db_root, "000001.sst.trash"): 3,
            os.path.join(remote_output_dir, "019471.sst"): 5,
        }
        for path, size in files.items():
            with open(path, "wb") as f:
                f.write(b"x" * size)

        diagnostics = db_crashtest.build_out_of_space_diagnostics(
            "",
            (
                "IO error: No space left on device: While appending to file: "
                f"{os.path.join(remote_output_dir, '019471.sst')}: "
                "No space left on device\n"
            ),
            [db_root],
            include_dev_shm=False,
        )

        self.assertIn("=== Out-of-space diagnostics ===", diagnostics)
        self.assertIn(f"Directory usage for {db_root}:", diagnostics)
        self.assertIn(".sst.trash files=1 bytes=3B", diagnostics)
        self.assertIn(".sst files=1 bytes=5B", diagnostics)
        self.assertIn("<no_ext> files=1 bytes=7B", diagnostics)
        self.assertIn(
            f"{remote_output_dir} subtree=5B local=5B local_files=1 local_dirs=0",
            diagnostics,
        )


if __name__ == "__main__":
    unittest.main()
