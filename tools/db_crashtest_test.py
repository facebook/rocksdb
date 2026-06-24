#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import importlib.util
import io
import os
import shutil
import struct
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace


_DB_CRASHTEST_PATH = os.path.join(os.path.dirname(__file__), "db_crashtest.py")
_FAULT_INJECTION_LOG_PARSER_PATH = os.path.join(
    os.path.dirname(__file__), "fault_injection_log_parser.py"
)
_TEST_DIR_ENV_VAR = "TEST_TMPDIR"
_TEST_EXPECTED_DIR_ENV_VAR = "TEST_TMPDIR_EXPECTED"
_TSAN_OPTIONS_ENV_VAR = "TSAN_OPTIONS"


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


def load_fault_injection_log_parser_module():
    spec = importlib.util.spec_from_file_location(
        "fault_injection_log_parser_under_test", _FAULT_INJECTION_LOG_PARSER_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DBCrashTestTest(unittest.TestCase):
    def setUp(self):
        self.test_tmpdir = tempfile.mkdtemp(prefix="db_crashtest_test_")
        self.expected_dir = os.path.join(
            self.test_tmpdir, "rocksdb_crashtest_expected"
        )
        self.old_test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
        self.old_test_expected_tmpdir = os.environ.get(_TEST_EXPECTED_DIR_ENV_VAR)
        self.old_tsan_options = os.environ.get(_TSAN_OPTIONS_ENV_VAR)
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

        if self.old_tsan_options is None:
            os.environ.pop(_TSAN_OPTIONS_ENV_VAR, None)
        else:
            os.environ[_TSAN_OPTIONS_ENV_VAR] = self.old_tsan_options

        shutil.rmtree(self.test_tmpdir)

    def load_db_crashtest(self):
        return load_db_crashtest_module()

    def load_fault_injection_log_parser(self):
        return load_fault_injection_log_parser_module()

    def build_params(self, base_params, overrides=None):
        params = dict(base_params)
        params["db"] = self.test_tmpdir
        if overrides:
            params.update(overrides)
        return params

    def build_mode_args(self, test_type="liveness", **overrides):
        args = {
            "test_type": test_type,
            "simple": False,
            "cf_consistency": False,
            "txn": False,
            "optimistic_txn": False,
            "test_best_efforts_recovery": False,
            "enable_ts": False,
            "test_multiops_txn": False,
            "test_tiered_storage": False,
            "print_stderr_separately": False,
        }
        args.update(overrides)
        return SimpleNamespace(**args)

    def test_stress_cmd_env_defaults_tsan_suppressions(self):
        os.environ.pop(_TSAN_OPTIONS_ENV_VAR, None)
        db_crashtest = self.load_db_crashtest()

        env = db_crashtest.stress_cmd_env()

        self.assertEqual(
            "suppressions="
            + os.path.abspath(
                os.path.join(os.path.dirname(__file__), "tsan_suppressions.txt")
            ),
            env[_TSAN_OPTIONS_ENV_VAR],
        )

    def test_stress_cmd_env_preserves_tsan_options(self):
        os.environ[_TSAN_OPTIONS_ENV_VAR] = "halt_on_error=1"
        db_crashtest = self.load_db_crashtest()

        env = db_crashtest.stress_cmd_env()

        self.assertEqual("halt_on_error=1", env[_TSAN_OPTIONS_ENV_VAR])

    def test_get_ev_parent_dir_preserves_existing_contents(self):
        os.makedirs(self.expected_dir)
        marker = os.path.join(self.expected_dir, "marker")
        with open(marker, "w") as f:
            f.write("keep")

        db_crashtest = self.load_db_crashtest()

        expected_dir = db_crashtest.get_ev_parent_dir()

        self.assertEqual(self.expected_dir, expected_dir)
        self.assertTrue(os.path.exists(marker))

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

    def test_finalize_disables_sqfc_range_queries_with_range_conversion(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {
                "test_batches_snapshots": 0,
                "use_multiscan": 0,
                "use_sqfc_for_range_queries": 1,
                "min_tombstones_for_range_conversion": 2,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(2, finalized["min_tombstones_for_range_conversion"])
        self.assertEqual(0, finalized["use_sqfc_for_range_queries"])

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

    def test_strip_expected_sigterm_stderr_suppresses_retryable_wait_cqe(self):
        db_crashtest = self.load_db_crashtest()
        stdout = "Received signal 15 (Terminated)\n"

        # This models blackbox crash test SIGTERM interrupting wait_cqe before
        # the C++ retry path can avoid stderr; only retryable codes are expected.
        for caller in ("Poll", "AbortIO"):
            for err in (-4, -11):
                stderr = f"{caller}: io_uring_wait_cqe failed: {err}\n"
                filtered_stdout, filtered_stderr = (
                    db_crashtest.strip_expected_sigterm_stderr(stdout, stderr, True)
                )

                self.assertEqual("", filtered_stderr)
                self.assertEqual(
                    stdout
                    + "Ignored expected post-SIGTERM stderr while handling timeout:\n"
                    + stderr,
                    filtered_stdout,
                )

    def test_strip_expected_sigterm_stderr_preserves_terminal_wait_cqe(self):
        db_crashtest = self.load_db_crashtest()
        stdout = "Received signal 15 (Terminated)\n"
        stderr = "Poll: io_uring_wait_cqe failed: -5\n"

        # This guards against hiding real io_uring failures: even after SIGTERM,
        # non-retryable wait_cqe errors must remain visible on stderr.
        filtered_stdout, filtered_stderr = db_crashtest.strip_expected_sigterm_stderr(
            stdout, stderr, True
        )

        self.assertEqual(stderr, filtered_stderr)
        self.assertEqual(stdout, filtered_stdout)

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

    def test_finalize_sanitizes_incompatible_flags_for_multi_db(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {
                "num_dbs": 3,
                "clear_column_family_one_in": 10,
                "test_multi_ops_txns": 1,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(0, finalized["clear_column_family_one_in"])
        self.assertEqual(0, finalized["test_multi_ops_txns"])

    def test_finalize_preserves_flags_for_single_db(self):
        db_crashtest = self.load_db_crashtest()
        params = self.build_params(
            db_crashtest.default_params,
            {
                "num_dbs": 1,
                "clear_column_family_one_in": 10,
            },
        )

        finalized = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(10, finalized["clear_column_family_one_in"])

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

    def test_liveness_params_keep_mixed_workload_and_enable_watchdog(self):
        db_crashtest = self.load_db_crashtest()

        # Liveness mode should inherit the normal mixed workload. The C++
        # watchdog tracks per-thread active operation type, so Python does not
        # need to force an all-write workload to keep the signal readable.
        params = db_crashtest.gen_cmd_params(self.build_mode_args())
        params["db"] = self.test_tmpdir
        finalized_params = db_crashtest.finalize_and_sanitize(params)

        self.assertEqual(db_crashtest.DEFAULT_LIVENESS_TIMEOUT_SEC, params["duration"])
        self.assertEqual(1, params["liveness_check_interval_sec"])
        self.assertEqual(300, params["liveness_no_progress_timeout_sec"])
        self.assertEqual(1, params["enable_thread_tracking"])
        self.assertEqual(1, params["progress_reports"])
        self.assertEqual(100000000, params["ops_per_thread"])
        for fault_param in [
            "error_recovery_with_no_fault_injection",
            "exclude_wal_from_write_fault_injection",
            "metadata_read_fault_one_in",
            "metadata_write_fault_one_in",
            "open_metadata_read_fault_one_in",
            "open_metadata_write_fault_one_in",
            "open_read_fault_one_in",
            "open_write_fault_one_in",
            "read_fault_one_in",
            "secondary_cache_fault_one_in",
            "sync_fault_injection",
            "write_fault_one_in",
        ]:
            self.assertEqual(0, finalized_params[fault_param])
        self.assertIn(
            finalized_params["abort_and_resume_compactions_one_in"],
            [0, 1000, 10000],
        )
        self.assertEqual(db_crashtest.default_params["readpercent"], params["readpercent"])
        self.assertEqual(
            db_crashtest.default_params["prefixpercent"], params["prefixpercent"]
        )
        self.assertEqual(
            db_crashtest.default_params["writepercent"], params["writepercent"]
        )
        self.assertGreaterEqual(params["writepercent"], 20)
        self.assertEqual(db_crashtest.default_params["delpercent"], params["delpercent"])
        self.assertEqual(
            db_crashtest.default_params["delrangepercent"], params["delrangepercent"]
        )
        self.assertEqual(db_crashtest.default_params["iterpercent"], params["iterpercent"])

        params = db_crashtest.gen_cmd_params(
            self.build_mode_args(read_fault_one_in=32, sync_fault_injection=1)
        )
        self.assertEqual(0, params["read_fault_one_in"])
        self.assertEqual(0, params["sync_fault_injection"])

    def test_liveness_command_passes_watchdog_flags_not_wrapper_duration(self):
        db_crashtest = self.load_db_crashtest()
        check_interval_sec = 2
        no_progress_timeout_sec = 7
        wrapper_duration_sec = 17
        args = self.build_mode_args(
            duration=wrapper_duration_sec,
            liveness_check_interval_sec=check_interval_sec,
            liveness_no_progress_timeout_sec=no_progress_timeout_sec,
        )
        params = db_crashtest.gen_cmd_params(args)
        params["db"] = self.test_tmpdir

        # The Python wrapper owns duration; db_stress only needs the watchdog
        # interval and no-progress timeout flags.
        cmd, _ = db_crashtest.gen_cmd(params, [])
        cmd_flags = {}
        for arg in cmd:
            if arg.startswith("--") and "=" in arg:
                key, value = arg[2:].split("=", 1)
                cmd_flags[key] = value

        self.assertEqual(
            str(check_interval_sec), cmd_flags["liveness_check_interval_sec"]
        )
        self.assertEqual(
            str(no_progress_timeout_sec),
            cmd_flags["liveness_no_progress_timeout_sec"],
        )
        self.assertNotIn("duration", cmd_flags)

    def test_liveness_timeout_zero_uses_default_duration(self):
        db_crashtest = self.load_db_crashtest()

        # A zero/None duration still needs a finite wrapper runtime.
        self.assertEqual(
            db_crashtest.DEFAULT_LIVENESS_TIMEOUT_SEC,
            db_crashtest.liveness_timeout({"duration": 0}),
        )
        self.assertEqual(
            db_crashtest.DEFAULT_LIVENESS_TIMEOUT_SEC,
            db_crashtest.liveness_timeout({"duration": None}),
        )
        self.assertEqual(30, db_crashtest.liveness_timeout({"duration": 30}))

    def test_liveness_wrapper_timeout_is_successful_end_of_run(self):
        db_crashtest = self.load_db_crashtest()
        execute_calls = []
        cleanups = []

        def fake_execute_cmd(cmd, timeout=None, timeout_pstack=False, expected_to_timeout=True):
            execute_calls.append((cmd, timeout, timeout_pstack, expected_to_timeout))
            return (
                True,
                -15,
                "Received signal 15 (Terminated)\n",
                "",
                123,
            )

        db_crashtest.execute_cmd = fake_execute_cmd
        db_crashtest.print_fault_injection_log = lambda pid: None
        db_crashtest.cleanup_after_success = lambda db_arg, num_dbs=1: cleanups.append(
            (db_arg, num_dbs)
        )

        db_crashtest.liveness_main(self.build_mode_args(duration=17), [])

        self.assertEqual(1, len(execute_calls))
        self.assertEqual(17, execute_calls[0][1])
        self.assertFalse(execute_calls[0][2])
        self.assertFalse(execute_calls[0][3])
        self.assertEqual(1, len(cleanups))

    def test_liveness_can_target_transaction_lock_manager(self):
        db_crashtest = self.load_db_crashtest()

        # Transaction mode remains composable with liveness mode, which lets
        # the same watchdog cover point-lock-manager deadlocks/livelocks.
        params = db_crashtest.gen_cmd_params(self.build_mode_args(txn=True))

        self.assertEqual(1, params["use_txn"])
        self.assertEqual(0, params["use_optimistic_txn"])
        self.assertIn("use_per_key_point_lock_mgr", params)
        self.assertEqual(0, params.get("kill_random_test", 0))
        self.assertGreater(params["liveness_no_progress_timeout_sec"], 0)

    # Goal: verify db_crashtest decodes the headerless streaming binary fault
    # injection log format used on crash paths while keeping stdout concise.
    # The test writes a raw .bin file with two complete entries plus a
    # truncated tail, then checks the decoded text artifact and the summary
    # line printed to stdout.
    def test_print_fault_injection_log_decodes_streaming_raw_trace(self):
        db_crashtest = self.load_db_crashtest()
        fault_parser = self.load_fault_injection_log_parser()
        pid = 5151
        log_dir = os.path.join(self.test_tmpdir, "fault_injection_logs")
        os.makedirs(log_dir)
        raw_log = os.path.join(log_dir, f"fault_injection_{pid}_1.bin")
        decoded_log = raw_log + ".txt"

        entry0 = fault_parser.ENTRY_STRUCT.pack(
            123456789,
            17,
            7,
            4,
            0,
            0,
            b"abcd".ljust(48, b"\0"),
            fault_parser.DETAIL_KIND_OFFSET_SIZE_AND_HEAD,
            4,
            1,
            0,
            b"Append\0".ljust(32, b"\0"),
            b"/tmp/000001.log\0".ljust(72, b"\0"),
            b"injected write error\0".ljust(56, b"\0"),
        )
        entry1 = fault_parser.ENTRY_STRUCT.pack(
            123456790,
            23,
            0,
            6,
            0,
            0,
            b"/tmp/b".ljust(48, b"\0"),
            fault_parser.DETAIL_KIND_TWO_FILES,
            6,
            0,
            1,
            b"Rename\0".ljust(32, b"\0"),
            b"/tmp/a\0".ljust(72, b"\0"),
            b"injected metadata read error\0".ljust(56, b"\0"),
        )
        with open(raw_log, "wb") as f:
            f.write(entry0)
            f.write(entry1)
            f.write(b"tail")

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            db_crashtest.print_fault_injection_log(pid)

        self.assertTrue(os.path.exists(decoded_log))
        with open(decoded_log) as f:
            decoded_text = f.read()

        self.assertIn(
            'Append("/tmp/000001.log", offset=7, size=4, head=[61 62 63 64])',
            decoded_text,
        )
        self.assertIn("IO error: injected write error [retryable]", decoded_text)
        self.assertIn(
            'Rename("/tmp/a", "/tmp/b") -> IO error: injected metadata read error [data_loss]',
            decoded_text,
        )
        self.assertIn("max=unbounded", decoded_text)
        self.assertIn(
            "Fault injection log saved: raw=%s decoded=%s entries=2"
            % (raw_log, decoded_log),
            stdout.getvalue(),
        )

    # Goal: verify fault-injection logs do not follow a remote TEST_TMPDIR.
    # The test marks the DB as remote, points TEST_TMPDIR at a remote-looking
    # path, and checks db_crashtest uses the same local staging root as
    # db_stress.
    def test_fault_injection_log_dir_uses_local_tmp_for_remote_db(self):
        db_crashtest = self.load_db_crashtest()
        os.environ[_TEST_DIR_ENV_VAR] = "/dev_test/rocksdb_crash_test/job123"
        db_crashtest.is_remote_db = True

        self.assertEqual(
            os.path.join("/tmp", "fault_injection_logs"),
            db_crashtest._fault_injection_log_dir(),
        )


if __name__ == "__main__":
    unittest.main()
