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


_DB_CRASHTEST_PATH = os.path.join(os.path.dirname(__file__), "db_crashtest.py")
_DB_STRESS_TRACE_PARSER_PATH = os.path.join(
    os.path.dirname(__file__), "db_stress_trace_parser.py"
)
_FAULT_INJECTION_LOG_PARSER_PATH = os.path.join(
    os.path.dirname(__file__), "fault_injection_log_parser.py"
)
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


def load_db_stress_trace_parser_module():
    spec = importlib.util.spec_from_file_location(
        "db_stress_trace_parser_under_test", _DB_STRESS_TRACE_PARSER_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
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

    def load_db_stress_trace_parser(self):
        return load_db_stress_trace_parser_module()

    def load_fault_injection_log_parser(self):
        return load_fault_injection_log_parser_module()

    def build_params(self, base_params, overrides=None):
        params = dict(base_params)
        params["db"] = self.test_tmpdir
        if overrides:
            params.update(overrides)
        return params

    def write_sample_public_iterator_trace(self, raw_trace):
        trace_parser = self.load_db_stress_trace_parser()

        def key_sample(key):
            head = key[:32]
            tail = key[len(head) :][-16:] if len(key) > len(head) else b""
            return struct.pack(
                "<HBB32s16s",
                len(key),
                len(head),
                len(tail),
                head.ljust(32, b"\0"),
                tail.ljust(16, b"\0"),
            )

        header = struct.pack(
            "<8sQQQQQIIIIIIII",
            trace_parser.TRACE_FILE_MAGIC,
            32 << 20,
            0,
            2,
            1,
            123456,
            trace_parser.TRACE_FILE_VERSION,
            80,
            32,
            256,
            32,
            4096,
            1,
            0,
        )
        slot_header = struct.pack("<QQIIII", 99, 1, 0, 1, 0, 0)
        entry = struct.pack(
            "<QQQQQIII8B52s52s92s",
            123456789,
            1,
            7,
            0,
            0,
            99,
            3,
            0,
            0,
            2,
            0,
            0,
            0,
            1,
            0xFF,
            0,
            key_sample(b"k1"),
            key_sample(b"k2"),
            b"\0" * 92,
        )
        with open(raw_trace, "wb") as f:
            f.write(header)
            f.write(slot_header)
            f.write(entry)

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

    def test_print_and_cleanup_public_iterator_trace_decodes_raw_trace(self):
        db_crashtest = self.load_db_crashtest()
        pid = 4242
        raw_trace = os.path.join(
            self.test_tmpdir, f"db_stress_public_iterator_trace_{pid}_1.bin"
        )
        self.write_sample_public_iterator_trace(raw_trace)

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            archived = db_crashtest.print_and_cleanup_public_iterator_trace(
                pid, "blackbox_run_0001_exit-0"
            )

        self.assertEqual(1, len(archived))
        archived_raw, decoded_trace = archived[0]
        self.assertTrue(os.path.exists(archived_raw))
        self.assertTrue(os.path.exists(decoded_trace))
        self.assertIn("blackbox_run_0001_exit-0", os.path.basename(decoded_trace))
        with open(decoded_trace) as f:
            decoded_text = f.read()

        self.assertIn("op=Seek", decoded_text)
        self.assertIn("target=len=2:6b31", decoded_text)
        self.assertIn("result=len=2:6b32", decoded_text)
        self.assertIn(archived_raw, stdout.getvalue())
        self.assertIn(decoded_trace, stdout.getvalue())

    def test_print_and_cleanup_public_iterator_trace_keeps_last_five_runs(self):
        db_crashtest = self.load_db_crashtest()
        archived = []
        for run_index in range(6):
            raw_trace = os.path.join(
                self.test_tmpdir,
                "db_stress_public_iterator_trace_%d_%d.bin"
                % (5000 + run_index, run_index + 1),
            )
            self.write_sample_public_iterator_trace(raw_trace)
            archived.extend(
                db_crashtest.print_and_cleanup_public_iterator_trace(
                    5000 + run_index,
                    "blackbox_run_%04d_exit-0" % run_index,
                    keep_last_runs=5,
                )
            )

        artifact_dir = os.path.join(
            self.test_tmpdir, db_crashtest._TRACE_ARTIFACT_DIRNAME
        )
        raw_logs = sorted(
            os.path.basename(path)
            for path in os.listdir(artifact_dir)
            if path.endswith(".bin")
        )
        decoded_logs = sorted(
            os.path.basename(path)
            for path in os.listdir(artifact_dir)
            if path.endswith(".log")
        )

        self.assertEqual(5, len(raw_logs))
        self.assertEqual(5, len(decoded_logs))
        self.assertFalse(
            any("blackbox_run_0000_exit-0" in path for path in raw_logs + decoded_logs)
        )
        self.assertTrue(
            any("blackbox_run_0005_exit-0" in path for path in raw_logs + decoded_logs)
        )

    def test_print_and_cleanup_fault_injection_log_decodes_raw_trace(self):
        db_crashtest = self.load_db_crashtest()
        fault_parser = self.load_fault_injection_log_parser()
        pid = 5151
        raw_log = os.path.join(self.test_tmpdir, f"fault_injection_{pid}_1.bin")
        decoded_log = raw_log + ".txt"

        header = struct.pack(
            "<8sQIIIIII",
            fault_parser.TRACE_FILE_MAGIC,
            2,
            fault_parser.TRACE_FILE_VERSION,
            40,
            fault_parser.ENTRY_V2_STRUCT.size,
            1000,
            2,
            0,
        )
        entry0 = fault_parser.ENTRY_V2_STRUCT.pack(
            123456789,
            17,
            7,
            4,
            0,
            0,
            fault_parser.DETAIL_KIND_OFFSET_SIZE_AND_HEAD,
            4,
            1,
            0,
            b"Append\0".ljust(32, b"\0"),
            b"/tmp/000001.log\0".ljust(72, b"\0"),
            b"injected write error\0".ljust(56, b"\0"),
            b"abcd".ljust(48, b"\0"),
        )
        entry1 = fault_parser.ENTRY_V2_STRUCT.pack(
            123456790,
            23,
            0,
            6,
            0,
            0,
            fault_parser.DETAIL_KIND_TWO_FILES,
            6,
            0,
            1,
            b"Rename\0".ljust(32, b"\0"),
            b"/tmp/a\0".ljust(72, b"\0"),
            b"injected metadata read error\0".ljust(56, b"\0"),
            b"/tmp/b".ljust(48, b"\0"),
        )
        with open(raw_log, "wb") as f:
            f.write(header)
            f.write(entry0)
            f.write(entry1)

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            db_crashtest.print_and_cleanup_fault_injection_log(pid)

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
        self.assertIn(decoded_log, stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
