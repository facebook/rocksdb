#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the COPYING file
# in the root directory) and the Apache 2.0 License (found in the
# LICENSE.Apache file in the root directory).

import argparse
import struct


TRACE_FILE_MAGIC = b"DBSPITR1"
TRACE_FILE_VERSION = 1

_FILE_HEADER_STRUCT = struct.Struct("<8sQQQQQIIIIIIII")
_SLOT_HEADER_STRUCT = struct.Struct("<QQIIII")
_KEY_SAMPLE_STRUCT = struct.Struct("<HBB32s16s")
_ENTRY_STRUCT = struct.Struct("<QQQQQIII8B52s52s92s")
_ENTRY_SEQUENCE_OFFSET = 8
_ENTRY_EVENT_TYPE_OFFSET = 53


_EVENT_TYPE_NAMES = {
    1: "NewIterator",
    2: "Seek",
    3: "SeekForPrev",
    4: "SeekToFirst",
    5: "SeekToLast",
    6: "Next",
    7: "Prev",
    8: "PrepareValue",
    9: "Refresh",
}

_STATUS_CODE_NAMES = {
    0: "OK",
    1: "NotFound",
    2: "Corruption",
    3: "NotSupported",
    4: "InvalidArgument",
    5: "IOError",
    6: "MergeInProgress",
    7: "Incomplete",
    8: "ShutdownInProgress",
    9: "TimedOut",
    10: "Aborted",
    11: "Busy",
    12: "Expired",
    13: "TryAgain",
    14: "CompactionTooLarge",
    15: "ColumnFamilyDropped",
}

_STATUS_SUBCODE_NAMES = {
    0: "-",
    1: "MutexTimeout",
    2: "LockTimeout",
    3: "LockLimit",
    4: "NoSpace",
    5: "Deadlock",
    6: "StaleFile",
    7: "MemoryLimit",
    8: "SpaceLimit",
    9: "PathNotFound",
    10: "MergeOperandsInsufficientCapacity",
    11: "ManualCompactionPaused",
    12: "Overwritten",
    13: "TxnNotPrepared",
    14: "IOFenced",
    15: "MergeOperatorFailed",
    16: "MergeOperandThresholdExceeded",
    17: "PrefetchLimitReached",
    18: "NotExpectedCodePath",
    19: "CompactionAborted",
}

_FLAG_NAMES = (
    (1 << 0, "snapshot"),
    (1 << 1, "lower"),
    (1 << 2, "upper"),
    (1 << 3, "allow_unprepared"),
    (1 << 4, "total_order_seek"),
    (1 << 5, "prefix_same_as_start"),
    (1 << 6, "tailing"),
    (1 << 7, "pin_data"),
    (1 << 8, "auto_refresh"),
)


def _read_exact(infile, size):
    data = infile.read(size)
    if len(data) != size:
        raise ValueError("truncated trace file")
    return data


def _format_key_sample(blob):
    full_len, head_len, tail_len, head, tail = _KEY_SAMPLE_STRUCT.unpack(blob)
    if full_len == 0:
        return "-"
    out = f"len={full_len}:{head[:head_len].hex()}"
    if tail_len > 0 and full_len > head_len:
        out += f"..{tail[:tail_len].hex()}"
    return out


def _format_flags(flags):
    names = [name for bit, name in _FLAG_NAMES if (flags & bit) != 0]
    return "|".join(names) if names else "-"


def _event_type_name(event_type):
    return _EVENT_TYPE_NAMES.get(event_type, f"Unknown({event_type})")


def _status_code_name(status_code):
    return _STATUS_CODE_NAMES.get(status_code, f"Unknown({status_code})")


def _status_subcode_name(status_subcode):
    return _STATUS_SUBCODE_NAMES.get(
        status_subcode, f"Unknown({status_subcode})"
    )


def _format_entry(entry_blob):
    (
        timestamp_us,
        sequence,
        object_id,
        aux0,
        _aux1,
        os_thread_id_hash,
        cf_id,
        flags,
        slot,
        event_type,
        status_code,
        status_subcode,
        valid_before,
        valid_after,
        result_bool,
        _reserved0,
        key0_blob,
        key1_blob,
        _reserved,
    ) = _ENTRY_STRUCT.unpack(entry_blob)

    secs = timestamp_us // 1000000
    usecs = timestamp_us % 1000000
    event_name = _event_type_name(event_type)
    code_name = _status_code_name(status_code)
    subcode_name = _status_subcode_name(status_subcode)
    key0 = _format_key_sample(key0_blob)
    key1 = _format_key_sample(key1_blob)

    if event_type == 1:
        flags_str = _format_flags(flags)
        return (
            f"[{secs}.{usecs:06d}] seq={sequence} slot={slot} "
            f"thread={os_thread_id_hash} iter={object_id} cf={cf_id} "
            f"op={event_name} status={code_name}/{subcode_name} "
            f"flags={flags_str} snapshot=0x{aux0:x} lower={key0} upper={key1}\n"
        )
    if event_type in (2, 3):
        return (
            f"[{secs}.{usecs:06d}] seq={sequence} slot={slot} "
            f"thread={os_thread_id_hash} iter={object_id} cf={cf_id} "
            f"op={event_name} valid={valid_before}->{valid_after} "
            f"status={code_name}/{subcode_name} target={key0} result={key1}\n"
        )
    if event_type == 8:
        ok = 0 if result_bool == 0xFF else result_bool
        return (
            f"[{secs}.{usecs:06d}] seq={sequence} slot={slot} "
            f"thread={os_thread_id_hash} iter={object_id} cf={cf_id} "
            f"op={event_name} valid={valid_before}->{valid_after} "
            f"status={code_name}/{subcode_name} ok={ok} key={key0} "
            f"result={key1}\n"
        )
    if event_type == 9:
        return (
            f"[{secs}.{usecs:06d}] seq={sequence} slot={slot} "
            f"thread={os_thread_id_hash} iter={object_id} cf={cf_id} "
            f"op={event_name} valid={valid_before}->{valid_after} "
            f"status={code_name}/{subcode_name} snapshot=0x{aux0:x} "
            f"before={key0} result={key1}\n"
        )
    return (
        f"[{secs}.{usecs:06d}] seq={sequence} slot={slot} "
        f"thread={os_thread_id_hash} iter={object_id} cf={cf_id} "
        f"op={event_name} valid={valid_before}->{valid_after} "
        f"status={code_name}/{subcode_name} before={key0} result={key1}\n"
    )


def decode_public_iterator_trace(raw_path, output_path=None):
    if output_path is None:
        output_path = raw_path + ".log"

    with open(raw_path, "rb") as infile, open(output_path, "w") as outfile:
        (
            magic,
            trace_budget_bytes,
            dropped_threads,
            next_sequence,
            next_iterator_id,
            dump_timestamp_us,
            version,
            header_size,
            slot_header_size,
            entry_size,
            max_threads,
            entries_per_thread,
            used_slots,
            _reserved0,
        ) = _FILE_HEADER_STRUCT.unpack(_read_exact(infile, _FILE_HEADER_STRUCT.size))

        if magic != TRACE_FILE_MAGIC:
            raise ValueError(f"unexpected trace magic: {magic!r}")
        if version != TRACE_FILE_VERSION:
            raise ValueError(f"unsupported trace version: {version}")
        if header_size != _FILE_HEADER_STRUCT.size:
            raise ValueError(
                f"unexpected trace header size: {header_size} != {_FILE_HEADER_STRUCT.size}"
            )
        if slot_header_size != _SLOT_HEADER_STRUCT.size:
            raise ValueError(
                f"unexpected slot header size: {slot_header_size} != {_SLOT_HEADER_STRUCT.size}"
            )
        if entry_size != _ENTRY_STRUCT.size:
            raise ValueError(
                f"unexpected entry size: {entry_size} != {_ENTRY_STRUCT.size}"
            )

        outfile.write(
            "=== db_stress public iterator trace (decoded from raw dump) ===\n"
        )
        outfile.write(
            "budget=%dB threads=%d entries_per_thread=%d entry_size=%dB "
            "dropped_threads=%d used_slots=%d dump_timestamp_us=%d "
            "next_sequence=%d next_iterator_id=%d\n"
            % (
                trace_budget_bytes,
                max_threads,
                entries_per_thread,
                entry_size,
                dropped_threads,
                used_slots,
                dump_timestamp_us,
                next_sequence,
                next_iterator_id,
            )
        )

        for _ in range(used_slots):
            (
                thread_id_hash,
                total_entries,
                slot,
                entry_count,
                _reserved1,
                _reserved2,
            ) = _SLOT_HEADER_STRUCT.unpack(
                _read_exact(infile, _SLOT_HEADER_STRUCT.size)
            )
            outfile.write(
                "-- slot=%d thread=%d entries=%d total=%d --\n"
                % (slot, thread_id_hash, entry_count, total_entries)
            )
            for _ in range(entry_count):
                entry_blob = _read_exact(infile, _ENTRY_STRUCT.size)
                sequence = struct.unpack_from(
                    "<Q", entry_blob, _ENTRY_SEQUENCE_OFFSET
                )[0]
                event_type = entry_blob[_ENTRY_EVENT_TYPE_OFFSET]
                if sequence == 0 or event_type == 0:
                    continue
                outfile.write(_format_entry(entry_blob))

        outfile.write("=== End db_stress public iterator trace ===\n")

    return output_path


def _main():
    parser = argparse.ArgumentParser(
        description="Decode db_stress public iterator raw trace files."
    )
    parser.add_argument("raw_trace", help="Path to the raw .bin trace file")
    parser.add_argument(
        "--output",
        help="Path for decoded text output. Defaults to <raw_trace>.log",
    )
    args = parser.parse_args()
    output_path = decode_public_iterator_trace(args.raw_trace, args.output)
    print(output_path)


if __name__ == "__main__":
    _main()
