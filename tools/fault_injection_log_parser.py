#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the COPYING file
# in the root directory) and the Apache 2.0 License (found in the
# LICENSE.Apache file in the root directory).

import argparse
import struct


TRACE_FILE_MAGIC = b"FINJLOG1"
LEGACY_TRACE_FILE_VERSION = 1
TRACE_FILE_VERSION = 2

DETAIL_KIND_NONE = 0
DETAIL_KIND_TWO_FILES = 1
DETAIL_KIND_SIZE_AND_HEAD = 2
DETAIL_KIND_OFFSET_SIZE_AND_HEAD = 3
DETAIL_KIND_OFFSET_AND_SIZE = 4
DETAIL_KIND_SIZE = 5
DETAIL_KIND_COUNT = 6
DETAIL_KIND_REQ_OFFSET_AND_SIZE = 7

HEADER_STRUCT = struct.Struct("<8sQIIIIII")
LEGACY_ENTRY_STRUCT = struct.Struct("<QQ256s")
ENTRY_V2_STRUCT = struct.Struct("<QQQQIIBBBB32s72s56s48s4x")


def _read_exact(infile, size):
    data = infile.read(size)
    if len(data) != size:
        raise ValueError("truncated injected error log")
    return data


def _decode_c_string(raw):
    return raw.split(b"\0", 1)[0].decode("utf-8", "replace")


def _format_hex_head(payload, total_size):
    head = " ".join(f"{byte:02x}" for byte in payload)
    if total_size > len(payload):
        return f"{head} ..." if head else "..."
    return head


def _format_v2_detail(detail_kind, offset, size, count, req_idx, payload):
    if detail_kind == DETAIL_KIND_NONE:
        return ""
    if detail_kind == DETAIL_KIND_TWO_FILES:
        suffix = "..." if size > len(payload) else ""
        return f"\"{payload.decode('utf-8', 'replace')}{suffix}\""
    if detail_kind == DETAIL_KIND_SIZE_AND_HEAD:
        return f"size={size}, head=[{_format_hex_head(payload, size)}]"
    if detail_kind == DETAIL_KIND_OFFSET_SIZE_AND_HEAD:
        return (
            f"offset={offset}, size={size}, "
            f"head=[{_format_hex_head(payload, size)}]"
        )
    if detail_kind == DETAIL_KIND_OFFSET_AND_SIZE:
        return f"offset={offset}, size={size}"
    if detail_kind == DETAIL_KIND_SIZE:
        return f"size={size}"
    if detail_kind == DETAIL_KIND_COUNT:
        return f"num_reqs={count}"
    if detail_kind == DETAIL_KIND_REQ_OFFSET_AND_SIZE:
        return f"req[{req_idx}], offset={offset}, size={size}"
    return f"detail_kind={detail_kind}"


def _decode_legacy_entries(infile, outfile, dumped_entries):
    printed = 0
    for _ in range(dumped_entries):
        timestamp_us, thread_id, context = LEGACY_ENTRY_STRUCT.unpack(
            _read_exact(infile, LEGACY_ENTRY_STRUCT.size)
        )
        if timestamp_us == 0:
            continue
        secs = timestamp_us // 1000000
        usecs = timestamp_us % 1000000
        context_str = _decode_c_string(context)
        outfile.write(f"[{secs}.{usecs:06d}] thread={thread_id}: {context_str}\n")
        printed += 1
    return printed


def _decode_v2_entries(infile, outfile, dumped_entries):
    printed = 0
    for _ in range(dumped_entries):
        (
            timestamp_us,
            thread_id,
            offset,
            size,
            count,
            req_idx,
            detail_kind,
            payload_size,
            retryable,
            data_loss,
            op_name,
            file_name,
            status_message,
            detail_payload,
        ) = ENTRY_V2_STRUCT.unpack(_read_exact(infile, ENTRY_V2_STRUCT.size))
        if timestamp_us == 0:
            continue
        if payload_size > len(detail_payload):
            raise ValueError(f"invalid payload size in trace entry: {payload_size}")
        secs = timestamp_us // 1000000
        usecs = timestamp_us % 1000000
        op_name = _decode_c_string(op_name)
        file_name = _decode_c_string(file_name)
        status_message = _decode_c_string(status_message)
        payload = detail_payload[:payload_size]
        detail = _format_v2_detail(detail_kind, offset, size, count, req_idx, payload)
        line = f"[{secs}.{usecs:06d}] thread={thread_id}: {op_name}(\"{file_name}\""
        if detail:
            line += f", {detail}"
        line += f") -> IO error: {status_message}"
        flags = []
        if retryable:
            flags.append("retryable")
        if data_loss:
            flags.append("data_loss")
        if flags:
            line += " [" + ",".join(flags) + "]"
        outfile.write(line + "\n")
        printed += 1
    return printed


def decode_fault_injection_log(raw_path, output_path=None):
    if output_path is None:
        output_path = raw_path + ".txt"

    with open(raw_path, "rb") as infile, open(output_path, "w") as outfile:
        (
            magic,
            total_entries,
            version,
            header_size,
            entry_size,
            max_entries,
            dumped_entries,
            reserved,
        ) = HEADER_STRUCT.unpack(_read_exact(infile, HEADER_STRUCT.size))

        if magic != TRACE_FILE_MAGIC:
            raise ValueError(f"unexpected trace magic: {magic!r}")
        if version not in (LEGACY_TRACE_FILE_VERSION, TRACE_FILE_VERSION):
            raise ValueError(f"unsupported trace version: {version}")
        if header_size != HEADER_STRUCT.size:
            raise ValueError(
                f"unexpected trace header size: {header_size} != {HEADER_STRUCT.size}"
            )

        outfile.write(
            "=== Recently Injected Fault Injection Errors (most recent last) ===\n"
        )

        if version == LEGACY_TRACE_FILE_VERSION:
            if entry_size != LEGACY_ENTRY_STRUCT.size:
                raise ValueError(
                    "unexpected legacy trace entry size: "
                    f"{entry_size} != {LEGACY_ENTRY_STRUCT.size}"
                )
            if reserved != 256:
                raise ValueError(f"unexpected legacy max message len: {reserved}")
            printed = _decode_legacy_entries(infile, outfile, dumped_entries)
        else:
            if entry_size != ENTRY_V2_STRUCT.size:
                raise ValueError(
                    f"unexpected trace entry size: {entry_size} != {ENTRY_V2_STRUCT.size}"
                )
            printed = _decode_v2_entries(infile, outfile, dumped_entries)

        if printed == 0:
            outfile.write("(none)\n")

        outfile.write(
            "=== End of injected error log (%d entries, total=%d, max=%d) ===\n"
            % (printed, total_entries, max_entries)
        )

    return output_path


def _main():
    parser = argparse.ArgumentParser(
        description="Decode raw fault injection logs emitted by db_stress."
    )
    parser.add_argument("raw_log", help="Path to the raw .bin fault injection log")
    parser.add_argument(
        "--output",
        help="Path for decoded text output. Defaults to <raw_log>.txt",
    )
    args = parser.parse_args()
    output_path = decode_fault_injection_log(args.raw_log, args.output)
    print(output_path)


if __name__ == "__main__":
    _main()
