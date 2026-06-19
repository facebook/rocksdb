#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).
"""
Pre-download packages with unreliable mirrors using fallback mirrors.
Reads package info from folly's getdeps manifest files.
"""
import configparser
import hashlib
import os
import shutil
import sys
import urllib.request

DOWNLOAD_TIMEOUT_SECONDS = 120
DOWNLOAD_CHUNK_BYTES = 64 * 1024
MAX_DOWNLOAD_BYTES = 50 * 1024 * 1024

MIRROR_FALLBACKS = {
    "ftpmirror.gnu.org/gnu/": [
        "https://mirrors.kernel.org/gnu/",
        "https://ftpmirror.gnu.org/gnu/",
        "https://ftp.gnu.org/gnu/",
    ],
    "ftp.gnu.org/gnu/": [
        "https://mirrors.kernel.org/gnu/",
        "https://ftpmirror.gnu.org/gnu/",
        "https://ftp.gnu.org/gnu/",
    ],
}

# These packages must have URLs matching MIRROR_FALLBACKS; other packages are
# left for getdeps.py's normal download path.
PACKAGES_TO_CHECK = ("autoconf", "automake", "libtool", "libiberty")


def sha256_file(path):
    """Calculate SHA256 hash of a file."""
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def parse_manifest(manifest_path):
    """Parse a getdeps manifest file to extract download info."""
    # folly manifests can contain bare keys in sections unrelated to downloads.
    config = configparser.ConfigParser(allow_no_value=True, interpolation=None)
    try:
        with open(manifest_path, encoding="utf-8") as manifest_file:
            config.read_file(manifest_file)
    except Exception as ex:
        print(f"  {os.path.basename(manifest_path)}: WARNING - parse failed: {ex}")
        return None

    if "download" in config:
        return {
            "url": config["download"].get("url", ""),
            "sha256": config["download"].get("sha256", ""),
        }
    return None


def file_size(path):
    try:
        return os.path.getsize(path)
    except Exception:
        return None


def get_fallback_mirrors(url):
    """Get fallback mirror URLs for a given URL."""
    for pattern, mirrors in MIRROR_FALLBACKS.items():
        if pattern in url:
            # Extract the path after the pattern
            path_start = url.find(pattern) + len(pattern)
            path = url[path_start:]
            return [mirror + path for mirror in mirrors]
    return []


def download_url(url, filepath):
    """Download URL to filepath without leaving partial files behind."""
    tmp_filepath = filepath + ".tmp"
    if os.path.exists(tmp_filepath):
        os.remove(tmp_filepath)

    request = urllib.request.Request(
        url, headers={"User-Agent": "rocksdb-getdeps-fallback/1.0"}
    )
    try:
        with urllib.request.urlopen(
            request, timeout=DOWNLOAD_TIMEOUT_SECONDS
        ) as response, open(tmp_filepath, "wb") as output:
            copied = 0
            while True:
                chunk = response.read(DOWNLOAD_CHUNK_BYTES)
                if not chunk:
                    break

                copied += len(chunk)
                if copied > MAX_DOWNLOAD_BYTES:
                    raise Exception(
                        f"download exceeds {MAX_DOWNLOAD_BYTES} bytes"
                    )
                output.write(chunk)
        os.replace(tmp_filepath, filepath)
    finally:
        if os.path.exists(tmp_filepath):
            os.remove(tmp_filepath)


def prepare_download(package, info, download_dir, cache_dir):
    url = info["url"]
    expected_sha256 = info["sha256"]
    mirrors = get_fallback_mirrors(url)
    if not mirrors:
        return False

    if not expected_sha256:
        print(f"  {package}: WARNING - skipped fallback without sha256")
        return False

    # getdeps uses format: {package}-{filename}
    filename = f"{package}-{os.path.basename(url)}"
    filepath = os.path.join(download_dir, filename)
    cache_path = os.path.join(cache_dir, filename)

    # Check if already valid.
    actual_sha256 = sha256_file(filepath) if os.path.exists(filepath) else None
    if actual_sha256 == expected_sha256:
        print(f"  {filename}: OK (already downloaded)")
        return True
    if actual_sha256 is not None:
        print(
            f"  {filename}: WARNING - removing invalid download "
            f"sha256={actual_sha256}"
        )
        os.remove(filepath)

    # The cache is only an opportunistic single-build accelerator; callers
    # should not share it across concurrent builds without external locking.
    actual_sha256 = sha256_file(cache_path) if os.path.exists(cache_path) else None
    if actual_sha256 == expected_sha256:
        print(f"  {filename}: OK (from cache)")
        shutil.copy2(cache_path, filepath)
        return True
    if actual_sha256 is not None:
        print(
            f"  {filename}: WARNING - removing invalid cache "
            f"sha256={actual_sha256}"
        )
        os.remove(cache_path)

    # Try fallback mirrors.
    for mirror_url in mirrors:
        print(f"  {filename}: trying {mirror_url}...")
        try:
            download_url(mirror_url, filepath)
        except Exception as ex:
            print(f"  {filename}: WARNING - download failed: {ex}")
            continue

        actual_sha256 = sha256_file(filepath)
        if actual_sha256 == expected_sha256:
            size = file_size(filepath)
            print(f"  {filename}: OK (downloaded, {size} bytes)")
            shutil.copy2(filepath, cache_path)
            return True

        size = file_size(filepath)
        print(
            f"  {filename}: WARNING - sha256 mismatch from {mirror_url}: "
            f"expected={expected_sha256} actual={actual_sha256} size={size}"
        )
        os.remove(filepath)

    print(f"  {filename}: WARNING - all mirrors failed")
    return False


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <download_dir> <cache_dir> <manifests_dir>")
        sys.exit(1)

    download_dir, cache_dir, manifests_dir = sys.argv[1], sys.argv[2], sys.argv[3]
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(cache_dir, exist_ok=True)

    checked = 0
    ready = 0
    for package in PACKAGES_TO_CHECK:
        manifest_path = os.path.join(manifests_dir, package)
        if not os.path.isfile(manifest_path):
            continue

        info = parse_manifest(manifest_path)
        if not info or not info["url"]:
            continue

        if not info["sha256"]:
            print(f"  {package}: WARNING - skipped fallback without sha256")
            continue

        if not get_fallback_mirrors(info["url"]):
            print(
                f"  {package}: WARNING - skipped fallback without known mirror "
                f"for {info['url']}"
            )
            continue

        checked += 1
        try:
            if prepare_download(package, info, download_dir, cache_dir):
                ready += 1
        except Exception as ex:
            print(f"  {package}: WARNING - fallback preparation failed: {ex}")

    print(f"  fallback mirror downloads ready: {ready}/{checked}")

if __name__ == "__main__":
    main()
