#!/usr/bin/env python3
"""
Pre-download packages with unreliable mirrors using fallback mirrors.
Reads package info from folly's getdeps manifest files.
"""
import sys
import os
import hashlib
import subprocess
import configparser

def sha256_file(path):
    """Calculate SHA256 hash of a file."""
    h = hashlib.sha256()
    try:
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def parse_manifest(manifest_path):
    """Parse a getdeps manifest file to extract download info."""
    config = configparser.ConfigParser()
    try:
        config.read(manifest_path)
        if 'download' in config:
            return {
                'url': config['download'].get('url', ''),
                'sha256': config['download'].get('sha256', ''),
            }
    except Exception:
        pass
    return None

def get_fallback_mirrors(url):
    """Get fallback mirror URLs for a given URL."""
    # Fallback mirror patterns for known unreliable hosts
    mirror_fallbacks = {
        "ftp.gnu.org/gnu/": [
            "https://mirrors.kernel.org/gnu/",
            "https://ftpmirror.gnu.org/gnu/",
            "https://ftp.gnu.org/gnu/",
        ],
        "ftpmirror.gnu.org/gnu/": [
            "https://mirrors.kernel.org/gnu/",
            "https://ftpmirror.gnu.org/gnu/",
            "https://ftp.gnu.org/gnu/",
        ],
    }

    for pattern, mirrors in mirror_fallbacks.items():
        if pattern in url:
            # Extract the path after the pattern
            path_start = url.find(pattern) + len(pattern)
            path = url[path_start:]
            return [mirror + path for mirror in mirrors]
    return [url]  # No fallback, use original

def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <download_dir> <cache_dir> <manifests_dir>")
        sys.exit(1)

    download_dir, cache_dir, manifests_dir = sys.argv[1], sys.argv[2], sys.argv[3]

    # Packages known to have unreliable mirrors
    packages_to_check = ["autoconf", "automake", "libtool"]

    for package in packages_to_check:
        manifest_path = os.path.join(manifests_dir, package)
        if not os.path.exists(manifest_path):
            continue

        info = parse_manifest(manifest_path)
        if not info or not info['url'] or not info['sha256']:
            continue

        # Determine filename from URL
        url = info['url']
        expected_sha256 = info['sha256']
        url_filename = os.path.basename(url)

        # getdeps uses format: {package}-{filename}
        filename = f"{package}-{url_filename}"
        filepath = os.path.join(download_dir, filename)
        cache_path = os.path.join(cache_dir, filename)

        # Check if already valid
        if os.path.exists(filepath) and sha256_file(filepath) == expected_sha256:
            print(f"  {filename}: OK (already downloaded)")
            continue

        # Check cache
        if os.path.exists(cache_path) and sha256_file(cache_path) == expected_sha256:
            print(f"  {filename}: OK (from cache)")
            subprocess.run(['cp', cache_path, filepath], check=True)
            continue

        # Try fallback mirrors
        mirrors = get_fallback_mirrors(url)
        downloaded = False
        for mirror_url in mirrors:
            print(f"  {filename}: trying {mirror_url}...")
            try:
                subprocess.run(['wget', '-q', '-O', filepath, mirror_url], check=True, timeout=120)
                if sha256_file(filepath) == expected_sha256:
                    print(f"  {filename}: OK (downloaded)")
                    subprocess.run(['cp', filepath, cache_path], check=False)
                    downloaded = True
                    break
                else:
                    os.remove(filepath)
            except Exception:
                if os.path.exists(filepath):
                    os.remove(filepath)

        if not downloaded:
            print(f"  {filename}: WARNING - all mirrors failed")

if __name__ == "__main__":
    main()
