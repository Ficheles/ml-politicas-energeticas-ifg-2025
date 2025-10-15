#!/usr/bin/env python3
"""Upload files listed in /opt/drive/files.txt to S3.

Features:
- Parses the tree in files.txt to determine relative file paths under /opt/drive
- Walks /opt/drive to find matching files
- Uploads each file to S3 using boto3
- Logs start/end timestamps and elapsed time per file
- Computes file size (bytes) and number of lines (for text files)
- Optionally uploads a metrics JSON next to the file in S3

Usage:
    python3 upload_files_to_s3.py --bucket my-bucket --prefix my/prefix --dry-run

Environment / IAM:
- AWS credentials should be available via environment variables, shared config, or role
- The IAM principal must have s3:PutObject on the target bucket/prefix

"""
from __future__ import annotations

import argparse
import boto3
import logging
import os
import re
import json
import time
from pathlib import Path
from typing import List, Tuple

DRIVE_ROOT = Path("/opt/drive")
FILES_TXT = DRIVE_ROOT / "files.txt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("uploader")

FILELINE_RE = re.compile(r"\s+└──\s+(.+)$")
FILE_RE = re.compile(r"\s+├──\s+(.+)$")


def parse_files_txt(files_txt: Path) -> List[str]:
    """Parse the tree-like files.txt and return a list of relative file paths.

    The file contains lines with box-drawing characters. We extract the tail filenames
    and build relative paths based on indentation levels.
    """
    # Return a list of file basenames extracted from the tree listing.
    filenames: List[str] = []
    if not files_txt.exists():
        logger.error("files.txt not found at %s", files_txt)
        return rel_paths

    with files_txt.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if not line.strip() or line.startswith("``"):
                continue

            # Extract the tail name after the last '──' marker when present
            if '──' in line:
                idx = line.rfind('──')
                name = line[idx + 2 :].strip()
            else:
                name = line.strip()

            name = name.replace('\u00A0', ' ').strip()

            # If it looks like a file (has an extension), collect basename only
            if os.path.splitext(name)[1] != "":
                filenames.append(name)

    return filenames


def discover_files(file_names: List[str]) -> List[Path]:
    """Locate files under DRIVE_ROOT that match the filenames extracted from files.txt.

    For each filename, search recursively under DRIVE_ROOT. If multiple matches exist we'll include all.
    """
    found: List[Path] = []
    for name in file_names:
        matches = list(DRIVE_ROOT.rglob(name))
        if not matches:
            logger.warning("Listed file not found under %s: %s", DRIVE_ROOT, name)
            continue
        if len(matches) > 1:
            logger.warning("Multiple matches found for %s; adding all matches", name)
        found.extend(matches)
    return found


def file_metrics(path: Path) -> Tuple[int, int]:
    """Return (size_bytes, n_lines). For binary files, n_lines may be 0.

    We attempt to read as text with utf-8 and fall back to latin-1; if that fails treat as binary.
    """
    size = path.stat().st_size
    n_lines = 0
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for _ in f:
                n_lines += 1
    except Exception:
        n_lines = 0
    return size, n_lines


def upload_file(s3_client, bucket: str, key: str, path: Path, dry_run: bool = False) -> None:
    logger.info("Uploading %s -> s3://%s/%s", path, bucket, key)
    start = time.time()
    if dry_run:
        logger.info("Dry run: skipping upload for %s", path)
        return
    s3_client.upload_file(str(path), bucket, key)
    elapsed = time.time() - start
    logger.info("Uploaded %s in %.3f s", path, elapsed)


def upload_metrics(s3_client, bucket: str, key: str, metrics: dict, dry_run: bool = False) -> None:
    metrics_key = key + ".metrics.json"
    logger.info("Uploading metrics to s3://%s/%s", bucket, metrics_key)
    if dry_run:
        logger.info("Dry run: skipping metrics upload for %s", metrics_key)
        return
    s3_client.put_object(Bucket=bucket, Key=metrics_key, Body=json.dumps(metrics).encode('utf-8'))


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload files listed in /opt/drive/files.txt to S3")
    parser.add_argument("--bucket", default="ml-politicas-energeticas", help="Target S3 bucket (default ml-politicas-energeticas)")
    parser.add_argument("--prefix", default="drive", help="Optional S3 prefix (no leading slash)")
    parser.add_argument("--flatten", action="store_true", help="Upload files using only their basename (no directory structure). Matches DAG behavior which uses only the filename as S3 key suffix.")
    parser.add_argument("--dry-run", action="store_true", help="Do not perform uploads; just show what would be done")
    parser.add_argument("--upload-metrics", action="store_true", help="Upload a metrics JSON for each file next to it in S3")
    args = parser.parse_args()

    rel_paths = parse_files_txt(FILES_TXT)
    if not rel_paths:
        logger.error("No files parsed from %s", FILES_TXT)
        return

    files = discover_files(rel_paths)
    if not files:
        logger.error("No existing files found under %s", DRIVE_ROOT)
        return

    s3_client = boto3.client('s3')

    for path in files:
        rel = path.relative_to(DRIVE_ROOT)
        # build s3 key; optionally flatten to basename only (as the DAG does)
        if args.flatten:
            key_suffix = path.name
        else:
            key_suffix = str(rel.as_posix())

        # join prefix and suffix safely
        if args.prefix:
            s3_key = "/".join([args.prefix.strip('/'), key_suffix.lstrip('/')])
        else:
            s3_key = key_suffix.lstrip('/')

        size, n_lines = file_metrics(path)
        logger.info("File metrics: path=%s size=%d bytes lines=%d", path, size, n_lines)

        # upload and capture elapsed time
        start_upload = time.time()
        upload_file(s3_client, args.bucket, s3_key, path, dry_run=args.dry_run)
        upload_elapsed = time.time() - start_upload

        if args.upload_metrics:
            metrics = {
                "path": str(rel.as_posix()),
                "size_bytes": size,
                "n_lines": n_lines,
                "uploaded_at": time.time(),
                "upload_elapsed_seconds": upload_elapsed,
            }
            upload_metrics(s3_client, args.bucket, s3_key, metrics, dry_run=args.dry_run)

    logger.info("All done.")


if __name__ == "__main__":
    main()
