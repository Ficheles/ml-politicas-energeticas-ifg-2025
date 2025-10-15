Upload files listed in `/opt/drive/files.txt` to S3

This script walks the structure described in `/opt/drive/files.txt`, finds matching files under `/opt/drive`, computes simple metrics (size in bytes and number of lines), uploads the files to the provided S3 bucket/prefix and optionally uploads a metrics JSON next to each file.

Usage

    python3 upload_files_to_s3.py --bucket my-bucket --prefix data/drive --upload-metrics
    
    -- Execute to validate path
    python3 /opt/scripts/upload_files_to_s3.py --dry-run --prefix drive

    -- execute to upload files
    python3 /opt/scripts/upload_files_to_s3.py --prefix drive

Options
- --bucket: (required) target S3 bucket
- --prefix: optional key prefix
- --dry-run: do not perform uploads, only report
- --upload-metrics: upload a small JSON file with metrics next to the object

Permissions
- The executing principal must have s3:PutObject permission for the target bucket/prefix.

Notes
- The script expects `/opt/drive/files.txt` to be a tree-like listing (as present in this workspace). It will warn about entries not found under `/opt/drive`.
- CSV/text files are counted for lines using UTF-8 with replacement on decode errors.
