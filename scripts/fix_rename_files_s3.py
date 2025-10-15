#!/usr/bin/env python3
"""
Script to fix file names in S3 bucket by replacing spaces with underscores.
Specifically targets files in the INMET data bucket that may have spaces in their names.

Author: Data Engineering Team
Date: October 2025
"""

import boto3
import logging
from typing import List, Dict, Tuple
from botocore.exceptions import ClientError
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# S3 Configuration
S3_BUCKET = "ml-politicas-energeticas"
START_YEAR = 2000
END_YEAR = 2025
DRY_RUN = True  # Set to False to actually rename files

class S3FileRenamer:
    """Class to handle S3 file renaming operations."""
    
    def __init__(self, bucket_name: str, dry_run: bool = True):
        """
        Initialize the S3 renamer.
        
        Args:
            bucket_name: Name of the S3 bucket
            dry_run: If True, only simulate changes without actually renaming
        """
        self.bucket_name = bucket_name
        self.dry_run = dry_run
        self.s3_client = boto3.client('s3')
        self.renamed_count = 0
        self.error_count = 0
        self.skipped_count = 0
        
    def list_files_with_spaces(self, prefix: str) -> List[str]:
        """
        List all files in the bucket with the given prefix that contain spaces.
        
        Args:
            prefix: S3 prefix to search within
            
        Returns:
            List of S3 keys that contain spaces
        """
        files_with_spaces = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        try:
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            for page in page_iterator:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    key = obj['Key']
                    # Check if the filename (not the full path) contains spaces
                    filename = key.split('/')[-1]
                    if ' ' in filename:
                        files_with_spaces.append(key)
                        
        except ClientError as e:
            logger.error(f"Error listing files with prefix {prefix}: {e}")
            
        return files_with_spaces
    
    def generate_new_key(self, old_key: str) -> str:
        """
        Generate new key by replacing spaces with underscores in filename.
        
        Args:
            old_key: Original S3 key
            
        Returns:
            New S3 key with spaces replaced by underscores
        """
        parts = old_key.split('/')
        filename = parts[-1]
        new_filename = filename.replace(' ', '_')
        parts[-1] = new_filename
        return '/'.join(parts)
    
    def rename_file(self, old_key: str, new_key: str) -> bool:
        """
        Rename a file in S3 by copying to new key and deleting old key.
        
        Args:
            old_key: Original S3 key
            new_key: New S3 key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would rename: {old_key} -> {new_key}")
                return True
            
            # Copy object to new key
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': old_key
            }
            
            logger.info(f"Copying: {old_key} -> {new_key}")
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=new_key
            )
            
            # Delete old object
            logger.info(f"Deleting old file: {old_key}")
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=old_key
            )
            
            logger.info(f"✓ Successfully renamed: {old_key}")
            return True
            
        except ClientError as e:
            logger.error(f"✗ Error renaming {old_key}: {e}")
            return False
    
    def process_year(self, year: int) -> Dict[str, int]:
        """
        Process all files for a specific year.
        
        Args:
            year: Year to process
            
        Returns:
            Dictionary with statistics (renamed, errors, skipped)
        """
        prefix = f"inmet/{year}/"
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing year: {year}")
        logger.info(f"Prefix: {prefix}")
        logger.info(f"{'='*60}")
        
        # Get list of files with spaces
        files_with_spaces = self.list_files_with_spaces(prefix)
        
        if not files_with_spaces:
            logger.info(f"No files with spaces found for year {year}")
            return {'renamed': 0, 'errors': 0, 'skipped': 0}
        
        logger.info(f"Found {len(files_with_spaces)} files with spaces in names")
        
        year_renamed = 0
        year_errors = 0
        
        for old_key in files_with_spaces:
            new_key = self.generate_new_key(old_key)
            
            if old_key == new_key:
                logger.warning(f"Skipping {old_key} - no changes needed")
                self.skipped_count += 1
                continue
            
            if self.rename_file(old_key, new_key):
                year_renamed += 1
                self.renamed_count += 1
            else:
                year_errors += 1
                self.error_count += 1
        
        logger.info(f"Year {year} summary: {year_renamed} renamed, {year_errors} errors")
        
        return {
            'renamed': year_renamed,
            'errors': year_errors,
            'skipped': 0
        }
    
    def process_all_years(self, start_year: int, end_year: int):
        """
        Process all years in the given range.
        
        Args:
            start_year: Starting year (inclusive)
            end_year: Ending year (inclusive)
        """
        logger.info(f"\n{'#'*60}")
        logger.info(f"Starting S3 File Renaming Process")
        logger.info(f"Bucket: {self.bucket_name}")
        logger.info(f"Years: {start_year} to {end_year}")
        logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}")
        logger.info(f"{'#'*60}\n")
        
        start_time = datetime.now()
        
        for year in range(start_year, end_year + 1):
            self.process_year(year)
        
        end_time = datetime.now()
        elapsed = end_time - start_time
        
        # Print final summary
        logger.info(f"\n{'#'*60}")
        logger.info(f"FINAL SUMMARY")
        logger.info(f"{'#'*60}")
        logger.info(f"Total files renamed: {self.renamed_count}")
        logger.info(f"Total errors: {self.error_count}")
        logger.info(f"Total skipped: {self.skipped_count}")
        logger.info(f"Elapsed time: {elapsed}")
        logger.info(f"Mode: {'DRY RUN - No actual changes made' if self.dry_run else 'LIVE - Changes applied'}")
        logger.info(f"{'#'*60}\n")


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Rename S3 files by replacing spaces with underscores'
    )
    parser.add_argument(
        '--bucket',
        default=S3_BUCKET,
        help=f'S3 bucket name (default: {S3_BUCKET})'
    )
    parser.add_argument(
        '--start-year',
        type=int,
        default=START_YEAR,
        help=f'Start year (default: {START_YEAR})'
    )
    parser.add_argument(
        '--end-year',
        type=int,
        default=END_YEAR,
        help=f'End year (default: {END_YEAR})'
    )
    parser.add_argument(
        '--live',
        action='store_true',
        help='Actually perform the renaming (default is dry-run)'
    )
    parser.add_argument(
        '--year',
        type=int,
        help='Process only a specific year'
    )
    
    args = parser.parse_args()
    
    # Create renamer instance
    renamer = S3FileRenamer(
        bucket_name=args.bucket,
        dry_run=not args.live
    )
    
    # Process files
    if args.year:
        logger.info(f"Processing single year: {args.year}")
        renamer.process_year(args.year)
    else:
        renamer.process_all_years(args.start_year, args.end_year)


if __name__ == "__main__":
    main()
