#!/bin/bash
# Quick wrapper script for S3 file renaming

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/fix_rename_files_s3.py"

echo "=================================="
echo "S3 File Renamer - Quick Menu"
echo "=================================="
echo ""
echo "1) DRY RUN - All years (2000-2025)"
echo "2) DRY RUN - Specific year"
echo "3) DRY RUN - Year range"
echo "4) LIVE - Specific year (⚠️  CAUTION!)"
echo "5) LIVE - All years (⚠️⚠️  EXTREME CAUTION!)"
echo "6) Exit"
echo ""
read -p "Select option: " option

case $option in
    1)
        echo "Running DRY RUN for all years..."
        python3 "$PYTHON_SCRIPT"
        ;;
    2)
        read -p "Enter year: " year
        echo "Running DRY RUN for year $year..."
        python3 "$PYTHON_SCRIPT" --year "$year"
        ;;
    3)
        read -p "Enter start year: " start_year
        read -p "Enter end year: " end_year
        echo "Running DRY RUN for years $start_year to $end_year..."
        python3 "$PYTHON_SCRIPT" --start-year "$start_year" --end-year "$end_year"
        ;;
    4)
        read -p "Enter year: " year
        echo ""
        echo "⚠️  WARNING: This will make REAL changes to S3!"
        read -p "Are you sure? (type 'YES' to confirm): " confirm
        if [ "$confirm" = "YES" ]; then
            echo "Running LIVE mode for year $year..."
            python3 "$PYTHON_SCRIPT" --year "$year" --live
        else
            echo "Cancelled."
        fi
        ;;
    5)
        echo ""
        echo "⚠️⚠️  EXTREME WARNING: This will rename ALL files from 2000-2025!"
        read -p "Are you ABSOLUTELY sure? (type 'YES I AM SURE' to confirm): " confirm
        if [ "$confirm" = "YES I AM SURE" ]; then
            echo "Running LIVE mode for ALL years..."
            python3 "$PYTHON_SCRIPT" --live
        else
            echo "Cancelled."
        fi
        ;;
    6)
        echo "Exiting..."
        exit 0
        ;;
    *)
        echo "Invalid option"
        exit 1
        ;;
esac

echo ""
echo "Done!"
