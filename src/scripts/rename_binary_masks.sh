#!/bin/bash
# rename_files.sh
# This script renames all files in a given directory and its subdirectories
# according to the following logic:
# - If a file does not end with ".png", it prints an error and exits with error.
# - If a file already ends with "_mask.png", it is skipped.
# - If the file ends with ".png", it renames it by replacing ".png" with "_mask.png".
#
# Usage: ./rename_files.sh /path/to/BINARY_DIR

# Check if BINARY_DIR is passed as parameter.
if [ -z "$1" ]; then
    echo "Usage: $0 /path/to/BINARY_DIR" >&2
    exit 1
fi

BINARY_DIR="$1"
echo "Searching for files in: $BINARY_DIR"

# Use 'find' to recursively search for all files in BINARY_DIR.
find "$BINARY_DIR" -type f -print0 | while IFS= read -r -d '' file; do
    # Check if the file ends with ".png". If not, print an error message and exit with error.
    if [[ "$file" != *.png ]]; then
        echo "Error: file '$file' does not end with .png" >&2
        exit 1
    fi

    # If the file already ends with "_mask.png", skip renaming.
    if [[ "$file" == *_mask.png ]]; then
        echo "File '$file' already contains '_mask'. Skipping."
        continue
    fi

    # Generate new file name by replacing the ending ".png" with "_mask.png"
    new_file="${file%.png}_mask.png"
    echo "Renaming '$file' to '$new_file'"
    mv "$file" "$new_file"
done
