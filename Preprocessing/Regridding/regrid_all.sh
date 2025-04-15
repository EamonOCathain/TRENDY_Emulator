#!/bin/bash

# Takes an input directory to search

# Base directories (fixed)
BASE_INPUT_DIR="TRENDY_cluster_path"
BASE_OUTPUT_DIR="path_to_outputs"

# Require relative path as argument
if [ $# -eq 0 ]; then
    echo "Usage: $0 <relative_path_to_subdirectory>"
    exit 1
fi

SUBDIR="$1"
INPUT_DIR="${BASE_INPUT_DIR%/}/$SUBDIR"
OUTPUT_DIR="${BASE_OUTPUT_DIR%/}/$SUBDIR"

# Remove trailing slash from input directory if present
INPUT_DIR="${INPUT_DIR%/}"

echo "Searching in: $INPUT_DIR"
echo "Output to:    $OUTPUT_DIR"
echo ""

# Process files
find "$INPUT_DIR" -type f -name "*.nc" | while read -r file; do
    rel_path="${file#$INPUT_DIR/}"
    output_file="$OUTPUT_DIR/$rel_path"
    mkdir -p "$(dirname "$output_file")"

    echo "Checking: $rel_path"


    if [[ -f "$output_file" ]]; then
        if cdo -s griddes "$output_file" &> /dev/null; then
            echo "→ Skipping (already valid): $rel_path"
            continue
        else
            echo "→ File exists but is invalid. Reprocessing..."
            rm -f "$output_file"
        fi
    else
        echo "→ File not found in output. Reprocessing..."
    fi

    # Regrid
    cdo remapbil,r720x360 "$file" "$output_file"
    echo "✓ Regridded: $rel_path"
    echo ""
done

echo "Done."