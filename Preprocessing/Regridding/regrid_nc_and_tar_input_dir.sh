#!/bin/bash

# Check for directory argument
if [ $# -eq 0 ]; then
    echo "Error: Please specify a directory name as argument"
    echo "Usage: $0 <directory>"
    exit 1
fi

target_dir="$1"

# Base directories
BASE_INPUT_DIR="/Net/Groups/BGI/data/DataStructureMDI/DATA/Incoming/trendy/gcb2024/LAND/OUTPUT"
BASE_OUTPUT_DIR="/Net/Groups/BSI/work_scratch/ecathain/mpi2/TRENDY/Raw/OUTPUT"

# Construct full paths
INPUT_DIR="${BASE_INPUT_DIR%/}/$target_dir"
OUTPUT_DIR="${BASE_OUTPUT_DIR%/}/$target_dir"

# Verify input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Input directory does not exist - $INPUT_DIR"
    exit 1
fi

echo "Processing directory: $target_dir"
echo "Input directory: $INPUT_DIR"
echo "Output directory: $OUTPUT_DIR"

# Corrected find command
find "$INPUT_DIR" -type f \( -name "*.nc" -o -name "*.tar" \) | while read -r file; do
    # Get relative path within INPUT_DIR
    rel_path="${file#$INPUT_DIR/}"
    
    if [[ "$file" == *.tar ]]; then
        # Handle .tar files
        output_nc_file="$OUTPUT_DIR/${rel_path%.tar}"
        output_dir="$(dirname "$output_nc_file")"
        
        # Skip if output already exists and is valid
        if [ -f "$output_nc_file" ] && cdo -s griddes "$output_nc_file" &> /dev/null; then
            echo "Skipping (valid): ${rel_path%.tar}"
            continue
        fi
        
        # Extract and process
        tmp_dir=$(mktemp -d)
        tar -xf "$file" -C "$tmp_dir"
        extracted_nc=$(find "$tmp_dir" -type f -name "*.nc" -print -quit)
        
        if [ -f "$extracted_nc" ]; then
            mkdir -p "$output_dir"
            cdo remapbil,r720x360 "$extracted_nc" "$output_nc_file"
            echo "Processed (from tar): ${rel_path%.tar}"
        else
            echo "ERROR: No .nc found in $file"
        fi
        rm -rf "$tmp_dir"
        
    elif [[ "$file" == *.nc ]]; then
        # Handle .nc files directly
        output_file="$OUTPUT_DIR/$rel_path"
        output_dir="$(dirname "$output_file")"
        
        # Skip if output already exists and is valid
        if [ -f "$output_file" ] && cdo -s griddes "$output_file" &> /dev/null; then
            echo "Skipping (valid): $rel_path"
            continue
        fi
        
        # Process directly
        mkdir -p "$output_dir"
        cdo remapbil,r720x360 "$file" "$output_file"
        echo "Processed: $rel_path"
    fi
done