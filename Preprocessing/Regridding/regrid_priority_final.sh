#!/bin/bash

# this script takes a directory in the MPI cluster as an argument in bash to be recursively searched.
# The files matching the list of target models, scenarios and variables are regridded.
# It does 8 jobs at a time.
# it excludes CARDAMOM as it has a different file structure.
# If the file exists already it skips that variable.

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

echo "Processing directory: $INPUT_DIR"
echo "Output directory: $OUTPUT_DIR"

# Parallel processing settings
MAX_PARALLEL_JOBS=8

# List of variable suffixes to process
suffixes=(
    pr rsds mrso mrro evapotrans evapotranspft transpft evapo albedopft snow_depthpft shflxpft rnpft
    cVeg cLitter cSoil cProduct cVegpft cSoilpft
    gpp ra npp rh fFire
    fLuc soilr nbp
    gpppft npppft rhpft nbppft
    landCoverFrac oceanCoverFrac burntArea lai laipft
    cLeaf cWood cRoot cCwd cSoilpools
    fVegLitter fLeafLitter fWoodLitter fRootLitter fLitterSoil fVegSoil rhpool
    fAllocLeaf fAllocWood fAllocRoot fFireCveg fFireLitter fFireCsoil
)

# Build find command correctly
echo "Building find command..."
find_args=(-type f \()
for suffix in "${suffixes[@]}"; do
    find_args+=(-name "*${suffix}.nc" -o)
done
find_args+=(-name "*.tar" \) -a ! -path "*CARDAMOM*" -a -path "*/S[0-3]/*")

# Find files with debug output
echo "Find command:"
echo "find \"$INPUT_DIR\" ${find_args[@]}"
mapfile -t files < <(find "$INPUT_DIR" "${find_args[@]}" -print)

echo "Found ${#files[@]} files to process"

# Export needed variables
export INPUT_DIR OUTPUT_DIR

# Processing function
process_file() {
    local file="$1"
    local rel_path="${file#$INPUT_DIR/}"
    local output_base="${rel_path%.tar}"
    local output_path="$OUTPUT_DIR/$output_base"
    
    if [[ -f "$output_path" ]] && cdo -s griddes "$output_path" &>/dev/null; then
        echo "VALID: $output_base"
        return 0
    fi
    
    mkdir -p "$(dirname "$output_path")"
    
    if [[ "$file" == *.tar ]]; then
        tmp_dir=$(mktemp -d)
        echo "EXTRACTING: $rel_path"
        tar -xf "$file" -C "$tmp_dir"
        
        while IFS= read -r -d $'\0' extracted; do
            for suffix in "${suffixes[@]}"; do
                if [[ "$extracted" == *"$suffix.nc" ]]; then
                    cdo remapbil,r720x360 "$extracted" "$output_path"
                    echo "REGRIDDED: $output_base"
                    rm -rf "$tmp_dir"
                    return 0
                fi
            done
        done < <(find "$tmp_dir" -type f -name "*.nc" -print0)
        
        echo "NO_MATCH: $rel_path contains no valid .nc"
        rm -rf "$tmp_dir"
    else
        cdo remapbil,r720x360 "$file" "$output_path"
        echo "REGRIDDED: $output_base"
    fi
}
export -f process_file

# Parallel processing
echo "Starting processing with $MAX_PARALLEL_JOBS parallel jobs..."
printf "%s\n" "${files[@]}" | xargs -P $MAX_PARALLEL_JOBS -I {} bash -c 'process_file "$@"' _ {}

echo "Script completed successfully"