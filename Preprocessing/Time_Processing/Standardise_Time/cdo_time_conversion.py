# Run it in bash like this: python cdo_time_conversion.py --model CABLE-POP
# It takes a model directory as an argument

import argparse
import os
import subprocess
import tempfile
import pandas as pd
from tqdm import tqdm
import shutil

# Settings
replace_files = False # This decides whether or not existing files should be overwritten

# Initialize argument parser
parser = argparse.ArgumentParser(description='Standardise Timestamps of TRENDY data')
parser.add_argument('--model', required=True, help='Model name to process (e.g., "CESM2")')
args = parser.parse_args()

# Read both CSVs
csv_path = "/Net/Groups/BSI/work_scratch/ecathain/mpi2/Outputs/CSVs/Timesteps/Outputs/Num_Timesteps.csv"
data_path = "/Net/Groups/BSI/work_scratch/ecathain/mpi2/TRENDY/Raw/OUTPUT"
output_path = "/Net/Groups/BSI/work_scratch/ecathain/mpi2/TRENDY/Standard_Time/OUTPUT"

# Read in the Timesteps DF
df = pd.read_csv(os.path.join(csv_path), index_col=0)
df = df.astype(str)

# Filter columns for the specified model
model_columns = [col for col in df.columns if col.startswith(f"{args.model}/")]

if not model_columns:
    raise ValueError(f"No scenarios found for model: {args.model}")

# Calculate total tasks for progress bar
total_variables = len(df.index)
total_scenarios = len(model_columns)
total_tasks = total_variables * total_scenarios

# --- Modified processing loop ---
with tqdm(total=total_tasks, desc=f"Processing {args.model}", unit="task") as pbar:
    for col in model_columns:
        model, scenario = col.split("/")
        
        for var in df.index:
            pbar.update(1)
            
            if pd.isna(df.loc[var, col]):
                continue

            subdir = os.path.join(data_path, model, scenario)
            
            if not os.path.isdir(subdir):
                tqdm.write(f"Directory not found: {subdir}")
                continue

            matched_file = None
            for f in os.listdir(subdir):
                if f.endswith(".nc") and f[:-3].split("_")[-1] == var:
                    matched_file = os.path.join(subdir, f)
                    break

            if not matched_file:
                tqdm.write(f"No file match for {model}/{scenario} and variable {var}")
                continue
            
            # Path setup and existence check
            relative_path = os.path.relpath(matched_file, data_path)
            new_path = os.path.join(output_path, relative_path)
            os.makedirs(os.path.dirname(new_path), exist_ok=True)
            
            # Skip processing if output exists
            if replace_files == False:
                if os.path.isfile(new_path):
                    tqdm.write(f"Skipping existing file: {new_path}")
                    continue  # This skips to next variable
                    
            # Define valid timestep mappings
            timestep_map = {
                "3888.0": ("1700-01-01", "1mon"), # 3880 steps means monthly from 1700-01
                "324.0":  ("1700-01-01", "365day"), # 324: Yearly from 1700-01
                "3876.0": ("1701-01-01", "1mon"), # 3876: Monthly from 1701-01 
                "323.0":  ("1700-01-01", "365day"), # 323: Yearly from 1700-01
                "1968.0": ("1860-01-01", "1mon"), # 1968: Monthly from 1860-01 
                "164.0":  ("1860-01-01", "365day"), # 164: Yearly from 1860-01
                "1488.0": ("1900-01-01", "1mon"), # 1488: Monthly from 1900-01
            }
            
            timestep = df.loc[var, col]
            
            if timestep in ["1.0", "10.0"]:
                # This retains only the first timestamp. The only ones with 10 timestamps are CABLE-POP and they are all 0.
                    try:
                        with tempfile.TemporaryDirectory() as tmpdir:
                            temp1 = os.path.join(tmpdir, "step1.nc")
                            temp2 = os.path.join(tmpdir, "step2.nc")
                            temp3 = os.path.join(tmpdir, "step3.nc")

                            # 1. Select only the first timestep
                            cmd1 = ["cdo", "seltimestep,1", matched_file, temp1]
                            tqdm.write(f"Running: {' '.join(cmd1)}")
                            subprocess.run(cmd1, check=True)

                            # 2. Set calendar to 365_day
                            cmd2 = ["cdo", "-setcalendar,365_day", temp1, temp2]
                            tqdm.write(f"Running: {' '.join(cmd2)}")
                            subprocess.run(cmd2, check=True)

                            # 3. Set reference time (time unit: days since ...)
                            cmd3 = ["cdo", f"-settaxis,1700-01-01,00:00:00,365day", temp2, temp3]
                            tqdm.write(f"Running: {' '.join(cmd3)}")
                            subprocess.run(cmd3, check=True)

                            # 4. Move to final path
                            shutil.move(temp3, new_path)

                        tqdm.write(f"Successfully processed (short time trimmed): {new_path}")

                    except subprocess.CalledProcessError as e:
                        tqdm.write(f"\nFailed processing {matched_file}:")
                        tqdm.write(f"Error: {e.stderr or 'No stderr captured'}")
                        tqdm.write(f"Output: {e.stdout or 'No stdout captured'}")

                    continue  # Skip rest of loop for these cases
            
            # Catch unknown cases
            if timestep not in timestep_map:
                tqdm.write(f"Unknown timestep amount for {model}/{scenario} and variable {var}")
                continue

            # Use the known values
            start_date, interval = timestep_map[timestep]
            start_time = "00:00:00"

            try:
                with tempfile.TemporaryDirectory() as tmpdir:
                    temp1 = os.path.join(tmpdir, "step1.nc")
                    temp2 = os.path.join(tmpdir, "step2.nc")
                    temp3 = os.path.join(tmpdir, "step3.nc")

                    # 1. Set calendar first
                    cmd1 = [
                        "cdo",
                        "-setcalendar,standard",
                        matched_file,
                        temp1
                    ]
                    tqdm.write(f"Running: {' '.join(cmd1)}") 
                    subprocess.run(cmd1, check=True)

                    # 2. Set time axis
                    cmd2 = [
                        "cdo",
                        f"-settaxis,{start_date},{start_time},{interval}",
                        temp1,
                        temp2
                    ]
                    tqdm.write(f"Running: {' '.join(cmd2)}") 
                    subprocess.run(cmd2, check=True)

                    # 3. Set reference time last
                    cmd3 = [
                        "cdo",
                        f"-setreftime,{start_date},{start_time}",
                        temp2,
                        temp3
                    ]
                    tqdm.write(f"Running: {' '.join(cmd3)}") 
                    subprocess.run(cmd3, check=True)
                    
                    # 4. Trim the time to 1900-2023
                    cmd4 = [
                        "cdo",
                        "-seldate,1900-01-01,2023-12-31",
                        temp3,
                        new_path
                    ]
                    tqdm.write(f"Running: {' '.join(cmd4)}") 
                    subprocess.run(cmd4, check=True)
                    
                tqdm.write(f"Successfully processed: {new_path}")

            except subprocess.CalledProcessError as e:
                tqdm.write(f"\nFailed processing {matched_file}:")
                tqdm.write(f"Error: {e.stderr or 'No stderr captured'}")
                tqdm.write(f"Output: {e.stdout or 'No stdout captured'}")
                
                
