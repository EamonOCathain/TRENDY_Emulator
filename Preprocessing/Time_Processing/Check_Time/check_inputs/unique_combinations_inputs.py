import pandas as pd

# Load the CSV
df = pd.read_csv("/Net/Groups/BSI/work_scratch/ecathain/mpi2/Outputs/CSVs/Timesteps/inputs_timestamp_info.csv")  # Replace with your actual filename

# Define the columns to compare for uniqueness
key_cols = ['has_time', 'calendar', 'units', 'dtype']

# Drop rows with missing key info (optional, depending on your data quality)
df_filtered = df.dropna(subset=key_cols + ['file_path'])

# Group by the unique combinations and get the first file_path as example
example_map = df_filtered.groupby(key_cols)['file_path'].first().reset_index()

# Rename the file_path column to 'example_file'
example_map = example_map.rename(columns={'file_path': 'example_file'})

# Save to a new CSV
example_map.to_csv("/Net/Groups/BSI/work_scratch/ecathain/mpi2/Outputs/CSVs/Timesteps/inputs_unique_combos.csv", index=False)

print(f"✅ Found {len(example_map)} unique combinations.")