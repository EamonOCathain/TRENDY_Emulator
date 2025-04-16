import os
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import gridspec
from tqdm import tqdm

def analyze_netcdf_time(root_dir):
    """Recursively analyze NetCDF files for time dimension properties."""
    results = []
    
    for root, dirs, files in tqdm(list(os.walk(root_dir)), desc="Scanning files"):
        for file in files:
            if file.endswith('.nc'):
                file_path = os.path.join(root, file)
                entry = {
                    'file_path': file_path,
                    'has_time': False,
                    'calendar': None,
                    'units': None,
                    'dtype': None,
                    'time_start': None,
                    'n_timesteps': None,  # 👈 NEW FIELD
                    'can_decode_times': None,
                    'error': None
                }

                # Try decoding time
                try:
                    with xr.open_dataset(file_path, decode_times=True) as ds:
                        entry['can_decode_times'] = True
                except Exception:
                    entry['can_decode_times'] = False

                # Try opening with decode_times=False to extract metadata
                with xr.open_dataset(file_path, decode_times=False) as ds:
                    if 'time' in ds.variables:
                        time_var = ds['time']
                        entry['has_time'] = True
                        entry['dtype'] = str(time_var.dtype)

                        # time.values[0] safely
                        try:
                            entry['time_start'] = str(time_var.values[0])
                        except Exception:
                            entry['time_start'] = 'unavailable'

                        # 👇 Add number of timesteps
                        try:
                            entry['n_timesteps'] = len(time_var)
                        except Exception:
                            entry['n_timesteps'] = 'unavailable'

                        entry['calendar'] = time_var.encoding.get('calendar', time_var.attrs.get('calendar', 'not_specified'))
                        entry['units'] = time_var.encoding.get('units', time_var.attrs.get('units', 'not_specified'))
                except Exception as e:
                    entry['error'] = str(e)

                results.append(entry)
    
    return pd.DataFrame(results)

# --- Main Execution ---
if __name__ == "__main__":
    root_directory = "/Net/Groups/BSI/work_scratch/ecathain/mpi2/TRENDY/Raw/INPUT"
    
    df = analyze_netcdf_time(root_directory)
    df.to_csv('/Net/Groups/BSI/work_scratch/ecathain/mpi2/Outputs/CSVs/Timesteps/Inputs/Current/inputs_timestamps_with_num_timesteps.csv', index=False)

    print("✅ Analysis complete.")
    print("📄 Saved to: time_dimension_analysis.csv")
    print("📊 Plot saved as: time_dimension_analysis.png")