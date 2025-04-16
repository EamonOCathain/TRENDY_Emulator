import os 
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import ListedColormap
import matplotlib.cm as cm
import sys

print("Starting script")

# Directories
sys.path.append(os.path.abspath("/Net/Groups/BSI/work_scratch/ecathain/mpi2/Finished_Scripts/Classes"))
import paths

csv_dir = "/Net/Groups/BSI/work_scratch/ecathain/mpi2/Outputs/CSVs/Timesteps/Outputs/STD_TIME"
plot_dir = "/Net/Groups/BSI/work_scratch/ecathain/mpi2/Outputs/Graphs/Standard_Time"

print(f"Processing CSVs in {csv_dir}")

if not os.path.exists(plot_dir):
    os.mkdir(plot_dir)

# Data categories and titles
list_keys = ['Exists', 'Units', 'Calendar', 'Num_Timesteps', 'dtype']    
list_titles = [
    "Presence of Time Variable", "Unit of Time", "Calendar Type", "Number of Timesteps",
 "Data Type of Time"
]         

# Store paths
dict_csv_paths = {k: os.path.join(csv_dir, f"{k}.csv") for k in list_keys}
dict_plot_paths = {k: os.path.join(plot_dir, f"{k}.csv") for k in list_keys}

# Load CSVs into dictionary
df_dict = {}
for key, path in dict_csv_paths.items():
    if os.path.isfile(path):
        df_dict[key] = pd.read_csv(path, index_col=0)

# Titles
dict_titles = dict(zip(list_keys, list_titles))

# Loop over each CSV
for key in list_keys:
    df = df_dict[key]
    plot_data = df

   # Check if the data is numeric
    if pd.api.types.is_numeric_dtype(plot_data.values.ravel()):
        numeric_data = plot_data.astype(float)
        unique_values = np.unique(numeric_data.values.ravel())
        value_to_code = {v: i for i, v in enumerate(unique_values)}
        code_to_value = {i: v for v, i in value_to_code.items()}
        coded_data = plot_data.replace(value_to_code)
        color_labels = [str(code_to_value[i]) for i in range(len(code_to_value))]
    else:
        unique_values = pd.unique(plot_data.values.ravel())
        value_to_code = {v: i for i, v in enumerate(unique_values)}
        code_to_value = {i: v for v, i in value_to_code.items()}
        coded_data = plot_data.replace(value_to_code)
        color_labels = [str(code_to_value[i])[:40] for i in range(len(code_to_value))]

    # Plotting
   # Get unique codes actually used in the data
    used_codes = np.unique(coded_data.values.ravel())

    # Build color map only for used codes
    max_code = max(used_codes) + 1  # ensure the colormap is large enough
    base_colors = plt.cm.get_cmap('hsv', max_code)  
    cmap = ListedColormap([base_colors(i) for i in used_codes])

    # Plot with remapped colormap
    plt.figure(figsize=(16, 10))
    img = plt.imshow(coded_data, cmap=ListedColormap([base_colors(i) for i in used_codes]), aspect='auto', interpolation='none')

    # Colorbar
    cbar = plt.colorbar(img, ticks=np.arange(len(used_codes)))
    cbar.ax.set_yticklabels([str(code_to_value[i]) for i in used_codes])

    print(f"{key}")
    plt.title(dict_titles.get(key, key))
    plt.tight_layout()
    plt.savefig(os.path.join(plot_dir, f'{key}.png'))
    plt.close()

print("Script complete")