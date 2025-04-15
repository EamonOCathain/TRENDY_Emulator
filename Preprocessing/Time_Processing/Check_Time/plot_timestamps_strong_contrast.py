import os 
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import ListedColormap
import matplotlib.cm as cm

print("Starting script")

# Directories
sys.path.append(os.path.abspath("/Net/Groups/BSI/work_scratch/ecathain/mpi2/Finished_Scripts/Classes"))
import paths

csv_dir = os.path.join(paths.CSV)
plot_dir = os.path.join(paths.GRAPHS)

# Data categories and titles
list_keys = ['Exists', 'Units', 'Calendar', 'Num_Timesteps', 'Interval', 'dtype', 'date_range']    
list_titles = [
    "Presence of Time Variable", "Unit of Time", "Calendar Type", "Number of Timesteps",
    "Average Interval of Time", "Data Type of Time", "Date Range"
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

    # Map strings to codes
    unique_strings = pd.unique(plot_data.values.ravel())
    string_to_code = {s: i for i, s in enumerate(unique_strings)}
    code_to_string = {v: k for k, v in string_to_code.items()}
    numeric_data = plot_data.replace(string_to_code)

    # Generate a long enough unique colormap
    n_colors = len(unique_strings)
    base_colors = plt.cm.get_cmap('hsv', n_colors)  # 'hsv' gives good separation for many colors
    cmap = ListedColormap([base_colors(i) for i in range(n_colors)])

    # Plot
    plt.figure(figsize=(16, 10))
    img = plt.imshow(numeric_data, cmap=cmap, aspect='auto', interpolation='none')

    plt.xticks(ticks=np.arange(plot_data.shape[1]), labels=plot_data.columns, rotation=90)
    plt.yticks(ticks=np.arange(plot_data.shape[0]), labels=plot_data.index, rotation=0)

    # Grid
    for x in range(plot_data.shape[1] + 1):
        plt.axvline(x - 0.5, color='white', linewidth=0.5)
    for y in range(plot_data.shape[0] + 1):
        plt.axhline(y - 0.5, color='white', linewidth=0.5)

    # Colorbar
    cbar = plt.colorbar(img, ticks=np.arange(n_colors))
    cbar.ax.set_yticklabels([str(code_to_string[i])[:40] for i in range(n_colors)])
    cbar.set_label('Value')

    print(f"{key}")
    plt.title(dict_titles.get(key, key))
    plt.tight_layout()
    plt.savefig(os.path.join(plot_dir, f'{key}.png'))
    plt.close()

print("Script complete")