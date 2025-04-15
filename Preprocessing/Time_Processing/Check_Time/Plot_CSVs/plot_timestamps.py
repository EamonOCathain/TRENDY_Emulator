import os 
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

print("Starting script")

# Root directory 
csv_dir = "/Net/Groups/BSI/work_scratch/ecathain/mpi2/Outputs/CSVs/Timesteps/Outputs"
plot_dir = "/Net/Groups/BSI/work_scratch/ecathain/mpi2/Outputs/Graphs/Timesteps"

# Save the paths
# Store all data to collect in a list
list_keys = ['Exists', 'Units', 'Calendar', 'Num_Timesteps', 'Interval', 'dtype', 'date_range']    
list_titles = ["Presence of Time Variable", "Unit of Time", "Calendar Type", "Number of Timesteps", "Average Interval of Time", "Data Type of Time", 'date_range']         

# Store all paths in a dictionary
dict_csv_paths = {}
for i in list_keys:
    dict_csv_paths[i] = os.path.join(csv_dir, f"{i}.csv")
    
dict_plot_paths = {}
for i in list_keys:
    dict_plot_paths[i] = os.path.join(plot_dir, f"{i}.csv")

# Check if the CSV exists, if so read it in and if not create it. Store as a dictionary.
df_dict = {}
for key, path in dict_csv_paths.items():
    if os.path.isfile(path):
        df_dict[key] = pd.read_csv(path, index_col=0)


dict_titles = dict(zip(list_keys, list_titles))

for key in list_keys:
    df = df_dict[key]
    
    plot_data = df

    # Suppose plot_data is your DataFrame with string values
    unique_strings = pd.unique(plot_data.values.ravel())
    string_to_code = {s: i for i, s in enumerate(unique_strings)}
    code_to_string = {v: k for k, v in string_to_code.items()}

    # Map strings to integer codes
    numeric_data = plot_data.replace(string_to_code)

    # Create a colormap with enough unique colors
    cmap = plt.get_cmap('tab20', len(unique_strings))  # You can also try 'tab10', 'Set3', etc.

    # Plotting
    plt.figure(figsize=(14, 10))
    img = plt.imshow(numeric_data, cmap=cmap, aspect='auto', interpolation='none')

    # Tick labels
    plt.xticks(ticks=np.arange(plot_data.shape[1]), labels=plot_data.columns, rotation=90)
    plt.yticks(ticks=np.arange(plot_data.shape[0]), labels=plot_data.index, rotation=0)

    # Add grid lines between cells
    for x in range(plot_data.shape[1] + 1):
        plt.axvline(x - 0.5, color='white', linewidth=0.5)
    for y in range(plot_data.shape[0] + 1):
        plt.axhline(y - 0.5, color='white', linewidth=0.5)

    # Colorbar with truncated string labels
    cbar = plt.colorbar(img, ticks=np.arange(len(unique_strings)))
    cbar.ax.set_yticklabels([str(code_to_string[i])[:40] for i in range(len(unique_strings))])
    cbar.set_label('Value')
    
    print(key)
    plt.title(dict_titles.get(key, key))
    plt.tight_layout()

    plt.savefig(os.path.join(plot_dir, f'{key}.png'))

print("Script complete")