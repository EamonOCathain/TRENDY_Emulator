import os
import pandas as pd
import xarray as xr
from tqdm import tqdm
import sys

# Add the directory of the classes
sys.path.append(os.path.abspath("/Net/Groups/BSI/work_scratch/ecathain/mpi2/Finished_Scripts/Classes"))
import paths

INPUT_DIR = os.path.join(paths.RAW_OUTPUT)
OUTPUT_DIR = os.path.join(paths.CSV)
SCENARIOS = ['S0', 'S1', 'S2', 'S3']

print(INPUT_DIR)

CSV_KEYS = ['Exists', 'Units', 'Calendar', 'Num_Timesteps', 'dtype']

def setup_directories():
    """Create output directory if needed"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_models_vars():
    """Return filtered lists of models and variables"""
    models = ["CABLE-POP", "CLASSIC", "CLM5.0", "ED", "ELM", "IBIS", "iMAPLE", 
             "JSBACH", "JULES", "LPJ-GUESS", "LPJml", "LPJwsl", "LPX", "OCN", 
             "ORCHIDEE", "SDGVM", "VISIT", "VISIT-UT"]
    
    variables = ["mrso", "mrro", "evapotrans", "evapo", "cVeg", "cLitter", 
                "cSoil", "gpp", "ra", "npp", "rh", "fFire", "fLuc", "nbp", 
                "landCoverFrac", "burntArea", "lai"]
    
    return models, variables

def build_model_scenarios(models):
    """Create dictionary of model/scenario file listings"""
    model_scenarios = {}
    for model in models:
        for scenario in SCENARIOS:
            path = os.path.join(INPUT_DIR, model, scenario)
            if not os.path.exists(path):
                continue
            
            files = [f for f in os.listdir(path) 
                    if f.endswith('.nc') and any(f'_{var}.nc' in f for var in variables)]
            
            model_scenarios[f"{model}/{scenario}"] = files
    return model_scenarios

def initialize_dataframes(variables):
    """Create or load existing DataFrames"""
    dfs = {}
    for key in CSV_KEYS:
        path = os.path.join(OUTPUT_DIR, f"{key}.csv")
        if os.path.exists(path):
            dfs[key] = pd.read_csv(path, index_col=0)
        else:
            dfs[key] = pd.DataFrame(index=variables)
    return dfs

def process_file(file_path):
    """Extract metadata from a NetCDF file"""
    try:
        with xr.open_dataset(file_path, decode_times=False) as ds:
            if 'time' not in ds:
                return {'Exists': 'Missing'}
            
            time_var = ds.time
            return {
                'Exists': 'Present',
                'Units': str(time_var.units),
                'Calendar': str(time_var.calendar),
                'Num_Timesteps': len(time_var),
                'dtype': str(time_var.dtype)
            }
            
    except Exception as e:
        return {'Exists': f"Error: {str(e)}"}

if __name__ == "__main__":
    print("Starting metadata extraction...")
    setup_directories()
    models, variables = get_models_vars()
    model_scenarios = build_model_scenarios(models)
    dfs = initialize_dataframes(variables)
    
    total_files = sum(len(files) for files in model_scenarios.values())
    
    with tqdm(total=total_files, desc="Processing files") as pbar:
        for scenario_path, files in model_scenarios.items():
            model, scenario = scenario_path.split('/')
            
            for file in files:
                var = file.split('_')[-1].replace('.nc', '')
                file_path = os.path.join(INPUT_DIR, model, scenario, file)
                
                metadata = process_file(file_path)
                
                for key in CSV_KEYS:
                    if key in metadata:
                        dfs[key].loc[var, f"{model}/{scenario}"] = metadata[key]
                
                pbar.update(1)
    
    # Save results
    for key, df in dfs.items():
        df.to_csv(os.path.join(OUTPUT_DIR, f"{key}.csv"))
    
    print("Metadata extraction complete.")