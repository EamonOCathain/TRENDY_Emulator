import os
import pandas as pd
import xarray as xr
import cftime
from tqdm import tqdm
from cftime import _parse_date
import sys

# This script doesn't succeed in decoding all time units

# Add the directory of the classes
sys.path.append(os.path.abspath("/Net/Groups/BSI/work_scratch/ecathain/mpi2/Finished_Scripts/Classes"))
import paths

INPUT_DIR = paths.STD_TIME_OUTPUT # This is where the data to be analysed is. Target the folder which contains the models as directories
OUTPUT_DIR = paths.CSV # This is where the CSVs will be written
SCENARIOS = ['S0', 'S1', 'S2', 'S3']
check_decodes = False  # Set to True to enable decoding validation

print(INPUT_DIR)

# Dynamic CSV keys based on check_decodes flag
CSV_KEYS = ['Exists', 'Units', 'Calendar', 'Num_Timesteps', 
            'Interval', 'dtype', 'date_range']
if check_decodes:
    CSV_KEYS.insert(6, 'decodes')  # Insert at position 6

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
            
            # Find relevant NetCDF files
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

def parse_dates(time_var):
    """Convert time variable to start/end dates using native calendar"""
    try:
        times = time_var.values
        if times.size == 0:
            return "Empty dataset", "Empty dataset"

        units = str(time_var.units)
        calendar = str(time_var.calendar)
        first, last = None, None

        # Handle special case units
        if "day as %Y%m%d.%f" in units:
            def parse_ymdfloat(value):
                try:
                    date_float = float(value)
                    date_int = int(date_float)
                    year = date_int // 10000
                    month = (date_int % 10000) // 100
                    day = date_int % 100
                    
                    # Handle CLM5.0's zero dates
                    if year == 0 or month == 0 or day == 0:
                        return "Zero date not allowed"
                        
                    # Validate using actual calendar
                    return cftime.datetime(year, month, day, 
                                         calendar=calendar,
                                         has_year_zero=True)
                except ValueError:
                    return "Invalid YMD format"

            first = parse_ymdfloat(times[0])
            last = parse_ymdfloat(times[-1])

        elif 'years' in units:
            try:
                # Robust base year extraction
                date_part = units.split('since')[-1].strip()
                parsed = cftime._parse_date(date_part)
                base_year = parsed[0]  # Get year from parsed tuple
                base_date = cftime.datetime(base_year, 1, 1, calendar=calendar)
                
                # Handle iMAPLE's "years since 1700" 
                days = times * 365.2425  # Average year length
                first = base_date + datetime.timedelta(days=float(days[0]))
                last = base_date + datetime.timedelta(days=float(days[-1]))
            except Exception as e:
                return f"Year Error: {str(e)}", f"Year Error: {str(e)}"

        elif 'months' in units:
            try:
                if calendar == '360_day':
                    first = cftime.num2date(times[0], units, calendar=calendar)
                    last = cftime.num2date(times[-1], units, calendar=calendar)
                else:
                    # Handle CABLE-POP's non-360_day calendars
                    date_part = units.split('since')[-1].strip()
                    parsed = cftime._parse_date(date_part)
                    base_date = cftime.datetime(*parsed[:6], calendar=calendar)
                    
                    days = times * 30.4368  # Average month length
                    first = base_date + datetime.timedelta(days=float(days[0]))
                    last = base_date + datetime.timedelta(days=float(days[-1]))
            except Exception as e:
                return f"Month Error: {str(e)}", f"Month Error: {str(e)}"

        else:
            try:
                first = cftime.num2date(times[0], units, calendar=calendar)
                last = cftime.num2date(times[-1], units, calendar=calendar)
            except Exception as e:
                return f"Standard Error: {str(e)}", f"Standard Error: {str(e)}"

        def safe_format(d):
            if isinstance(d, cftime.datetime):
                return f"{d.year}-{d.month:02d}-{d.day:02d}"
            return str(d)

        return safe_format(first), safe_format(last)

    except Exception as e:
        return f"General Error: {str(e)}", f"General Error: {str(e)}"

def check_decoding(file_path):
    """Validate time decoding capability"""
    try:
        with xr.open_dataset(file_path) as ds:
            _ = ds.time.values
        return "Decodes successfully"
    except Exception as e:
        return f"Decoding failed: {str(e)}"

def process_file(file_path):
    """Extract metadata and date range from a NetCDF file"""
    try:
        with xr.open_dataset(file_path, decode_times=False) as ds:
            if 'time' not in ds:
                return {'date_range': 'Time dimension missing'}
            
            time_var = ds.time
            metadata = {
                'Exists': 'Time exists',
                'Units': str(time_var.units),
                'Calendar': str(time_var.calendar),
                'Num_Timesteps': len(time_var),
                'Interval': 'Calculated in main loop',
                'dtype': str(time_var.dtype),
                'date_range': None
            }
            
            if check_decodes:
                metadata['decodes'] = check_decoding(file_path)
            
            # Skip invalid entries
            if metadata['Units'] in ['No Calendar', 'File Doesnt Exist']:
                return metadata
                
            # Get date range
            start_date, end_date = parse_dates(time_var)
            metadata['date_range'] = f"{start_date} : {end_date}"
            
            return metadata
            
    except Exception as e:
        return {'date_range': f"File error: {str(e)}"}

if __name__ == "__main__":
    print("Starting date range extraction...")
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
                
                # Update dataframes
                for key in CSV_KEYS:
                    if key in metadata:
                        dfs[key].loc[var, f"{model}/{scenario}"] = metadata[key]
                
                pbar.update(1)
    
    # Save results
    for key, df in dfs.items():
        df.to_csv(os.path.join(OUTPUT_DIR, f"{key}.csv"))
    
    print("Date range extraction complete.")
    
# Check for unique combinations  
df1 = pd.read_csv(os.path.join(OUTPUT_DIR,"Calendar.csv"), index_col=0)
df2 = pd.read_csv(os.path.join(OUTPUT_DIR,"Units.csv"), index_col=0)
df3 = pd.read_csv(os.path.join(OUTPUT_DIR,"Num_Timesteps.csv"), index_col=0)
df4 = pd.read_csv(os.path.join(OUTPUT_DIR,"date_range.csv"), index_col=0)

# Convert DataFrames to xarray DataArrays
da1 = xr.DataArray(df1, dims=["var", "scenario"])
da2 = xr.DataArray(df2, dims=["var", "scenario"])
da3 = xr.DataArray(df3, dims=["var", "scenario"])
da4 = xr.DataArray(df4, dims=["var", "scenario"])

# Combine into a Dataset with a new dimension
ds = xr.concat([da1, da2, da3, da4], dim="source")
ds["source"] = ["Calendar", "Units", "Timesteps", "Date Range"]

# Step 1: Get the "Date Range" layer
date_range = ds.sel(source="Date Range")

# Step 2: Create a mask for entries containing 'error' or 'ymd'
# Update the error pattern match to include new error types
mask = date_range.astype(str).str.lower().str.contains(
    r"error|invalid|ymd|failed|zero", 
    regex=True
)

# Step 3: Apply the mask to extract matching entries
error_date_ranges = date_range.where(mask)

# Step 4: Extract the corresponding Calendar and Unit values
calendars = ds.sel(source="Calendar").where(mask)
units = ds.sel(source="Units").where(mask)

# Step 5: Stack into a tidy DataFrame and drop rows with any NaNs
error_df = xr.Dataset({
    "date_range": error_date_ranges,
    "calendar": calendars,
    "unit": units
}).stack(entry=["var", "scenario"]).to_dataframe().dropna()

# Step 6: Drop duplicates of the same error/calendar/unit/var/scenario combo
error_df = error_df.drop_duplicates(subset=["date_range", "calendar", "unit", "var", "scenario"])

# Step 7: Group by the unique error messages
grouped = error_df.groupby("date_range")

# Display
for error_msg, group in grouped:
    print(f"\n{error_msg}")
    print(group[["var", "scenario", "calendar", "unit"]])