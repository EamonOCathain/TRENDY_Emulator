import xarray as xr
import numpy as np

def extract_np_array(path, lat = 51, lon = 11):
    ds = xr.open_dataarray(path, decode_times=False)

    ds_arr = np.array(ds.sel(lat=lat, lon=lon, method="nearest"))
    
    ds.close()
    
    return(ds_arr)
    
def trim_outputs_array(np_array, years_after_1900):
    years = years_after_1900
    months = years * 12
    # Format - start date, interval, trim index
    timestep_map = {
                3888: ("1700-01-01", "monthly", 2400 + months),
                324:  ("1700-01-01", "yearly", 200 + years),
                3876: ("1701-01-01", "monthly", 2388 + months),
                323:  ("1701-01-01", "yearly", 199 + years),
                1968: ("1860-01-01", "monthly", 480 + months),
                164:  ("1860-01-01", "yearly", 40 + years),
                1488: ("1900-01-01", "monthly", 0 + months),
                1 : ("1700-01-01", "monthly", 1),
                10 : ("1700-01-01", "once", 9) # For all data with 10 timesteps they are all 0 and can be reduced to a single timestep
            }
    
    length = len(np_array)
    
     # Save the start_date, interval and the point at which to trim the array so that it starts at 1900-01-01
    trim_index = timestep_map[length][2]
    
    # Trim the array
    np_trimmed = np_array[trim_index:]
    
    return(np_trimmed)
    
    
def generate_dt(trimmed_array, years_after_1900):
    # Trim the array
    length = len(trimmed_array)
    
    years = years_after_1900
    months = years * 12
    
    if length == 1488 - months:
        start = np.datetime64("1900-01", "M") + months  # use month precision
        dates = start + np.arange(length).astype("timedelta64[M]")
        return dates.astype("datetime64[D]")  # optional: convert to full date

    elif length == 124 - years:
        start = np.datetime64("1900", "Y") + years  # use year precision
        dates = start + np.arange(length).astype("timedelta64[Y]")
        return dates.astype("datetime64[D]")  # optional: convert to full date

    elif length == 1:
        return np.array([np.datetime64("1900", "Y") + years], dtype="datetime64[D]")

    else:
        raise ValueError(f"Unexpected number of timesteps: {length}")

def extract_and_trim(path, years_after_1900):
    np_arr = extract_np_array(path)
    trimmed_arr = trim_outputs_array(np_arr, years_after_1900)
    return trimmed_arr
        
def extract_trim_and_datetime(path, years_after_1900):
    np_arr=extract_np_array(path)
    trimmed_arr = trim_outputs_array(np_arr, years_after_1900)
    dt = generate_dt(trimmed_arr, years_after_1900)
    return(trimmed_arr,dt)
