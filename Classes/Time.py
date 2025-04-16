import xarray as xr
import numpy as np

def extract_np_array(path, lat = 51, lon = 11):
    ds = xr.open_dataarray(path, decode_times=False)

    ds_arr = np.array(ds.sel(lat=lat, lon=lon, method="nearest"))
    
    ds.close()
    
    return(ds_arr)
    
def trim_array(np_array):
    # Format - start date, interval, trim index
    timestep_map = {
                3888: ("1700-01-01", "monthly", 2400),
                324:  ("1700-01-01", "yearly", 200),
                3876: ("1701-01-01", "monthly", 2388),
                323:  ("1701-01-01", "yearly", 199),
                1968: ("1860-01-01", "monthly", 480),
                164:  ("1860-01-01", "yearly", 40),
                1488: ("1900-01-01", "monthly", 0),
                1 : ("1700-01-01", "monthly", 1),
                10 : ("1700-01-01", "once", 9) # For all data with 10 timesteps they are all 0 and can be reduced to a single timestep
            }
    
    length = len(np_array)
    
     # Save the start_date, interval and the point at which to trim the array so that it starts at 1900-01-01
    trim_index = timestep_map[length][2]
    
    # Trim the array
    np_trimmed = np_array[trim_index:]
    
    return(np_trimmed)
    
    
def generate_dt(trimmed_array):
    # Trim the array
    length = len(trimmed_array)
    
    if length == 1488:
        start = np.datetime64("1900-01", "M")  # use month precision
        dates = start + np.arange(length).astype("timedelta64[M]")
        return dates.astype("datetime64[D]")  # optional: convert to full date
    elif length in [124, 1]:
        start = np.datetime64("1900", "Y")  # use year precision
        dates = start + np.arange(length).astype("timedelta64[Y]")
        return dates.astype("datetime64[D]")  # optional: convert to full date
    else:
        print(f"Invalid number of timesteps after trimming. Should be 1488 or 324 but it's {length}")
        
def full_extract(path):
    np_arr=extract_np_array(path)
    trimmed_arr = trim_array(np_arr)
    dt = generate_dt(trimmed_arr)
    return(trimmed_arr,dt)
