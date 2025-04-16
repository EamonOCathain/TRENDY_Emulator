def standardize_series(series):
    mean = np.mean(series)
    std = np.std(series)
    
    if std == 0:
        raise ValueError("Standard deviation is zero; cannot standardize.")
    
    return (series - mean) / std