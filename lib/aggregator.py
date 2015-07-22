import numpy as np

def simple(series):
    return series.tolist()

def change(series):
    df = series.pct_change()
    df.replace(to_replace=[np.nan, np.inf], value=[0, 1], inplace=True)
    
    return simple(df.values[1:])

def average(series):
    return [ series.mean() ]

def difference(series):
    return simple(series.diff()[1:])
