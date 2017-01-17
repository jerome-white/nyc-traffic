import numpy as np
import pandas as pd

class Segment:
    def __init__(self, csv_file, name=None, freq='T'):
        columns = [ 'speed', 'travel' ]
        df = pd.read_csv(str(csv_file),
                         index_col='as_of',
                         parse_dates=True,
                         dtype={ x: np.float64 for x in columns })
        
        self.frequency = df.index.to_series().diff().mean().total_seconds()
        self.df = df.resample(freq).mean()
        self.name = name if name is not None else csv_file.stem

    def __str__(self):
        return self.name

    def roller(self, window, column='speed'):
        return self.df[column].rolling(len(window), min_periods=0)
    
    # def lag(self, with_respect_to, multiplier):
    #     lag = self.df.travel.mean() * multiplier
    #     self.df = self.df.shift(round(lag))
    #     self.df.fillna(method='bfill', inplace=True)

class Cluster(list):
    def __init__(self, root):
        self.append(root)
        self.reference = root.df.index        
        
    def combine(self, interpolate=True):
        df = pd.concat([ x.df.speed for x in self ], axis=1)
        df = df.resample(self.reference.freq).mean()
        df = df.loc[self.reference.min():self.reference.max()]
        df.columns = [ x.name for x in self ]
        
        if interpolate:
            df.interpolate(inplace=True)
            for i in 'bf':
                method = i + 'fill'
                df.fillna(method=method, inplace=True)
            
        return df
