import numpy as np

class Aggregator():
    def __init__(self, size):
        self.size = size
        
    def aggregate(self, data):
        '''
        Pandas Series -> list
        '''
        return []

    def complete(self, row):
        return np.isfinite(row).all() and len(row) == self.columns + 1

class PctChangeAggregator(Aggregator):
    def __init__(self, size, window):
        super().__init__(size)
        self.window = window

        # (size + self) * (pct change - first nan value)
        self.columns = (self.size + 1) * (self.window - 1)
        
    def aggregate(self, data):
        df = data.pct_change()
        df.replace(to_replace=[np.nan, np.inf], value=[0, 1], inplace=True)
        
        return df.values[1:].tolist()

class MeanAggregator(Aggregator):
    '''
    Distills a series of observations into a single value
    '''
    def __init__(self, size):
        super().__init__(size)
        self.columns = self.size + 1
    
    def aggregate(self, data):
        return [ data[data.columns[0]].mean() ]
