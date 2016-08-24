import itertools

import pandas as pd

names_ = [ 'observation', 'prediction', 'target' ]

def window_from_config(config):
    (*required, optional) = names_
    wcfg = config['window']

    args = [ int(wcfg[x]) for x in required ]
    if optional in wcfg:
        args.append(int(wcfg[optional]))

    return Window(*args)
    
def idx_range(index, end=None, size=1):
    '''
    Given a starting index, builds windows over that index of a given
    size, stopping when the right-most time value is larger than end.
    '''
    
    if end is None:
        end = index.max()
        
    for i in map(lambda x: index.min() + x, itertools.count()):
        drange = pd.date_range(i, periods=size, freq=index.freq)
        if drange.max() > end:
            break
        
        yield drange

def idx_range_parallel(index, window):
    extended = index + window.observation + window.prediction
    
    yield from zip(idx_range(index, size=window.observation),
                   idx_range(extended, index.max(), window.target))
        
class Window:
    def __init__(self, observation, prediction, target=None):
        self.observation = observation
        self.prediction = prediction
        self.target = self.observation if target is None else target

        self.__elements = []

    def __len__(self):
        return self.observation + self.prediction + self.target

    def __repr__(self):
        m = map(str, [ self.observation, self.prediction, self.target ])
        return ','.join(m)

    def __str__(self):
        return repr(self)

    def __list__(self):
        return [ self.observation, self.prediction, self.target ]

    def __iter__(self):
        self.__elements = [ self.observation, self.prediction, self.target ]
        return self

    def __next__(self):
        if self.__elements:
            return self.__elements.pop(0)
        
        raise StopIteration
    
    def tail(self):
        return Window(self.target, self.prediction, self.target)

    def slide(self, index):
        yield from idx_range(index, size=len(self))
