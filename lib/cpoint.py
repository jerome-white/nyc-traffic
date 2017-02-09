import warnings
import operator as op

import numpy as np
import scipy.constants as constant

Selector = lambda x: {
    'percentage': Percentage,
    'derivative': Derivative,
    'acceleration': Acceleration,
}[x]

class ChangePoint:
    def __init__(self, threshold, relation=op.gt):
        self.isjam = lambda x: relation(x, threshold)

    def classify(self, seq, window, aggregate=np.mean):
        if len(seq) != len(window):
            return np.nan

        segments = []
        for i in window.split(seq):
            if np.isnan(i).any():
                return np.nan
            segments.append(aggregate(i))
        diff = self.change(window.offset, *segments)

        return self.isjam(diff)

    def change(self, duration, before, after):
        '''
        duration: minutes
        before, after: miles per hour
        '''
        raise NotImplementedError()

class Acceleration(ChangePoint):
    def change(self, duration, before, after):
        meters = (before - after) * constant.mile # miles to meters
        seconds = duration * constant.minute # minutes to seconds
        acceleration = meters / constant.hour / seconds
    
        return acceleration / constant.g

class Percentage(ChangePoint):
    def change(self, duration, before, after):
        return (after - before) / before

class Derivative(ChangePoint):
    def __init__(self, threshold, order=1):
        super().__init__(threshold)
        self.order = order

    def change(self, duration, before, after):
        rate = constant.mile / constant.hour
        x = (0, before * rate)
        y = (duration, after * rate)

        with warnings.catch_warnings():
            warnings.filterwarnings('error')
            try:
                (slope, *_) = np.polyfit(x, y, self.order)
            except (RuntimeWarning, numpy.RankWarning) as error:
                raise ValueError()

        return slope
    
def bucket(tstamp):
    assert(tstamp.second == 0)
    return tstamp.hour * 60 + tstamp.minute

def aoc(distribution):
    keys = len(distribution.keys())
    assert(keys > 0)
    area = [ sum(i) for i in distribution.values() ]
    
    return sum(area) / keys
