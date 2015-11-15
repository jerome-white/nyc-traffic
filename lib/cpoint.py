import warnings
import operator

import numpy as np
import scipy.constants as constant

class ChangePoint:
    def __init__(self, threshold, categories=2):
        self.threshold = threshold
        self.categories = categories

    def change(self, duration, before, after):
        raise NotImplementedError()
    
    def classify(self, duration, before, after):
        '''
        duration: minutes
        before, after: miles per hour
        '''
        if self.threshold < 0:
            args = [ before, after ]
            f = operator.le
        else:
            args = [ after, before ]
            f = operator.gt
        diff = self.change(duration, *args)

        return f(diff, self.threshold)

class Acceleration(ChangePoint):
    def change(self, duration, before, after):
        meters = (after - before) * constant.mile # miles to meters
        seconds = duration * constant.minute # minutes to seconds
        acceleration = meters / constant.hour / seconds
    
        return acceleration / constant.g

    # def compare(measurement, threshold, direction):
    #     allowable = measurement <= threshold
        
    #     return allowable if direction else not allowable


class Percentage(ChangePoint):
    def change(self, duration, before, after):
        return (after - before) / before

class Derivative(ChangePoint):
    def __init__(self, threshold, categories=2, order=1):
        super().__init__(threshold, categories)
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
