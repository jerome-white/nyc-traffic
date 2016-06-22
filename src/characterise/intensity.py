import itertools

import numpy as np
import pandas as pd
import scipy.constants as constant

class Intensity:
    def __init__(self, readings, classifier):
        self.readings = readings
        self.classifier = classifier

    def slide(self, drange):
        index = self.readings.index
        stop = len(index[index >= drange.min()]) - len(drange)
        
        yield from map(lambda x: drange + x, range(stop + 1))
        
    def incomplete(self, data):
        return data.isnull().values.any()
        
    def duration(self, left, right):
        # readings should be complete
        (l_reads, r_reads) = [ self.readings[x] for x in (right, left) ]
        if self.incomplete(l_reads) or self.incomplete(r_reads):
            return np.nan

        # Calculate the prediction window size by looking at the
        # difference between left and right. For this to work, the
        # frequencies must be in minutes.
        assert(type(left.freq) == pd.tseries.offsets.Minute)
        assert(type(right.freq) == pd.tseries.offsets.Minute)
        delta = right.min() - left.max()
        delta = delta.total_seconds() / constant.minute
        
        return self._duration(l_reads, right, delta)
        
    def _duration(self, observation, right, delta):
        raise NotImplementedError

class IncreasingWindow(Intensity):
    def _duration(self, observation, right, delta):
        lmean = observation.mean()
        index = pd.date_range(right.min(), periods=1, freq=right.freq)
        
        for i in self.slide(index.copy()):
            index = index.union(i)
            r = self.readings[index]
            if self.incomplete(r):
                break
            
            rmean = r.mean() # XXX weighted average?
            if not self.classifier.classify(delta, lmean, rmean):
                break

        return len(index) - 1
    
class SlidingWindow(Intensity):
    def __init__(self, readings, classifier, inclusive=True):
        super().__init__(readings, classifier)
        self.inclusive = inclusive
        
    def _duration(self, observation, right, delta):
        lmean = observation.mean()
        
        for i in self.slide(right):
            index = right.union(i) if self.inclusive else i
            r = self.readings[index]
            if self.incomplete(r):
                break
            
            rmean = r.mean() # XXX weighted average?
            if not self.classifier.classify(delta, lmean, rmean):
                break

        return len(right.union(i[:-1]))

class StandardDeviation(Intensity):
    def __init__(self, readings, classifier, deviations=1):
        super().__init__(readings, classifier)
        self.deviations = deviations
        
    def _duration(self, observation, right, delta):
        epsilon = observation.std()
        
        for i in self.slide(right):
            r = self.readings[i]
            if self.incomplete(r) or abs(r.mean() - epsilon) > self.deviations:
                break

        return len(right.union(i[:-1]))
