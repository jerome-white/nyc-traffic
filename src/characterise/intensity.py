import itertools

import numpy as np
import pandas as pd

class Intensity:
    def __init__(self, readings, prediction, classifier):
        self.readings = readings
        self.prediction = prediction
        self.classifier = classifier

    def incomplete(self, data):
        return data.isnull().values.any()
        
    def duration(self, left, right):
        # readings should be complete
        readings = [ self.readings[x] for x in (right, left) ]
        if [ self.incomplete(x) for x in readings ].any():
            return np.nan

        return self._duration(readings.pop(), right)

    def slide(self, drange):
        stop = len(drange.union(self.readings.index)) - len(drange)
        assert(stop > 0)
        
        yield from map(lambda x: drange + x, range(stop + 1))
        
    def _duration(self, observation, right):
        raise NotImplementedError

class IncreasingWindow(Intensity):
    def _duration(self, observation, right):
        lmean = observation.mean()

        for i in range(len(right)):
            index = right[:i + 1]
            r = self.readings[index]
            if self.incomplete(r):
                break
            
            rmean = r.mean() # XXX weighted average?
            if not self.classifier.classify(self.prediction, lmean, rmean):
                break

        return i
    
class SlidingWindow(Intensity):
    def __init__(self, readings, prediction, classifier, inclusive=True):
        super().__init__(readings, prediction, classifier)
        self.inclusive = inclusive
        
    def _duration(self, observation, right):
        lmean = observation.mean()
        
        for i in self.slide(right):
            index = right.union(i) if self.inclusive else i
            r = self.readings[index]
            if self.incomplete(r):
                break
            
            rmean = r.mean() # XXX weighted average?
            if not self.classifier.classify(self.prediction, lmean, rmean):
                break

        return len(right.union(i[:-1]))

class StandardDeviation(Intensity):
    def __init__(self, readings, prediction, classifier, deviations=1):
        super().__init__(readings, prediction, classifier)
        self.deviations = deviations
        
    def _duration(self, observation, right):
        epsilon = observation.std()
        
        for i in self.slide(right):
            r = self.readings[i]
            if self.incomplete(r) or abs(r.mean() - epsilon) > self.deviations:
                break

        return len(right.union(i[:-1]))
