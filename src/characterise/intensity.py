import itertools

import pandas as pd

def slide(drange):
    for i in itertools.count():
        yield drange + i

class Intensity:
    def __init__(self, readings, prediction, classifier):
        self.readings = readings
        self.prediction = prediction
        self.classifier = classifier

    def incomplete(self, data):
        return data.isnull().values.any()
        
    def duration(self, left, right):
        observation = self.readings[left]
        if self.incomplete(observation):
            raise ValueError()

        extended = self._duration(observation, right)
        if extended.identical(right):
            raise ValueError()
        
        assert(extended.freq == right.freq)
        rng = pd.date_range(right[0], extended[-1], freq=right.freq)
        
        return len(rng)

    def _duration(self, observation, right):
        raise NotImplementedError

class SlidingWindow(Intensity):
    def _duration(self, observation, right):
        lmean = observation.mean()
        
        for i in slide(right):
            index = right.union(i)
            r = self.readings[index]
            if self.incomplete(r):
                break
            rmean = r.mean() # XXX weighted average?
            
            if not self.classifier.classify(self.prediction, lmean, rmean):
                break

        return i

class StandardDeviation(Intensity):
    def __init__(self, readings, prediction, classifier, deviations=1):
        super().__init__(readings, prediction, classifier)
        self.deviations = deviations
        
    def _duration(self, observation, right):
        epsilon = observation.std()
        
        for i in slide(right):
            r = self.readings[i]
            if self.incomplete(r) or abs(r.mean() - epsilon) > self.deviations:
                break

        return i
