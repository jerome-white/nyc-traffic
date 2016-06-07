import pandas as pd
import rollingtools as rt

from lib import ngen
from lib import logger

#############################################################################

def slide(drange):
    for i in itertools.count():
        yield drange + i

class Intensity:
    def __init__(self, readings, prediction, classifier):
        self.readings = readings
        self.prediction = prediction
        self.classifier = classifier

    def duration(self, left, right):
        observation = self.readings[left]
        if observation.isnull().values.any():
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
        lmean = left.mean()
        
        for i in slide(right):
            index = right.union(i)
            r = self.readings[index]
            if r.isnull().values.any():
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
        e = observation.std()
        
        for i in slide(right):
            r = self.readings[i]
            if r.isnull().values.any() or abs(r.mean() - e) > self.deviations:
                break

        return i
    
def f(args):
    (index, nid, (config, )) = args
    
    args = rt.mkargs(config)
    intensity = StandardDeviation(args.node.readings.speed,
                                  args.window.prediction,
                                  args.classifier)
    df = pd.Series()
    
    log = logger.getlogger()
    log.info('{0} +'.format(nid))
    for (left, right) in args.node.range(window):
        try:
            duration = intensity.duration(left, right)
            df.set_value(right[0], duration)
        except ValueError:
            pass
    log.info('{0} -'.format(nid))
    
    return (nid, df)

#############################################################################

engine = ProcessingEngine('prediction')
results = engine.run(f, ngen.SequentialGenerator())
panel = pd.Panel(dict(results))
engine.dump(panel)
