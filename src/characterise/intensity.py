import pandas as pd
import rollingtools as rt

from lib import ngen
from lib import logger

#############################################################################

def jam_duration(left, right, data, prediction, classify):
    l = data[left]
    if l.isnull().values.any():
        raise ValueError()
    
    lmean = l.mean()
    
    for i in itertools.count():
        index = right.union(right + i)
        r = data[index]
        if r.isnull().values.any():
            break
        
        rmean = r.mean()
            
        if not classify(prediction, lmean, rmean):
            break

    size = len(index) - 1
    if size < len(right):
        raise ValueError()

    return size

def f(args):
    (index, nid, (config, )) = args
    log = logger.getlogger()
    
    log.info('{0} create'.format(nid))
    args = rt.mkargs(config)

    log.info('{0} apply'.format(nid))

    df = pd.Series()
    for (left, right) in args.node.range(window):
        try:
            duration = jam_duration(left, right,
                                    args.node.readings.speed,
                                    args.window.prediction,
                                    args.classifier.classify)
            df.set_value(right[0], duration)
        except ValueError:
            continue

    log.info('{0} finish'.format(nid))
    
    return (nid, df)

#############################################################################

engine = ProcessingEngine('prediction')
results = engine.run(f, ngen.SequentialGenerator())
panel = pd.Panel(dict(results))
engine.dump(panel)
