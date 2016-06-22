import numpy as np
import pandas as pd

from lib import ngen
from lib import logger
from pathlib import Path

from lib import engine as eng
from lib import rollingtools as rt
from intensity import IncreasingWindow as Intensity

#############################################################################

def mkrange(config, key, inclusive=True):
    vals = [ int(x) for x in config[key].split(',') ]
    if len(vals) > 1:
        if inclusive:
            vals[-1] += 1
        return range(*vals)
    else:
        return vals
    
def frequency(args):
    return args.roller.apply(rt.apply, args=[ args.window, args.classifier ])

def duration(args):
    df = pd.Series()
    intensity = Intensity(args.node.readings.speed, args.classifier)

    for (left, right) in args.node.range(args.window):
        if left[0].is_month_start and not left[0].hour and not left[0].minute:
            log = logger.getlogger()
            log.info('{0}: {1} {2}'.format(repr(args.node), left[0], right[0]))
        df.set_value(right.min(), intensity.duration(left, right))
    
    return df

def f(args):
    (index, nid, (config, )) = args
    logger.getlogger().info('+ {0}'.format(nid))

    g = activity_[config['parameters']['activity']]
    df = g(rt.mkargs(nid, config))
    df.rename(nid, inplace=True)

    return (nid, df)

#############################################################################

activity_ = {
    'classify': frequency,
    'intensity': duration,
    }

engine = eng.ProcessingEngine('prediction', init_db=True)
assert(engine.config['parameters']['activity'] in activity_)

destination = Path(engine.config['output']['destination'])

windows = engine.config['window']
keys = [ 'observation', 'prediction' ]
(observation, prediction) = [ mkrange(windows, x) for x in keys ]

log = logger.getlogger(True)

for o in observation:
    for i in [ 'observation', 'target' ]:
        windows[i] = '{0:02d}'.format(o)

    for p in prediction:
        log.info('o: {0} p: {1}'.format(o, p))
        
        windows['prediction'] = '{0:02d}'.format(p)
        path = Path(destination, windows['observation'], windows['prediction'])
        path.mkdir(parents=True, exist_ok=True)
        
        for (nid, df) in engine.run(f, ngen.SequentialGenerator()):
            log.info('- {0}'.format(nid))

            n = '{0:03d}'.format(nid)
            fname = path.joinpath(n).with_suffix('.pkl')
            engine.dump(df, str(fname))
